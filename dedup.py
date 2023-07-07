#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
 Authors:
   yifengyou <842056007@qq.com>
"""

import argparse
import datetime
import multiprocessing
import os.path
import subprocess
import sys
import os
import hashlib
import sqlite3
import select
import tempfile

CURRENT_VERSION = "0.1.0"


def beijing_timestamp():
    utc_time = datetime.datetime.utcnow()
    beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
    beijing_time = utc_time.astimezone(beijing_tz)
    return beijing_time.strftime("%Y/%m/%d %H:%M:%S")


def perror(str):
    print("Error: ", str)
    sys.exit(1)


def check_python_version():
    current_python = sys.version_info[0]
    if current_python == 3:
        return
    else:
        raise Exception('Invalid python version requested: %d' % current_python)


def do_exe_cmd(cmd, print_output=False, shell=False):
    stdout_output = ''
    stderr_output = ''
    if isinstance(cmd, str):
        cmd = cmd.split()
    elif isinstance(cmd, list):
        pass
    else:
        raise Exception("unsupported type when run do_exec_cmd", type(cmd))

    # print("Run cmd:" + " ".join(cmd))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
    while True:
        # 使用select模块，监控stdout和stderr的可读性，设置超时时间为0.1秒
        rlist, _, _ = select.select([p.stdout, p.stderr], [], [], 0.1)
        # 遍历可读的文件对象
        for f in rlist:
            # 读取一行内容，解码为utf-8
            line = f.readline().decode('utf-8').strip()
            # 如果有内容，判断是stdout还是stderr，并打印到屏幕，并刷新缓冲区
            if line:
                if f == p.stdout:
                    if print_output == True:
                        print("STDOUT", line)
                    stdout_output += line + '\n'
                    sys.stdout.flush()
                elif f == p.stderr:
                    if print_output == True:
                        print("STDERR", line)
                    stderr_output += line + '\n'
                    sys.stderr.flush()
        if p.poll() is not None:
            break
    return p.returncode, stdout_output, stderr_output


# 定义一个函数，计算一个文件的md5值
def get_file_md5(file_path):
    # 打开文件，以二进制模式读取
    with open(file_path, "rb") as f:
        # 创建一个md5对象
        md5 = hashlib.md5()
        # 循环读取文件内容，更新md5对象
        while True:
            data = f.read(4096)  # 每次读取4KB
            if not data:  # 如果没有数据，跳出循环
                break
            md5.update(data)  # 更新md5对象
        # 返回md5值的十六进制字符串
        return md5.hexdigest()


def get_file_inode(file_path):
    try:
        fstat = os.stat(file_path)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return -1
    except PermissionError:
        print(f"Permission denied: {file_path}")
        return -2
    except OSError as e:
        print(f"System error: {e}")
        return -3
    return fstat.st_ino


def get_all_directories(dir):
    dirs = []
    result = []
    dirs.append(dir)
    while dirs:
        dir = dirs.pop()
        result.append(dir)
        for entry in os.scandir(dir):
            if entry.is_dir():
                dirs.append(entry.path)
    return result


def process_per_dir(dir_with_index):
    (index, total, db_file_path, dir_path) = dir_with_index

    # print(f"process_per_dir {dir_path}")
    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        # print(f"file path {item_path}")

        if os.path.isfile(item_path):
            cursor.execute("SELECT MTIME FROM DEDUP WHERE PATH = ?", (item_path,))
            result = cursor.fetchone()
            if not result:
                # 第一种情况，如果MTIME不存在，添加
                file_md5 = get_file_md5(item_path)

                mtime = str(os.path.getmtime(item_path))

                file_inode = get_file_inode(item_path)
                if file_inode < 0:
                    continue

                print(f"add {item_path}")
                cursor.execute(
                    "INSERT INTO DEDUP (PATH, MTIME, MD5, INODE ) VALUES (?, ?, ?, ?)",
                    (item_path, mtime, file_md5, file_inode)
                )
                conn.commit()
                continue
            mtime = str(os.path.getmtime(item_path))
            # print(result, mtime)
            if result[0] != mtime:
                # 第二种情况，MTIME发生改变，更新
                file_md5 = get_file_md5(item_path)

                file_inode = get_file_inode(item_path)
                if file_inode < 0:
                    continue

                print(f"update {item_path}")
                cursor.execute(
                    "UPDATE DEDUP SET MTIME = ?, MD5 = ?, INODE = ?  WHERE PATH = ?",
                    (mtime, file_md5, file_inode, item_path)
                )
                conn.commit()
                continue
            # 第二种情况，MTIME、PATH均不变，没有更新，什么也不做
            print(f"nochange {item_path}")
    cursor.close()
    conn.close()
    print(f"[ {index}/{total} ] Dir: {dir_path}")


def handle_scan(args):
    begin_time = beijing_timestamp()
    workdir = os.path.abspath(args.workdir)
    print(f"WORKDIR {workdir}")

    conn = sqlite3.connect(args.output)
    cursor = conn.cursor()

    # 判断表是否存在，支持多次执行
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS DEDUP ("
        f" ID INTEGER PRIMARY KEY AUTOINCREMENT, "
        f" PATH TEXT UNIQUE,"
        f" MTIME TEXT NOT NULL,"
        f" MD5 TEXT NOT NULL,"
        f" INODE INT NOT NULL,"
        f" PPATH TEXT"
        f")"
    )
    # 创建MD5索引，没有必要创建PATH索引，因为UNIQUE约束本身就会创建一个唯一索引
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS MD5HASH ON DEDUP (MD5);"
    )

    cursor.close()
    conn.close()

    print(" scan ...")
    dir_list = get_all_directories(workdir)
    dir_with_index = []
    total = len(dir_list)
    # print("dir total", total)
    for i, dir in enumerate(dir_list):
        dir = dir.strip()
        dir_with_index.append(
            (i + 1, total, args.output, dir)
        )

    pool = multiprocessing.Pool(args.job)
    pool.imap_unordered(process_per_dir, dir_with_index)
    pool.close()
    pool.join()

    end_time = beijing_timestamp()
    print(f"handle dedup scan done! {begin_time} - {end_time}")


def handle_stat(args):
    begin_time = beijing_timestamp()

    workdir = os.path.abspath(args.workdir)
    print(f"WORKDIR {workdir}")

    db_file_path = os.path.abspath(args.output)
    if not os.path.isfile(db_file_path):
        perror(f" Database file {db_file_path} not found!")
    print(f"using {db_file_path}")

    conn = sqlite3.connect(args.output)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM DEDUP")
    except Exception:
        print(f" Makesure {db_file_path} exists and file type ok")
        cursor.close()
        conn.close()
        exit(1)

    result = cursor.fetchone()
    if result:
        print(f"DEDUP 表格的记录数量为 {result[0]}")
    else:
        print("DEDUP 为空表，请先执行 ' dedup scan ' 生成数据")
        exit(0)

    try:
        cursor.execute("SELECT COUNT(*) FROM DEDUP WHERE PPATH IS NOT NULL ")
    except Exception:
        print(f" Makesure {db_file_path} exists and file type ok")
        cursor.close()
        conn.close()
        exit(1)

    result = cursor.fetchone()
    if result:
        print(f"DEDUP 重复文件数量为 {result[0]}")
    else:
        print("没有发现重复的文件")

    cursor.close()
    conn.close()

    end_time = beijing_timestamp()
    print(f"handle dedup stat done! {begin_time} - {end_time}")


def safe_link(src, dst):
    with tempfile.TemporaryDirectory(dir=os.path.dirname(dst)) as tmpdir:
        tmpname = os.path.join(tmpdir, "tmp")
        os.link(src, tmpname)
        os.replace(tmpname, dst)


def process_per_dup(row_with_index):
    (index, total, md5, count, db_file_path) = row_with_index

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    cursor.execute("SELECT PATH, INODE, MTIME, PPATH  FROM DEDUP WHERE MD5 = ?", (md5,))
    rows = cursor.fetchall()

    first = None
    for row in rows:
        if first is None:
            first = row
            continue
        # if row[1] != first[1] or row[3] is None:
        # inode 不一致，但是md5一致，则硬链接
        try:
            safe_link(first[0], row[0])
            cursor.execute(
                "UPDATE DEDUP SET MTIME = ?, INODE = ?, PPATH = ?  WHERE PATH = ?",
                (first[2], first[1], first[0], row[0])
            )
            conn.commit()
        except Exception as e:
            print(f" hard link failed! {str(e)}")
            continue
        print(f" -> clean PATH: {row[0]},  INODE: {row[1]}")

    cursor.close()
    conn.close()
    print(f"[ {index}/{total} ] : {md5} ({count})")


def handle_clean(args):
    begin_time = beijing_timestamp()
    workdir = os.path.abspath(args.workdir)
    print(f"WORKDIR {workdir}")

    db_file_path = os.path.abspath(args.output)
    if not os.path.isfile(db_file_path):
        perror(f" Database file {db_file_path} not found!")
    print(f"using {db_file_path}")

    conn = sqlite3.connect(args.output)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT COUNT(*) FROM DEDUP")
    except Exception:
        print(f" Makesure {db_file_path} exists and file type ok")
        cursor.close()
        conn.close()
        exit(1)
    result = cursor.fetchone()
    if result:
        print(f"DEDUP 表格的记录数量为 {result[0]}")
    else:
        print("DEDUP 为空表，请先执行 ' dedup scan ' 生成数据")
        exit(0)

    sql_select = "SELECT MD5, COUNT(*) AS CNT FROM DEDUP WHERE PPATH IS NULL GROUP BY MD5 HAVING CNT > 1"
    cursor.execute(sql_select)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    total = len(rows)
    if len(rows) < 1:
        print("No duplicated file!")
        exit(0)

    print(f"find duplicate {total} {rows}")
    print(" clean ...")

    row_with_index = []
    for i, row in enumerate(rows):
        md5 = row[0]
        count = row[1]
        row_with_index.append(
            (i + 1, total, md5, count, args.output)
        )

    pool = multiprocessing.Pool(args.job)
    pool.imap_unordered(process_per_dup, row_with_index)
    pool.close()
    pool.join()

    end_time = beijing_timestamp()
    print(f"handle dedup clean done! {begin_time} - {end_time}")


def main():
    global CURRENT_VERSION
    check_python_version()

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true",
                        help="show program's version number and exit")
    parser.add_argument("-h", "--help", action="store_true",
                        help="show this help message and exit")

    subparsers = parser.add_subparsers()

    # 定义base命令用于集成
    parent_parser = argparse.ArgumentParser(add_help=False, description="kdev - a tool for kernel development")
    parent_parser.add_argument("-V", "--verbose", default=None, action="store_true", help="show verbose output")
    parent_parser.add_argument("-j", "--job", default=os.cpu_count(), type=int, help="job count")
    parent_parser.add_argument("-o", "--output", default="dedup.db", help="dedup database file path")
    parent_parser.add_argument("-w", "--workdir", default=".", help="setup workdir")
    parent_parser.add_argument('-l', '--log', default=None, help="log file path")
    parent_parser.add_argument('-d', '--debug', default=None, action="store_true", help="enable debug output")

    # 添加子命令 stat
    parser_stat = subparsers.add_parser('stat', parents=[parent_parser])
    parser_stat.set_defaults(func=handle_stat)

    # 添加子命令 scan
    parser_scan = subparsers.add_parser('scan', parents=[parent_parser])
    parser_scan.set_defaults(func=handle_scan)

    # 添加子命令 clean
    parser_clean = subparsers.add_parser('clean', parents=[parent_parser])
    parser_clean.set_defaults(func=handle_clean)

    # 开始解析命令
    args = parser.parse_args()

    if args.version:
        print("dedup %s" % CURRENT_VERSION)
        sys.exit(0)
    elif args.help or len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)
    else:
        args.func(args)


if __name__ == "__main__":
    main()
