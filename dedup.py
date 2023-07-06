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


# 定义一个函数，递归遍历一个目录下的所有文件，并返回一个列表，每个元素是一个元组，包含文件路径和md5值
def get_dir_files_md5(dir_path, conn):
    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isfile(item_path):
            # 先判断数据库中是否存在记录，如果不存在，则计算MD5并写入
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM DEDUP WHERE PATH = ?", (item_path,))
            result = cursor.fetchone()
            if not result:
                # 第一种情况，如果路径不存在，添加
                file_md5 = get_file_md5(item_path)
                mtime = os.path.getmtime(item_path)
                print(f"add {item_path}")
                cursor.execute("INSERT INTO DEDUP (PATH, MTIME, MD5) VALUES (?, ?, ?)", (item_path, mtime, file_md5))
                conn.commit()
                cursor.close()
                continue
            mtime = os.path.getmtime(item_path)
            if result[2] != mtime:
                # 第二种情况，mtime发生改变，更新
                file_md5 = get_file_md5(item_path)
                print(f"update {item_path}")
                cursor.execute("UPDATE DEDUP SET MTIME = ?, MD5 = ? WHERE PATH = ?", (mtime, file_md5, item_path))
                conn.commit()
                cursor.close()
                continue
            # 第二种情况，mtime、path均不变，没有更新，什么也不做
            print(f"nochange {item_path}")
        elif os.path.isdir(item_path):
            # 如果是目录，递归调用自身，并将返回的列表扩展到结果列表中
            get_dir_files_md5(item_path, conn)


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

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()

    for item in os.listdir(dir_path):
        item_path = os.path.join(dir_path, item)
        if os.path.isfile(item_path):
            cursor.execute("SELECT * FROM DEDUP WHERE PATH = ?", (item_path,))
            result = cursor.fetchone()
            if not result:
                # 第一种情况，如果路径不存在，添加
                file_md5 = get_file_md5(item_path)
                mtime = os.path.getmtime(item_path)
                print(f"add {item_path}")
                cursor.execute("INSERT INTO DEDUP (PATH, MTIME, MD5) VALUES (?, ?, ?)", (item_path, mtime, file_md5))
                conn.commit()
                continue
            mtime = os.path.getmtime(item_path)
            if result[2] != mtime:
                # 第二种情况，mtime发生改变，更新
                file_md5 = get_file_md5(item_path)
                print(f"update {item_path}")
                cursor.execute("UPDATE DEDUP SET MTIME = ?, MD5 = ? WHERE PATH = ?", (mtime, file_md5, item_path))
                conn.commit()
                continue
            # 第二种情况，mtime、path均不变，没有更新，什么也不做
            print(f"nochange {item_path}")
    cursor.close()
    conn.close()
    print(f"[ {index}/{total} ] Dir: {dir}")


def handle_dedup(args):
    begin_time = beijing_timestamp()
    workdir = os.path.abspath(args.workdir)
    print(f"workdir {workdir}")

    conn = sqlite3.connect(args.output)
    cursor = conn.cursor()
    # 判断表是否存在，支持多次执行
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS DEDUP ("
        f" ID INTEGER PRIMARY KEY AUTOINCREMENT, "
        f" PATH TEXT UNIQUE,"
        f" MTIME REAL,"
        f" MD5 TEXT"
        f")"
    )
    cursor.close()
    conn.close()

    dir_list = get_all_directories(workdir)
    dir_with_index = []
    total = len(dir_list)
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
    print(f"handle download done! {begin_time} - {end_time}")


def main():
    global CURRENT_VERSION
    check_python_version()

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true",
                        help="show program's version number and exit")
    parser.add_argument("-h", "--help", action="store_true",
                        help="show this help message and exit")
    parser.add_argument("-w", "--workdir", default=".",
                        help="setup workdir")
    parser.add_argument("--output", default="dedup.db",
                        help="dedup database file path")
    parser.add_argument("-c", "--clean", action="store_true",
                        help="clean duplicated file")
    parser.add_argument("-j", "--job", default=os.cpu_count(), type=int,
                        help="job count")

    # 开始解析命令
    args = parser.parse_args()

    if args.version:
        print("dedup %s" % CURRENT_VERSION)
        sys.exit(0)
    elif args.help or len(sys.argv) < 2:
        parser.print_help()
        sys.exit(0)
    elif args.workdir is not None:
        handle_dedup(args)
    else:
        parser.print_help()
        sys.exit(0)


if __name__ == "__main__":
    main()
