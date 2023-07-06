.PHONY: help default install uninstall

help: default


default:
	@echo "install                install dedup to /usr/bin/"
	@echo "uninstall              uninstall dedup"

install:
	ln -f dedup.py /usr/bin/dedup

uninstall:
	rm -f /usr/bin/dedup

