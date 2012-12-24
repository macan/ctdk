##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2012-12-21 13:43:10 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
CFLAGS = -Wall -DNO_LINK -pg -g -DUSE_CSV
LDFLAGS = -Llib -lhiredis -lhvfs -lpthread
HUADAN_1D = ctdk_huadan_1d
WATCHER = watcher
FILES = ipsearch.c
OBJS = $(HUADAN_1D) $(WATCHER)

all: $(OBJS)
	@$(ECHO) -e "Build OK."

$(HUADAN_1D): $(HUADAN_1D).c
	$(GCC) $(CFLAGS) -Llib $(HUADAN_1D).c $(FILES) -o $(HUADAN_1D) $(LDFLAGS)

$(WATCHER) : $(WATCHER).c
	$(GCC) $(CFLAGS) -I./inotify-tools-3.14/libinotifytools/src/ -L./inotify-tools-3.14/libinotifytools/src/.libs $(WATCHER).c -o $(WATCHER) $(LDFLAGS) -linotifytools

clean:
	-@rm -rf $(OBJS)
