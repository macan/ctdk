##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2013-01-23 18:08:09 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
CFLAGS = -Wall -DNO_LINK -pg -g -DUSE_CSV -DUSE_LYLX -O2
LDFLAGS = -Llib -lhiredis -lhvfs -lpthread -lrt
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
