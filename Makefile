##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2012-12-03 14:12:46 macan>
#
# This is the makefile for HVFS project.
#
# Armed by EMACS.

GCC = gcc
ECHO = /bin/echo
CFLAGS = -DNO_LINK
LDFLAGS = -Llib -lhiredis -lhvfs -lpthread
HUADAN_1D = ctdk_huadan_1d
FILES = ipsearch.c
OBJS = $(HUADAN_1D)

all: $(OBJS)
	@$(ECHO) -e "Build OK."

$(HUADAN_1D): $(HUADAN_1D).c
	$(GCC) $(CFLAGS) -Llib $(HUADAN_1D).c $(FILES) -o $(HUADAN_1D) $(LDFLAGS)

clean:
	-@rm -rf $(OBJS)
