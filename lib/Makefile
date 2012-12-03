##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2012-11-23 16:25:26 macan>
#
# This is the makefile for HVFS project.
#
# Armed with EMACS.

HOME_PATH = $(shell pwd)

include ./Makefile.inc

all: hvfs_lib
	@$(ECHO) End build lib.

%.o : %.c
	@$(ECHO) -e " " CC"\t" $@
	@$(CC) $(CFLAGS) -c $(patsubst %.c, $(LIB_PATH)/%.c, $<)

hvfs_lib : $(LIB_AR_SOURCE:.c=.o)
	@$(ECHO) -e " " AR"\t" $@
	@$(AR) rcs libhvfs.a $(^:.c=.o)
	@$(ECHO) -e " " SL"\t" $(LIB_SO)
	@$(CC) -shared -Wl,-soname,libhvfs.so.1 -o $(LIB_PATH)/libhvfs.so.1.0 $(^:.c=.o) -lc -lrt -lpthread

clean : 
	-@rm -rf $(LIB_AR_SOURCE:.c=.o)
	-@rm -rf libhvfs.a libhvfs.so.1.0