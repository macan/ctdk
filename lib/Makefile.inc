##
# Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
#                           <macan@ncic.ac.cn>
#
# Time-stamp: <2013-01-16 12:24:29 macan>
#
# This is the makefile for HVFS project.
#
# Armed with EMACS.

CC = gcc
LD = gcc
AR = ar
PYTHON = env python
GIT = env git
ECHO = /bin/echo

LIB_PATH = $(HOME_PATH)

ifdef USE_GIT_TAG
GIT_SHA = `$(GIT) rev-parse HEAD`
endif

COMPILE_DATE = `date`
COMPILE_HOST = `hostname`
PYTHON_IX = /usr/include/python
PYTHON_VERSION = $(shell env python -V 2>&1 | awk '{print $$2}' | sed -e 's/\.[0-9]$$//g')
PYTHON_LIB = /usr/lib/

# for now we know the number of cores of the target machines is 8, so just set
# it manually.
# GET_CORE = `grep -c processor /proc/cpuinfo`
GET_CORE = 8

ifeq ($(CPU_CORE),)
__CORES__=$(GET_CORE)
endif

# Debuging the memory and locks
XCFLAGS = -O2 -rdynamic -g -pg -fPIC
DEADCODESTRIP := -fdata-sections -ffunction-sections \
				 -Wl,--gc-sections
O3_OPT = -frename-registers -funroll-loops

CFLAGS += $(XCFLAGS) $(O3_OPT)
CFLAGS += -Wall -DCDATE="\"$(COMPILE_DATE)\"" -DGIT_SHA="\"$(GIT_SHA)\""\
			-DCHOST="\"$(COMPILE_HOST)\"" -I$(INC_PATH) \
			-I$(LIB_PATH) -D_GNU_SOURCE \
			-DCPU_CORE=$(__CORES__) -DHVFS_TRACING -DOPTIMIZE

ifndef JEMALLOC_HOME
ifeq ($(JEMALLOC),)
else
$(info USE JEMALLOC malloc function @ $(JEMALLOC).)
export JEMALLOC_HOME=$(JEMALLOC)
$(info ========================================================)
endif
endif

ifeq ($(JEMALLOC_HOME),)
else
CFLAGS += -DUSE_JEMALLOC=1 -I$(JEMALLOC_HOME)/include
LFLAGS += -L$(JEMALLOC_HOME)/lib -ljemalloc
endif

# Region for depend files
LIB_AR_SOURCE = lib.c ring.c conf.c csv_parser.c segv.c
LIB_H_SOURCE = lib.h ring.h memory.h hvfs_u.h tracing.h err.h xlist.h xlock.h \
               csv_parser.h

lib_h_depend_files = $(patsubst %.h, $(LIB_PATH)/%.h, $(LIB_H_SOURCE))

lib_depend_files = $(patsubst %.c, $(LIB_PATH)/%.c, $(LIB_AR_SOURCE)) \
					$(lib_h_depend_files)

# shared libs
LIB_SO = $(LIB_PATH)/libhvfs.so.1.0

HVFS_LIB = $(LIB_PATH)/libhvfs.a
