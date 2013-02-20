/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-01-31 14:15:39 macan>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#ifndef __LIB_H__
#define __LIB_H__

#include "hvfs_u.h"
#include "memory.h"
#include "xlock.h"
#include "tracing.h"

#include "hash.c"

/* conf.c to provide config file parsing */
typedef enum {
    PARSER_INIT,
    PARSER_EXPECT_SITE,
    PARSER_EXPECT_FS
} parser_state_t;

enum {
    PARSER_OK = 0,
    PARSER_NEED_RETRY,
    PARSER_CONTINUE,
    PARSER_FAILED = 1000
};

/* this is just a proxy array of config file line */
struct conf_site
{
    char *type;
    char *node;
    int port;
    int id;
};

int conf_parse(char *conf_file, struct conf_site *cs, int *csnr);

#ifdef HVFS_TRACING
extern u32 hvfs_lib_tracing_flags;
#endif

struct xnet_group_entry
{
    u64 site_id;
#define XNET_GROUP_RECVED       0x00001
    u64 flags;
    char *node;
    int port;

    void *context;
    u64 nr;

    /* private struct */
    void *private;
};

struct xnet_group
{
#define XNET_GROUP_ALLOC_UNIT   64
    int asize, psize;           /* actual size, physical size */
    struct xnet_group_entry sites[0];
};

#define __UNUSED__ __attribute__((unused))

static char *hvfs_ccolor[] __attribute__((unused)) = 
{
    "\033[0;40;31m",            /* red */
    "\033[0;40;32m",            /* green */
    "\033[0;40;33m",            /* yellow */
    "\033[0;40;34m",            /* blue */
    "\033[0;40;35m",            /* pink */
    "\033[0;40;36m",            /* yank */
    "\033[0;40;37m",            /* white */
    "\033[0m",                  /* end */
};
#define HVFS_COLOR(x)   (hvfs_ccolor[x])
#define HVFS_COLOR_RED  (hvfs_ccolor[0])
#define HVFS_COLOR_GREEN        (hvfs_ccolor[1])
#define HVFS_COLOR_YELLOW       (hvfs_ccolor[2])
#define HVFS_COLOR_BLUE         (hvfs_ccolor[3])
#define HVFS_COLOR_PINK         (hvfs_ccolor[4])
#define HVFS_COLOR_YANK         (hvfs_ccolor[5])
#define HVFS_COLOR_WHITE        (hvfs_ccolor[6])
#define HVFS_COLOR_END          (hvfs_ccolor[7])

static char *si_code[] 
__attribute__((unused)) = {"",
                           "address not mapped to object",
                           "invalid permissions for mapped object",
};
#define SIGCODES(i) ({                                      \
        char *res = NULL;                                   \
        if (likely(i == SEGV_MAPERR || i == SEGV_ACCERR)) { \
            res = si_code[i];                               \
        }                                                   \
        res;                                                \
    })

void lib_backtrace(void);
void lib_segv(int, siginfo_t *, void *);

/* APIs */
void lib_timer_start(struct timeval *begin);
void lib_timer_stop(struct timeval *end);
void lib_timer_echo(struct timeval *begin, struct timeval *end, int loop);
void lib_timer_acc(struct timeval *, struct timeval *, double *);
void lib_timer_echo_plus(struct timeval *, struct timeval *, int, char *);

#define lib_timer_def() struct timeval begin, end
#define lib_timer_B() lib_timer_start(&begin)
#define lib_timer_E() lib_timer_stop(&end)
#define lib_timer_O(loop, str) lib_timer_echo_plus(&begin, &end, loop, str)
#define lib_timer_A(ACC) lib_timer_acc(&begin, &end, (ACC))

#endif
