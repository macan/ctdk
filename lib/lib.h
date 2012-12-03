/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2012-11-23 15:21:48 macan>
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
};

struct xnet_group
{
#define XNET_GROUP_ALLOC_UNIT   64
    int asize, psize;           /* actual size, physical size */
    struct xnet_group_entry sites[0];
};

#define __UNUSED__ __attribute__((unused))

#endif
