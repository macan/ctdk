/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-01-23 18:16:04 macan>
 *
 */

#ifndef __COMMON_H__
#define __COMMON_H__

#define HVFS_TRACING

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <getopt.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include "hiredis.h"
#include "lib/ring.h"
#include "lib/csv_parser.h"

#define TABLE_DEF(table, nr, accept) char *__T_##table##_names[nr], \
        *__T_##table##_types[nr];                                   \
    int __T_##table##_cnt = 0;                                      \
    int __T_##table##_accept = accept;

#define TABLE_FIELD(table, TYPE, VAR) do {                  \
        __T_##table##_names[__T_##table##_cnt] = #VAR;      \
        __T_##table##_types[__T_##table##_cnt] = #TYPE;     \
        __T_##table##_cnt++;                                \
    } while (0)

#define TABLE_NR(table) (__T_##table##_cnt)
#define TABLE_NR_ACCEPT(table) (__T_##table##_accept)
#define TABLE_TYPE(table, i) (__T_##table##_types[i])
#define TABLE_NAME(table, i) (__T_##table##_names[i])
#define TABLE_TYPES(table) (__T_##table##_types)
#define TABLE_NAMES(table) (__T_##table##_names)

struct orig_log
{
    int rzlx;                   /* 0 */
    unsigned long jlsj;         /* 1 */
    unsigned long jcsj;         /* 2 */
    unsigned int cljip;         /* 3 */
#ifdef USE_LYLX
    unsigned int lylx;          /* 4 */
#define LYLX_SHIFT(nr) (nr)
#else
#define LYLX_SHIFT(nr) (nr > 3 ? (nr - 1) : nr)
#endif
    unsigned int fwqip;         /* 5 */
    unsigned int fwqdk;         /* 6 */
    unsigned int khdip;         /* 7 */
    unsigned int khddk;         /* 8 */
    char rzdbxx[128];           /* 9 */
    unsigned int bs;            /* 10 */
    unsigned long zjs;          /* 11 */
    unsigned int gjlx;          /* 12 */
    unsigned int bak1;          /* 13 */
    unsigned int bak2;          /* 14 */
    unsigned int bak3;          /* 15 */
    unsigned int bak4;          /* 16 */
    unsigned int bak5;          /* 17 */
};

struct streamid
{
    unsigned int fwqip;         /* server ip */
    unsigned int fwqdk;         /* server port */
    unsigned int khdip;         /* client ip */
    unsigned int khddk;         /* client port */
#define STREAM_IN               0x01
#define STREAM_OUT              0x02
#define STREAM_INNIL            0x04
#define STREAM_OUTNIL           0x08
    unsigned int direction:4;   /* IN: 1; OUT: 2
                                 * INNIL: 4; OUTNIL 8
                                 */
#define STREAM_TCP              1
#define STREAM_UDP              2
    unsigned int protocol:28;   /* TCP: 1; UDA: 2 */

    unsigned long jlsj;         /* recored time */
    unsigned long jcsj;         /* detect time */
    unsigned int cljip;         /* detect machine ip */
    unsigned int bs;            /* packet number */
    unsigned long zjs;          /* byte num */
    unsigned int gjlx;          /* tool type, should AND */
};

struct huadan
{
    unsigned int fwqip;         /* server ip */
    unsigned int fwqdk;         /* server port */
    unsigned int khdip;         /* client ip */
    unsigned int khddk;         /* client port */
#define STREAM_TCP              1
#define STREAM_UDP              2
    unsigned int protocol;      /* TCP: 1; UDA: 2 */

    unsigned int gjlx;          /* tool type */
    unsigned long qssj;         /* begin time */
    unsigned long jssj;         /* end time */
    unsigned long cxsj;         /* dural time */

    unsigned int in_bs;
    unsigned int out_bs;
    unsigned long in_zjs;
    unsigned long out_zjs;
};

struct streamstat
{
    unsigned int DON;
    
    unsigned int igjlx;         /* inb stream tool type set */
    unsigned long ijlsj;        /* inb stream end time */
    unsigned long ijlks;        /* inb stream begin time */
    unsigned long ijcsj;        /* inb stream detect end time */
    unsigned long ijcks;        /* inb stream detect begin time */
    unsigned long izjs;         /* inb stream max byte num */
    unsigned int ibs;           /* inb stream max packet num */
    
    unsigned int ogjlx;
    unsigned long ojlsj;
    unsigned long ojlks;
    unsigned long ojcsj;
    unsigned long ojcks;
    unsigned long ozjs;
    unsigned int obs;
};

struct ARecord
{
    unsigned int ip;
    unsigned int port;
    unsigned int gjlx;
    unsigned int rzlx;
    unsigned long sxsj;         /* effective time [-20,20] */
};

struct BRecord
{
    unsigned int ip;
    unsigned long sxsj;         /* effective time [0,20] */
};

struct stream_config
{
    int ignoreact;
};

/* Database Name */
#define DB_STREAM               0x00
#define DB_HUADAN               0x01
#define DB_STREAM_BACKUP        0x03

#define DB_STREAM_BACKUP_MAX    0x0f

/* Update results */
#define CANCEL                  0x00 /* ignore it */
#define INB_INIT                0x01 /* ok */
#define OUT_INIT                0x02 /* ok */
#define INB_UPDATED             0x03 /* ok */
#define OUT_UPDATED             0x04 /* ok */
#define INB_CLOSED              0x05 /* inb stream is closed */
#define OUT_CLOSED              0x06 /* out stream is closed */
#define ALL_CLOSED              0x07 /* should do MOVE now */
#define INB_IGNORE              0x08 /* update on a closed stream */
#define OUT_IGNORE              0x09 /* update on a closed stream */
#define TIMED_IGNORE            0x0a /* update can not pass time restricts */

void ipconv(char *ip, char *address, FILE *fp);
int pop_huadan(struct streamid *id, int suffix, int flag);

/* Pipeline */
struct pp_rec
{
    struct streamid id;
    int suffix;
};

struct pp_rec_header
{
    struct pp_rec *array;
    int nr;
};

struct pipeline
{
    /* pipeline buffer */
    void *buf;
    void *buf2;                 /* atomic exchange the two buffer */
    sem_t pbs;
    int pnr;
    int pnr2;
    struct pp_rec_header prh;
    xlock_t lock;
};

#endif
