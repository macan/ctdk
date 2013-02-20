/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2013-02-05 13:24:03 macan>
 *
 */

#include "common.h"

HVFS_TRACING_INIT();

struct manager
{
    u64 client_id;
    int table_cnr;              /* # of columns */
    int cnr_accept;             /* min # of columns accepted */
    char **table_names;
    char **table_types;
};

static struct chring server_ring;
static struct xnet_group *xg = NULL;
static struct xnet_group *xg_client = NULL;
static struct manager m;
static char *g_input = NULL;
static char *g_output = NULL;
static int huadan_fd = -1;
static FILE *dbfp = NULL;
static FILE *g_dfp = NULL;
static int g_dfd = -1;
static long g_doffset = 0;
static long g_last_offset = 0;
static int g_interval = 10;
static int g_pnr = 7000;        /* 7k */
static int pnr = 0;             /* internal use */
static int g_pipeline = 0;      /* disable by default */
static int g_pp_sr = 0;
static int g_suffix_max = 0;
static int g_parse_byfd = 0;
static int g_fast_bsize = 128 * 1024;
static sem_t *g_pp_sr_sem = NULL;

#define HUADAN_FSYNC_NR         1000
static long pop_huadan_nr = 0;
static long peek_huadan_nr = 0;
static long process_nr = 0;
static long corrupt_nr = 0;
static long ignore_nr = 0;
static atomic64_t poped = {.counter = 0,};
static long flushed = 0;
static atomic64_t canceled = {.counter = 0,};
static long newstream_nr = 0;
static int g_id = -1;
static int g_round = -1;
static int g_local = 0;
#define OUTPUT_TXT              0x00
#define OUTPUT_CSV              0x01
static int g_output_format = OUTPUT_TXT;
#define ACTION_NOOP             0x00
#define ACTION_LOAD_DATA        0x01
#define ACTION_PEEK_HUADAN      0x02
#define ACTION_POP_HUADAN       0x04
#define ACTION_TEST             0x80
static int g_action = 0;
static int g_client_nr = 0;

static pthread_t g_timer_thread = 0;
static int g_timer_thread_stop = 0;
static int g_pp_sr_thread_stop = 0;
static sem_t g_timer_sem;
static sem_t g_exit_sem;

static struct stream_config g_sc = {
    .ignoreact = 1,
};

lib_timer_def();
double ACC = 0.0;

/* for getStreamStat() use */
#define DO_PEEK         0
#define DO_POP_RENAME   1
#define DO_POP_ONLY     2

#define GEN_HASH_KEY(key, id, klen) do {                    \
        klen = snprintf(key, 127, "S:%u:%u:%u:%u:%u",       \
                        id->fwqip,                          \
                        id->fwqdk,                          \
                        id->khdip,                          \
                        id->khddk,                          \
                        id->protocol                        \
            );                                              \
    } while (0)

#define GEN_FULL_KEY(key, id, klen, suffix) do {            \
        if (unlikely(suffix > 0)) {                         \
            snprintf(key + klen, 127, "+%d",                \
                     suffix                                 \
                );                                          \
        }                                                   \
    } while (0)

char *printStreamid(struct streamid *id)
{
    char *str;

    str = malloc(2048);
    if (!str) {
        printf("malloc() str buffer failed\n");
        return NULL;
    }
    sprintf(str, "S:%u:%u:%u:%u:%u %s",
            id->fwqip,
            id->fwqdk,
            id->khdip,
            id->khddk,
            id->protocol,
            ((id->direction & STREAM_IN) ? "INB   " :
             ((id->direction & STREAM_OUT) ? "OUT   " :
              ((id->direction & STREAM_INNIL) ? "INBNIL":
               ((id->direction & STREAM_OUTNIL) ? "OUTNIL" : "??????")))));

    return str;
}

char *printStreamStat(struct streamid *id, struct streamstat *stat)
{
    char *str, *p;

    str = printStreamid(id);
    if (!str)
        return NULL;
    p = str + strlen(str);

    sprintf(p, " DON 0x%x "
            "INB{gjlx %x jlsj %ld jlks %ld jcsj %ld jcks %ld zjs %ld bs %d} "
            "OUT{gjlx %x jlsj %ld jlks %ld jcsj %ld jcks %ld zjs %ld bs %d}\n",
            stat->DON,
            stat->igjlx, stat->ijlsj, stat->ijlks, stat->ijcsj, stat->ijcks,
            stat->izjs, stat->ibs,
            stat->ogjlx, stat->ojlsj, stat->ojlks, stat->ojcsj, stat->ojcks,
            stat->ozjs, stat->obs);

    return str;
}

char *printHuadan(struct huadan *hd)
{
    char address[1000] = {0,}; 
    char tstr1[64], tstr2[64];
    char ip1[32], ip2[32];
    char *str;
    struct tm *tmp1, *tmp2;

    str = malloc(2048);
    if (!str) {
        hvfs_err(lib, "malloc() str buffer failed\n");
        return NULL;
    }
    tmp1 = localtime((time_t *)&hd->qssj);
    if (!tmp1) {
        hvfs_err(lib, "localtime() failed w/ %s\n", strerror(errno));
        goto free;
    }
    if (strftime(tstr1, sizeof(tstr1), "%Y-%m-%d %H:%M:%S", tmp1) == 0) {
        hvfs_err(lib, "strftime() failed w/ %s\n", strerror(errno));
        goto free;
    }
    tmp2 = localtime((time_t *)&hd->jssj);
    if (!tmp2) {
        hvfs_err(lib, "localtime() failed w/ %s\n", strerror(errno));
        goto free;
    }
    if (strftime(tstr2, sizeof(tstr2), "%Y-%m-%d %H:%M:%S", tmp2) == 0) {
        hvfs_err(lib, "strftime() failed w/ %s\n", strerror(errno));
        goto free;
    }
    sprintf(ip1, "%d.%d.%d.%d", 
            (hd->fwqip >> 24) & 0xff,
            (hd->fwqip >> 16) & 0xff,
            (hd->fwqip >> 8) & 0xff,
            hd->fwqip & 0xff);
    sprintf(ip2, "%d.%d.%d.%d", 
            (hd->khdip >> 24) & 0xff,
            (hd->khdip >> 16) & 0xff,
            (hd->khdip >> 8) & 0xff,
            hd->khdip & 0xff);
    ipconv(ip2, address, dbfp);
    sprintf(str, "1\t%x\t%s\t%s\t%ld\t%s\t%d\t%s\t%d\t%s\t%d\t%ld\t%d\t%ld\t%s",
            hd->gjlx,
            tstr1,
            tstr2,
            hd->cxsj,
            ip1,
            hd->fwqdk,
            ip2,
            hd->khddk,
            (hd->protocol == STREAM_TCP ? "TCP" :
             (hd->protocol == STREAM_UDP ? "UDP" : "UKN")),
            hd->out_bs,
            hd->out_zjs,
            hd->in_bs,
            hd->in_zjs,
            address);

    return str;
free:
    free(str);
    return NULL;
}

char *printHuadanCSV(struct huadan *hd)
{
    char address[1000] = {0,}; 
    char ip2[32];
    char *str;

    str = malloc(2048);
    if (!str) {
        hvfs_err(lib, "malloc() str buffer failed\n");
        return NULL;
    }
    sprintf(ip2, "%d.%d.%d.%d", 
            (hd->khdip >> 24) & 0xff,
            (hd->khdip >> 16) & 0xff,
            (hd->khdip >> 8) & 0xff,
            hd->khdip & 0xff);
    ipconv(ip2, address, dbfp);
    sprintf(str, "\"1\",\"%x\",\"%ld\",\"%ld\",\"%ld\",\"%d\",\"%d\",\"%d\",\"%d\",\"%s\",\"%d\",\"%ld\",\"%d\",\"%ld\",\"%s\"$EOF$",
            hd->gjlx,
            hd->qssj,
            hd->jssj,
            hd->cxsj,
            hd->fwqip,
            hd->fwqdk,
            hd->khdip,
            hd->khddk,
            (hd->protocol == STREAM_TCP ? "TCP" :
             (hd->protocol == STREAM_UDP ? "UDP" : "UKN")),
            hd->out_bs,
            hd->out_zjs,
            hd->in_bs,
            hd->in_zjs,
            address);

    return str;
}

char *printHuadanLine(struct huadan *hd)
{
    char *str = NULL;

    if (g_output_format == OUTPUT_TXT)
        str = printHuadan(hd);
    else
        str = printHuadanCSV(hd);

    if (str) {
        strcat(str, "\n");
    }

    return str;
}

/* this function calls most frequently, optimize it!
 */
static inline
redisContext *get_server_from_key(char *key, int klen)
{
    struct chp *p;
    struct xnet_group_entry *e;

    p = ring_get_point(key, klen, &server_ring);
    hvfs_debug(lib, "KEY %s => Server %s:%d\n", key, p->node, p->port);
    e = p->private;
#ifndef OPTIMIZE
    if (unlikely(!e)) {
        hvfs_err(lib, "Server %ld doesn't exist.\n", p->site_id);
        return NULL;
    }
#endif
    e->nr++;

    return e->context;
}

static inline
struct xnet_group_entry *get_xg_from_key(char *key, int klen)
{
    struct chp *p;

    p = ring_get_point(key, klen, &server_ring);
    hvfs_debug(lib, "KEY %s => Server %s:%d\n", key, p->node, p->port);
    return p->private;
}

int new_stream(struct streamid *id, int suffix);

/* Return value: 0 => OK and updated; -1 => Internal Err; 1 => not updated
 */
int addOrUpdateStream(struct streamid *id, int suffix)
{
    char key[128];
    int klen;
    redisContext *c;
    redisReply *reply = NULL;

#define STREAM_HEAD     "HHD %s D %d "
#define INB_STREAM      STREAM_HEAD "iz %d ig %d ijlsj %ld ijcsj %ld ib %d"
#define OUT_STREAM      STREAM_HEAD "oz %d og %d ojlsj %ld ojcsj %ld ob %d"

    /* for HUPDBY command, we accept >4 arguments */
    GEN_HASH_KEY(key, id, klen);
    
    c = get_server_from_key(key, klen);

    GEN_FULL_KEY(key, id, klen, suffix);

    if (id->direction & (STREAM_IN | STREAM_INNIL))
        reply = redisCommand(c, INB_STREAM,
                             key,
                             id->direction,
                             id->zjs, /* if id->zjs > current value, then do
                                       * update, otherwise, do not update */
                             id->gjlx, /* tool type is ORed */
                             id->jlsj,
                             id->jcsj,
                             id->bs);
    else if (id->direction & (STREAM_OUT | STREAM_OUTNIL))
        reply = redisCommand(c, OUT_STREAM,
                             key,
                             id->direction,
                             id->zjs, /* if id->zjs > current value, then do
                                       * update, otherwise, do not update */
                             id->gjlx, /* tool type is ORed */
                             id->jlsj,
                             id->jcsj,
                             id->bs);
    else
        return -1;

    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n", c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        /* the update flag */
        switch (reply->integer) {
        case CANCEL:
            hvfs_debug(lib, "CANCEL record\n");
            atomic64_inc(&canceled);
            break;
        case INB_INIT:
        case OUT_INIT:
            hvfs_debug(lib, "INIT STREAM\n");
            break;
        case INB_UPDATED:
        case OUT_UPDATED:
            hvfs_debug(lib, "UPDATE STREAM\n");
            break;
        case INB_CLOSED:
        case OUT_CLOSED:
            hvfs_debug(lib, "Half CLOSE STREAM\n");
            break;
        case ALL_CLOSED:
            hvfs_debug(lib, "FULLY CLOSE STREAM\n");
            atomic64_inc(&poped);
            pop_huadan(id, suffix, DO_POP_RENAME);
            break;
        case INB_IGNORE:
        case OUT_IGNORE:
            hvfs_debug(lib, "IGNORE record\n");
            new_stream(id, suffix);
            break;
        case TIMED_IGNORE:
            /* new stream? */
            hvfs_debug(lib, "NEW STREAM\n");
            new_stream(id, suffix);
            break;
        }
    } else {
        hvfs_warning(lib, "%s", reply->str);
    }
    freeReplyObject(reply);

    return 0;
}

static inline
void handle_pp_reply(redisReply *reply, struct pp_rec_header *prh, struct pp_rec *spr)
{
    if (reply->type == REDIS_REPLY_INTEGER) {
        /* the update flag */
        switch (reply->integer) {
        case CANCEL:
            hvfs_debug(lib, "CANCEL record\n");
            atomic64_inc(&canceled);
            break;
        case INB_INIT:
        case OUT_INIT:
            hvfs_debug(lib, "INIT STREAM\n");
            break;
        case INB_UPDATED:
        case OUT_UPDATED:
            hvfs_debug(lib, "UPDATE STREAM\n");
            break;
        case INB_CLOSED:
        case OUT_CLOSED:
            hvfs_debug(lib, "Half CLOSE STREAM\n");
            break;
        case ALL_CLOSED:
            hvfs_debug(lib, "FULLY CLOSE STREAM\n");
            atomic64_inc(&poped);
            /* do not pop any huadan */
            break;
        case INB_IGNORE:
        case OUT_IGNORE:
        case TIMED_IGNORE:
        {
            struct pp_rec *pr;
            
            hvfs_debug(lib, "IGNORE record\n");

            /* alloc a pp_rec entry */
            pr = xrealloc(prh->array, (prh->nr + 1) * sizeof(*pr));
            if (pr) {
                prh->array = pr;
                prh->array[prh->nr] = *spr;
                prh->nr++;
            }
            break;
        }
        }
    } else {
        hvfs_warning(lib, "%s", reply->str);
    }

    return;
}

int addOrUpdateStreamPipeline(struct streamid *id, int suffix, int flush);

int new_stream_pipeline(struct streamid *id, int suffix)
{
    char str[128];
    int klen;
    
    suffix += 1;

    GEN_HASH_KEY(str, id, klen);
    GEN_FULL_KEY(str, id, klen, suffix);
    hvfs_debug(lib, "New -> %s\n", str);
    
    /* try next stream until 1000 */
    if (suffix > g_suffix_max) {
        g_suffix_max = suffix;
        if (suffix >= 10000) {
            return EINVAL;
        }
    }

    return addOrUpdateStreamPipeline(id, suffix, 0);
}

int prh_ready()
{
    struct pipeline *pp;
    int i, ready = 0;
    
    for (i = 0; i < xg->asize; i++) {
        int val;
        
        pp = xg->sites[i].private;
        sem_getvalue(&pp->pbs, &val);
        ready += val;
    }

    return (ready == xg->asize);
}

static inline void prh_handle_pp()
{
    struct pipeline *pp;
    int nr = 0;
    int i, j;

    /* wait for any prs */
    for (i = 0; i < xg->asize; i++) {
        int val;
        
        pp = xg->sites[i].private;
    reget:
        sem_getvalue(&pp->pbs, &val);
        if (val != 1) {
            sched_yield();
            goto reget;
        }
        
        if (pp->prh.nr > 0) {
            for (j = 0; j < pp->prh.nr; j++) {
                if (nr >= g_pnr) {
                    while (!prh_ready())
                        sched_yield();
                    nr = 0;
                }
                new_stream_pipeline(&pp->prh.array[j].id, pp->prh.array[j].suffix);
                newstream_nr++;
                nr++;
            }
            xfree(pp->prh.array);
            memset(&pp->prh, 0, sizeof(pp->prh));
        }
    }
}

int addOrUpdateStreamPipeline(struct streamid *id, int suffix, int flush)
{
    struct xnet_group_entry *e;
    struct pipeline *pp;
    redisContext *c;
    struct pp_rec *spr;
    char key[128];
    int klen;

    if (!id && flush)
        goto do_flush;
    
    GEN_HASH_KEY(key, id, klen);
    e = get_xg_from_key(key, klen);
    GEN_FULL_KEY(key, id, klen, suffix);

    e->nr++;
    c = e->context;
    pp = e->private;

    if (id->direction & (STREAM_IN | STREAM_INNIL)) {
        xlock_lock(&pp->lock);
        redisAppendCommand(c, INB_STREAM,
                           key,
                           id->direction,
                           id->zjs, /* if id->zjs > current value,
                                     * then do update, otherwise, do
                                     * not update */
                           id->gjlx, /* tool type is ORed */
                           id->jlsj,
                           id->jcsj,
                           id->bs);
        xlock_unlock(&pp->lock);
    } else if (id->direction & (STREAM_OUT | STREAM_OUTNIL)) {
        xlock_lock(&pp->lock);
        redisAppendCommand(c, OUT_STREAM,
                           key,
                           id->direction,
                           id->zjs, /* if id->zjs > current value,
                                     * then do update, otherwise, do
                                     * not update */
                           id->gjlx, /* tool type is ORed */
                           id->jlsj,
                           id->jcsj,
                           id->bs);
        xlock_unlock(&pp->lock);
    } else {
        return -1;
    }

    spr = pp->buf + sizeof(*spr) * pp->pnr;
    spr->id = *id;
    spr->suffix = suffix;
    pp->pnr++;
    pnr++;

    if (unlikely(pnr >= g_pnr)) {
        /* ok, we should get the reply */
        void *tmp;
        int i, j, err;

    do_flush:
        if (g_pp_sr) {
            for (i = 0; i < xg->asize; i++) {
                pp = xg->sites[i].private;
                if (pp->pnr) {
                retry:
                    err = sem_wait(&pp->pbs);
                    if (err < 0) {
                        if (errno == EINTR)
                            goto retry;
                        hvfs_err(lib, "sem_wait pipeline sem failed w/ %s\n",
                                 strerror(errno));
                        exit(-1);
                    }
                    /* exchange buf2 and buf */
                    tmp = pp->buf;
                    pp->buf = pp->buf2;
                    pp->buf2 = tmp;
                    pp->pnr2 = pp->pnr;
                    sem_post(&g_pp_sr_sem[i]);
                    pp->pnr = 0;
                }
            }
            /* reset counters */
            pnr = 0;

            if (flush) {
                while (!prh_ready())
                    sched_yield();
                prh_handle_pp();
            } else if (prh_ready()) {
                prh_handle_pp();
            }

        } else {
            struct pp_rec_header prh;
            redisReply *reply = NULL;

            memset(&prh, 0, sizeof(prh));
            
            /* iterate the xg group to get replies */
            for (i = 0; i < xg->asize; i++) {
                pp = xg->sites[i].private;
                if (pp->pnr) {
                    for (j = 0; j < pp->pnr; j++) {
                        if (redisGetReply(xg->sites[i].context, (void **)&reply) == 
                            REDIS_OK) {
                            handle_pp_reply(reply, &prh, pp->buf + 
                                            sizeof(struct pp_rec) * j);
                            freeReplyObject(reply);
                        } else {
                            hvfs_err(lib, "get reply failed w/ %d\n", 
                                     ((redisContext *)(xg->sites[i].context))->err);
                        }
                    }
                    pp->pnr = 0;
                }
            }
            /* reset counters */
            pnr = 0;
            
            if (prh.nr > 0) {
                for (i = 0; i < prh.nr; i++) {
                    new_stream_pipeline(&prh.array[i].id, prh.array[i].suffix);
                    newstream_nr++;
                }
                xfree(prh.array);
            }
        }
    }
    
    return 0;
}

static void *__pp_sr_thread_main(void *arg)
{
    struct pipeline *pp;
    redisReply *reply;
    sigset_t set;
    int idx = (long)arg, i;

    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    while (!g_pp_sr_thread_stop) {
        if (sem_wait(&g_pp_sr_sem[idx]) < 0) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        pp = xg->sites[idx].private;

        /* iterate the buf2 array */
        for (i = 0; i < pp->pnr2; i++) {
            xlock_lock(&pp->lock);
            if (redisGetReply(xg->sites[idx].context, (void **)&reply) == REDIS_OK) {
                xlock_unlock(&pp->lock);
                handle_pp_reply(reply, &pp->prh, pp->buf2 +
                                sizeof(struct pp_rec) * i);
                freeReplyObject(reply);
            } else {
                xlock_unlock(&pp->lock);
                hvfs_err(lib, "get reply failed w/ %d\n",
                         ((redisContext *)(xg->sites[idx].context))->err);
            }
        }
        pp->pnr2 = 0;

        sem_post(&pp->pbs);
    }

    pthread_exit(0);
}

int setup_sr_threads(int nr)
{
    pthread_t g_pp_sr_threads[nr];
    int err = 0, i;
    
    g_pp_sr_sem = xmalloc(sizeof(sem_t) * nr);
    if (!g_pp_sr_sem) {
        hvfs_err(lib, "xmalloc pp_sr_sem array failed\n");
        return -ENOMEM;
    }

    for (i = 0; i < nr; i++) {
        sem_init(&g_pp_sr_sem[i], 0, 0);
        err = pthread_create(&g_pp_sr_threads[i], NULL, &__pp_sr_thread_main,
                             (void *)(long)i);
        if (err) {
            hvfs_err(lib, "create sr thread failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
    }
out:
    return err;
}

int new_stream(struct streamid *id, int suffix)
{
    char str[128];
    int klen;
    
    suffix += 1;

    GEN_HASH_KEY(str, id, klen);
    GEN_FULL_KEY(str, id, klen, suffix);
    hvfs_debug(lib, "New -> %s\n", str);
    
    /* try next stream until 1000 */
    if (suffix >= 10000) {
        return EINVAL;
    }
    newstream_nr++;

    return addOrUpdateStream(id, suffix);
}

/* rename suffix + 1 to suffix until there is no (suffix + 1) key */
int rename_stream(struct streamid *id, int suffix)
{
    char from_key[128], to_key[128];
    redisContext *c;
    redisReply *reply;
    int klen1, klen2;

    GEN_HASH_KEY(from_key, id, klen1);
    GEN_HASH_KEY(to_key, id, klen2);

    c = get_server_from_key(from_key, klen1);

    GEN_FULL_KEY(from_key, id, klen1, suffix + 1);
    GEN_FULL_KEY(to_key, id, klen2, suffix);

    reply = redisCommand(c, "RENAME %s %s", from_key, to_key);
    if (!reply) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_ERROR) {
        /* ok, we can safely stop here */
        hvfs_debug(lib, "RENAME from %s to %s failed %s\n", 
                   from_key, to_key, reply->str);
        freeReplyObject(reply);
        return 0;
    } else {
        freeReplyObject(reply);
        hvfs_info(lib, "RENAME from %s to %s\n", from_key, to_key);
        return rename_stream(id, suffix + 1);
    }
}

/* pop: 0 => peek, 1 => pop and rename, 2 => pop and not rename
 */
int getStreamStat(struct streamid *id, int suffix, struct streamstat *stat, int pop)
{
    char key[128];
    redisContext *c;
    redisReply *reply;
    int klen;

    memset(stat, 0, sizeof(*stat));

    GEN_HASH_KEY(key, id, klen);

    c = get_server_from_key(key, klen);

    GEN_FULL_KEY(key, id, klen, suffix);

    /* for HGETALL command */
    reply = redisCommand(c, "HGETALL %s", key);
    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_ARRAY) {
        int j;

        for (j = 0; j < reply->elements; j += 2) {
            hvfs_debug(lib, "%s = %s\n", 
                       reply->element[j]->str, reply->element[j + 1]->str);

            if (strcmp(reply->element[j]->str, "D") == 0) {
                stat->DON = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "ig") == 0) {
                stat->ogjlx = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "ijlsj") == 0) {
                stat->ijlsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ijlks") == 0) {
                stat->ijlks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ijcsj") == 0) {
                stat->ijcsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ijcks") == 0) {
                stat->ijcks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "iz") == 0) {
                stat->izjs = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ib") == 0) {
                stat->ibs = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "og") == 0) {
                stat->ogjlx = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "ojlsj") == 0) {
                stat->ojlsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ojlks") == 0) {
                stat->ojlks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ojcsj") == 0) {
                stat->ojcsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ojcks") == 0) {
                stat->ojcks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "oz") == 0) {
                stat->ozjs = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ob") == 0) {
                stat->obs = atoi(reply->element[j + 1]->str);
            }
        }
    } else if (reply->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "HGETALL %s => %s\n", key, reply->str);
        freeReplyObject(reply);
        return -1;
    }
    freeReplyObject(reply);

    if (pop) {
        /* remove the entry now */
        reply = redisCommand(c, "DEL %s", key);
        if (unlikely(!reply)) {
            hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                     c->err);
            exit(-1);
        }
        if (reply->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "DEL %s => %s\n", key, reply->str);
            freeReplyObject(reply);
            return -1;
        }
        freeReplyObject(reply);
    }

    /* try to rename same five tuple streams */
    if (pop == DO_POP_RENAME)
        rename_stream(id, suffix);

    return 0;
}

void generate_huadan(struct streamid *id, struct streamstat *stat, struct huadan *hd)
{
    long long begin, end;

    if (!id || !stat || !hd)
        return;
    
    hd->fwqip = id->fwqip;
    hd->fwqdk = id->fwqdk;
    hd->khdip = id->khdip;
    hd->khddk = id->khddk;
    hd->protocol = id->protocol;

    hd->gjlx = stat->igjlx | stat->ogjlx;

    if (stat->ijlks == 0)
        hd->qssj = stat->ojlks;
    else if (stat->ojlks == 0)
        hd->qssj = stat->ijlks;
    else
        hd->qssj = min(stat->ijlks, stat->ojlks);
    hd->jssj = max(stat->ijlsj, stat->ojlsj);

    /* dural time computing */
    if (stat->DON & (STREAM_INNIL | STREAM_OUTNIL)) {
        /* have detect a NIL packet, based on jcsj */
        if (stat->ijcks == 0)
            begin = stat->ojcks;
        else if (stat->ojcks == 0)
            begin = stat->ijcks;
        else
            begin = min(stat->ijcks, stat->ojcks);
        end = max(stat->ijcsj, stat->ojcsj);
        
        hd->cxsj = end - begin + 1;
        if (end - begin + 1 < 0) {
            char *str = printStreamStat(id, stat);
            hvfs_warning(lib, "Detect minus CXSJ: %s\n", str);
            free(str);
        }
    } else {
        /* no NIL packet, based on jlsj */
        hd->cxsj = hd->jssj - hd->qssj + 1;
    }

    hd->in_bs = stat->ibs;
    hd->out_bs = stat->obs;
    hd->in_zjs = stat->izjs;
    hd->out_zjs = stat->ozjs;
}

int pop_huadan(struct streamid *id, int suffix, int flag)
{
    struct streamstat stat;
    struct huadan hd;
    char *str;
    int err = 0, len, bl, bw;

    memset(&stat, 0, sizeof(stat));
    memset(&hd, 0, sizeof(hd));
    
    err = getStreamStat(id, suffix, &stat, flag);
    if (err)
        return err;

    generate_huadan(id, &stat, &hd);

    /* append to local file */
    str = printHuadanLine(&hd);
    if (!str) {
        hvfs_err(lib, "printHuadanLine failed, no memory?\n");
        return -1;
    }
    len = strlen(str);
    bl = 0;
    do {
        bw = write(huadan_fd, str + bl, len - bl);
        if (bw < 0) {
            hvfs_err(lib, "write huadan file failed w/ %s\n",
                     strerror(errno));
            break;
        }
        bl += bw;
    } while (bl < len);

    pop_huadan_nr++;
    if (pop_huadan_nr % HUADAN_FSYNC_NR == 0)
        fsync(huadan_fd);
    free(str);

    return 0;
}

int peek_huadan(struct streamid *id, int suffix)
{
    struct streamstat stat;
    struct huadan hd;
    char *str;
    int err = 0, len, bl, bw;

    memset(&stat, 0, sizeof(stat));
    memset(&hd, 0, sizeof(hd));
    
    err = getStreamStat(id, suffix, &stat, DO_PEEK);
    if (err)
        return err;

    generate_huadan(id, &stat, &hd);

    /* append to local file */
    str = printHuadanLine(&hd);
    if (!str) {
        hvfs_err(lib, "printHuadanLine failed, no memory?\n");
        return -1;
    }
    len = strlen(str);
    bl = 0;
    do {
        bw = write(huadan_fd, str + bl, len - bl);
        if (bw < 0) {
            hvfs_err(lib, "write huadan file failed w/ %s\n",
                     strerror(errno));
            break;
        }
        bl += bw;
    } while (bl < len);

    peek_huadan_nr++;
    if (peek_huadan_nr % HUADAN_FSYNC_NR == 0)
        fsync(huadan_fd);
    free(str);

    return 0;
}

#define SET_STREAM(n, idx, name, func, value) do {   \
        if (n == idx) {                              \
            id->name = func(value);                  \
        }                                            \
    } while (0)

int parse_streamid(char *ID, struct streamid *id)
{
    char *q, *p = ID;
    int n = 0, suffix = 0;

    memset(id, 0, sizeof(*id));
    do {
        q = strtok(p, ":+\n");
        if (!q) {
            break;
        }
        SET_STREAM(n, 1, fwqip, atoi, q);
        SET_STREAM(n, 2, fwqdk, atoi, q);
        SET_STREAM(n, 3, khdip, atoi, q);
        SET_STREAM(n, 4, khddk, atoi, q);
        SET_STREAM(n, 5, protocol, atoi, q);
        if (n == 6) {
            suffix = atoi(q);
        }
        n++;
    } while (p = NULL, 1);

    return suffix;
}

void pop_stream(redisContext *c)
{
    struct streamid id;
    redisReply *reply;
    int suffix, err;

    reply = redisCommand(c, "KEYS S:*");
    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_ARRAY) {
        int j;

        for (j = 0; j < reply->elements; j++) {
            suffix = parse_streamid(reply->element[j]->str, &id);
            flushed++;
            /* do not need rename streams */
            err = pop_huadan(&id, suffix, DO_POP_ONLY);
            if (err) {
                hvfs_err(lib, "pop_huadan() '%s' failed w/ %d\n",
                         reply->element[j]->str, err);
            }
        }
    }
    freeReplyObject(reply);
}

void pop_all_stream()
{
    redisContext *c;
    int i;

    /* only pop the streams on my own host */
    if (g_local) {
        /* get interface address */
        struct ifaddrs *ifaddr, *ifa;
        int family, s;
        char host[NI_MAXHOST];

        if (getifaddrs(&ifaddr) == -1) {
            hvfs_err(lib, "getifaddrs() failed w/ %s\n", strerror(-errno));
            return;
        }
        for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
            if (ifa->ifa_addr == NULL)
                continue;

            family = ifa->ifa_addr->sa_family;
            if (family == AF_INET) {
                s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                                host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
                if (s != 0) {
                    printf("getnameinfo() failed: %s\n", gai_strerror(s));
                    exit(EXIT_FAILURE);
                }

                for (i = 0; i < xg->asize; i++) {
                    if (strcmp(host, xg->sites[i].node) == 0) {
                        hvfs_info(lib, "POP local server[%d] %s:%d\n",
                                  i, xg->sites[i].node, xg->sites[i].port);
                        c = xg->sites[i].context;
                        pop_stream(c);
                    }
                }
            }
        }
    } else {
        for (i = 0; i < xg->asize; i++) {
            c = xg->sites[i].context;
            pop_stream(c);
        }
    }
}

void peek_stream(redisContext *c)
{
    struct streamid id;
    redisReply *reply;
    int suffix, err;

    reply = redisCommand(c, "KEYS S:*");
    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_ARRAY) {
        int j;

        for (j = 0; j < reply->elements; j++) {
            suffix = parse_streamid(reply->element[j]->str, &id);
            flushed++;
            err = peek_huadan(&id, suffix);
            if (err) {
                hvfs_err(lib, "peek_huadan() '%s' failed w/ %d\n",
                         reply->element[j]->str, err);
            }
        }
    }
    freeReplyObject(reply);
}

void peek_all_stream()
{
    redisContext *c;
    int i;

    if (g_local) {
        /* get interface address */
        struct ifaddrs *ifaddr, *ifa;
        int family, s;
        char host[NI_MAXHOST];

        if (getifaddrs(&ifaddr) == -1) {
            hvfs_err(lib, "getifaddrs() failed w/ %s\n", strerror(-errno));
            return;
        }
        for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
            if (ifa->ifa_addr == NULL)
                continue;

            family = ifa->ifa_addr->sa_family;
            if (family == AF_INET) {
                s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in),
                                host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
                if (s != 0) {
                    printf("getnameinfo() failed: %s\n", gai_strerror(s));
                    exit(EXIT_FAILURE);
                }

                for (i = 0; i < xg->asize; i++) {
                    if (strcmp(host, xg->sites[i].node) == 0) {
                        hvfs_info(lib, "PEEK local server[%d] %s:%d\n",
                                  i, xg->sites[i].node, xg->sites[i].port);
                        c = xg->sites[i].context;
                        peek_stream(c);
                    }
                }
            }
        }
    } else {
        for (i = 0; i < xg->asize; i++) {
            c = xg->sites[i].context;
            peek_stream(c);
        }
    }
}

int use_db(int db)
{
    redisContext *c;
    redisReply *reply;
    int err = 0, i;
    
    for (i = 0; i < xg->asize; i++) {
        c = xg->sites[i].context;
        do {
            reply = redisCommand(c, "SELECT %d", db);
            if (unlikely(!reply)) {
                hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                         c->err);
                return -EINVAL;
            } else if (reply->type == REDIS_REPLY_STATUS) {
                if (strncmp(reply->str, "OK", 2) == 0) {
                    hvfs_info(lib, "Server[%d]: Select to use Database %d\n", 
                              i, db);
                    break;
                }
            }
            hvfs_err(lib, "Select to use Database %d on site %s failed: %s\n", 
                     db, xg->sites[i].node, reply->str);
            
            freeReplyObject(reply);
            return -EINVAL;
        } while (0);
        freeReplyObject(reply);
    }

    return err;
}

int set_file_size(redisContext *c, char *pathname, long osize)
{
    redisReply *reply;
    long saved = -1;
    int err = 0;

    if (!pathname)
        return 0;
    

    /* update the g_doffset */
    if (!g_parse_byfd) {
        if (g_dfp) {
            long __off = ftell(g_dfp);
            
            if (__off > 0) {
                g_doffset = __off;
            }
        }
    } else {
        if (g_dfd > 0) {
            long __off = lseek(g_dfd, 0, SEEK_CUR);
            
            if (__off > 0) {
                g_doffset = __off;
            }
        }
    }

    /* default use db 0 */
    reply = redisCommand(c, "GETSET FS:%s %ld", pathname, g_doffset);
    if (!reply) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        err = c->err;
        goto out;

    }
    if (reply->type == REDIS_REPLY_NIL) {
        hvfs_info(lib, "Init file %s size to %ld!\n",
                  pathname, g_doffset);
        saved = osize;
    } else if (reply->type == REDIS_REPLY_STRING) {
        if (reply->str)
            saved = atol(reply->str);
    } else {
        hvfs_warning(lib, "Not an integer string reply? set failed %s\n",
                     reply->str);
        err = -EINVAL;
    }
    freeReplyObject(reply);

    if (saved != osize) {
        hvfs_warning(lib, "Someone changed the file length? new(%ld) vs %ld\n",
                     saved, osize);
    }
    hvfs_info(lib, "Update file offset to %ld B (%lf MB/s)\n", g_doffset,
              (double)(g_doffset - g_last_offset) / 1024.0 / 1024.0 / g_interval);
    g_last_offset = g_doffset;

out:
    return err;
}

int c_set_file_size(char *pathname, long osize)
{
    redisContext *c;
    /* Use 0the server, connect it each time */
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds

    hvfs_info(lib, "SET: Connect to Server[%ld] %s:%d ... ", 
              xg->sites[0].site_id, xg->sites[0].node, xg->sites[0].port);
    c = redisConnectWithTimeout(xg->sites[0].node, xg->sites[0].port,
                                timeout);
    if (c->err) {
        hvfs_plain(lib, "failed w/ %d\n", c->err);
        return -1;
    } else {
        hvfs_plain(lib, "ok.\n");
    }

    set_file_size(c, pathname, osize);
    
    redisFree(c);

    return 0;
}

int check_conflict(char *pathname, pid_t pid)
{
    redisContext *c;
    redisReply *reply;
    int err = 0;

    /* Use 0the server, connect it each time */
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds

    hvfs_info(lib, "CC: Connect to Server[%ld] %s:%d ... ", 
              xg->sites[0].site_id, xg->sites[0].node, xg->sites[0].port);
    c = redisConnectWithTimeout(xg->sites[0].node, xg->sites[0].port,
                                timeout);
    if (c->err) {
        hvfs_plain(lib, "failed w/ %d\n", c->err);
        return -1;
    } else {
        hvfs_plain(lib, "ok.\n");
    }

    reply = redisCommand(c, "SETNX HB:%s %ld", pathname, (long)pid);
    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 0) {
            /* key conflict */
            err = -EEXIST;
            goto out;
        } else {
            hvfs_info(lib, "SET HB:%s to %ld!\n", pathname, (long)pid);
        }
    } else {
        hvfs_warning(lib, "Not an integer string reply? set failed %s\n",
                     reply->str);
        err = -EINVAL;
        goto out;
    }
    freeReplyObject(reply);

    /* update TTL */
    reply = redisCommand(c, "EXPIRE HB:%s %d", 
                         pathname, 3 * g_interval);
    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 0) {
            /* key not exist?!! */
            err = check_conflict(pathname, pid);
            if (err) {
                hvfs_err(lib, "Someone else started, let us exit now.\n");
                exit(err);
            }
        }
    } else {
        hvfs_warning(lib, "Not an integer reply? HB failed %s\n",
                     reply->str);
        err = -EINVAL;
    }
    freeReplyObject(reply);

out:
    redisFree(c);

    return err;
}

int remove_conflict(char *pathname)
{
    redisContext *c;
    redisReply *reply;
    int err = 0;

    /* Use 0the server, connect it each time */
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds

    hvfs_info(lib, "CC: Connect to Server[%ld] %s:%d ... ", 
              xg->sites[0].site_id, xg->sites[0].node, xg->sites[0].port);
    c = redisConnectWithTimeout(xg->sites[0].node, xg->sites[0].port,
                                timeout);
    if (c->err) {
        hvfs_plain(lib, "failed w/ %d\n", c->err);
        return -1;
    } else {
        hvfs_plain(lib, "ok.\n");
    }

    reply = redisCommand(c, "DEL HB:%s", pathname);
    if (unlikely(!reply)) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        exit(-1);
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        hvfs_info(lib, "Clear HB:%s w/ %lld!\n", pathname, reply->integer);
    } else {
        hvfs_warning(lib, "Not an integer reply? del failed %s\n",
                     reply->str);
        err = -EINVAL;
        goto out;
    }
    freeReplyObject(reply);

out:
    redisFree(c);

    return err;
}

int do_HB(redisContext *c, char *pathname, pid_t pid)
{
    redisReply *reply;
    int err = 0;

    if (!pathname)
        return 1;
    
    /* default use db 0 */
    reply = redisCommand(c, "EXPIRE HB:%s %d", 
                         pathname, 3 * g_interval);
    if (!reply) {
        hvfs_err(lib, "Connection corrupted, failed w/ %d\n",
                 c->err);
        goto out;
    }
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 0) {
            /* key not exist?!! */
            err = check_conflict(pathname, pid);
            if (err) {
                hvfs_err(lib, "Someone else started, let us exit now.\n");
                exit(err);
            }
        }
    } else {
        hvfs_warning(lib, "Not an integer reply? HB failed %s\n",
                     reply->str);
        err = -EINVAL;
    }
    freeReplyObject(reply);
    
out:

    return err;
}

void __update_server_ring(struct chring *ring, struct xnet_group *xg)
{
    int i, j;

    for (i = 0; i < xg->asize; i++) {
        for (j = 0; j < ring->used; j++) {
            if (ring->array[j].site_id == xg->sites[i].site_id) {
                ring->array[j].private = &xg->sites[i];
            }
        }
    }
}

int init_server_ring(char *conf_file, struct chring *ring)
{
    int err = 0, nr = 10000, i, j = 0;
    struct conf_site cs[nr];
    
    err = conf_parse(conf_file, cs, &nr);
    if (err) {
        hvfs_err(lib, "conf_parse failed w/ %d\n", err);
        goto out;
    }
    
    memset(ring, 0, sizeof(*ring));
    for (i = 0; i < nr; i++) {
        if (strcmp(cs[i].type, "redis") == 0) {
            err = ring_add(ring, cs[i].node, cs[i].port, cs[i].id, 100);
            if (err) {
                hvfs_err(lib, "ring_add() failed w/ %d\n", err);
                goto out;
            }
            j++;
        } else if (strcmp(cs[i].type, "client") == 0) {
            g_client_nr++;
            err = xnet_group_add(&xg_client, cs[i].id, cs[i].node, cs[i].port);
            if (err) {
                hvfs_err(lib, "add client to XG failed w/ %d\n", err);
                goto out;
            }
        }
    }
    hvfs_info(lib, "Total Server number is %d\n", j);
    hvfs_info(lib, "Total Client number is %d\n", g_client_nr);
    ring_resort_nolock(ring);
    ring_stat(ring, j);

out:
    return err;
}

#define UPDATE_FIELD(stream, idx, name, func, str) do {         \
        stream.name = func(str[idx]);                           \
    } while (0)

int process_table(void *data, int cnt, const char **cv)
{
    struct manager *m = (struct manager *)data;
    struct streamid id;
    char *p, *q;
    int EOS = 0, isact = 0, isfrg = 0;

    if (unlikely(cnt < m->cnr_accept)) {
        /* ignore this line */
        corrupt_nr++;
        return 0;
    }

    memset(&id, 0, sizeof(id));
    UPDATE_FIELD(id, LYLX_SHIFT(1), jlsj, atol, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(2), jcsj, atol, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(3), cljip, atoi, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(5), fwqip, atoi, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(6), fwqdk, atoi, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(7), khdip, atoi, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(8), khddk, atoi, cv);
    {
        /* parse the string */
        p = (char *)cv[LYLX_SHIFT(9)];
        do {
            q = strtok(p, ";,");
            if (!q) {
                break;
            } 

            if (unlikely(!id.direction)) {
                if (strcmp(q, "INB") == 0) {
                    id.direction = STREAM_IN;
                } else if (strcmp(q, "OUT") == 0) {
                    id.direction = STREAM_OUT;
                }
            }
            
            if (unlikely(!id.protocol)) {
                if (strcmp(q, "UDA") == 0) {
                    id.protocol = STREAM_UDP;
                } else if (strcmp(q, "TDA") == 0) {
                    id.protocol = STREAM_TCP;
                }
            }

            if (strcmp(q, "NIL") == 0) {
                /* this is the stream end flag, we should POP the
                 * six-entry tuple from db[0] to db[1] */
                EOS = 1;
            } else if (strcmp(q, "ACT") == 0) {
                isact = 1;
            } else if (strcmp(q, "FRG") == 0) {
                isfrg = 1;
            }
        } while (p = NULL, 1);
    }
    UPDATE_FIELD(id, LYLX_SHIFT(10), bs, atoi, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(11), zjs, atol, cv);
    UPDATE_FIELD(id, LYLX_SHIFT(12), gjlx, atoi, cv);

    if (EOS) {
        id.direction = (id.direction << 2) & 0xf;
        if (!id.protocol || !id.direction) {
            /* invalid direction, ignore it */
            ignore_nr++;
            return 0;
        }
    } else {
        if (unlikely(g_sc.ignoreact)) {
            if (!id.direction || !id.protocol) {
                /* invalid direction, ignore it */
                ignore_nr++;
                return 0;
            }
        } else {
            if (!id.direction || !id.protocol || !(isact && !isfrg)) {
                /* invalid direction, ignore it */
                ignore_nr++;
                return 0;
            }
        }
    }

    if (IS_HVFS_DEBUG(lib)) {
        p = printStreamid(&id);
        hvfs_debug(lib, " => %s\n", p); free(p);
    }

    if (g_pipeline) {
        lib_timer_B();
        addOrUpdateStreamPipeline(&id, 0, 0);
        lib_timer_E();
        lib_timer_A(&ACC);
    } else {
        lib_timer_B();
        addOrUpdateStream(&id, 0);
        lib_timer_E();
        lib_timer_A(&ACC);
    }
    process_nr++;
    if (process_nr % 100000 == 99999) {
        hvfs_info(lib, "LATENCY %.2f us\n", ACC / process_nr);
    }

    return 0;
}

int parse_csv_file(char *filepath, long offset)
{
    FILE *fp;
    
    if ((fp = fopen(filepath, "r")) == NULL) {
        hvfs_err(lib, "Cannot open input file '%s' for %s\n", 
                 filepath, strerror(errno));
        return -errno;
    }
    if (offset > 0) {
        if (fseek(fp, offset, SEEK_SET) < 0) {
            hvfs_err(lib, "fseek stream to POS %ld failed w/ %s\n",
                     offset, strerror(errno));
            return -errno;
        }
        g_doffset = offset;
    }

    g_dfp = fp;
    
    switch (csv_parse(fp, process_table, &m)) {
    case E_LINE_TOO_WIDE:
        hvfs_err(lib, "Error parsing csv: line too wide.\n");
        break;
    case E_QUOTED_STRING:
        hvfs_err(lib, "Error parsing csv: ill-formatted quoted string.\n");
        break;
    case E_PARTITAL_LINE:
        hvfs_err(lib, "Error parsing csv: partitial line.\n");
        break;
    }

    /* if it is in pipeline mode, we should do final flush */
    while (g_pipeline && pnr > 0) {
        hvfs_info(lib, "PNR is %d, Begin Flush ...\n", pnr);
        addOrUpdateStreamPipeline(NULL, 0, 1);
    }
    
    /* update the g_doffset finally */
    {
        long __off = ftell(g_dfp);
        
        if (__off > 0) {
            g_doffset = __off;
        }
    }

    return offset;
}

int parse_csv_file_byfd(char *filepath, long offset)
{
    int fd;
    
    if ((fd = open(filepath, O_RDONLY)) < 0) {
        hvfs_err(lib, "Cannot open input file '%s' for %s\n", 
                 filepath, strerror(errno));
        return -errno;
    }
    if (offset > 0) {
        if (lseek(fd, offset, SEEK_SET) < 0) {
            hvfs_err(lib, "fseek stream to POS %ld failed w/ %s\n",
                     offset, strerror(errno));
            return -errno;
        }
        g_doffset = offset;
    }

    g_dfd = fd;
    
    /* default as 128KB */
    switch (csv_parse_fast(fd, g_fast_bsize, process_table, &m, 150)) {
    case E_LINE_TOO_WIDE:
        hvfs_err(lib, "Error parsing csv: line too wide.\n");
        break;
    case E_QUOTED_STRING:
        hvfs_err(lib, "Error parsing csv: ill-formatted quoted string.\n");
        break;
    case E_PARTITAL_LINE:
        hvfs_err(lib, "Error parsing csv: partitial line.\n");
        break;
    }

    /* if it is in pipeline mode, we should do final flush */
    while (g_pipeline && pnr > 0) {
        hvfs_info(lib, "PNR is %d, Begin Flush ...\n", pnr);
        addOrUpdateStreamPipeline(NULL, 0, 1);
    }
    
    /* update the g_doffset finally */
    {
        long __off = lseek(g_dfd, 0, SEEK_CUR);

        if (__off > 0) {
            g_doffset = __off;
        }
    }

    return offset;
}

void fina_xg(struct xnet_group *xg)
{
    int i;

    for (i = 0; i < xg->asize; i++) {
        redisFree(xg->sites[i].context);
    }
    
    xfree(xg);
}

int is_action_valid(char *action)
{
    int valid = 1;
    
    if (!action)
        return 1;

    if (strcmp(action, "load") == 0) {
        g_action |= ACTION_LOAD_DATA;
    } else if (strcmp(action, "peek") == 0) {
        g_action |= ACTION_PEEK_HUADAN;
    } else if (strcmp(action, "pop") == 0) {
        g_action |= ACTION_POP_HUADAN;
    } else if (strcmp(action, "noop") == 0) {
        g_action |= ACTION_NOOP;
    } else if (strcmp(action, "test") == 0) {
        g_action |= ACTION_TEST;
    } else {
        valid = 0;
    }

    return valid;
}

void do_help()
{
    hvfs_plain(lib, 
               "Copyright (c) 2012 Ma Can, IIE\n"
               "Version 1.0.0a\n\n"
               "Arguments:\n"
               "-d, --id #num\tClient ID.\n"
               "-r, --round #num\tWhich round this client belongs to.\n"
               "-a, --action STR\t'load', 'peek', or 'pop'.\n"
        );
}

static inline int do_load_data(char *data_file, long offset)
{
    if (!g_parse_byfd)
        return parse_csv_file(data_file, offset);
    else
        return parse_csv_file_byfd(data_file, offset);
}

static inline void do_peek_huadan()
{
    peek_all_stream();
}

static inline void do_pop_huadan()
{
    pop_all_stream();
}

void do_test()
{
    struct timeval begin, end;
    /* add or update a stream statis entry */
    struct streamid stream = {
        .fwqip = 1010812,
        .fwqdk = 80,
        .khdip = 200020802,
        .khddk = 2000,
        .direction = 1,
        .protocol = 1,

        .jlsj = 11800,
        .jcsj = 11701,
        .cljip = 30002,
        .bs = 800,
        .zjs = 3800,
        .gjlx = 8,
    };
    struct streamstat stat;
    int i, j, err;
    char *str;

    gettimeofday(&begin, NULL);
    for (i = 0; i < 1000; i++) {
        stream.fwqip++;
        for (j = 0; j < 10; j++) {
            stream.direction = j % 2 + 1;
            if (j > 7) {
                stream.direction <<= 2;
            }
            stream.zjs++;
            stream.jlsj++;
            stream.jcsj++;
            str = printStreamid(&stream);
            hvfs_debug(lib, "+ %s DON %d\n", str, stream.direction); free(str);
            err = addOrUpdateStream(&stream, 0);
        }
    }
    gettimeofday(&end, NULL);
    printf("+ err %d\n", err);
    printf("P %.2lf\n", 10000 / ((end.tv_sec + end.tv_usec / 1000000.0) - 
                                   (begin.tv_sec + begin.tv_usec / 1000000.0)));
    stream.fwqip = 10813;
    getStreamStat(&stream, 0, &stat, DO_POP_RENAME);
    str = printStreamStat(&stream, &stat);
    printf("= %s\n", str); free(str);
}

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
    hvfs_verbose(lib, "Did this signal handler called?\n");

    return;
}

static redisContext *rebuild_connection(redisContext *c)
{
    redisContext *r = c;
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    
rebuild:
    if (!c || c->err) {
        redisFree(c);
        hvfs_debug(lib, "REConnect to Server %s:%d ... ",
                   xg->sites[0].node, xg->sites[0].port);
        r = redisConnectWithTimeout(xg->sites[0].node, xg->sites[0].port, timeout);
        if (r->err) {
            hvfs_err(lib, "Connect failed w/ %s\n", r->errstr);
            c = r;
            goto rebuild;
        }
    }

    return r;
}

static void *__timer_thread_main(void *arg)
{
    sigset_t set;
    time_t cur, last = 0;
    long last_process = 0;
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    redisContext *c;
    int err, nr = 0;


    hvfs_debug(lib, "I am running...\n");

    /* unblock the SIGALRM signal */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_UNBLOCK, &set, NULL);

    sigemptyset(&set);
    sigaddset(&set, SIGABRT);
    pthread_sigmask(SIG_BLOCK, &set, NULL);

    /* Use 0the server, connect it on startup */
    hvfs_info(lib, "TT: Connect to Server[%ld] %s:%d ... ", 
              xg->sites[0].site_id, xg->sites[0].node, xg->sites[0].port);
    c = redisConnectWithTimeout(xg->sites[0].node, xg->sites[0].port,
                                timeout);
    if (c->err) {
        hvfs_plain(lib, "failed w/ %d\n", c->err);
    } else {
        hvfs_plain(lib, "ok.\n");
    }

    /* then, we loop for the timer events */
    while (!g_timer_thread_stop) {
        err = sem_wait(&g_timer_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        c = rebuild_connection(c);
        cur = time(NULL);
        if (last == 0)
            last = cur;
        /* should we work now */
        if (cur - last > g_interval) {
            hvfs_info(lib, ">>>>> Update Offset and HB Info >>>>\n");
            last = cur;
            if (g_action & ACTION_LOAD_DATA) {
                set_file_size(c, g_input, g_last_offset);
                do_HB(c, g_input, getpid());
            } else if (g_action & (ACTION_PEEK_HUADAN | ACTION_POP_HUADAN)) {
                do_HB(c, g_output, getpid());
            }
            /* check if we are blocked? */
            if (process_nr + ignore_nr + corrupt_nr + atomic64_read(&poped) 
                + flushed > last_process) {
                last_process = process_nr + ignore_nr + corrupt_nr + 
                    atomic64_read(&poped) + flushed;
                nr = 0;
            } else {
                nr++;
            }
        }
        if (nr > 10) {
            hvfs_info(lib, "Detect ourself is not working, exit myself!\n");
            exit(-1);
        }
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_timers(int interval)
{
    struct sigaction ac;
    sigset_t set;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL;
    int err;

    sem_init(&g_timer_sem, 0, 0);

    err = pthread_create(&g_timer_thread, NULL, &__timer_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create timer thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */

    memset(&ac, 0, sizeof(ac));
    sigemptyset(&ac.sa_mask);
    ac.sa_flags = 0;
    ac.sa_sigaction = __itimer_default;
    err = sigaction(SIGALRM, &ac, NULL);
    if (err) {
        err = -errno;
        goto out;
    }
    err = getitimer(which, &pvalue);
    if (err) {
        err = -errno;
        goto out;
    }

    if (interval) {
        value.it_interval.tv_sec = 1;
        value.it_interval.tv_usec = 0;
        value.it_value.tv_sec = 1;
        value.it_value.tv_usec = 0;
        err = setitimer(which, &value, &ovalue);
        if (err) {
            err = -errno;
            goto out;
        }
        hvfs_debug(lib, "OK, we have created a timer thread to "
                   " do scans every %d second(s).\n", interval);
    } else {
        hvfs_debug(lib, "Hoo, there is no need to setup itimers based on the"
                   " configration.\n");
        g_timer_thread_stop = 1;
    }

    err = getitimer(which, &pvalue);
    if (err) {
        err = -errno;
        goto out;
    }

out:
    return err;
}

static void __sigaction_default(int signo, siginfo_t *info, void *arg)
{
    if (signo == SIGSEGV || signo == SIGABRT) {
        hvfs_info(lib, "Recv %sSIGSEGV/SIGABRT%s %s\n",
                  HVFS_COLOR_RED,
                  HVFS_COLOR_END,
                  SIGCODES(info->si_code));
        lib_segv(signo, info, arg);
    } else if (signo == SIGBUS) {
        hvfs_info(lib, "Recv %sSIGBUS%s %s\n",
                  HVFS_COLOR_RED,
                  HVFS_COLOR_END,
                  SIGCODES(info->si_code));
        lib_segv(signo, info, arg);
    } else if (signo == SIGHUP || signo == SIGUSR1) {
        /* do not print anything to eliminate deadlock */
        sem_post(&g_exit_sem);
    }
}

static int __init_signal(void)
{
    struct sigaction ac;
    int err;

    ac.sa_sigaction = __sigaction_default;
    err = sigemptyset(&ac.sa_mask);
    if (err) {
        err = errno;
        goto out;
    }
    ac.sa_flags = SA_SIGINFO;

#ifndef UNIT_TEST
    err = sigaction(SIGTERM, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGABRT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGHUP, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGINT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGSEGV, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGBUS, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGQUIT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGUSR1, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
#endif

out:
    return err;
}

struct pipeline *__alloc_pipeline()
{
    struct pipeline *p = NULL;

    p = xzalloc(sizeof(*p));
    if (!p) {
        hvfs_err(lib, "xzalloc() pipeline struct failed\n");
        return NULL;
    }
    
    p->pnr = 0;
    p->buf = xzalloc(sizeof(struct pp_rec) * g_pnr);
    if (!p->buf) {
        hvfs_err(lib, "xzalloc() pipeline buffer failed\n");
        goto free;
    }
    p->pnr2 = 0;
    p->buf2 = xzalloc(sizeof(struct pp_rec) * g_pnr);
    if (!p->buf2) {
        hvfs_err(lib, "xzalloc() pipeline buffer failed\n");
        xfree(p->buf);
        goto free;
    }
    sem_init(&p->pbs, 0, 1);
    xlock_init(&p->lock);

    return p;
free:
    xfree(p);
    return NULL;
}

int main(int argc, char *argv[]) {
    redisContext *c;
    char *action = NULL, *conf_file = NULL, *data_file = NULL, 
        *huadan_file = NULL, *log_file = NULL, *ipdb_file = NULL, *value;
    char hostname[128];
    long offset = 0;
    int err = 0, is_fork = 0, i, db = 3, mode = 0;

    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    char *shortflags = "c:b:A:d:r:a:s:i:o:x:fh?lgvD:I:m:MF::p::";
    struct option longflags[] = {
        {"id", required_argument, 0, 'd'},
        {"round", required_argument, 0, 'r'},
        {"action", required_argument, 0, 'a'},
        {"offset", required_argument, 0, 's'},
        {"input", required_argument, 0, 'i'},
        {"output", required_argument, 0, 'o'},
        {"log", required_argument, 0, 'x'},
        {"config", required_argument, 0, 'c'},
        {"ipdb", required_argument, 0, 'b'},
        {"ignoreact", required_argument, 0, 'A'},
        {"database", required_argument, 0, 'D'},
        {"interval", required_argument, 0, 'I'},
        {"mode", required_argument, 0, 'm'},
        {"pipeline", optional_argument, 0, 'p'},
        {"multithread", no_argument, 0, 'M'},
        {"byfd", optional_argument, 0, 'F'},
        {"isfork", no_argument, 0, 'f'},
        {"local", no_argument, 0, 'l'},
        {"debug", no_argument, 0, 'g'},
        {"csv", no_argument, 0, 'v'},
        {"help", no_argument, 0, 'h'},
    };
#ifdef USE_LYLX
    TABLE_DEF(orig_log, 18, 13);
#else
    TABLE_DEF(orig_log, 17, 12);
#endif
    TABLE_FIELD(orig_log, int, rzlx);
    TABLE_FIELD(orig_log, unsigned long, jlsj);
    TABLE_FIELD(orig_log, unsigned long, jcsj);
    TABLE_FIELD(orig_log, unsigned int, cljip);
#ifdef USE_LYLX
    TABLE_FIELD(orig_log, unsigned int, lylx);
#endif
    TABLE_FIELD(orig_log, unsigned int, fwqip);
    TABLE_FIELD(orig_log, unsigned int, fwqdk);
    TABLE_FIELD(orig_log, unsigned int, khdip);
    TABLE_FIELD(orig_log, unsigned int, khddk);
    TABLE_FIELD(orig_log, char, rzdbxx[128]);
    TABLE_FIELD(orig_log, unsigned int, bs);
    TABLE_FIELD(orig_log, unsigned long, zjs);
    TABLE_FIELD(orig_log, unsigned int, gjlx);
    TABLE_FIELD(orig_log, unsigned int, bak1);
    TABLE_FIELD(orig_log, unsigned int, bak2);
    TABLE_FIELD(orig_log, unsigned int, bak3);
    TABLE_FIELD(orig_log, unsigned int, bak4);
    TABLE_FIELD(orig_log, unsigned int, bak5);
    m.table_names = TABLE_NAMES(orig_log);
    m.table_types = TABLE_TYPES(orig_log);
    m.table_cnr = TABLE_NR(orig_log);
    m.cnr_accept = TABLE_NR_ACCEPT(orig_log);

    sem_init(&g_exit_sem, 0, 0);
    
    /* setup signals */
    err = __init_signal();
    if (err) {
        hvfs_err(lib, "Init signals failed w/ %d\n", err);
        return err;
    }

    /* get env */
    value = getenv("config");
    if (value) {
        conf_file = strdup(value);
    }
    value = getenv("ipdb");
    if (value) {
        ipdb_file = strdup(value);
    }
    value = getenv("ignoreact");
    if (value) {
        g_sc.ignoreact = atoi(value) > 0 ? 1 : 0;
    }

    /* get args */
    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch (opt) {
        case 'd':
            g_id = atoi(optarg);
            break;
        case 'r':
            g_round = atoi(optarg);
            break;
        case 'a':
            action = strdup(optarg);
            break;
        case 's':
            g_last_offset = offset = atol(optarg);
            break;
        case 'i':
            g_input = data_file = strdup(optarg);
            break;
        case 'o':
            g_output = huadan_file = strdup(optarg);
            break;
        case 'x':
            log_file = strdup(optarg);
            break;
        case 'f':
            is_fork = 1;
            break;
        case 'm':
            mode = atoi(optarg); /* != 0 means append, 0 means trunc */
            break;
        case 'c':
            if (conf_file)
                xfree(conf_file);
            conf_file = strdup(optarg);
            break;
        case 'b':
            if (ipdb_file)
                xfree(ipdb_file);
            ipdb_file = strdup(optarg);
            break;
        case 'A':
            g_sc.ignoreact = atoi(optarg) > 0 ? 1 : 0;
            break;
        case 'D':
            db = atoi(optarg);
            break;
        case 'I':
            g_interval = atoi(optarg);
            break;
        case 'v':
            g_output_format = OUTPUT_CSV;
            break;
        case 'p':
            g_pipeline = 1;
            if (optarg)
                g_pnr = atoi(optarg);
            break;
        case 'M':
            g_pp_sr = 1;
            break;
        case 'F':
            g_parse_byfd = 1;
            if (optarg)
                g_fast_bsize = atoi(optarg);
            break;
        case 'h':
        case '?':
            do_help();
            return EINVAL;
        case 'l':
            g_local = 1;
            break;
        case 'g':
            hvfs_lib_tracing_flags = 0xffffffff;
            break;
        default:
            hvfs_err(lib, "Invalid arguments!\n");
            return EINVAL;
        }
    };

    if (log_file) {
        /* close stdout */
        close(1);
        int fd = open(log_file, O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            hvfs_err(lib, "open() failed w/ %s\n", strerror(errno));
            return EINVAL;
        }
        if (fd != 1) {
            hvfs_err(lib, "log file open failed != 1\n");
            return EINVAL;
        }
    }
    
    if (!is_action_valid(action)) {
        hvfs_err(lib, "Invalid action '%s'\n", action);
        return EINVAL;
    }
    if (!data_file && (g_action & (ACTION_POP_HUADAN | ACTION_PEEK_HUADAN))) {
        /* it is ok */
        ;
    } else if (!huadan_file && (g_action & ACTION_LOAD_DATA)) {
        /* it is ok */
        ;
    } else if (!data_file || !huadan_file || !conf_file || !ipdb_file) {
        hvfs_err(lib, "Please set input/output/config/ipdb files in ARGV.\n");
        return EINVAL;
    }

    /* get hostname */
    err = gethostname(hostname, 128);
    if (err) {
        hvfs_err(lib, "gethostname() failed w/ %s\n", strerror(errno));
        return EINVAL;
    }
    if (g_output) {
        char *tmp = xzalloc(1024);
        if (!tmp)
            return ENOMEM;
        sprintf(tmp, "%s-%s", g_output, hostname);
        g_output = tmp;
    }

    if (g_id != 0 && g_local) {
        hvfs_warning(lib, "When argument 'local' is set, you have to "
                     "keep client ID to ZERO.\n");
        return EINVAL;
    }

    hvfs_info(lib, "Self ID: %d\n", g_id);
    hvfs_info(lib, "Round  : %d\n", g_round);
    hvfs_info(lib, "ACTION : %s\n", action);

    /* open ipdb file */
    dbfp = fopen(ipdb_file, "rb");
    if (dbfp == NULL) {
        hvfs_err(lib, "fopen() ipdb file '%s' failed w/ %s\n",
                 ipdb_file, strerror(errno));
        return EINVAL;
    }

    /* init server ring */
    err = init_server_ring(conf_file, &server_ring);
    if (err) {
        hvfs_err(lib, "init_server_ring() failed w/ %d\n", err);
        return err;
    }

    xg = __get_active_site(&server_ring);
    if (!xg) {
        hvfs_err(lib, "get_active_site() failed, no memory\n");
        return ENOMEM;
    }
    xnet_group_sort(xg);

    for (i = 0; i < xg->asize; i++) {
        hvfs_info(lib, "Connect to Server[%ld] %s:%d ... ", 
                  xg->sites[i].site_id, xg->sites[i].node, xg->sites[i].port);
        c = redisConnectWithTimeout(xg->sites[i].node, xg->sites[i].port, timeout);
        if (c->err) {
            hvfs_plain(lib, "failed w/ %d\n", c->err);
            return c->err;
        } else {
            hvfs_plain(lib, "ok.\n");
        }
        xg->sites[i].context = c;

        /* alloc the pipeline buffer */
        xg->sites[i].private = __alloc_pipeline();
        if (!xg->sites[i].private) {
            hvfs_err(lib, "xzalloc pipeline buffer failed!\n");
            return ENOMEM;
        }
    }
    __update_server_ring(&server_ring, xg);
    hvfs_info(lib, "Server connections has established!\n");

    /* setup pipeline threads */
    if (setup_sr_threads(xg->asize) < 0) {
        hvfs_err(lib, "Setup sr threads failed!");
        return EINVAL;
    }

    /* setup timer thread */
    if (setup_timers(g_interval) < 0) {
        hvfs_err(lib, "Setup timers and thread failed!");
        return EINVAL;
    }
    
    if (g_action & ACTION_LOAD_DATA) {
        err = check_conflict(g_input, getpid());
        if (err) {
            hvfs_err(lib, "Processes conflicts on file %s\n", data_file);
            return EINVAL;
        }
    } else if (g_action & (ACTION_PEEK_HUADAN | ACTION_POP_HUADAN)) {
        err = check_conflict(g_output, getpid());
        if (err) {
            hvfs_err(lib, "Processes conflicts on file %s\n", g_output);
            return EINVAL;
        }
    }

    if (mode) {
        /* append mode */
        huadan_fd = open(huadan_file, O_WRONLY | O_TRUNC | O_CREAT, S_IRUSR | S_IWUSR);
    } else {
        /* trunc mode */
        huadan_fd = open(huadan_file, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
    }
    if (huadan_fd < 0) {
        hvfs_err(lib, "open file '%s' failed w/ %s\n",
                 huadan_file, strerror(errno));
        goto out_disconnect;
    }
    hvfs_info(lib, "Open HUADAN file '%s' in %s mode\n", huadan_file, 
              mode ? "O_APPEND" : "O_TRUNC");

    /* which db should i use? */
    err = use_db(db);
    if (err) {
        hvfs_err(lib, "USE DB %d failed w/ %d\n", db, err);
        goto out_disconnect;
    } else {
        hvfs_info(lib, "USE DB %d OK\n", db);
    }
    
    if (g_action == ACTION_NOOP) {
    } else {
        if (g_action & ACTION_LOAD_DATA) {
            do_load_data(data_file, offset);
        }
        if (g_action & ACTION_PEEK_HUADAN) {
            do_peek_huadan();
        }
        if (g_action & ACTION_POP_HUADAN) {
            do_pop_huadan();
        }
        if (g_action & ACTION_TEST) {
            do_test();
        }
    }

    hvfs_info(lib, "Address Translation Stats:\n");
    for (i = 0; i < xg->asize; i++) {
        hvfs_info(lib, " Server[%ld]: %ld\n", xg->sites[i].site_id, xg->sites[i].nr);
    }

    if (g_action & ACTION_POP_HUADAN)
        close(huadan_fd);
    fclose(dbfp);

    /* update the file length now */
    switch (g_action & (ACTION_LOAD_DATA | ACTION_POP_HUADAN | ACTION_PEEK_HUADAN)) {
    case ACTION_LOAD_DATA:
        break;
    case ACTION_POP_HUADAN:
        g_doffset = -1;
        break;
    case ACTION_PEEK_HUADAN:
        g_doffset = -2;
        break;
    default:
        g_doffset = -3;
    }

    if (g_doffset >= 0) {
        if (c_set_file_size(data_file, g_last_offset)) {
            /* last offset write failed, do not report to parent process! */
            return -1;
        }
    }

    /* it is safe to exit the timer thread */
    g_timer_thread_stop = 1;
    if (g_timer_thread) {
        sem_post(&g_timer_sem);
        pthread_join(g_timer_thread, NULL);
    }
    
    if (is_fork) {
        /* write the g_doffset to stdin */
        int bw, bl = 0, err;
        struct timespec ts;
        
        do {
            bw = write(0, ((void *)&g_doffset) + bl, sizeof(g_doffset) - bl);
            if (bw < 0) {
                hvfs_err(lib, "write offset back to parent process failed w/ %s\n",
                         strerror(errno));
                break;
            }
            bl += bw;
        } while (bl < sizeof(g_doffset));
        hvfs_info(lib, "Write offset %ld back to parent process done.\n",
                  g_doffset);
        fsync(0);

        /* wait for the signal to exit */
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 10;
        do {
            err = sem_timedwait(&g_exit_sem, &ts);
            if (err < 0) {
                if (errno == EINTR)
                    continue;
                hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
                break;
            }
            break;
        } while (1);
    }

    if (!g_parse_byfd) {
        if (g_dfp)
            fclose(g_dfp);
    } else {
        if (g_dfd)
            close(g_dfd);
    }

    /* clean the huadan file */
    if (!pop_huadan_nr && !peek_huadan_nr) {
        unlink(huadan_file);
    }

    hvfs_info(lib, "+Success to OFFSET %ld \n"
              "Stream Line Stat NRs => "
              "Process %ld (Cancel %ld), Corrupt %ld, Ignore %ld, "
              "Huadan {pop %ld {pop %ld, flush %ld}, peek %ld} \n"
              "NewStream %ld MaxSuffix %d\n",
              g_doffset,
              process_nr, atomic64_read(&canceled), corrupt_nr, ignore_nr, 
              pop_huadan_nr, atomic64_read(&poped), flushed, peek_huadan_nr,
              newstream_nr, g_suffix_max);

out_disconnect:
    if (g_action & ACTION_LOAD_DATA)
        remove_conflict(g_input);
    else if (g_action & (ACTION_PEEK_HUADAN | ACTION_POP_HUADAN))
        remove_conflict(g_output);
    fina_xg(xg);

    return 0;
}
