/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2012-12-04 17:22:04 macan>
 *
 */

#include "common.h"

struct manager
{
    u64 client_id;
    int table_cnr;              /* # of columns */
    char **table_names;
    char **table_types;
};

struct chring server_ring;
struct xnet_group *xg = NULL;
struct manager m;
int huadan_fd = 1;
FILE *dbfp = NULL;
#define HUADAN_FSYNC_NR         1000
long pop_huadan_nr = 0;
long peek_huadan_nr = 0;
long process_nr = 0;
long ignore_nr = 0;
long poped = 0;
long flushed = 0;
int g_id = -1;
int g_flush = 0;

static struct stream_config g_sc = {
    .ignoreact = 1,
};

#define GEN_HASH_KEY(key, id) do {                          \
        snprintf(key, 127, "S:%u:%u:%u:%u:%u",              \
                 id->fwqip,                                 \
                 id->fwqdk,                                 \
                 id->khdip,                                 \
                 id->khddk,                                 \
                 id->protocol                               \
            );                                              \
    } while (0)

#define GEN_FULL_KEY(key, id, suffix) do {                  \
        if (unlikely(suffix > 0)) {                         \
            snprintf(key + strlen(key), 127, "+%d",         \
                     suffix                                 \
                );                                          \
        }                                                   \
    } while (0)

char *printStreamid(struct streamid *id)
{
    char *str;

    str = malloc(1024);
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
               ((id->direction & STREAM_OUTNIL) ? "OUTNIL" : "XXXXXX")))));

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

    str = malloc(1024);
    if (!str) {
        hvfs_err(lib, "malloc() str buffer failed\n");
        return NULL;
    }
    tmp1 = localtime(&hd->qssj);
    if (!tmp1) {
        hvfs_err(lib, "localtime() failed w/ %s\n", strerror(errno));
        goto free;
    }
    if (strftime(tstr1, sizeof(tstr1), "%Y-%m-%d %H:%M:%S", tmp1) == 0) {
        hvfs_err(lib, "strftime() failed w/ %s\n", strerror(errno));
        goto free;
    }
    tmp2 = localtime(&hd->jssj);
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

char *printHuadanLine(struct huadan *hd)
{
    char *str = printHuadan(hd);

    if (str) {
        strcat(str, "\n");
    }

    return str;
}

static inline
redisContext *get_server_from_key(char *key)
{
    struct chp *p;
    struct xnet_group_entry *e;

    if (!xg) {
        hvfs_err(lib, "Server group has not been initialized.\n");
        return NULL;
    }
    p = ring_get_point(key, &server_ring);
    hvfs_debug(lib, "KEY %s => Server %s:%d\n", key, p->node, p->port);
    e = find_site(xg, p->site_id);
    if (!e) {
        hvfs_err(lib, "Server %ld doesn't exist.\n", p->site_id);
        return NULL;
    }
    e->nr++;

    return e->context;
}

int selectNextDB(redisContext *c, int current)
{
    redisReply *reply;
    int nextDB = DB_STREAM_BACKUP;
    int err = 0;

    if (current == DB_HUADAN)
        return -1;
    if (current >= DB_STREAM_BACKUP)
        nextDB = current + 1;
    if (nextDB > DB_STREAM_BACKUP_MAX) {
        hvfs_err(lib, "select new DB %d exceed max valid DBs(16).\n", current);
        return -1;
    }
    reply = redisCommand(c, "SELECT %d", nextDB);
    if (reply->type == REDIS_REPLY_STRING) {
        if (strncmp(reply->str, "+OK", 3) != 0) {
            err = -1;
            goto out;
        }
    }
out:
    return err;
}

int selectDB(redisContext *c, int db)
{
    redisReply *reply;
    int err = 0;

    if (db > DB_STREAM_BACKUP_MAX) {
        hvfs_err(lib, "select new DB %d exceed max valid DBs(16).\n", db);
        return -1;
    }
    reply = redisCommand(c, "SELECT %d", db);
    if (reply->type == REDIS_REPLY_STRING) {
        if (strncmp(reply->str, "+OK", 3) != 0) {
            err = -1;
            goto out;
        }
    }
out:
    return err;
}

int new_stream(struct streamid *id, int suffix);

/* Return value: 0 => OK and updated; -1 => Internal Err; 1 => not updated
 */
int addOrUpdateStream(struct streamid *id, int suffix)
{
    char key[128];
    redisContext *c;
    redisReply *reply = NULL;
    int err = 0;

#define STREAM_HEAD     "HUPDBY %s DON %d "
#define INB_STREAM      STREAM_HEAD "izjs %d igjlx %d ijlsj %ld ijcsj %ld icljip %d ibs %d"
#define OUT_STREAM      STREAM_HEAD "ozjs %d ogjlx %d ojlsj %ld ojcsj %ld ocljip %d obs %d"

    /* for HUPDBY command, we accept >4 arguments */
    GEN_HASH_KEY(key, id);
    
    c = get_server_from_key(key);

    GEN_FULL_KEY(key, id, suffix);

    if (id->direction & STREAM_IN ||
        id->direction & STREAM_INNIL)
        reply = redisCommand(c, INB_STREAM,
                             key,
                             id->direction,
                             id->zjs, /* if id->zjs > current value, then do
                                       * update, otherwise, do not update */
                             id->gjlx, /* tool type is ORed */
                             id->jlsj,
                             id->jcsj,
                             id->cljip,
                             id->bs);
    else if (id->direction & STREAM_OUT||
             id->direction & STREAM_OUTNIL)
        reply = redisCommand(c, OUT_STREAM,
                             key,
                             id->direction,
                             id->zjs, /* if id->zjs > current value, then do
                                       * update, otherwise, do not update */
                             id->gjlx, /* tool type is ORed */
                             id->jlsj,
                             id->jcsj,
                             id->cljip,
                             id->bs);
    else
        return -1;

    if (reply->type == REDIS_REPLY_INTEGER) {
        /* the update flag */
        switch (reply->integer) {
        case CANCEL:
        {
            hvfs_debug(lib, "CANCEL record\n");
            break;
        }
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
            poped++;
            pop_huadan(id, suffix);
            break;
        case INB_IGNORE:
        case OUT_IGNORE:
            hvfs_debug(lib, "IGNORE record\n");
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

    return err;
}

int new_stream(struct streamid *id, int suffix)
{
    char str[128];
    suffix += 1;

    GEN_HASH_KEY(str, id);
    GEN_FULL_KEY(str, id, suffix);
    hvfs_info(lib, "New -> %s\n", str);
    
    /* try next stream until 1000 */
    if (suffix >= 1000) {
        return EINVAL;
    }

    return addOrUpdateStream(id, suffix);
}

/* rename suffix + 1 to suffix until there is no (suffix + 1) key */
int rename_stream(struct streamid *id, int suffix)
{
    char from_key[128], to_key[128];
    redisContext *c;
    redisReply *reply;

    GEN_HASH_KEY(from_key, id);
    GEN_HASH_KEY(to_key, id);

    c = get_server_from_key(from_key);

    GEN_FULL_KEY(from_key, id, suffix + 1);
    GEN_FULL_KEY(to_key, id, suffix);

    reply = redisCommand(c, "RENAME %s %s", from_key, to_key);
    if (reply->type == REDIS_REPLY_ERROR) {
        /* ok, we can safely stop here */
        freeReplyObject(reply);
        return 0;
    } else {
        freeReplyObject(reply);
        hvfs_info(lib, "RENAME from %s to %s\n", from_key, to_key);
        return rename_stream(id, suffix + 1);
    }
}

/* pop: 0 => peek, 1 => pop
 */
int getStreamStat(struct streamid *id, int suffix, struct streamstat *stat, int pop)
{
    char key[128];
    redisContext *c;
    redisReply *reply;
    unsigned long begin, end;
    int err = 0;

    memset(stat, 0, sizeof(*stat));

    GEN_HASH_KEY(key, id);

    c = get_server_from_key(key);

    GEN_FULL_KEY(key, id, suffix);

    /* for HGETALL command */
    reply = redisCommand(c, "HGETALL %s", key);
    if (reply->type == REDIS_REPLY_ARRAY) {
        int j;

        for (j = 0; j < reply->elements; j += 2) {
            hvfs_debug(lib, "%s = %s\n", 
                       reply->element[j]->str, reply->element[j + 1]->str);

            if (strcmp(reply->element[j]->str, "DON") == 0) {
                stat->DON = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "igjlx") == 0) {
                stat->ogjlx = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "ijlsj") == 0) {
                stat->ijlsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ijlks") == 0) {
                stat->ijlks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ijcsj") == 0) {
                stat->ijcsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ijcks") == 0) {
                stat->ijcks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "izjs") == 0) {
                stat->izjs = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ibs") == 0) {
                stat->ibs = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "ogjlx") == 0) {
                stat->ogjlx = atoi(reply->element[j + 1]->str);
            } else if (strcmp(reply->element[j]->str, "ojlsj") == 0) {
                stat->ojlsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ojlks") == 0) {
                stat->ojlks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ojcsj") == 0) {
                stat->ojcsj = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ojcks") == 0) {
                stat->ojcks = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "ozjs") == 0) {
                stat->ozjs = strtoul(reply->element[j + 1]->str, NULL, 10);
            } else if (strcmp(reply->element[j]->str, "obs") == 0) {
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
        if (reply->type == REDIS_REPLY_ERROR) {
            hvfs_err(lib, "DEL %s => %s\n", key, reply->str);
            freeReplyObject(reply);
            return -1;
        }
        freeReplyObject(reply);
    }

    /* try to rename same five tuple streams */
    if (pop && !g_flush)
        rename_stream(id, suffix);

    return 0;
}

void generate_huadan(struct streamid *id, struct streamstat *stat, struct huadan *hd)
{
    unsigned long begin, end;

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
    if (stat->DON & STREAM_INNIL ||
        stat->DON & STREAM_OUTNIL) {
        /* have detect a NIL packet, based on jcsj */
        if (stat->ijcks == 0)
            begin = stat->ojcks;
        else if (stat->ojcks == 0)
            begin = stat->ijcks;
        else
            begin = min(stat->ijcks, stat->ojcks);
        end = max(stat->ijcsj, stat->ojcsj);
        
        hd->cxsj = end - begin + 1;
    } else {
        /* no NIL packet, based on jlsj */
        hd->cxsj = hd->jssj - hd->qssj + 1;
    }

    hd->in_bs = stat->ibs;
    hd->out_bs = stat->obs;
    hd->in_zjs = stat->izjs;
    hd->out_zjs = stat->ozjs;
}

int pop_huadan(struct streamid *id, int suffix)
{
    struct streamstat stat;
    struct huadan hd;
    char *str;
    int err = 0, len, bl, bw;

    memset(&stat, 0, sizeof(stat));
    memset(&hd, 0, sizeof(hd));
    
    err = getStreamStat(id, suffix, &stat, 1);
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
    
    err = getStreamStat(id, suffix, &stat, 0);
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
    if (reply->type == REDIS_REPLY_ARRAY) {
        int j;

        for (j = 0; j < reply->elements; j++) {
            suffix = parse_streamid(reply->element[j]->str, &id);
            flushed++;
            err = pop_huadan(&id, suffix);
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

    /* FIXME: send an inc to REDIS:0 */
    g_flush = 1;
    for (i = 0; i < xg->asize; i++) {
        c = xg->sites[i].context;
        pop_stream(c);
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
        }
    }
    hvfs_info(lib, "Total Server number is %d\n", j);
    ring_resort_nolock(ring);
    ring_stat(ring, j);

out:
    return err;
}

#define UPDATE_FIELD(stream, iter, idx, name, func, str) do {   \
        if (iter == idx) {                                      \
            stream.name = func(str[iter]);                      \
            continue;                                           \
        }                                                       \
    } while (0)

int process_table(void *data, int cnt, const char **cv)
{
    struct manager *m = (struct manager *)data;
    struct streamid id = {0,};
    char *p, *q;
    int i, EOS = 0, isact = 0, isfrg = 0;

    for (i = 0; i < m->table_cnr; i++) {
        hvfs_debug(lib, "%s = %s\n", m->table_names[i], cv[i]);
        UPDATE_FIELD(id, i, 1, jlsj, atol, cv);
        UPDATE_FIELD(id, i, 2, jcsj, atol, cv);
        UPDATE_FIELD(id, i, 3, cljip, atoi, cv);
        UPDATE_FIELD(id, i, 5, fwqip, atoi, cv);
        UPDATE_FIELD(id, i, 6, fwqdk, atoi, cv);
        UPDATE_FIELD(id, i, 7, khdip, atoi, cv);
        UPDATE_FIELD(id, i, 8, khddk, atoi, cv);
        if (i == 9) {
            /* parse the string */
            p = (char *)cv[i];
            do {
                q = strtok(p, ";,\n");
                if (!q) {
                    break;
                } else if (strcmp(q, "INB") == 0) {
                    id.direction = STREAM_IN;
                } else if (strcmp(q, "OUT") == 0) {
                    id.direction = STREAM_OUT;
                } else if (strcmp(q, "UDA") == 0) {
                    id.protocol = STREAM_UDP;
                } else if (strcmp(q, "TDA") == 0) {
                    id.protocol = STREAM_TCP;
                } else if (strcmp(q, "NIL") == 0) {
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
        UPDATE_FIELD(id, i, 10, bs, atoi, cv);
        UPDATE_FIELD(id, i, 11, zjs, atol, cv);
        UPDATE_FIELD(id, i, 12, gjlx, atoi, cv);
    }

    if (g_sc.ignoreact) {
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

    if (EOS) {
        id.direction = (id.direction << 2) & 0xf;
    }
    
    p = printStreamid(&id);
    hvfs_debug(lib, " => %s\n", p); free(p);

    addOrUpdateStream(&id, 0);
    process_nr++;

    return 0;
}

int parse_csv_file(char *filepath)
{
    FILE *fp;
    int err = 0;

    if ((fp = fopen(filepath, "r")) == NULL) {
        hvfs_err(lib, "Cannot open input file '%s'\n", filepath);
        return errno;
    }

    switch (csv_parse(fp, process_table, &m)) {
    case E_LINE_TOO_WIDE:
        hvfs_err(lib, "Error parsing csv: line too wide.\n");
        break;
    case E_QUOTED_STRING:
        hvfs_err(lib, "Error parsing csv: ill-formatted quoted string.\n");
        break;
    }
    
    fclose(fp);

    return err;
}

void fina_xg(struct xnet_group *xg)
{
    int i;

    for (i = 0; i < xg->asize; i++) {
        redisFree(xg->sites[i].context);
    }
    
    xfree(xg);
}

int main(int argc, char *argv[]) {
    struct timeval begin, end;
    redisContext *c;
    redisReply *reply;
    char *str, *conf_file, *data_file, *huadan_file, *ipdb_file, *value;
    int err = 0, i, j, id = -1;

    struct timeval timeout = { 10, 500000 }; // 10.5 seconds
    TABLE_DEF(orig_log, 18);
    TABLE_FIELD(orig_log, int, rzlx);
    TABLE_FIELD(orig_log, unsigned long, jlsj);
    TABLE_FIELD(orig_log, unsigned long, jcsj);
    TABLE_FIELD(orig_log, unsigned int, cljip);
    TABLE_FIELD(orig_log, unsigned int, lylx);
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

    if (argc >= 2) {
        id = atoi(argv[1]);
    }
    g_id = id;
    hvfs_info(lib, "Self ID: %d\n", id);
    
    value = getenv("config");
    if (value) {
        conf_file = strdup(value);
    } else {
        hvfs_err(lib, "Please set config=/path/to/config/file in ENV.\n");
        return EINVAL;
    }
    value = getenv("data");
    if (value) {
        data_file = strdup(value);
    } else {
        hvfs_err(lib, "Please set data=/path/to/data/file in ENV.\n");
        return EINVAL;
    }
    value = getenv("huadan");
    if (value) {
        huadan_file = strdup(value);
    } else {
        hvfs_err(lib, "Please set huadan=/path/to/huadan/file in ENV.\n");
        return EINVAL;
    }
    value = getenv("ipdb");
    if (value) {
        ipdb_file = strdup(value);
    } else {
        hvfs_err(lib, "Please set ipdb=/path/to/ip/db in ENV.\n");
        return EINVAL;
    }
    value = getenv("ignoreact");
    if (value) {
        g_sc.ignoreact = atoi(value) > 0 ? 1 : 0;
    }

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
    }
    hvfs_info(lib, "Server connections has established!\n");

    huadan_fd = open(huadan_file, O_WRONLY | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR);
    if (huadan_fd < 0) {
        hvfs_err(lib, "open file '%s' failed w/ %s\n",
                 huadan_file, strerror(errno));
        return errno;
    }
    hvfs_info(lib, "Open HUADAN file '%s' in APPEND mode\n", huadan_file);

    err = parse_csv_file(data_file);

    goto out;
    
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
    printf("P %.2lf\n", 1000000 / ((end.tv_sec + end.tv_usec / 1000000.0) - 
                                   (begin.tv_sec + begin.tv_usec / 1000000.0)));
    stream.fwqip = 10813;
    getStreamStat(&stream, 0, &stat, 1);
    str = printStreamStat(&stream, &stat);
    printf("= %s\n", str); free(str);

    for (i = 0; i < xg->asize; i++) {
        hvfs_info(lib, "Server[%ld]: %ld\n", xg->sites[i].site_id, xg->sites[i].nr);
    }

out:
    pop_all_stream();
    close(huadan_fd);
    fclose(dbfp);
    hvfs_info(lib, "+Success. \n"
              "Stream Line Stat NRs => "
              "Process %ld, Ignore %ld, "
              "Huadan {pop %ld {pop %ld, flush %ld}, peek %ld} \n",
              process_nr, ignore_nr, pop_huadan_nr, poped, flushed, peek_huadan_nr);
    fina_xg(xg);

    return 0;
}
