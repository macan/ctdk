/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2012-12-03 09:42:08 macan>
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
#define HUADAN_FSYNC_NR         1000
int huadan_nr = 0;

char *printStreamid(struct streamid *id)
{
    char *str;

    str = malloc(1024);
    if (!str) {
        printf("malloc() str buffer failed\n");
        return NULL;
    }
    sprintf(str, "STREAM:%d:%d:%d:%d:%d %s",
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
    sprintf(str, "1\t%d\t%s\t%s\t%ld\t%s\t%d\t%s\t%d\t%s\t%d\t%ld\t%d\t%ld\t%s",
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
            "GEO_INFO");

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

/* Return value: 0 => OK and updated; -1 => Internal Err; 1 => not updated
 */
int addOrUpdateStream(struct streamid *id)
{
    char key[128];
    redisContext *c;
    redisReply *reply = NULL;
    int err = 0;

#define STREAM_HEAD     "HUPDBY %s DON %d "
#define INB_STREAM      STREAM_HEAD "izjs %d igjlx %d ijlsj %ld ijcsj %ld icljip %d ibs %d"
#define OUT_STREAM      STREAM_HEAD "ozjs %d ogjlx %d ojlsj %ld ojcsj %ld ocljip %d obs %d"

    /* for HUPDBY command, we accept >4 arguments */
    snprintf(key, 127, "STREAM:%d:%d:%d:%d:%d",
             id->fwqip,
             id->fwqdk,
             id->khdip,
             id->khddk,
             id->protocol
        );
    c = get_server_from_key(key);

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
        char *str;

        str = printStreamid(id);
        /* the update flag */
        switch (reply->integer) {
        case CANCEL:
        {
            hvfs_info(lib, "CANCEL record {%s}\n", str);
            break;
        }
        case INB_INIT:
        case OUT_INIT:
            hvfs_info(lib, "INIT STREAM {%s}\n", str);
            break;
        case INB_UPDATED:
        case OUT_UPDATED:
            hvfs_info(lib, "UPDATE STREAM {%s}\n", str);
            break;
        case INB_CLOSED:
        case OUT_CLOSED:
            hvfs_info(lib, "Half CLOSE STREAM {%s}\n", str);
            break;
        case ALL_CLOSED:
            hvfs_info(lib, "FULLY CLOSE STREAM {%s}\n", str);
            pop_huadan(id);
            break;
        case INB_IGNORE:
        case OUT_IGNORE:
            hvfs_info(lib, "IGNORE record {%s}\n", str);
            break;
        case TIMED_IGNORE:
            /* new stream? */
            hvfs_info(lib, "NEW STREAM {%s}\n", str);
            new_stream(id);
            break;
        }
    } else {
        hvfs_info(lib, "%s", reply->str);
    }
    freeReplyObject(reply);

    return err;
}

int getStreamStat(struct streamid *id, struct streamstat *stat)
{
    char key[128];
    redisContext *c;
    redisReply *reply;
    unsigned long begin, end;
    int err = 0;

    memset(stat, 0, sizeof(*stat));

    snprintf(key, 127, "STREAM:%d:%d:%d:%d:%d",
             id->fwqip,
             id->fwqdk,
             id->khdip,
             id->khddk,
             id->protocol
        );
    c = get_server_from_key(key);

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

    /* remove the entry now */
    reply = redisCommand(c, "DEL %s", key);
    if (reply->type == REDIS_REPLY_ERROR) {
        hvfs_err(lib, "DEL %s => %s\n", key, reply->str);
        freeReplyObject(reply);
        return -1;
    }
    freeReplyObject(reply);

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
    begin = min(stat->ijcks, stat->ojcks);
    end = max(stat->ijcsj, stat->ojcsj);

    hd->qssj = min(stat->ijlks, stat->ojlks);
    hd->jssj = max(stat->ijlsj, stat->ojlsj);
    hd->cxsj = end - begin;

    hd->in_bs = stat->ibs;
    hd->out_bs = stat->obs;
    hd->in_zjs = stat->izjs;
    hd->out_zjs = stat->ozjs;
}

int pop_huadan(struct streamid *id)
{
    struct streamstat stat;
    struct huadan hd;
    char *str;
    int err = 0, len, bl, bw;

    memset(&stat, 0, sizeof(stat));
    memset(&hd, 0, sizeof(hd));
    
    err = getStreamStat(id, &stat);
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

    huadan_nr++;
    if (huadan_nr % HUADAN_FSYNC_NR == 0)
        fsync(huadan_fd);

    return 0;
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
    int i, EOS = 0;

    for (i = 0; i < m->table_cnr; i++) {
        hvfs_info(lib, "%s = %s\n", m->table_names[i], cv[i]);
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
                q = strtok(p, ";\n");
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
                }
            } while (p = NULL, 1);
        }
        UPDATE_FIELD(id, i, 10, bs, atoi, cv);
        UPDATE_FIELD(id, i, 11, zjs, atol, cv);
        UPDATE_FIELD(id, i, 12, gjlx, atoi, cv);
    }

    p = printStreamid(&id);
    hvfs_info(lib, " => %s\n", p); free(p);

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

int main(int argc, char *argv[]) {
    struct timeval begin, end;
    redisContext *c;
    redisReply *reply;
    char *str, *conf_file, *data_file, *huadan_file, *value;
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

    /* add or update a stream statis entry */
    struct streamid stream = {
        .fwqip = 10812,
        .fwqdk = 10009,
        .khdip = 20802,
        .khddk = 2000,
        .direction = 1,
        .protocol = 1,

        .jlsj = 800,
        .jcsj = 701,
        .cljip = 30002,
        .bs = 800,
        .zjs = 3800,
        .gjlx = 8,
    };
    struct streamstat stat;

    gettimeofday(&begin, NULL);
    for (i = 0; i < 10; i++) {
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
            printf("+ %s DON %d\n", str, stream.direction), free(str);
            err = addOrUpdateStream(&stream);
        }
    }
    gettimeofday(&end, NULL);
    printf("+ err %d\n", err); free(str);
    printf("P %.2lf\n", 10000 / ((end.tv_sec + end.tv_usec / 1000000.0) - 
                                   (begin.tv_sec + begin.tv_usec / 1000000.0)));
    stream.fwqip = 10813;
    getStreamStat(&stream, &stat);
    str = printStreamStat(&stream, &stat);
    printf("= %s\n", str); free(str);

    for (i = 0; i < xg->asize; i++) {
        hvfs_info(lib, "Server[%ld]: %ld\n", xg->sites[i].site_id, xg->sites[i].nr);
    }

    redisFree(c);

    close(huadan_fd);

    return 0;
}
