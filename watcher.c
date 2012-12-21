/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2012-12-20 17:19:27 macan>
 *
 */

#define HVFS_TRACING

#include <sys/wait.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <getopt.h>
#include "hiredis.h"
#include "lib/ring.h"
#include "lib/csv_parser.h"
#include <inotifytools/inotifytools.h>
#include <inotifytools/inotify.h>
#include <dirent.h>
#include <libgen.h>

HVFS_TRACING_INIT();

#define EPOLL_QUEUE_LEN         4096
#define EPOLL_CHECK             64

struct watcher_config
{
#define SCAN_INTERVAL           30
    int scan_interval;
};

struct watcher_config g_config = {
    .scan_interval = SCAN_INTERVAL,
};

static LIST_HEAD(jobs);
static pthread_t g_timer_thread = 0;
static pthread_t g_event_thread = 0;
static pthread_t g_poll_thread = 0;
static sem_t g_timer_sem;
static sem_t g_main_sem;
extern char **environ;
static char **environ_saved;
static char *g_dir = NULL;
static char *server = "127.0.0.1";
static char hostname[128];
static xlock_t job_lock;
static xlock_t g_huadan_lock;
static long g_huadan = 0;
static long g_jobid = 0;
static long g_propose = -1;
static long g_poped = 0;
static int g_id = -1;
static int port = 9087;
static int g_epfd = 0;
static int g_current_db = 3;
static int g_timer_thread_stop = 0;
static int g_main_thread_stop = 0;
static int g_poll_thread_stop = 0;
static unsigned int g_flags = 0;
static int g_has_proposed = 0;
static int g_should_clr = 0;
static int g_no_propose = 0;
static int g_mode = 0;

#define NEXT_JOBID() (++g_jobid)

/* defines for g_poped */
#define G_POPED_NOOP    0x00
#define G_POPED_LOAD    0x01
#define G_POPED_POP     0x02
#define G_POPED_MAX     0x10

struct job_entry
{
    struct list_head list;
    char *filename;
    long offset;
    long jobid;
    long date;
    pid_t pid;
    int fd;                     /* 0 for read, 1 for write */
};

struct job_info
{
    char *name;
    char *env;
    char *cmd;
    char *config;               /* config file */
    char *ipdb;                 /* ipdb file */
    char *output;               /* output file */
    char *log;                  /* log file */
    char *action;               /* which action should we do */
    int ignoreact;

    int rid;
    int cid;

    /* properities */
    int REG_NUM;                /* # of registers */
    int CLI_NUM;                /* # of slice files */
    int DATE_COL;               /* index of date column */
    int REG_COL;                /* index of register column */
    int CLI_COL;                /* index of client column */
};

static struct job_info job_infos[] = {
    {"ctdk_1d_normal", "LD_LIBRARY_PATH=lib", "ctdk_huadan_1d", 
     "conf/cloud.conf", "conf/qqwry.dat", "data/huadan-%s-%d",
     "logs/CTDK-CLI-LOG-%s-r%d-c%d-off%ld-jid%ld", "load", 0, 0, 0, 2, 10, 10, 13, 14,},
    {"ctdk_1d_pop", "LD_LIBRARY_PATH=lib", "ctdk_huadan_1d", 
     "conf/cloud.conf", "conf/qqwry.dat", "data/huadan-%s",
     "logs/CTDK-CLI-LOG-%s-r%d-c%d-FINAL-jid%ld", "pop", 0, 0, 0, 2, 10, 10, 13, 14,},
};
static struct job_info *g_sort_ji = NULL;
#define JI_1D_NORMAL    0
#define JI_1D_POP       1

int add_job(redisContext *c, char *pathname, long offset, struct job_info *ji);
struct job_entry *del_job(int fd);
void fina_job(redisContext *c, struct job_entry *job);
int watcher_poll_add(int fd);
int watcher_poll_del(int fd);

#define TOGGLE_DB_INIT          0
#define TOGGLE_DB_FLIP          1

struct file_info
{
    char *pathname;             /* file name */
    long osize;                 /* oldfile size */
    long size;                  /* file size */
};

struct file_gather
{
    int asize, psize;
    struct file_info *fis;
};

struct file_gather_all
{
    struct file_gather add, rmv;
};

void do_help()
{
    hvfs_plain(lib, "Arguments:\n\n"
               "-d, --dir         which directory you want to WATCH.\n"
               "-r, --server      Redis server ip address, default to localhost.\n"
               "-p, --port        Redis server port, default to 9087\n"
               "-i, --interval    directory scan interval, default to 30 seconds.\n"
               "-t, --instant     use IN_MODIFY, would generate a lot of evnets.\n"
               "-h, -?, -help     print this help.\n"
        );
}

static void abort_handler(int signr)
{
    int pid = getpid();
    hvfs_err(lib, "pid %d ABORTed\n", pid);
}

static inline
struct file_gather *__enlarge_fg(struct file_gather *in)
{
#define FG_DEFAULT_SIZE         (512)
    void *out = NULL;

    if (!in) {
        return NULL;
    } else if (in->psize == 0) {
        /* totally new alloc */
        out = xzalloc(FG_DEFAULT_SIZE * sizeof(struct file_info));
        if (!out) {
            hvfs_err(lib, "Failed to alloc new FG region\n");
            return NULL;
        }
        in->psize = FG_DEFAULT_SIZE;
        in->fis = out;
    } else {
        /* realloc a region */
        out = xrealloc(in->fis, (in->psize + FG_DEFAULT_SIZE) *
                       sizeof(struct file_info));
        if (!out) {
            hvfs_err(lib, "Failed to realloc new FG region\n");
            return NULL;
        }
        in->psize += FG_DEFAULT_SIZE;
        in->fis = out;
    }

    return in;
}

static inline
void __free_fg(struct file_gather *fg)
{
    if (fg->psize > 0)
        xfree(fg->fis);
}

typedef void (*__dir_iterate_func)(char *path, char *name, void *data);

void __gather_files(char *path, char *name, void *data)
{
    struct file_gather_all *fga = (struct file_gather_all *)data;
    char pathname[PATH_MAX];
    struct stat buf;

    if (!fga)
        return;
    if (fga->add.asize >= fga->add.psize) {
        void *out = __enlarge_fg(&fga->add);
        if (!out) {
            hvfs_err(lib, "enlarge FG region failed, file %s leaking\n",
                     name);
            return;
        }
    }

    /* get file length */
    sprintf(pathname, "%s/%s", path, name);
    if (stat(pathname, &buf) < 0) {
        hvfs_err(lib, "Failed to stat file %s w/ %s\n",
                 pathname, strerror(errno));
        return;
    } else {
        fga->add.fis[fga->add.asize].pathname = strdup(pathname);
        fga->add.fis[fga->add.asize].size = buf.st_size;
    }
    fga->add.asize++;
}

static inline 
int __ignore_self_parent(char *dir)
{
    if ((strcmp(dir, ".") == 0) ||
        (strcmp(dir, "..") == 0)) {
        return 0;
    }
    return 1;
}

int __dir_iterate(char *parent, char *name, __dir_iterate_func func,
                  void *data)
{
    char path[PATH_MAX];
    struct dirent entry;
    struct dirent *result;
    DIR *d;
    int err = 0;

    if (name)
        sprintf(path, "%s/%s", parent, name);
    else
        sprintf(path, "%s", parent);
    do {
        int len = strlen(path);
        
        if (len == 1)
            break;
        if (path[len - 1] == '/') {
            path[len - 1] = '\0';
        } else
            break;
    } while (1);
    
    d = opendir(path);
    if (!d) {
        hvfs_err(lib, "opendir(%s) failed w/ %s(%d)\n",
                 path, strerror(errno), errno);
        goto out;
    }

    for (err = readdir_r(d, &entry, &result);
         err == 0 && result != NULL;
         err = readdir_r(d, &entry, &result)) {
        /* ok, we should iterate over the dirs */
        if (entry.d_type == DT_DIR && __ignore_self_parent(entry.d_name)) {
            err = __dir_iterate(path, entry.d_name, func, data);
            if (err) {
                hvfs_err(lib, "Dir %s: iterate to func failed w/ %d\n",
                         entry.d_name, err);
            }
        } else if (entry.d_type == DT_REG) {
            /* call the function now */
            func(path, entry.d_name, data);
        } else if (entry.d_type == DT_UNKNOWN) {
            hvfs_warning(lib, "File %s with unknown file type?\n",
                         entry.d_name);
        }
    }
    closedir(d);

out:
    return err;
}

struct file_gather_all *full_scan(char *top_dir)
{
    char dir[PATH_MAX] = {0,}, *full_dir = NULL;
    struct file_gather_all *fga;
    int err = 0;

    /* Step 0: alloc the file info array */
    fga = xzalloc(sizeof(*fga));
    if (!fga) {
        hvfs_err(lib, "Alloc file_gather_all failed!\n");
        err = -ENOMEM;
        goto out;
    }

    /* Step 1: open the top_dir, get entries recursively */
    if (top_dir[0] != '/') {
        if (getcwd(dir, PATH_MAX) == NULL) {
            hvfs_err(lib, "getcwd() failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
        strcat(dir, "/");
        strcat(dir, top_dir);
        full_dir = dir;
    } else {
        full_dir = top_dir;
    }

    err = __dir_iterate(full_dir, NULL, __gather_files, (void *)fga);
    if (err) {
        hvfs_err(lib, "iterate directory %s failed w/ %d\n",
                 top_dir, err);
        goto out;
    }
out:
    if (err) {
        if (fga) {
            __free_fg(&fga->add);
            __free_fg(&fga->rmv);
            xfree(fga);
        }
        return ERR_PTR(err);
    }

    return fga;
}

int file_changed(redisContext *c, struct file_info *fi)
{
    redisReply *reply;
    long size = 0;

    reply = redisCommand(c, "GET FS:%s", fi->pathname);
    if (reply->type == REDIS_REPLY_NIL) {
        hvfs_warning(lib, "File %s not exist in DB, default as changed!\n",
                     fi->pathname);
    } else if (reply->type == REDIS_REPLY_STRING) {
        size = atol(reply->str);
    } else {
        hvfs_warning(lib, "Reply None? file %s default as changed!\n",
                     fi->pathname);
    }
    freeReplyObject(reply);

    if (fi->size > size) {
        fi->osize = size;
        return 1;
    } else
        return 0;
}

int set_file_size(redisContext *c, char *pathname, long size)
{
    redisReply *reply;
    long saved = size;
    int err = 0;

redo:
    reply = redisCommand(c, "GETSET FS:%s %ld", pathname, size);
    if (reply->type == REDIS_REPLY_NIL) {
        hvfs_info(lib, "Init file %s size to %ld!\n",
                  pathname, size);
    } else if (reply->type == REDIS_REPLY_STRING) {
        if (reply->str)
            saved = atol(reply->str);
    } else {
        hvfs_warning(lib, "Not an integer string reply? set failed %s\n",
                     reply->str);
        err = -EINVAL;
    }
    freeReplyObject(reply);

    if (saved > size) {
        size = saved;
        goto redo;
    }

    return err;
}

void clr_file_size(redisContext *c, char *pathname)
{
    redisReply *reply;

    reply = redisCommand(c, "DEL FS:%s", pathname);
    if (reply->type == REDIS_REPLY_INTEGER) {
        hvfs_info(lib, "DEL FS:%s\n", pathname);
    } else {
        hvfs_warning(lib, "DEL FS:%s, Not an integer reply? del failed %s\n",
                     pathname, reply->str);
    }
    freeReplyObject(reply);
}

void set_huadan_next(redisContext *c, long next)
{
    redisReply *reply;

    g_has_proposed = 1;
    
    reply = redisCommand(c, "HSET HUADAN_NEXT %s %ld", hostname, next);
    if (reply->type == REDIS_REPLY_INTEGER) {
        /* either 0 or 1 is ok */
        hvfs_info(lib, "HSET HUADAN_NEXT for %s to %ld\n", hostname, next);
    } else {
        hvfs_warning(lib, "HSET HUADAN_NEXT not a integer reply?\n");
    }
    freeReplyObject(reply);
}

void clr_huadan_next(redisContext *c)
{
    redisReply *reply;

    reply = redisCommand(c, "HDEL HUADAN_NEXT %s", hostname);
    if (reply->type == REDIS_REPLY_INTEGER) {
        /* either 0 or 1 is ok */
        hvfs_info(lib, "HDEL HUADAN_NEXT for %s\n", hostname);
    } else {
        hvfs_warning(lib, "HDEL HUADAN_NEXT field not a integer reply?\n");
    }
    freeReplyObject(reply);
}

static int __fg_compare(const void *a, const void *b)
{
    const struct file_info *fia = a, *fib = b;
    long al = 0, bl = 0;
    char *p, *q, *str;
    int nr = 0;

    str = strdup(fia->pathname);
    p = basename(str);
    do {
        q = strtok(p, "-_\n");
        if (!q) {
            break;
        }
        nr++;
        if (nr == g_sort_ji->DATE_COL) {
            al = atol(q);
            break;
        }
    } while (p = NULL, 1);
    xfree(str);

    str = strdup(fib->pathname);
    p = basename(str);
    nr = 0;
    do {
        q = strtok(p, "-_\n");
        if (!q) {
            break;
        }
        nr++;
        if (nr == g_sort_ji->DATE_COL) {
            bl = atol(q);
            break;
        }
    } while (p = NULL, 1);
    xfree(str);

    return (al < bl ? -1 : (al > bl ? 1 : 0));
}

void __sort_files(struct file_gather_all *fga, struct job_info *ji)
{
    /* setup global gi */
    g_sort_ji = ji;
    
    qsort(fga->add.fis, fga->add.asize, sizeof(struct file_info),
          __fg_compare);
}

int check_or_add_to_ignore(redisContext *c, char *pathname) 
{
    redisReply *reply;
    int e = 0;

    reply = redisCommand(c, "SISMEMBER IGNORE_SET FS:%s", pathname);
    if (reply->type == REDIS_REPLY_INTEGER) {
        if (reply->integer == 1) {
            e = 1;
            goto out;
        }
    } else {
        hvfs_err(lib, "SISMEMBER not an integer reply?\n");
    }
    freeReplyObject(reply);

    reply = redisCommand(c, "SADD IGNORE_SET FS:%s", pathname);
    if (reply->type == REDIS_REPLY_INTEGER) {
        /* it is ok */
        ;
    } else {
        hvfs_err(lib, "SADD not an integer reply?\n");
    }
    
out:
    freeReplyObject(reply);
    
    return e;
}

static int can_scheduled(redisContext *c, struct file_info *fi, struct job_info *ji)
{
    char *p, *q, *str, *day;
    struct job_entry *pos;
    long today;
    int nr = 0, r = 1;

    str = strdup(fi->pathname);
    p = basename(str);
    do {
        q = strtok(p, "-_\n");
        if (!q) {
            break;
        }
        nr++;
        if (nr == ji->DATE_COL) {
            day = strdup(q);
            break;
        }
    } while (p = NULL, 1);

    day[strlen(day) - 2] = '\0';
    today = atol(day);

    /* check if we can schedule jobs */
    xlock_lock(&g_huadan_lock);
    if (!g_huadan) {
        /* init */
        r = 0;
        g_huadan = -1;
        set_huadan_next(c, atol(day));
        xlock_unlock(&g_huadan_lock);
        goto out;
    } else if (g_huadan < 0) {
        /* reject */
        r = 0;
        xlock_unlock(&g_huadan_lock);
        goto out;
    } else if (g_huadan != today) {
        /* reject, and gather propose info */
        r = 0;
        xlock_unlock(&g_huadan_lock);
        if (today > g_huadan) {
            if (g_propose < 0)
                g_propose = today;
            if (g_propose > today)
                g_propose = today;
        } else {
            /* ignore set management */
            if (!check_or_add_to_ignore(c, fi->pathname)) {
                hvfs_warning(lib, "FILE %s changed, but the HUADAN had been poped. "
                             "CUR(%ld)\n",
                             fi->pathname, g_huadan);
            }
        }
        goto out;
    }
    xlock_unlock(&g_huadan_lock);

    /* check if there are pending load jobs for last day */
    xlock_lock(&job_lock);
    list_for_each_entry(pos, &jobs, list) {
        /* if it is a load job, and not completed, then do not schedule */
        if (pos->date < today && pos->offset >= 0) {
            r = 0;
            break;
        }
    }
    xlock_unlock(&job_lock);

out:
    xfree(str);
    
    return r;
}

void do_scan(redisContext *c, time_t cur)
{
    static time_t last_ts = 0;
    static int last_interval;
    struct file_gather_all *fga = NULL;
    int i;
    
    if (last_ts == 0) {
        last_ts = cur;
        last_interval = g_config.scan_interval;
    }
    
    if (cur - last_ts >= g_config.scan_interval) {
        last_ts = cur;
        g_config.scan_interval = last_interval; /* reset to default interval */

        fga = full_scan(g_dir);
        if (IS_ERR(fga)) {
            hvfs_err(lib, "full_scan dir %s failed w/ %ld\n",
                     g_dir, PTR_ERR(fga));
            return;
        }

        /* we should sort the files to arrange job on time order! */
        __sort_files(fga, &job_infos[JI_1D_NORMAL]);
        
        for (i = 0; i < fga->add.asize; i++) {
            hvfs_debug(lib, "FILE %s SIZE %ld -> %ld\n",
                       fga->add.fis[i].pathname, fga->add.fis[i].osize, 
                       fga->add.fis[i].size);
            if (file_changed(c, &fga->add.fis[i])) {
                if (can_scheduled(c, &fga->add.fis[i], &job_infos[JI_1D_NORMAL])) {
                    hvfs_info(lib, "FILE %s changed.\n", fga->add.fis[i].pathname);
                    add_job(c, fga->add.fis[i].pathname, fga->add.fis[i].osize,
                            &job_infos[JI_1D_NORMAL]);
                } else {
                    hvfs_debug(lib, "FILE %s changed, and delayed.\n",
                               fga->add.fis[i].pathname);
                }
            }
        }
        if (g_poped == G_POPED_NOOP)
            g_poped = G_POPED_LOAD;
        
        /* if g_propose is large than 0, then try to update next propose */
        if (g_propose > g_huadan) {
            int do_update = 1;
            
            xlock_lock(&job_lock);
            if (g_no_propose || (g_poped == G_POPED_LOAD && !list_empty(&jobs))) {
                /* some jobs are running, do not update next propose */
                do_update = 0;
                if (g_no_propose) 
                    g_no_propose = 0;
            }
            xlock_unlock(&job_lock);
            if (do_update) {
                set_huadan_next(c, g_propose);
            }
            g_propose = -1;
        }
        
        if (fga) {
            __free_fg(&fga->add);
            __free_fg(&fga->rmv);
        }
    }
}

static void __sigaction_default(int signo, siginfo_t *info, void *arg)
{
    if (signo == SIGSEGV) {
        hvfs_info(lib, "Recv %sSIGSEGV%s %s\n",
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
    } else if (signo == SIGHUP || signo == SIGINT) {
        hvfs_info(lib, "Exit Watcher ...\n");
        g_main_thread_stop = 1;
        sem_post(&g_main_sem);
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

static void __itimer_default(int signo, siginfo_t *info, void *arg)
{
    sem_post(&g_timer_sem);
    hvfs_verbose(lib, "Did this signal handler called?\n");

    return;
}

int do_fork(struct job_entry *job, char *cmd, char *argv[])
{
    pid_t pid;
    int pipefd[2];
    int err = 0;

    /* create a pipe firstly */
    err = pipe(pipefd);
    if (err) {
        hvfs_err(lib, "create pipe failed w/ %s\n", strerror(errno));
        goto out;
    }

    pid = fork();
    if (pid < 0) {
        hvfs_err(lib, "fork() failed w/ %s\n", strerror(errno));
        goto out_close;
    }
    if (pid == 0) {
        /* child process */
        close(0);
        close(pipefd[0]);
        dup2(pipefd[1], 0);

        signal(SIGINT, SIG_IGN);
        err = execv(cmd, argv);
        if (signal(SIGABRT, abort_handler) < 0) {
            hvfs_err(lib, "signal ABORT %s\n", strerror(errno));
            exit(errno);
        }
        if (err == -1) {
            hvfs_err(lib, "Execute '%s' failed\n", cmd);
        } else if (WEXITSTATUS(err)) {
            hvfs_err(lib, "Execute '%s' return %d\n", cmd, WEXITSTATUS(err));
        }
        exit(-1);
    } else {
        /* do not close the write end to eliminate read(2) EOF */
        job->fd = pipefd[0];
        close(pipefd[1]);
        job->pid = pid;

        /* self process */
        {
            /* change env carefully */
            int __p;

        retry:
            __p = atomic_inc_return(&g_env_prot);
            if (__p > 1) {
                atomic_dec(&g_env_prot);
                sched_yield();
                goto retry;
            }
            environ = environ_saved;
            atomic_dec(&g_env_prot);
        }        
    }
out:
    return err;
out_close:
    close(pipefd[0]);
    close(pipefd[1]);
    goto out;
}

void set_huadan_date(redisContext *c, char *date);
int toggle_db(redisContext *c, int flag);
char *get_huadan_date(redisContext *c);
int update_db(redisContext *c);

int propose_or_update_huadan(redisContext *c)
{
    redisReply *reply;
    int r = 0;

    reply = redisCommand(c, "HGETALL HUADAN_NEXT");

    if (g_id == 0) {
        /* try to peek a valid HUADAN */
        int i;
        long proposed_huadan = -1;

        for (i = 0; i < reply->elements; i += 2) {
            if (proposed_huadan < 0) {
                proposed_huadan = atol(reply->element[i + 1]->str);
            }
            if (proposed_huadan > atol(reply->element[i + 1]->str)) {
                proposed_huadan = atol(reply->element[i + 1]->str);
            }
        }

        if (proposed_huadan > g_huadan) {
            if (g_huadan < proposed_huadan) {
                char date[32];
                int err;

                sprintf(date, "%ld", proposed_huadan);
                xlock_lock(&g_huadan_lock);
                err = toggle_db(c, TOGGLE_DB_FLIP);
                if (err) {
                    hvfs_err(lib, "FLIP DB from %d failed w/ %d\n",
                             g_current_db, err);
                }
                set_huadan_date(c, date);
                g_poped = g_huadan;
                g_huadan = proposed_huadan;
                xlock_unlock(&g_huadan_lock);

                hvfs_info(lib, "Propose to use HUADAN %s\n", date);

                if (g_poped > 0 && !g_has_proposed) {
                    /* there are no file changes from we start */
                    set_huadan_next(c, g_huadan);
                    g_has_proposed = 0;
                    g_should_clr = 1;
                }
            }
        }
    } else {
        /* try to update a new HUADAN */
        char *p = get_huadan_date(c);
        long got_huadan = -1;

        if (p)
            got_huadan = atol(p);
        xfree(p);

        /* update the db now */
        if (got_huadan > g_huadan) {
            g_poped = g_huadan;
            g_huadan = got_huadan;
            hvfs_info(lib, "Got active HUADAN %ld\n", g_huadan);
            update_db(c);

            if (g_poped > 0 && !g_has_proposed) {
                /* there are no file changes from we start */
                set_huadan_next(c, g_huadan);
                g_has_proposed = 0;
                g_should_clr = 1;
            }
        }
    }

    freeReplyObject(reply);
    
    return r;
}

/* pathname as:
 * /mnt/data1/src-data/tt/tt_ori_log/TT-ORI-LOG-LOAD1_TT_TT_ORI_LOG_datanode6_2012120606_tt-lq-reg1_4
 */
int add_job(redisContext *c, char *pathname, long offset, struct job_info *ji)
{
    char log[PATH_MAX];
    char output[PATH_MAX], args[1500];
    char *p, *q, *date, *day, *str;
    struct job_entry *pos;
    int nr = 0, rid = 0, cid = 0, found = 0, use_db, r = 0;

    str = strdup(pathname);
    p = basename(str);
    do {
        q = strtok(p, "-_\n");
        if (!q) {
            break;
        }
        nr++;
        if (nr == ji->DATE_COL) {
            date = strdup(q);
            /* round # */
            rid = atoi(q + 8);
        }
        if (nr == ji->REG_COL) {
            cid = atoi(q + 3) - 1;
            if (cid < 0)
                cid = 0;
            else
                cid *= ji->CLI_NUM;
        }
        if (nr == ji->CLI_COL) {
            cid += atoi(q);
        }
    } while (p = NULL, 1);

    day = strdup(date);
    day[strlen(day) - 2] = '\0';

    /* check if this job exist */
    xlock_lock(&job_lock);
    list_for_each_entry(pos, &jobs, list) {
        if (strcmp(pathname, pos->filename) == 0) {
            found = 1;
            break;
        }
    }
    if (!found) {
        /* add a new entry now */
        pos = xzalloc(sizeof(*pos));
        if (!pos) {
            hvfs_err(lib, "xzalloc job entry failed\n");
            xlock_unlock(&job_lock);
            r = -1;
            goto out;
        }
        pos->filename = strdup(pathname);
        pos->offset = offset;
        pos->jobid = NEXT_JOBID();
        pos->date = atol(day);
        list_add_tail(&pos->list, &jobs);
    }
    xlock_unlock(&job_lock);
    if (found)
        goto out;

    /* update the huadan id */
    xlock_lock(&g_huadan_lock);
    if (g_huadan == atol(day)) {
        /* the same hudan */
        use_db = g_current_db;
    } else {
        HVFS_BUGON("Invalid JOB scheduling");
    }
    xlock_unlock(&g_huadan_lock);
    
    hvfs_info(lib, "Add a new JOB %ld for file %s offset %ld\n", 
              pos->jobid, pathname, offset);
    
    sprintf(output, ji->output, date, cid);
    sprintf(log, ji->log, date, rid, cid, offset, pos->jobid);

    sprintf(args, "-a %s -r %d -d %d -s %ld -i %s -o %s -x %s -f -c %s -b %s -A %d -D %d.",
            ji->action, rid, cid, offset, pathname, output, log,
            ji->config, ji->ipdb, ji->ignoreact, use_db);
    hvfs_info(lib, "Start JOB: %s %s\n", ji->cmd, args);

    /* use fork to start job */
    {
        char ridstr[16], cidstr[16], offsetstr[32], ignoreactstr[16],
            dbstr[16];

        sprintf(ridstr, "%d", rid);
        sprintf(cidstr, "%d", cid);
        sprintf(offsetstr, "%ld", offset);
        sprintf(ignoreactstr, "%d", ji->ignoreact);
        sprintf(dbstr, "%d", use_db);
        
        char *argv[] = {
            ji->cmd,
            "-a", ji->action,
            "-r", ridstr,
            "-d", cidstr,
            "-s", offsetstr,
            "-i", pathname,
            "-o", output,
            "-x", log,
            "-c", ji->config,
            "-b", ji->ipdb,
            "-A", ignoreactstr,
            "-f",
            "-D", dbstr,
            NULL,
        };
        char *envp[] = {
            ji->env,
            NULL,
        };

        {
            /* change env carefully */
            int __p;

        retry:
            __p = atomic_inc_return(&g_env_prot);
            if (__p > 1) {
                atomic_dec(&g_env_prot);
                sched_yield();
                goto retry;
            }
            environ_saved = environ;
            environ = envp;
            atomic_dec(&g_env_prot);
        }

        do_fork(pos, ji->cmd, argv);
        int err = watcher_poll_add(pos->fd);
        if (err) {
            hvfs_err(lib, "add JOB fd %d to poll set failed w/ %d\n",
                     pos->fd, err);
            hvfs_warning(lib, "Failure push us to slow mode, wait for this JOB\n");
            fina_job(c, pos);
        }
    }
    /* add the cid and rid to JI array */
    if ((ji + 1)->rid <= rid)
        (ji + 1)->rid = rid;
    if ((ji + 1)->cid <= cid)
        (ji + 1)->cid = cid;

    /* change g_poped state */
    if (g_poped < G_POPED_MAX)
        g_poped = G_POPED_LOAD;
    
    /* free str */
out:
    xfree(str);
    
    return r;
}

int add_pop_job(redisContext *c, char *date, struct job_info *ji)
{
    char output[PATH_MAX], log[PATH_MAX];
    struct job_entry *pos;
    int found = 0;
    
    /* check if this job exist */
    xlock_lock(&job_lock);
    list_for_each_entry(pos, &jobs, list) {
        if (strcmp(date, pos->filename) == 0) {
            found = 1;
            break;
        }
        if (pos->date == atol(date)) {
            found = 1;
            break;
        }
    }
    if (!found) {
        /* add a new entry now */
        pos = xzalloc(sizeof(*pos));
        if (!pos) {
            hvfs_err(lib, "xzalloc job entry failed\n");
            xlock_unlock(&job_lock);
            return -1;
        }
        pos->filename = strdup(date);
        pos->offset = -1;
        pos->jobid = NEXT_JOBID();
        pos->date = atol(date);
        list_add_tail(&pos->list, &jobs);
    }
    xlock_unlock(&job_lock);
    if (found)
        return 0;

    sprintf(output, ji->output, date);
    sprintf(log, ji->log, date, ji->rid, ji->cid + 1, pos->jobid);
    hvfs_info(lib, "Add a new POP JOB id %ld for time %s\n", 
              pos->jobid, date);

    /* use fork to start job */
    {
        char ridstr[16], cidstr[16], dbstr[16], modestr[16];

        sprintf(ridstr, "%d", ji->rid);
        sprintf(cidstr, "%d", 0); /* reset cid to ZERO! */
        sprintf(dbstr, "%d", g_current_db == 1 ? 2 : 1);
        sprintf(modestr, "%d", g_mode);
        if (g_mode)
            g_mode = 0;

        char *argv[] = {
            ji->cmd,
            "-a", ji->action,
            "-r", ridstr,
            "-d", cidstr,
            "-o", output,
            "-x", log,
            "-c", ji->config,
            "-b", ji->ipdb,
            "-f",
            "-l",
            "-D", dbstr,
            "-m", modestr,
            NULL,
        };
        char *envp[] = {
            ji->env,
            NULL,
        };
        int err = 0;
        
        {
            /* change env carefully */
            int __p;

        retry:
            __p = atomic_inc_return(&g_env_prot);
            if (__p > 1) {
                atomic_dec(&g_env_prot);
                sched_yield();
                goto retry;
            }
            environ_saved = environ;
            environ = envp;
            atomic_dec(&g_env_prot);
        }

        hvfs_info(lib, "CMD: -a %s -r %s -d %s -o %s -x %s -D %s\n",
                  ji->action, ridstr, cidstr, output, log, dbstr);

        do_fork(pos, ji->cmd, argv);
        err = watcher_poll_add(pos->fd);
        if (err) {
            hvfs_err(lib, "add POP JOB fd %d to poll set failed w/ %d\n",
                     pos->fd, err);
            hvfs_warning(lib, "Failure push us to slow mode, wait for this JOB\n");
            fina_job(c, pos);
        }
    }
    /* FIXME: reset JI info */
    g_poped = G_POPED_POP;
    job_infos[JI_1D_POP].rid = 0;
    job_infos[JI_1D_POP].cid = 0;

    return 0;
}

struct job_entry *del_job(int fd)
{
    struct job_entry *tpos = NULL, *pos;
    
    xlock_lock(&job_lock);
    list_for_each_entry_safe(tpos, pos, &jobs, list) {
        if (fd == tpos->fd) {
            list_del_init(&tpos->list);
            break;
        }
    }
    xlock_unlock(&job_lock);
    
    return tpos;
}

void fina_job(redisContext *c, struct job_entry *job)
{
    long offset;
    int br, bl = 0;
    
    /* read in the last offset and update the database */
    do {
        br = read(job->fd, ((void *)&offset) + bl, sizeof(offset) - bl);
        if (br < 0) {
            hvfs_err(lib, "read() from fd %d failed w/ %s\n",
                     job->fd, strerror(errno));
            break;
        } else if (br == 0) {
            break;
        }
        bl += br;
    } while (bl < sizeof(offset));

    if (bl == sizeof(offset)) {
        if (offset >= 0) {
            hvfs_info(lib, "Update DB: %s -> %ld\n", job->filename, offset);
        } else if (offset == -1) {
            if (g_should_clr) {
                clr_huadan_next(c);
                g_should_clr = 0;
            }

            hvfs_info(lib, "POP Job %ld OK.\n", job->jobid);
        } else if (offset == -2) {
            /* peek */
            hvfs_info(lib, "PEEK Job %ld OK.\n", job->jobid);
        } else if (offset == -3) {
            hvfs_info(lib, "Job %ld Terminated, need restart\n", job->jobid);
        }
    } else {
        hvfs_warning(lib, "JOB %ld for file %s return ZERO(%d) offset buffer, "
                     "you should check it!\n", 
                     job->jobid, job->filename, bl);
        /* we should recheck this job: if it is a normal job => do not propose
         * next HUADAN; if it is a pop job => restart this POP job in append
         * mode */
        if (job->offset == -1) {
            g_no_propose = 1;
            g_poped = job->date;
            g_mode = 1;
        } else {
            g_no_propose = 1;
        }
    }

    /* wait4 the child thread */
    waitpid(job->pid, NULL, 0);
    close(job->fd);
    xfree(job->filename);
    xfree(job);
}

int watcher_poll_create(void)
{
    return epoll_create(EPOLL_QUEUE_LEN);
}

void watcher_poll_close(int epfd)
{
    close(epfd);
}

int watcher_poll_add(int fd)
{
    struct epoll_event ev;
    int res;

    if (fd < 0)
        return -EINVAL;

    res = fcntl(fd, F_GETFL);
    if (res < 0) {
        hvfs_err(lib, "fcntl get flags failed w/ %s\n", strerror(errno));
        res = -errno;
        goto out;
    }
    res = fcntl(fd, F_SETFL, res | O_NONBLOCK);
    if (res < 0) {
        hvfs_err(lib, "fcntl set NONBLOCK failed %s\n", strerror(errno));
        res = -errno;
        goto out;
    }
    ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP | EPOLLET;
    ev.data.fd = fd;

    res = epoll_ctl(g_epfd, EPOLL_CTL_ADD, fd, &ev);
    if (res < 0) {
        hvfs_err(lib, "epoll add fd %d failed %s\n", fd, strerror(errno));
        res = -errno;
    }
out:
    return res;
}

int watcher_poll_del(int fd)
{
    struct epoll_event ev;
    int res;

    ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP | EPOLLET;
    ev.data.fd = fd;

    res = epoll_ctl(g_epfd, EPOLL_CTL_DEL, fd, &ev);
    if (res < 0) {
        hvfs_err(lib, "epoll del fd %d failed %s\n", fd, strerror(errno));
    }

    return res;
}

int watcher_poll_wait(struct epoll_event *ev, int timeout)
{
    return epoll_wait(g_epfd, ev, EPOLL_CHECK, timeout);
}

static void *__poll_thread_main(void *arg)
{
    struct epoll_event ev[EPOLL_CHECK];
    redisContext *c = NULL;
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    sigset_t set;
    int nfds, i;

    hvfs_debug(lib, "Connect to Server %s:%d ... ", server, port);
    c = redisConnectWithTimeout(server, port, timeout);
    if (c->err) {
        hvfs_err(lib, "Connect failed w/ %d\n", c->err);
        return NULL;
    }

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then we loop for any events */
    do {
        nfds = watcher_poll_wait(ev, 50);
        if (nfds < 0 && errno == EINTR) {
            nfds = 0;
        } else if (nfds < 0) {
            hvfs_err(lib, "poll wait failed w/ %s\n", strerror(errno));
            continue;
        }
        for (i = 0; i < nfds; i++) {
            int fd = ev[i].data.fd;
            int err = watcher_poll_del(fd);

            if (err) {
                hvfs_err(lib, "delete fd %d from poll set failed w/ %d\n",
                         fd, err);
            } else {
                /* iterate over the job list to find the job_entry */
                struct job_entry *job = del_job(fd);
                
                if (!job) {
                    hvfs_err(lib, "find job by fd %d failed, no such job!\n", fd);
                    continue;
                }
                /* FIXME: race with del_job */
                if (ev[i].events & EPOLLHUP) {
                    g_no_propose = 1;
                }
                fina_job(c, job);
            }
        }
    } while (!g_poll_thread_stop);
    
    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

static void *__event_thread_main(void *arg)
{
    char path[PATH_MAX];
    redisContext *c = NULL;
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    struct inotify_event *ie;
    sigset_t set;
    int err;

    hvfs_debug(lib, "Connect to Server %s:%d ... ", server, port);
    c = redisConnectWithTimeout(server, port, timeout);
    if (c->err) {
        hvfs_err(lib, "Connect failed w/ %d\n", c->err);
        return NULL;
    }

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    ie = inotifytools_next_event(-1);
    while (ie) {
        char *file = inotifytools_filename_from_wd(ie->wd);

        inotifytools_printf(ie, "%T %w%f %e\n");
        if (file) {
            if (file[strlen(file) - 1] == '/')
                sprintf(path, "%s%s", file, ie->name);
            else
                sprintf(path, "%s/%s", file, ie->name);
            if (ie->mask & IN_CREATE) {
                if (ie->mask & IN_ISDIR) {
                    /* add this directory to watch list */
                    hvfs_info(lib, "add %s to watch list\n", path);
                    err = inotifytools_watch_file(path, 
                                                  IN_CLOSE_WRITE | IN_CREATE | IN_DELETE 
                                                  | g_flags);
                    if (!err) {
                        hvfs_err(lib, "add %s to watch list failed w/ %d\n",
                                 path, inotifytools_error());
                    }
                } else {
                    err = set_file_size(c, path, 0);
                    if (err) {
                        hvfs_err(lib, "set file %s size to 0 failed w/ %d\n",
                                 path, err);
                    }
                }
            } else if (ie->mask & IN_CLOSE_WRITE) {
                /* do file update NOW */
                hvfs_info(lib, "file %s closed_write\n", path);
                struct file_info fi = {
                    .pathname = path,
                    .osize = 0,
                };
                struct stat buf;

                err = stat(path, &buf);
                if (err) {
                    hvfs_err(lib, "stat file %s failed w/ %s\n",
                             path, strerror(errno));
                } else {
                    fi.size = buf.st_size;
                    if (file_changed(c, &fi)) {
                        hvfs_info(lib, "FILE %s closed.\n", fi.pathname);
                        if (can_scheduled(c, &fi, &job_infos[JI_1D_NORMAL])) {
                            add_job(c, fi.pathname, fi.osize,
                                    &job_infos[JI_1D_NORMAL]);
                        }
                    }
                }
            } else if (ie->mask & IN_DELETE) {
                hvfs_info(lib, "file %s deleted\n", path);
                clr_file_size(c, path);
            } else if (ie->mask & IN_MODIFY) {
                /* do file update NOW */
                hvfs_info(lib, "file %s modified\n", path);
            }
        }
        ie = inotifytools_next_event(-1);
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int is_day_start(redisContext *c, time_t cur)
{
    int r = 0;
    
    /* check localy */
    if (g_poped > G_POPED_MAX) {
        r = 1;
    }

    return r;
}

static void *__timer_thread_main(void *arg)
{
    redisContext *c = NULL;
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    sigset_t set;
    time_t cur;
    int err;

    hvfs_debug(lib, "Connect to Server %s:%d ... ", server, port);
    c = redisConnectWithTimeout(server, port, timeout);
    if (c->err) {
        hvfs_err(lib, "Connect failed w/ %d\n", c->err);
        return NULL;
    }

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!g_timer_thread_stop) {
        err = sem_wait(&g_timer_sem);
        if (err) {
            if (errno == EINTR)
                continue;
            hvfs_err(lib, "sem_wait() failed w/ %s\n", strerror(errno));
        }

        cur = time(NULL);
        /* scan dir: reset g_poped to LOAD */
        do_scan(c, cur);
        /* should we trigger a stream pop */
        if (is_day_start(c, cur)) {
            char date[128];

            sprintf(date, "%ld", g_poped);
            add_pop_job(c, date, &job_infos[JI_1D_POP]);
        }
        /* update HUADAN: set g_poped to ACTIVE_HUADAN in need */
        propose_or_update_huadan(c);
    }

    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int setup_timers(void)
{
    struct sigaction ac;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL, interval;
    int err;

    sem_init(&g_timer_sem, 0, 0);

    err = pthread_create(&g_timer_thread, NULL, &__timer_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create timer thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

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
    interval = 1;
    if (interval) {
        value.it_interval.tv_sec = interval;
        value.it_interval.tv_usec = 0;
        value.it_value.tv_sec = interval;
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
    
out:
    return err;
}

int setup_inotify(char *top_dir)
{
    char _dir[PATH_MAX] = {0,}, *full_dir = NULL;
    int err = 0;

    xlock_init(&job_lock);
    
    if (top_dir[0] != '/') {
        if (getcwd(_dir, PATH_MAX) == NULL) {
            hvfs_err(lib, "getcwd() failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
        strcat(_dir, "/");
        strcat(_dir, top_dir);
        full_dir = _dir;
    } else {
        full_dir = top_dir;
    }

    if (!inotifytools_initialize() ||
        !inotifytools_watch_recursively(full_dir, 
                                        g_flags | IN_CLOSE_WRITE | IN_CREATE |
                                        IN_DELETE)) {
        err = inotifytools_error();
        hvfs_err(lib, "watch dir %s failed w/ %s\n", full_dir, strerror(err));
        goto out;
    }
    err = pthread_create(&g_event_thread, NULL, &__event_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create event thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }
    g_dir = full_dir;

out:
    return err;
}

int setup_epoll()
{
    int err;

    err = watcher_poll_create();
    if (err < 0) {
        hvfs_err(lib, "epoll create failed w/ %d\n", err);
        goto out;
    }
    g_epfd = err;
    
    err = pthread_create(&g_poll_thread, NULL, &__poll_thread_main,
                         NULL);
    if (err) {
        hvfs_err(lib, "Create poll thread failed w/ %s\n", strerror(errno));
        err = -errno;
        goto out;
    }

out:
    return err;
}

void set_huadan_date(redisContext *c, char *date)
{
    redisReply *reply;
    
    /* Step 1: get the current active huadan */
    reply = redisCommand(c, "GET HUADAN");
    if (reply->type == REDIS_REPLY_STRING) {
        if (strcmp(date, reply->str) == 0) {
            goto free;
        } else {
            hvfs_warning(lib, "Try to update HUADAN from %s to %s\n",
                         reply->str, date);
        }
    }
    freeReplyObject(reply);

    /* Step 2: if needed, update the HUADAN */
    reply = redisCommand(c, "SET HUADAN %s", date);
    if (reply->type == REDIS_REPLY_STATUS) {
        if (strcmp(reply->str, "OK") == 0) {
            hvfs_info(lib, "Set HUADAN to %s\n", date);
        }
    }
free:
    freeReplyObject(reply);
}

char *get_huadan_date(redisContext *c)
{
    redisReply *reply;
    char *date = NULL;
    
    reply = redisCommand(c, "GET HUADAN");
    if (reply->type == REDIS_REPLY_NIL) {
        hvfs_info(lib, "None active HUADAN.\n");
    } else if (reply->type == REDIS_REPLY_STRING) {
        date = strdup(reply->str);
    } else {
        hvfs_err(lib, "get current db failed? %s\n", reply->str);
    }
    freeReplyObject(reply);

    return date;
}

int update_db(redisContext *c)
{
    redisReply *reply;
    int err = 0, db;

    /* get current db */
    db = 0;
    reply = redisCommand(c, "GET DB:current");
    if (reply->type == REDIS_REPLY_NIL) {
        hvfs_info(lib, "Init Job to use DB 1\n");
    } else if (reply->type == REDIS_REPLY_STRING) {
        if (reply->str)
            db = atoi(reply->str);
    } else {
        hvfs_err(lib, "get current db failed? %s\n", reply->str);
        err = -EINVAL;
    }
    freeReplyObject(reply);
    hvfs_info(lib, "Change current DB from %d to %d\n", g_current_db, db);
    g_current_db = db;

    return err;
}

int toggle_db(redisContext *c, int flag)
{
    redisReply *reply;
    int err = 0, db;

retry:
    /* get current db */
    db = 0;
    reply = redisCommand(c, "GET DB:current");
    if (reply->type == REDIS_REPLY_NIL) {
        hvfs_info(lib, "Init Job to use DB 1\n");
    } else if (reply->type == REDIS_REPLY_STRING) {
        if (reply->str)
            db = atoi(reply->str);
    } else {
        hvfs_err(lib, "get current db failed? %s\n", reply->str);
        err = -EINVAL;
    }
    freeReplyObject(reply);
    if (err)
        return err;

    /* switch to next db */
    switch (flag) {
    case TOGGLE_DB_INIT:
        if (db == 0)
            db = 1;
        else
            goto local_change;
        do {
            reply = redisCommand(c, "SETNX DB:current %d", db);
            if (reply->type == REDIS_REPLY_INTEGER) {
                if (reply->integer == 0) {
                    /* key conflict, retry */
                    freeReplyObject(reply);
                    goto retry;
                } else {
                    hvfs_info(lib, "Set Job to use Database %d\n", db);
                    break;
                }
            }
            freeReplyObject(reply);
            hvfs_err(lib, "Set Job to use Database %d failed\n", db);
            return err;
        } while (0);
        break;
    case TOGGLE_DB_FLIP:
    default:
        if (db == 0)
            db = 1;
        else if (db == 1)
            db = 2;
        else
            db = 1;
        do {
            reply = redisCommand(c, "SET DB:current %d", db);
            if (reply->type == REDIS_REPLY_STATUS) {
                if (strcmp(reply->str, "OK") == 0) {
                    hvfs_info(lib, "Set Job to use Database %d\n", db);
                    break;
                }
            }
            freeReplyObject(reply);
            hvfs_err(lib, "Set Job to use Database %d failed\n", db);
            return err;
        } while (0);
    }
    freeReplyObject(reply);

local_change:
    hvfs_info(lib, "Change current DB from %d to %d\n", g_current_db, db);
    g_current_db = db;

    return err;
}

int main(int argc, char *argv[])
{
    redisContext *c = NULL;
    char *shortflags = "I:d:r:p:i:h?g";
    struct option longflags[] = {
        {"id", required_argument, 0, 'I'},
        {"dir", required_argument, 0, 'd'},
        {"server", required_argument, 0, 'r'},
        {"port", required_argument, 0, 'p'},
        {"interval", required_argument, 0, 'i'},
        {"instant", no_argument, 0, 't'},
        {"debug", no_argument, 0, 'g'},
        {"help", no_argument, 0, 'h'},
    };
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    int err = 0, interval = -1;

    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch (opt) {
        case 'I':
            g_id = atoi(optarg);
            break;
        case 'd':
            g_dir = strdup(optarg);
            break;
        case 'r':
            server = strdup(optarg);
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'i':
            interval = atoi(optarg);
            break;
        case 't':
            g_flags |= IN_MODIFY;
            break;
        case 'g':
            hvfs_lib_tracing_flags = 0xffffffff;
            break;
        case 'h':
        case '?':
            do_help();
            return 0;
            break;
        default:
            hvfs_err(lib, "Invalid arguments!\n");
            return EINVAL;
        }
    };

    if (g_id < 0) {
        hvfs_plain(lib, "Invalid watcher ID %d\n", g_id);
        return EINVAL;
    }
    
    if (!g_dir) {
        hvfs_plain(lib, "Please set the directory you want to watch!\n");
        do_help();
        err = EINVAL;
        goto out;
    }

    /* get hostname */
    err = gethostname(hostname, 128);
    if (err) {
        hvfs_err(lib, "gethostname() failed w/ %s\n", strerror(errno));
        return EINVAL;
    }

    xlock_init(&g_huadan_lock);

    hvfs_info(lib, "Connect to Server %s:%d ... ", server, port);
    c = redisConnectWithTimeout(server, port, timeout);
    if (c->err) {
        hvfs_plain(lib, "failed w/ %d\n", c->err);
        return c->err;
    } else {
        hvfs_plain(lib, "ok.\n");
    }
    /* use db 0, w/o lock */
    do {
        redisReply *reply;
        
        reply = redisCommand(c, "SELECT 0");
        if (reply->type == REDIS_REPLY_STATUS) {
            if (strcmp(reply->str, "OK") == 0) {
                hvfs_info(lib, "Select to use Database 0\n");
                freeReplyObject(reply);
                break;
            }
        }
        hvfs_err(lib, "Select to use Database 0 failed!\n");
        freeReplyObject(reply);
        goto out_close;
    } while (0);

    err = toggle_db(c, TOGGLE_DB_INIT);
    if (err) {
        hvfs_err(lib, "Toggle db failed w/ %d\n", err);
        goto out_close;
    }

    {
        char *p = get_huadan_date(c);

        if (!p) {
            g_huadan = 0;
        } else {
            g_huadan = atol(p);
        }
        hvfs_info(lib, "Got active HUADAN %ld\n", g_huadan);
        xfree(p);
    }
    
    /* setup signals */
    err = __init_signal();
    if (err) {
        hvfs_err(lib, "Init signals failed w/ %d\n", err);
        goto out_close;
    }

    /* setup epoll system */
    err = setup_epoll();
    if (err) {
        hvfs_err(lib, "Setup epoll system failed w/ %d\n", err);
        goto out_close;
    }
    
    /* setup timers */
    if (interval >= 0) {
        hvfs_info(lib, "Reset directory scan interval to %d second(s).\n",
                  interval);
        g_config.scan_interval = interval;
    }
    err = setup_timers();
    if (err) {
        hvfs_err(lib, "Setup timers failed w/ %d\n", err);
        goto out_close;
    }

    /* setup inotify system */
    err = setup_inotify(g_dir);
    if (err) {
        hvfs_err(lib, "Setup inotify system failed w/ %d\n", err);
        goto out_close;
    }
    
    /* wait loop */
    while (1) {
        sem_wait(&g_main_sem);
        if (g_main_thread_stop)
            break;
    }
    
    g_timer_thread_stop = 1;
    if (g_timer_thread) {
        sem_post(&g_timer_sem);
        pthread_join(g_timer_thread, NULL);
    }
    g_poll_thread_stop = 1;
    if (g_poll_thread) {
        pthread_join(g_poll_thread, NULL);
    }
    close(g_epfd);

    hvfs_info(lib, "Main thread exiting ...\n");
    
out_close:
    if (c)
        redisFree(c);
out:
    return err;
}
