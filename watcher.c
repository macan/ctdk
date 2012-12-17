/**
 * Copyright (c) 2012 IIE, all rights reserved.
 *
 * Ma Can <ml.macana@gmail.com> OR <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2012-12-14 17:07:25 macan>
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

#define EPOLL_QUEUE_LEN         4096
#define EPOLL_CHECK             64

redisContext *c = NULL;

struct watcher_config
{
#define SCAN_INTERVAL           30
    int scan_interval;
};

struct watcher_config g_config = {
    .scan_interval = SCAN_INTERVAL,
};

extern char **environ;
static pthread_t g_timer_thread = 0;
static pthread_t g_event_thread = 0;
static pthread_t g_poll_thread = 0;
static sem_t g_timer_sem;
static sem_t g_main_sem;
static int g_timer_thread_stop = 0;
static int g_main_thread_stop = 0;
static int g_poll_thread_stop = 0;
static char *g_dir = NULL;
static unsigned int g_flags = 0;
static LIST_HEAD(jobs);
static xlock_t job_lock;
static xlock_t g_huadan_lock;
static int g_epfd = 0;
static int g_current_db = 3;
static long g_huadan = 0;
static long g_huadan_next = 0;
static long g_jobid = 0;

#define NEXT_JOBID() (++g_jobid)

struct job_entry
{
    struct list_head list;
    char *filename;
    long offset;
    long jobid;
    long date;
    int fd;
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
     "logs/CTDK-CLI-LOG-%s-r%d-c%d-off%ld", "load", 0, 0, 0, 2, 10, 10, 13, 14,},
    {"ctdk_1d_pop", "LD_LIBRARY_PATH=lib", "ctdk_huadan_1d", 
     "conf/cloud.conf", "conf/qqwry.dat", "data/huadan-%s",
     "logs/CTDK-CLI-LOG-%s-r%d-c%d-FINAL", "pop", 0, 0, 0, 2, 10, 10, 13, 14,},
};
#define JI_1D_NORMAL    0
#define JI_1D_POP       1

int add_job(char *pathname, long offset, struct job_info *ji);
struct job_entry *del_job(int fd);
void fina_job(struct job_entry *job);
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

int file_changed(struct file_info *fi)
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

int set_file_size(char *pathname, long size)
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

void do_scan(time_t cur)
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

        for (i = 0; i < fga->add.asize; i++) {
            hvfs_debug(lib, "FILE %s SIZE %ld -> %ld\n",
                       fga->add.fis[i].pathname, fga->add.fis[i].osize, 
                       fga->add.fis[i].size);
            if (file_changed(&fga->add.fis[i])) {
                hvfs_info(lib, "FILE %s changed.\n", fga->add.fis[i].pathname);
                add_job(fga->add.fis[i].pathname, fga->add.fis[i].osize,
                        &job_infos[JI_1D_NORMAL]);
            }
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
        /* self process */
        close(pipefd[1]);
        job->fd = pipefd[0];
    }
out:
    return err;
out_close:
    close(pipefd[0]);
    close(pipefd[1]);
    goto out;
}

void set_huadan_date(char *date);
int toggle_db(int flag);

/* pathname as:
 * /mnt/data1/src-data/tt/tt_ori_log/TT-ORI-LOG-LOAD1_TT_TT_ORI_LOG_datanode6_2012120606_tt-lq-reg1_4
 */
int add_job(char *pathname, long offset, struct job_info *ji)
{
    char log[PATH_MAX];
    char output[1024], args[1024];
    char *filename, *p, *q, *date, *day;
    struct job_entry *pos;
    int nr = 0, rid = 0, cid = 0, found = 0, use_db;
    
    filename = basename(strdup(pathname));
    p = filename;
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
            return -1;
        }
        pos->filename = strdup(pathname);
        pos->offset = offset;
        pos->jobid = NEXT_JOBID();
        pos->date = atol(day);
        list_add_tail(&pos->list, &jobs);
    }
    xlock_unlock(&job_lock);
    if (found)
        return 0;

    hvfs_info(lib, "Add a new JOB %ld for file %s offset %ld\n", 
              pos->jobid, pathname, offset);
    
    /* update the huadan id */
    xlock_lock(&g_huadan_lock);
    if (!g_huadan) {
        g_huadan = atol(day);
        set_huadan_date(day);
        use_db = g_current_db;
    } else if (g_huadan == atol(day)) {
        /* the same hudan */
        use_db = g_current_db;
    } else {
        /* different huadan, flip the DB */
        g_huadan_next = atol(day);
        use_db = g_current_db == 1 ? 2 : 1;
    }
    xlock_unlock(&g_huadan_lock);
    
    sprintf(output, ji->output, date, cid);
    sprintf(log, ji->log, date, rid, cid, offset);

    sprintf(args, "-a %s -r %d -d %d -s %ld -i %s -o %s -x %s -f -c %s -b %s -A %d",
            ji->action, rid, cid, offset, pathname, output, log,
            ji->config, ji->ipdb, ji->ignoreact);
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
        environ = envp;

        do_fork(pos, ji->cmd, argv);
        int err = watcher_poll_add(pos->fd);
        if (err) {
            hvfs_err(lib, "add JOB fd %d to poll set failed w/ %d\n",
                     pos->fd, err);
            hvfs_warning(lib, "Failure push us to slow mode, wait for this JOB\n");
            fina_job(pos);
        }
    }
    /* add the cid and rid to JI array */
    if ((ji + 1)->rid <= rid)
        (ji + 1)->rid = rid;
    if ((ji + 1)->cid <= cid)
        (ji + 1)->cid = cid;

    return 0;
}

int add_pop_job(char *date, struct job_info *ji)
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
    sprintf(log, ji->log, date, ji->rid, ji->cid + 1);
    hvfs_info(lib, "Add a new POP JOB id %ld for time %s\n", 
              pos->jobid, date);

    /* use fork to start job */
    {
        char ridstr[16], cidstr[16], dbstr[16];

        sprintf(ridstr, "%d", ji->rid);
        sprintf(cidstr, "%d", 0); /* reset cid to ZERO! */
        sprintf(dbstr, "%d", g_current_db);
        
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
            NULL,
        };
        char *envp[] = {
            ji->env,
            NULL,
        };
        int err = 0;
        
        environ = envp;
        hvfs_info(lib, "CMD: -a %s -r %s -d %s -o %s -x %s -D %s\n",
                  ji->action, ridstr, cidstr, output, log, dbstr);

        do_fork(pos, ji->cmd, argv);
        err = watcher_poll_add(pos->fd);
        if (err) {
            hvfs_err(lib, "add POP JOB fd %d to poll set failed w/ %d\n",
                     pos->fd, err);
            hvfs_warning(lib, "Failure push us to slow mode, wait for this JOB\n");
            fina_job(pos);
        }
    }

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

void fina_job(struct job_entry *job)
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
            set_file_size(job->filename, offset);
        } else if (offset == -1) {
            /* pop, so toggle the db change */
            char date[32];
            int err;

            sprintf(date, "%ld", g_huadan_next);
            xlock_lock(&g_huadan_lock);
            set_huadan_date(date);
            g_huadan = g_huadan_next;

            err = toggle_db(TOGGLE_DB_FLIP);
            if (err) {
                hvfs_err(lib, "FLIP DB from %d failed w/ %d\n",
                         g_current_db, err);

            }
            xlock_unlock(&g_huadan_lock);
            hvfs_info(lib, "POP Job %ld OK.\n", job->jobid);
        } else if (offset == -2) {
            /* peek */
            hvfs_info(lib, "PEEK Job %ld OK.\n", job->jobid);
        }
    } else {
        hvfs_warning(lib, "JOB for file %s return ZERO offset buffer, "
                     "you should check it!\n",
                     job->filename);
    }
    
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
    sigset_t set;
    int nfds, i;

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
            }

            /* iterate over the job list to find the job_entry */
            struct job_entry *job = del_job(fd);

            if (!job) {
                hvfs_err(lib, "find job by fd %d failed, no such job!\n", fd);
                continue;
            }
            fina_job(job);
        }
    } while (!g_poll_thread_stop);
    
    hvfs_debug(lib, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

static void *__event_thread_main(void *arg)
{
    char path[PATH_MAX];
    struct inotify_event *ie;
    sigset_t set;
    int err;

    hvfs_debug(lib, "I am running...\n");

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
                                                  IN_CLOSE_WRITE | IN_CREATE | g_flags);
                    if (!err) {
                        hvfs_err(lib, "add %s to watch list failed w/ %d\n",
                                 path, inotifytools_error());
                    }
                } else {
                    err = set_file_size(path, 0);
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
                    if (file_changed(&fi)) {
                        hvfs_info(lib, "FILE %s changed.\n", fi.pathname);
                        add_job(fi.pathname, fi.osize,
                                &job_infos[JI_1D_NORMAL]);
                    }
                }
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

int toggle_db(int flag);

int is_day_start(time_t cur)
{
    if (g_huadan < g_huadan_next) {
        return 1;
    } else
        return 0;
}

static void *__timer_thread_main(void *arg)
{
    sigset_t set;
    time_t cur;
    int err;

    hvfs_debug(lib, "I am running...\n");

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
        /* should we work now */
        do_scan(cur);
        /* should we trigger a stream pop */
        if (is_day_start(cur)) {
            char date[128];

            sprintf(date, "%ld", g_huadan);
            add_pop_job(date, &job_infos[JI_1D_POP]);

        }
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
                                        g_flags | IN_CLOSE_WRITE | IN_CREATE)) {
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

void set_huadan_date(char *date)
{
    redisReply *reply;
    
    reply = redisCommand(c, "SET HUADAN %s", date);
    if (reply->type == REDIS_REPLY_STATUS) {
        if (strcmp(reply->str, "OK") == 0) {
            hvfs_info(lib, "Set HUADAN to %s\n", date);
        }
    }
    freeReplyObject(reply);
}

char *get_huadan_date()
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
    hvfs_info(lib, "Got active HUADAN %s\n", date);

    return date;
}

int toggle_db(int flag)
{
    redisReply *reply;
    int err = 0, db = 0;

    /* get current db */
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
        break;
    case TOGGLE_DB_FLIP:
    default:
        if (db == 0)
            db = 1;
        else if (db == 1)
            db = 2;
        else
            db = 1;
    }
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
    freeReplyObject(reply);

    hvfs_info(lib, "Change current DB from %d to %d\n", g_current_db, db);
    g_current_db = db;

    return err;
}

int main(int argc, char *argv[])
{
    char *shortflags = "d:r:p:i:h?", *server = "127.0.0.1";
    struct option longflags[] = {
        {"dir", required_argument, 0, 'd'},
        {"server", required_argument, 0, 'r'},
        {"port", required_argument, 0, 'p'},
        {"interval", required_argument, 0, 'i'},
        {"instant", no_argument, 0, 't'},
        {"help", no_argument, 0, 'h'},
    };
    struct timeval timeout = { 20, 500000 }; // 20.5 seconds
    int err = 0, port = 9087, interval = -1;

    while (1) {
        int longindex = -1;
        int opt = getopt_long(argc, argv, shortflags, longflags, &longindex);
        if (opt == -1)
            break;
        switch (opt) {
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

    if (!g_dir) {
        hvfs_plain(lib, "Please set the directory you want to watch!\n");
        do_help();
        err = EINVAL;
        goto out;
    }

    hvfs_info(lib, "Connect to Server %s:%d ... ", server, port);
    c = redisConnectWithTimeout(server, port, timeout);
    if (c->err) {
        hvfs_plain(lib, "failed w/ %d\n", c->err);
        return c->err;
    } else {
        hvfs_plain(lib, "ok.\n");
    }
    /* use db 0 */
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

    err = toggle_db(TOGGLE_DB_INIT);
    if (err) {
        hvfs_err(lib, "Toggle db failed w/ %d\n", err);
        goto out_close;
    }

    {
        char *p = get_huadan_date();

        if (!p) {
            g_huadan = 0;
        } else {
            g_huadan = atol(p);
        }
    }
    
    xlock_init(&g_huadan_lock);

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
