/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2012-12-26 11:34:44 macan>
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

#ifndef __TRACING_H__
#define __TRACING_H__

#include <sys/timeb.h>
#include <time.h>
#include "atomic.h"

/* hvfs tracing flags */
#define HVFS_INFO       0x80000000
#define HVFS_WARN       0x40000000
#define HVFS_ERR        0x20000000
#define HVFS_PLAIN      0x10000000

#define HVFS_ENTRY      0x00000008
#define HVFS_VERBOSE    0x00000004 /* more infos than debug mode */
#define HVFS_PRECISE    0x00000002
#define HVFS_DEBUG      0x00000001

#define HVFS_DEFAULT_LEVEL      0xf0000000
#define HVFS_DEBUG_ALL          0x0000000f

#ifdef __KERNEL__
#define PRINTK printk
#define FFLUSH
#else  /* !__KERNEL__ */
#define PRINTK printf
#define FPRINTK fprintf
#define FFLUSH(f) fflush(f)
#define KERN_INFO       "[INFO] "
#define KERN_ERR        "[ERR ] "
#define KERN_WARNING    "[WARN] "
#define KERN_DEBUG      "[DBG ] "
#define KERN_VERB       "[VERB] "
#define KERN_PLAIN      ""
#endif

extern atomic_t g_env_prot;

#define HVFS_TRACING_INIT() atomic_t g_env_prot = {.counter = 0,}

#define HVFS_TRACING_SAVE(save) save = atomic_read(&g_env_prot)
#define HVFS_TRACING_RESET() atomic_set(&g_env_prot, 0)
#define HVFS_TRACING_RESTORE(saved) atomic_set(&g_env_prot, saved)

#ifdef HVFS_TRACING
#define hvfs_tracing(mask, flag, lvl, f, a...) do {                     \
        if (unlikely(mask & flag)) {                                    \
            struct timeval __cur;                                       \
            struct tm __tmp;                                            \
            char __ct[64];                                              \
            int __p;                                                    \
                                                                        \
            do {                                                        \
                __p = atomic_inc_return(&g_env_prot);                   \
                if (__p > 1) {                                          \
                    atomic_dec(&g_env_prot);                            \
                    sched_yield();                                      \
                } else                                                  \
                    break;                                              \
            } while (1);                                                \
            gettimeofday(&__cur, NULL);                                 \
            if (!localtime_r(&__cur.tv_sec, &__tmp)) {                  \
                PRINTK(KERN_ERR f, ## a);                               \
                FFLUSH(stdout);                                         \
                atomic_dec(&g_env_prot);                                \
                break;                                                  \
            }                                                           \
            strftime(__ct, 64, "%G-%m-%d %H:%M:%S", &__tmp);            \
            if (mask & HVFS_PRECISE) {                                  \
                PRINTK("%s.%03ld " lvl "HVFS (%16s, %5d): %s[%lx]: " f, \
                       __ct, (long)(__cur.tv_usec / 1000),              \
                       __FILE__, __LINE__, __func__,                    \
                       pthread_self(), ## a);                           \
                FFLUSH(stdout);                                         \
            } else if (mask & HVFS_PLAIN) {                             \
                PRINTK(lvl f, ## a);                                    \
                FFLUSH(stdout);                                         \
            } else {                                                    \
                PRINTK("%s.%03ld " lvl f,                               \
                       __ct, (long)(__cur.tv_usec / 1000), ## a);       \
                FFLUSH(stdout);                                         \
            }                                                           \
            atomic_dec(&g_env_prot);                                    \
        }                                                               \
    } while (0)
#else
#define hvfs_tracing(mask, flag, lvl, f, a...)
#endif  /* !HVFS_TRACING */

#define IS_HVFS_DEBUG(module) ({                            \
            int ret;                                        \
            if (hvfs_##module##_tracing_flags & HVFS_DEBUG) \
                ret = 1;                                    \
            else                                            \
                ret = 0;                                    \
            ret;                                            \
        })

#define IS_HVFS_VERBOSE(module) ({                              \
            int ret;                                            \
            if (hvfs_##module##_tracing_flags & HVFS_VERBOSE)   \
                ret = 1;                                        \
            else                                                \
                ret = 0;                                        \
            ret;                                                \
        })

#define hvfs_info(module, f, a...)                          \
    hvfs_tracing(HVFS_INFO, hvfs_##module##_tracing_flags,  \
                 KERN_INFO, f, ## a)

#define hvfs_plain(module, f, a...)                         \
    hvfs_tracing(HVFS_PLAIN, hvfs_##module##_tracing_flags, \
                 KERN_PLAIN, f, ## a)

#define hvfs_verbose(module, f, a...)           \
    hvfs_tracing((HVFS_VERBOSE | HVFS_PRECISE), \
                 hvfs_##module##_tracing_flags, \
                 KERN_VERB, f, ## a)

#define hvfs_debug(module, f, a...)             \
    hvfs_tracing((HVFS_DEBUG | HVFS_PRECISE),   \
                 hvfs_##module##_tracing_flags, \
                 KERN_DEBUG, f, ## a)

#define hvfs_entry(module, f, a...)             \
    hvfs_tracing((HVFS_ENTRY | HVFS_PRECISE),   \
                 hvfs_##module##_tracing_flags, \
                 KERN_DEBUG, "entry: " f, ## a)

#define hvfs_exit(module, f, a...)              \
    hvfs_tracing((HVFS_ENTRY | HVFS_PRECISE),   \
                 hvfs_##module##_tracing_flags, \
                 KERN_DEBUG, "exit: " f, ## a)

#define hvfs_warning(module, f, a...)           \
    hvfs_tracing((HVFS_WARN | HVFS_PRECISE),    \
                 hvfs_##module##_tracing_flags, \
                 KERN_WARNING, f, ##a)

#define hvfs_err(module, f, a...)               \
    hvfs_tracing((HVFS_ERR | HVFS_PRECISE),     \
                 hvfs_##module##_tracing_flags, \
                 KERN_ERR, f, ##a)

#define SET_TRACING_FLAG(module, flag) do {     \
        hvfs_##module##_tracing_flags |= flag;  \
    } while (0)
#define CLR_TRACING_FLAG(module, flag) do {     \
        hvfs_##module##_tracing_flags &= ~flag; \
    } while (0)

#define TRACING_FLAG(name, v) u32 hvfs_##name##_tracing_flags = v
#define TRACING_FLAG_DEF(name) extern u32 hvfs_##name##_tracing_flags

#ifdef __KERNEL__
#define ASSERT(i, m) BUG_ON(!(i))
#else  /* !__KERNEL__ */
#define ASSERT(i, m) do {                               \
        if (!(i)) {                                     \
            hvfs_err(m, "Assertion " #i " failed!\n");  \
            exit(-EINVAL);                              \
        }                                               \
    } while (0)
#endif

#define HVFS_VV PRINTK
/* Use HVFS_BUG() to get the SIGSEGV signal to debug in the GDB */
#define HVFS_BUG() do {                         \
        HVFS_VV(KERN_PLAIN "HVFS BUG :(\n");    \
        (*((int *)0) = 1);                      \
    } while (0)
#define HVFS_BUGON(str) do {                        \
        HVFS_VV(KERN_PLAIN "Bug on '" #str "'\n");  \
        HVFS_BUG();                                 \
    } while (0)

#define hvfs_pf(f, a...) do {                   \
        if (hmo.conf.pf_file) {                 \
            FPRINTK(hmo.conf.pf_file, f, ## a); \
            FFLUSH(hmo.conf.pf_file);           \
        } else {                                \
            PRINTK(f, ## a);                    \
            FFLUSH(stdout);                     \
        }                                       \
    } while (0)

#endif  /* !__TRACING_H__ */
