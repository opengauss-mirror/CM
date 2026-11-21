/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd
 * CM is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 *
 * pqsignal.h
 *	  prototypes for the reliable BSD-style signal(2) routine.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/pqsignal.h
 *
 * NOTES
 *	  This shouldn't be in libpq, but the monitor and some other
 *	  things need it...
 *
 * -------------------------------------------------------------------------
 */
#ifndef PQSIGNAL_H
#define PQSIGNAL_H

#include <signal.h>

#ifdef HAVE_SIGPROCMASK
#define PG_SETMASK(mask) pthread_sigmask(SIG_SETMASK, mask, NULL)
#else /* not HAVE_SIGPROCMASK */

#ifndef WIN32
#define PG_SETMASK(mask) sigsetmask(*((int*)(mask)))
#else
#define PG_SETMASK(mask) pqsigsetmask(*((int*)(mask)))
int pqsigsetmask(int mask);
#endif

#define sigaddset(set, signum) (*(set) |= (sigmask(signum)))
#define sigdelset(set, signum) (*(set) &= ~(sigmask(signum)))
#endif /* not HAVE_SIGPROCMASK */

typedef void (*pqsigfunc)(int);
typedef void (*sa_sigaction_t)(int, siginfo_t *, void *);

extern void pqinitmask(void);

extern pqsigfunc pqsignal(int signo, pqsigfunc func);
extern int install_signal(int signo, sa_sigaction_t func);

#endif /* PQSIGNAL_H */
