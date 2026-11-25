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
 *        prototypes for the reliable BSD-style signal(2) routine.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * IDENTIFICATION
 *        src/include/cm/pqsignal.h
 *
 * NOTES
 *	  This shouldn't be in libpq, but the monitor and some other
 *	  things need it...
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PQSIGNAL_H
#define PQSIGNAL_H

#include <signal.h>

extern sigset_t unblock_sig, block_sig;

typedef void (*sigfunc)(int);

extern void init_signal_mask(void);

#ifndef WIN32
extern void setup_signal_handle(int signo, sigfunc func);
#endif /* WIN32 */

#endif /* PQSIGNAL_H */
