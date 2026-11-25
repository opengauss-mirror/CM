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
 * Portions Copyright (c) 1987, 1993, 1994
 * The Regents of the University of California.  All rights reserved.
 *
 * Portions Copyright (c) 2003-2012, PostgreSQL Global Development Group
 *
 * src/include/getopt_long.h
 */
#ifndef GETOPT_LONG_H
#define GETOPT_LONG_H

#ifdef HAVE_GETOPT_H
#include <getopt.h>
#endif

/* These are picked up from the system's getopt() facility. */
extern int opterr;
extern int optind;
extern int optopt;
extern char* optarg;

#ifndef HAVE_STRUCT_OPTION

struct option {
    const char* name;
    int has_arg;
    int* flag;
    int val;
};

#define no_argument 0
#define required_argument 1
#endif

#ifndef HAVE_GETOPT_LONG

extern int getopt_long(
    int argc, char* const argv[], const char* optstring, const struct option* longopts, int* longindex);
#endif

#endif /* GETOPT_LONG_H */
