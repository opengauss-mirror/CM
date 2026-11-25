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
 * gs_threadlocal.h
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/gs_threadlocal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_THREADLOCAL_H_
#define GS_THREADLOCAL_H_

/* PClint */
#ifdef PC_LINT
#define THR_LOCAL
#endif

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

#endif

