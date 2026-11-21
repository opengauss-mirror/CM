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
 * postgres_fe.h
 * Primary include file for openGauss client-side .c files
 *
 * This should be the first file included by openGauss client libraries and
 * application programs --- but not by backend modules, which should include
 * postgres.h.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1995, Regents of the University of California
 *
 * src/include/postgres_fe.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef POSTGRES_FE_H
#define POSTGRES_FE_H

#ifndef FRONTEND
#define FRONTEND 1
#endif

#include "c.h"
#include "securec.h"
#include "securec_check.h"
#include <assert.h>

#ifndef Assert
#ifndef USE_ASSERT_CHECKING
#define Assert(p)
#else
#define Assert(p) assert(p)
#endif /* USE_ASSERT_CHECKING */
#endif /* Assert */


#ifndef AssertMacro
#define AssertMacro Assert
#endif

#ifndef BoolGetDatum
#define BoolGetDatum(X) ((Datum)((X) ? 1 : 0))
#endif

#ifndef PointerGetDatum
#define PointerGetDatum(X) ((Datum)(X))
#endif

#ifndef HAVE_DATABASE_TYPE
#define HAVE_DATABASE_TYPE
/* Type of database; increase for sql compatibility */
typedef enum {
    ORA_FORMAT,
    TD_FORMAT
} DatabaseType;
#endif // HAVE_DATABASE_TYPE

#endif /* POSTGRES_FE_H */
