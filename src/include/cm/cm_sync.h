/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 *
 * CM is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * cm_sync.h
 *
 * IDENTIFICATION
 *    include/cm/cm_sync.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CM_SYNC_H
#define CM_SYNC_H

#include "c.h"
#include "cm_defs.h"

#ifdef WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_cm_event {
#ifdef WIN32
    HANDLE evnt;
#else
    volatile uint8 status;
    pthread_mutex_t lock;
    pthread_cond_t cond;
    pthread_condattr_t attr;
#endif
} cm_event_t;

status_t cm_event_init(cm_event_t *event);
void cm_event_destory(cm_event_t *event);
void cm_event_notify(cm_event_t *event);
/* timeout unit: milliseconds */
status_t cm_event_timedwait(cm_event_t *event, uint32 timeout);

#ifndef WIN32
void cm_get_timespec(struct timespec *tim, uint32 timeout);
#endif

#ifdef __cplusplus
}
#endif

#endif
