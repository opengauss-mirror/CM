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
 * cm_sync.cpp
 *
 * IDENTIFICATION
 *    src/cm_common/cm_sync.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cm_sync.h"
#include "cm_util.h"

#ifndef WIN32
#include <errno.h>
#include <sys/time.h>
#endif

status_t cm_event_init(cm_event_t *evnt)
{
#ifdef WIN32
    evnt->evnt = CreateEvent(NULL, FALSE, FALSE, NULL);
    if (evnt->evnt == NULL) {
        return CM_ERROR;
    }
#else
    evnt->status = CM_FALSE;
    if (pthread_condattr_init(&evnt->attr) != 0) {
        return CM_ERROR;
    }

    if (pthread_mutex_init(&evnt->lock, 0) != 0) {
        (void)pthread_condattr_destroy(&evnt->attr);
        return CM_ERROR;
    }

    if (pthread_condattr_setclock(&evnt->attr, CLOCK_MONOTONIC) != 0) {
        (void)pthread_mutex_destroy(&evnt->lock);
        (void)pthread_condattr_destroy(&evnt->attr);
        return CM_ERROR;
    }

    if (pthread_cond_init(&evnt->cond, &evnt->attr) != 0) {
        (void)pthread_mutex_destroy(&evnt->lock);
        (void)pthread_condattr_destroy(&evnt->attr);
        return CM_ERROR;
    }
#endif
    return CM_SUCCESS;
}

void cm_event_destory(cm_event_t *evnt)
{
#ifdef WIN32
    (void)CloseHandle(evnt->evnt);
#else
    (void)pthread_mutex_destroy(&evnt->lock);
    (void)pthread_cond_destroy(&evnt->cond);
    (void)pthread_condattr_destroy(&evnt->attr);
#endif
}

#ifndef WIN32
static const long CM_NSEC_COUNT_PER_SEC = CM_MS_COUNT_PER_SEC * CM_NSEC_COUNT_PER_MS;

void cm_get_timespec(struct timespec *tim, uint32 timeout)
{
    struct timespec tv;
    (void)clock_gettime(CLOCK_MONOTONIC, &tv);

    tim->tv_sec = tv.tv_sec + timeout / CM_MS_COUNT_PER_SEC;
    tim->tv_nsec = tv.tv_nsec + (long)(timeout % CM_MS_COUNT_PER_SEC) * CM_NSEC_COUNT_PER_MS;
    if (tim->tv_nsec >= CM_NSEC_COUNT_PER_SEC) {
        tim->tv_sec++;
        tim->tv_nsec -= CM_NSEC_COUNT_PER_SEC;
    }
}
#endif

status_t cm_event_timedwait(cm_event_t *evnt, uint32 timeout)
{
#ifdef WIN32
    int ret = WaitForSingleObject(evnt->evnt, timeout);
    switch (ret) {
        case WAIT_OBJECT_0:
            return CM_SUCCESS;
        case WAIT_TIMEOUT:
            return CM_TIMEDOUT;
        default:
            return CM_ERROR;
    }
#else
    struct timespec tim;

    (void)pthread_mutex_lock(&evnt->lock);
    if (evnt->status) {
        evnt->status = CM_FALSE;
        (void)pthread_mutex_unlock(&evnt->lock);
        return CM_SUCCESS;
    }

    if (timeout == 0xFFFFFFFF) {
        while (!evnt->status) {
            (void)pthread_cond_wait(&evnt->cond, &evnt->lock);
        }
        evnt->status = CM_FALSE;
        (void)pthread_mutex_unlock(&evnt->lock);
        return CM_SUCCESS;
    }

    cm_get_timespec(&tim, timeout);
    while (!evnt->status) {
        int ret = pthread_cond_timedwait(&evnt->cond, &evnt->lock, &tim);
        if (ret == ETIMEDOUT) {
            break;
        }
        /* ret == 0: notified or spurious wakeup; re-check status */
    }
    if (evnt->status) {
        evnt->status = CM_FALSE;
        (void)pthread_mutex_unlock(&evnt->lock);
        return CM_SUCCESS;
    }
    (void)pthread_mutex_unlock(&evnt->lock);
    return CM_TIMEDOUT;
#endif
}

void cm_event_notify(cm_event_t *evnt)
{
#ifdef WIN32
    (void)SetEvent(evnt->evnt);
#else
    (void)pthread_mutex_lock(&evnt->lock);
    if (!evnt->status) {
        evnt->status = CM_TRUE;
        (void)pthread_cond_signal(&evnt->cond);
    }
    (void)pthread_mutex_unlock(&evnt->lock);
#endif
}
