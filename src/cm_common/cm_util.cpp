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
 * cm_util.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_common/cm_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <securec.h>
#include "cm_misc_base.h"
#include "cm_util.h"

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

int CmMkdirP(char *path, unsigned int omode)
{
    int retval = 0;
    char *cur = path;

    mode_t oldUmask = umask(0);
    mode_t newUmask = oldUmask & ~(S_IWUSR | S_IXUSR);
    (void)umask(newUmask);
    if (cur[0] == '/') {
        ++cur;
    }
    for (int last = 0; !last; ++cur) {
        if (cur[0] == '\0') {
            last = 1;
        } else if (cur[0] != '/') {
            continue;
        }
        *cur = '\0';
        if (!last && cur[1] == '\0') {
            last = 1;
        }
        if (last) {
            (void)umask(oldUmask);
        }
        // check for pre-existing directory
        struct stat sb = {0};
        if (stat(path, &sb) == 0) {
            if (!S_ISDIR(sb.st_mode)) {
                errno = last ? EEXIST : ENOTDIR;
                retval = -1;
                break;
            }
        } else if (mkdir(path, last ? (mode_t)omode : (S_IRWXU | S_IRWXG | S_IRWXO)) < 0) {
            retval = -1;
            break;
        }
        if (!last) {
            *cur = '/';
        }
    }
    (void)umask(oldUmask);
    return retval;
}

char *gs_getenv_r(const char *name)
{
    (void)pthread_mutex_lock(&g_cmEnvLock);
    char *ret = getenv(name);
    (void)pthread_mutex_unlock(&g_cmEnvLock);
    return ret;
}

uint64 GetMonotonicTimeMs()
{
    static const uint32 CM_NSEC_COUNT_PER_MS = 1000000;
    static const uint32 CM_MS_COUNT_PER_SEC = 1000;

    struct timespec ts;
    (void)clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64)ts.tv_sec * CM_MS_COUNT_PER_SEC + (uint64)ts.tv_nsec / CM_NSEC_COUNT_PER_MS;
}

void CMPrioMutexInit(CMPrioMutex &mutex)
{
    (void)pthread_mutex_init(&mutex.lock, NULL);
    (void)pthread_mutex_init(&mutex.innerLock, NULL);
    (void)pthread_cond_init(&mutex.cond, NULL);
    mutex.highPrioCount = 0;
    mutex.curPrio = CMMutexPrio::CM_MUTEX_PRIO_NONE;
}

int CMPrioMutexLock(CMPrioMutex &mutex, CMMutexPrio prio)
{
    if (prio == CMMutexPrio::CM_MUTEX_PRIO_HIGH) {
        (void)pthread_mutex_lock(&mutex.innerLock);
        mutex.highPrioCount++;
        (void)pthread_mutex_unlock(&mutex.innerLock);
        int ret = pthread_mutex_lock(&mutex.lock);
        if (ret == 0) {
            mutex.curPrio = CMMutexPrio::CM_MUTEX_PRIO_HIGH;
        } else {
            (void)pthread_mutex_lock(&mutex.innerLock);
            mutex.highPrioCount--;
            (void)pthread_mutex_unlock(&mutex.innerLock);
            (void)pthread_cond_broadcast(&mutex.cond);
        }

        return ret;
    }

    while (true) {
        if (mutex.highPrioCount == 0) {
            int ret = pthread_mutex_trylock(&mutex.lock);
            if (ret == 0) {
                mutex.curPrio = CMMutexPrio::CM_MUTEX_PRIO_NORMAL;
                return 0;
            } else if (ret != EBUSY) {
                return ret;
            }
        }

        (void)pthread_mutex_lock(&mutex.innerLock);
        (void)pthread_cond_wait(&mutex.cond, &mutex.innerLock);
        (void)pthread_mutex_unlock(&mutex.innerLock);
    }
}

void CMPrioMutexUnLock(CMPrioMutex &mutex)
{
    if (mutex.curPrio == CMMutexPrio::CM_MUTEX_PRIO_HIGH) {
        (void)pthread_mutex_lock(&mutex.innerLock);
        mutex.highPrioCount--;
        (void)pthread_mutex_unlock(&mutex.innerLock);
    }

    mutex.curPrio = CMMutexPrio::CM_MUTEX_PRIO_NONE;
    (void)pthread_mutex_unlock(&mutex.lock);
    (void)pthread_cond_broadcast(&mutex.cond);
}

char *GetDynamicMem(char *dynamicPtr, size_t *curSize, size_t memSize)
{
    size_t tmpCurSize = (*curSize);
    char *tmp = dynamicPtr + tmpCurSize;
    (*curSize) = tmpCurSize + memSize;
    return tmp;
}
