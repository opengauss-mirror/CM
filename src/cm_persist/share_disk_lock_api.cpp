/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * share_disk_lock_api.cpp
 *
 * IDENTIFICATION
 *    src/cm_persist/share_disk_lock_api.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <malloc.h>
#include <unistd.h>
#include <stdio.h>
#include <time.h>
#include "securec.h"
#include "share_disk_lock_api.h"

const int LOCK_WAIT_INTERVAL = 10 * 1000;
const int FORCE_LOCK_TRY_TIMES = 3;
const int LOCK_WAIT_MAX_TIMTS = 500;

status_t ShareDiskHandlerInit(diskLrwHandler *handler)
{
    if (CmAllocDlock(&handler->headerLock, handler->offset, handler->instId) != CM_SUCCESS) {
        return CM_ERROR;
    }
    CmInitDlock(&handler->headerLock, handler->offset, handler->instId);
    return CM_SUCCESS;
}

status_t InitDiskLockHandle(diskLrwHandler *sdLrwHandler, const char *scsi_dev, uint32 offset, int64 instId)
{
    if (realpath(scsi_dev, sdLrwHandler->scsiDev) == NULL) {
        (void)printf(_("InitDiskLockHandle: copy string %s failed\n"), scsi_dev);
        return CM_ERROR;
    }
    sdLrwHandler->instId = instId;
    sdLrwHandler->offset = offset;
    sdLrwHandler->fd = open(sdLrwHandler->scsiDev, O_RDWR | O_DIRECT | O_SYNC);
    if (sdLrwHandler->fd < 0) {
        (void)printf(_("InitDiskLockHandle: open disk %s failed\n"), sdLrwHandler->scsiDev);
        return CM_ERROR;
    }
    if (ShareDiskHandlerInit(sdLrwHandler) != CM_SUCCESS) {
        (void)printf(_("InitDiskLockHandle: init failed\n"));
        (void)close(sdLrwHandler->fd);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

status_t ShareDiskGetDlock(diskLrwHandler *handler)
{
    status_t ret = CM_SUCCESS;
    int32 times = 0;
    int32 delayTimes = 0;
    bool hasRefreshLockTime = false;

    time_t lockTime = 0;
    do {
        ret = CmDiskLockS(&handler->headerLock, handler->scsiDev, handler->fd);
        if (ret == CM_SUCCESS) {
            return CM_SUCCESS;
        }
        if (lockTime != LOCKR_LOCK_TIME(handler->headerLock)) {
            lockTime = LOCKR_LOCK_TIME(handler->headerLock);
            delayTimes = 0;
            if (hasRefreshLockTime) {
                (void)printf(_("Get lock failed for lock time has been refreshed by other process\n"));
                return CM_ERROR;
            }
            hasRefreshLockTime = true;
        }

        if (lockTime == LOCKR_LOCK_TIME(handler->headerLock)) {
            if (delayTimes < LOCK_WAIT_MAX_TIMTS) {
                (void)usleep(LOCK_WAIT_INTERVAL);
                delayTimes++;
                continue;
            }

            ret = CmDiskLockfS(&handler->headerLock, handler->scsiDev);
            if (ret != CM_SUCCESS) {
                (void)printf(_("Get lock failed when force %d times\n"), ++times);
                return CM_ERROR;
            }
            (void)printf(_("Get lock success when force %d times.\n"), ++times);
            return CM_SUCCESS;
        }

        (void)usleep(LOCK_WAIT_INTERVAL);
    } while (1);
}

