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
 * share_disk_lock_api.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_persist/share_disk_lock_api.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SHARE_DISK_LOCK_API_H
#define SHARE_DISK_LOCK_API_H

#include "share_disk_lock.h"

#define MAX_PATH_LENGTH (1024)

#ifndef NULL
#define NULL ((void *)0)
#endif

#ifndef O_DIRECT
#define O_DIRECT 0
#endif


typedef struct _DISK_LRW_HANDLER {
    char scsiDev[MAX_PATH_LENGTH];
    int64 instId;
    uint32 offset;
    dlock_t headerLock;
    int fd;
} diskLrwHandler;

status_t InitDiskLockHandle(diskLrwHandler *sdLrwHandler, const char *scsi_dev, uint32 offset, int64 instId);
#endif
