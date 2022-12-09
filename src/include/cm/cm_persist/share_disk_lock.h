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
 * share_disk_lock.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_persist/share_disk_lock.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_DISK_LOCK_H
#define CM_DISK_LOCK_H
#include <time.h>
#include "cm_scsi.h"

#define DISK_LOCK_HEADER_MAGIC (0x3847EEFFFFEE3847)
#define DISK_DEFAULT_LOCK_INTERVAL (100)
#define DISK_LOCK_ALIGN_SIZE_512 (512)
#define DISK_LOCK_BODY_LEN (128)
#define DISK_LOCK_HEADER_LEN (CM_DEF_BLOCK_SIZE - DISK_LOCK_BODY_LEN)
#define DISK_LOCK_VERSION (1)

#define CM_DLOCK_ERR_RETRY_INTERVAL (500)  // ms
#define CM_DLOCK_ERR_RETRY_COUNT (3)
#define CM_DLOCK_ERR_LOCK_OCCUPIED (-2)

#define off64_t int64

#ifndef O_DIRECT
#define O_DIRECT 0
#endif

typedef struct st_dlock_header {
    union {
        struct {
            int64 magicNum;
            int64 instId;
            time_t lockTime;
            time_t createTime;
            int32 version;
        };
        char data[DISK_LOCK_HEADER_LEN];
    };
} dlock_header;

typedef struct st_dlock_body {
    char data[DISK_LOCK_BODY_LEN];
} dlock_body;

typedef struct st_dlock_info {
    dlock_header header;
    dlock_body body;
} dlock_info;

typedef struct st_dlock_area {
    union {
        dlock_info lockInfo;
        char lock[CM_DEF_BLOCK_SIZE];
    };
} dlock_area;

typedef struct st_dlock {
    uint64 lockAddr;
    char *buff;  // malloc buff
    char *lockr;
    char *lockw;
    char *tmp;
} dlock_t;

#define LOCKR_INFO(lock) ((dlock_area *)(lock).lockr)
#define LOCKR_INST_ID(lock) (LOCKR_INFO(lock)->lockInfo.header.instId)
#define LOCKR_SET_INST_ID(lock, orgInstId) (LOCKR_INFO(lock)->lockInfo.header.instId = ((orgInstId) + 1))
#define LOCKR_ORG_INST_ID(lock) (LOCKR_INFO(lock)->lockInfo.header.instId - 1)
#define LOCKR_LOCK_TIME(lock) (LOCKR_INFO(lock)->lockInfo.header.lockTime)
#define LOCKR_LOCK_CREATE_TIME(lock) (LOCKR_INFO(lock)->lockInfo.header.createTime)
#define LOCKR_LOCK_MAGICNUM(lock) (LOCKR_INFO(lock)->lockInfo.header.magicNum)
#define LOCKR_LOCK_VERSION(lock) (LOCKR_INFO(lock)->lockInfo.header.version)
#define LOCKR_LOCK_BODY(lock) (LOCKR_INFO(lock)->lockInfo.body.data)
#define LOCKW_INFO(lock) ((dlock_area *)(lock).lockw)
#define LOCKW_INST_ID(lock) (LOCKW_INFO(lock)->lockInfo.header.instId)
#define LOCKW_ORG_INST_ID(lock) (LOCKW_INFO(lock)->lockInfo.header.instId - 1)
#define LOCKW_LOCK_TIME(lock) (LOCKW_INFO(lock)->lockInfo.header.lockTime)
#define LOCKW_LOCK_CREATE_TIME(lock) (LOCKW_INFO(lock)->lockInfo.header.createTime)
#define LOCKW_LOCK_MAGICNUM(lock) (LOCKW_INFO(lock)->lockInfo.header.magicNum)
#define LOCKW_LOCK_VERSION(lock) (LOCKW_INFO(lock)->lockInfo.header.version)
#define LOCKW_LOCK_BODY(lock) (LOCKW_INFO(lock)->lockInfo.body.data)

status_t CmAllocDlock(dlock_t *lock, uint64 lockAddr, int64 instId);
void CmInitDlock(dlock_t *lock, uint64 lockAddr, int64 instId);        // init lockw header and body
void CmInitDlockHeader(dlock_t *lock, uint64 lockAddr, int64 instId);  // init lockw and lockr header
status_t CmDiskLockS(dlock_t *lock, const char *scsiDev, int32 fd);

int32 CmDiskLock(dlock_t *lock, int32 fd);
status_t CmDiskLockf(dlock_t *lock, int32 fd, int64 lockTime);
status_t CmGetDlockInfo(dlock_t *lock, int32 fd);
#endif
