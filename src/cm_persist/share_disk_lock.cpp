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
 * share_disk_lock.cpp
 *
 * IDENTIFICATION
 *    src/cm_persist/share_disk_lock.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include "securec.h"
#include "share_disk_lock.h"

const int BLOCK_NUMS = 3;
const uint32 LOCK_BLOCK_NUMS = 2;

status_t CmAllocDlock(dlock_t *lock, uint64 lockAddr, int64 instId)
{
    uint64 buffSize = BLOCK_NUMS * CM_DEF_BLOCK_SIZE + DISK_LOCK_ALIGN_SIZE_512;

    if (lockAddr % CM_DEF_BLOCK_SIZE != 0) {
        (void)printf(
            _("Invalid lock addr %lu, the addr value must be an integer multiple of the block size.\n"), lockAddr);
        return CM_ERROR;
    }

    if (lock != NULL) {
        int32 rc = memset_sp(lock, sizeof(dlock_t), 0, sizeof(dlock_t));
        if (rc != 0) {
            (void)printf(_("memset_sp error, %d.\n"), rc);
            return CM_ERROR;
        }
        lock->buff = (char *)malloc(buffSize);
        if (lock->buff == NULL) {
            (void)printf(_("malloc error, %d.\n"), errno);
            return CM_ERROR;
        }

        rc = memset_sp(lock->buff, buffSize, 0, buffSize);
        if (rc != 0) {
            (void)printf(_("memset_sp error, %d.\n"), rc);
            FREE_AND_RESET(lock->buff);
            return CM_ERROR;
        }
        // three buff area, lockr buff|lockw buff|tmp buff
        uint64 offset = (DISK_LOCK_ALIGN_SIZE_512 - ((uint64)lock->buff) % DISK_LOCK_ALIGN_SIZE_512);
        lock->lockr = lock->buff + offset;
        lock->lockw = lock->lockr + CM_DEF_BLOCK_SIZE;
        lock->tmp = lock->lockw + CM_DEF_BLOCK_SIZE;

        rc = memset_sp(lock->lockw, CM_DEF_BLOCK_SIZE, 1, CM_DEF_BLOCK_SIZE);
        if (rc != 0) {
            (void)printf(_("memset_sp error, %d.\n"), rc);
            FREE_AND_RESET(lock->buff);
            return CM_ERROR;
        }
        CmInitDlockHeader(lock, lockAddr, instId);
    }
    return CM_SUCCESS;
}

void CmInitDlock(dlock_t *lock, uint64 lockAddr, int64 instId)
{
    uint64 buffSize = BLOCK_NUMS * CM_DEF_BLOCK_SIZE + DISK_LOCK_ALIGN_SIZE_512;

    if (lock != NULL) {
        int32 rc = memset_sp(lock->buff, buffSize, 0, buffSize);
        if (rc != 0) {
            (void)printf(_("memset_sp error, %d.\n"), rc);
            return;
        }
        rc = memset_sp(lock->lockw, CM_DEF_BLOCK_SIZE, 1, CM_DEF_BLOCK_SIZE);
        if (rc != 0) {
            (void)printf(_("memset_sp error, %d.\n"), rc);
            return;
        }
        CmInitDlockHeader(lock, lockAddr, instId);
    }
}

void CmInitDlockHeader(dlock_t *lock, uint64 lockAddr, int64 instId)
{
    if (lock != NULL) {
        // clear lockr header
        int32 rc = memset_sp(lock->lockr, DISK_LOCK_HEADER_LEN, 0, DISK_LOCK_HEADER_LEN);
        if (rc != 0) {
            (void)printf(_("CmInitDlockHeader memset_sp error, %d.\n"), rc);
            return;
        }
        // set lockw members
        // header magic num
        LOCKW_LOCK_MAGICNUM(*lock) = DISK_LOCK_HEADER_MAGIC;
        // tail magic num
        int64 *tailMagic = (int64 *)(lock->lockw + CM_DEF_BLOCK_SIZE - sizeof(int64));
        *tailMagic = DISK_LOCK_HEADER_MAGIC;
        LOCKW_INST_ID(*lock) = instId + 1;
        LOCKW_LOCK_VERSION(*lock) = DISK_LOCK_VERSION;
        lock->lockAddr = lockAddr;
    }
}

status_t CmDiskLockS(dlock_t *lock, const char *scsiDev, int32 fd)
{
    if (lock == NULL || scsiDev == NULL) {
        return CM_ERROR;
    }

    if (fd < 0) {
        (void)printf(_("CmDiskLockS fd errno, %d.\n"), fd);
        return CM_ERROR;
    }

    int32 rc = memset_sp(lock->lockr, CM_DEF_BLOCK_SIZE, 0, CM_DEF_BLOCK_SIZE);
    if (rc != 0) {
        (void)printf(_("CmDiskLockS memset_sp error, %d.\n"), rc);
        return CM_ERROR;
    }

    int32 ret = CmDiskLock(lock, fd);
    if (ret != 0) {
        (void)printf(_("CmDiskLockS errno ret %d.\n"), ret);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

status_t CmDiskLockfS(dlock_t *lock, const char *scsiDev)
{
    if (lock == NULL|| scsiDev == NULL) {
        return CM_ERROR;
    }

    int32 fd = open(scsiDev, O_RDWR | O_DIRECT | O_SYNC);
    if (fd < 0) {
        (void)printf(_("CmDiskLockfS Open dev %s failed, errno %d.\n"), scsiDev, errno);
        return CM_ERROR;
    }

    status_t ret = CmDiskLockf(lock, fd);
    if (ret != CM_SUCCESS) {
        (void)close(fd);
        return ret;
    }

    (void)close(fd);
    return CM_SUCCESS;
}

int32 CmDiskLock(dlock_t *lock, int32 fd)
{
    uint32 buffLen = LOCK_BLOCK_NUMS * CM_DEF_BLOCK_SIZE;

    if (lock == NULL || fd < 0) {
        return -1;
    }

    time_t t = time(NULL);
    LOCKW_LOCK_TIME(*lock) = t;
    LOCKW_LOCK_CREATE_TIME(*lock) = t;
    int32 ret = CmScsi3Caw(fd, lock->lockAddr / CM_DEF_BLOCK_SIZE, lock->lockr, buffLen);
    if (ret != (int)CM_SUCCESS) {
        if (ret != CM_SCSI_ERR_MISCOMPARE) {
            (void)printf(_("Scsi3 caw failed, addr %lu.\n"), lock->lockAddr);
            return -1;
        }
    } else {
        (void)printf(_("Scsi3 caw succ.\n"));
        return 0;
    }

    // there is a lock on disk, get lock info
    if (CmGetDlockInfo(lock, fd) != CM_SUCCESS) {
        (void)printf(_("Get lock info from dev failed.\n"));
        return -1;
    }

    // if the owner of the lock is zero, we can lock succ
    LOCKR_INST_ID(*lock) = 0;
    LOCKW_LOCK_CREATE_TIME(*lock) = LOCKR_LOCK_CREATE_TIME(*lock);
    ret = CmScsi3Caw(fd, lock->lockAddr / CM_DEF_BLOCK_SIZE, lock->lockr, buffLen);
    if (ret != (int)CM_SUCCESS) {
        if (ret != CM_SCSI_ERR_MISCOMPARE) {
            (void)printf(_("Scsi3 caw failed after reset, addr %lu.\n"), lock->lockAddr);
            return -1;
        }
    } else {
        (void)printf(_("Scsi3 caw succ after reset.\n"));
        return 0;
    }

    // if the owner of the lock on the disk is the current instance, we can lock succ
    LOCKR_INST_ID(*lock) = LOCKW_INST_ID(*lock);
    LOCKW_LOCK_CREATE_TIME(*lock) = LOCKR_LOCK_CREATE_TIME(*lock);
    ret = CmScsi3Caw(fd, lock->lockAddr / CM_DEF_BLOCK_SIZE, lock->lockr, buffLen);
    if (ret != (int)CM_SUCCESS) {
        if (ret == CM_SCSI_ERR_MISCOMPARE) {
            // the lock is hold by another instance
            LOCKW_LOCK_TIME(*lock) = LOCKR_LOCK_TIME(*lock);
            return CM_DLOCK_ERR_LOCK_OCCUPIED;
        } else {
            (void)printf(_("Scsi3 caw failed when reset instanceId, addr %lu.\n"), lock->lockAddr);
            LOCKW_LOCK_TIME(*lock) = LOCKR_LOCK_TIME(*lock);
            return -1;
        }
    }
    return 0;
}

status_t CmDiskLockf(dlock_t *lock, int32 fd)
{
    if (lock == NULL || fd < 0) {
        return CM_ERROR;
    }
    status_t status = CmGetDlockInfo(lock, fd);
    if (status != CM_SUCCESS) {
        (void)printf(_("Get lock info from dev failed, fd %d.\n"), fd);
        return CM_ERROR;
    }
    int32 ret = CmDiskLock(lock, fd);
    if (ret != (int)CM_SUCCESS) {
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

status_t CmGetDlockInfo(dlock_t *lock, int32 fd)
{
    if (lock == NULL || fd < 0) {
        return CM_ERROR;
    }
    if (lseek64(fd, (off64_t)lock->lockAddr, SEEK_SET) == -1) {
        (void)printf(_("Seek failed, addr %lu, errno %d.\n"), lock->lockAddr, errno);
        return CM_ERROR;
    }
    long size = read(fd, lock->lockr, (size_t)CM_DEF_BLOCK_SIZE);
    if (size == -1) {
        (void)printf(_("Read lockr info failed, ret %ld, errno %d.\n"), size, errno);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}
