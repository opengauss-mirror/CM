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
 * cm_ddb_sharedisk_disklock.cpp
 *
 * IDENTIFICATION
 *    src/cm_adapter/cm_sharedisk_adapter/cm_ddb_sharedisk_disklock.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cm_ddb_sharedisk_disklock.h"
#include "cm_dlock.h"
#include <time.h>

#define MAX_EXIT_STATUS 256

static dlock_t g_disk_lock = {0};

static int cm_alloc_disklock_dorado(uint64 lock_addr, int64 inst_id)
{
    return cm_alloc_dlock(&g_disk_lock, lock_addr, inst_id);
}

static int cm_init_disklock_dorado(uint64 lock_addr, int64 inst_id)
{
    return cm_init_dlock(&g_disk_lock, lock_addr, inst_id);
}

static int cm_lock_disklock_dorado(const char *scsi_dev)
{
    int res = cm_disk_lock_s(&g_disk_lock, scsi_dev);
    if (res == 0) {
        return 0;
    }
    time_t lockTime = LOCKR_LOCK_TIME(g_disk_lock);
    if (lockTime <= 0) {
        return -1;
    }
    return (int)(lockTime % MAX_EXIT_STATUS);
}

static int cm_lockf_disklock_dorado(const char *scsi_dev, int64 lock_time)
{
    LOCKR_LOCK_TIME(g_disk_lock) = lock_time;
    return cm_disk_lockf_s(&g_disk_lock, scsi_dev);
}

static int cm_unlock_disklock_dorado(const char *scsi_dev)
{
    return cm_disk_unlock_s(&g_disk_lock, scsi_dev);
}

static void cm_destroy_disklock_dorado()
{
    cm_destory_dlock(&g_disk_lock);
}

typedef enum {
    DISK_LOCK_MGR_DORADO = 0,
} disk_lock_mgr_type;

disk_lock_mgr_type g_disk_lock_mgr_type = DISK_LOCK_MGR_DORADO;

typedef struct disk_lock_mgr {
    int (*alloc_disklock)(uint64 lock_addr, int64 inst_id);
    int (*init_disklock)(uint64 lock_addr, int64 inst_id);
    int (*lock_disklock)(const char *scsi_dev);
    int (*lockf_disklock)(const char *scsi_dev, int64 lock_time);
    int (*unlock_disklock)(const char *scsi_dev);
    void (*destroy_disklock)(void);
} disk_lock_mgr;

static disk_lock_mgr disk_lock_mgr_func[] = {
    {
        cm_alloc_disklock_dorado,
        cm_init_disklock_dorado,
        cm_lock_disklock_dorado,
        cm_lockf_disklock_dorado,
        cm_unlock_disklock_dorado,
        cm_destroy_disklock_dorado
    }
};

int cm_alloc_disklock(uint64 lock_addr, int64 inst_id)
{
    return disk_lock_mgr_func[g_disk_lock_mgr_type].alloc_disklock(lock_addr, inst_id);
}

int cm_init_disklock(uint64 lock_addr, int64 inst_id)
{
    return disk_lock_mgr_func[g_disk_lock_mgr_type].init_disklock(lock_addr, inst_id);
}

int cm_lock_disklock(const char *scsi_dev)
{
    return disk_lock_mgr_func[g_disk_lock_mgr_type].lock_disklock(scsi_dev);
}

int cm_lockf_disklock(const char *scsi_dev, int64 lock_time)
{
    return disk_lock_mgr_func[g_disk_lock_mgr_type].lockf_disklock(scsi_dev, lock_time);
}

int cm_unlock_disklock(const char *scsi_dev)
{
    return disk_lock_mgr_func[g_disk_lock_mgr_type].unlock_disklock(scsi_dev);
}

void cm_destroy_disklock()
{
    disk_lock_mgr_func[g_disk_lock_mgr_type].destroy_disklock();
}