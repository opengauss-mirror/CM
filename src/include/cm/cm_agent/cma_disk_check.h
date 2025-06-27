/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * cma_disk_check.h
 *
 *
 * IDENTIFICATION
 *    src/include/cm/cm_agent/cma_disk_check.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CM_CMA_DISK_CHECK_H
#define CM_CMA_DISK_CHECK_H

#include "cma_status_check.h"

#define DISK_TEST_FILENAME "disc_readwrite_test"
#define DISK_CHECK_TIMEOUT_DEFAULT 2000 /* ms */
#define DISK_CHECK_INTERVAL_DEFAULT 1  /* second */
#define DISK_CHECK_BUFFER_SIZE_DEFAULT 1

typedef enum {
    DIR_STAT_INIT = 0,
    DIR_STAT_UNKNOWN,
    DIR_STAT_NORMAL,
    DIR_STAT_NOT_EXIST,
    DIR_STAT_NOT_DIR,
    DIR_STAT_PERMISSION_DENIED,
} DirStatus;

typedef enum {
    DISK_STAT_INIT = 0,
    DISK_STAT_UNKNOWN,
    DISK_STAT_NORMAL,
    DISK_STAT_HUNG,
    DISK_STAT_BROKEN,
    DISK_STAT_READONLY,
    DISK_STAT_IO_ERROR,
    DISK_STAT_NO_SPACE,
} DiskStatus;

typedef enum {
    DISK_ALARM_READ_WRITE_SLOW = 0,
    DISK_ALARM_HUNG,
    DISK_ALARM_CEIL,
} DiskAlarmType;

typedef struct {
    char path[MAX_DEVICE_DIR];
    int instanceType;
    DirStatus dirStatus;
    DiskStatus diskStatus;
    uint64 latestIoTime;
    Alarm* diskAlarm;
} DirHealthItem;

typedef struct {
    DirHealthItem* dir;
    uint32 dirCount;
} DirHealth;

typedef struct {
    char diskName[MAX_DEVICE_DIR];
    DirHealth dirHealth;
} DiskHealthItem;

typedef struct {
    DiskHealthItem* disk;
    uint32 diskCount;
} DiskHealth;

void LoadDiskCheckConfig(const char* configFile);
void CreateDiskHealthCheckThread();
uint32 GetDiskCheckInterval();
uint32 GetDiskCheckTimeout();
uint32 GetDiskCheckBufferSize();
#endif