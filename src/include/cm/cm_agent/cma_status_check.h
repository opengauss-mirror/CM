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
 * cma_status_check.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_agent/cma_status_check.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMA_STATUS_CHECK_H
#define CMA_STATUS_CHECK_H

#define DN_RESTART_COUNT_CHECK_TIME 600
#define DN_RESTART_COUNT_CHECK_TIME_HOUR 3600

#define MAX_COMMAND_LEN 1024
#define MAX_COMMAND_PATH 512
#define MAX_DEVICE_DIR 1024
#define MAX_DEVICE_STAT_INDEX 14
#define FILE_CPUSTAT "/proc/stat"
#define FILE_DISKSTAT "/proc/diskstats"
#define FILE_MOUNTS "/proc/mounts"
#define FILE_MEMINFO "/proc/meminfo"

#define ETCD_NODE_UNHEALTH_FRE 15
#define CHECK_INVALID_ETCD_TIMES 15

/* when report_interval has changed to bigger ,this number 3 will also change */
#define CHECK_DUMMY_STATE_TIMES 3
#define PERCENT (100)
#include <deque>

typedef enum {
    SVCTM_LEVEL_SLIGHT = 0,
    SVCTM_LEVEL_MODERATE,
    SVCTM_LEVEL_SERIOUS,
    SVCTM_LEVEL_CEIL,
} SlowIoLevel;

typedef struct {
    SlowIoLevel level;
    float threshold;
    uint32 weight;
} SlowIotLevelInfo;

typedef struct {
    uint64 idle;
    uint64 tot_ticks;
    uint64 uptime;
} IoStat;

typedef enum {
    MEM_STAT_TOTAL = 0,
    MEM_STAT_FREE = 1,
    MEM_STAT_AVAILABLE = 2,
    MEM_STAT_BUFFERS = 3,
    MEM_STAT_CACHED = 4,
    MEM_STAT_BUTT = 5,
} MemStatItem;

typedef struct {
    const char* name;
    const char* info;
    MemStatItem item;
} MemCheckInfo;

typedef struct {
    uint64 cpuUser;
    uint64 cpuNice;
    uint64 cpuSys;
    uint64 cpuIdle;
    uint64 cpuIwait;
    uint64 cpuHardirq;
    uint64 cpuSoftirq;
    uint64 cpuSteal;
    uint64 cpuGuest;
    uint64 cpuGuestNice;
} CpuInfo;

typedef struct {
    uint64 cpuIdleTime;
    uint64 cpuUserTime;
    uint64 cpuSystemTime;
    uint64 cpuTotalTime;
    CpuInfo cpuInfo;
} CpuSimpleInfo;

typedef struct {
    char diskName[MAX_DEVICE_DIR];
    float ioUtil;
    float svctm;
    float lastReportIoUtil;
    std::deque<uint32> weightWindow;
    uint64 totalWeight;
    uint64 lastReadCount;
    uint64 lastWriteCount;
    uint64 lastCheckTime;
    uint64 lastIoTime;
    uint64 lastReportTime;
} DisIoStatInfo;

typedef struct {
    uint64 memItemList[MEM_STAT_BUTT];
    float systemMemUsedUtil;
    float appMemUsedUtil;
    uint64 lastReportTime;
} MemoryStatInfo;

typedef struct {
    CpuSimpleInfo oldCpuInfo;
    float cpuUtil;
    float cpuUserUtil;
    float cpuSystemUtil;
    uint64 lastReportTime;
} CpuStatInfo;

typedef struct {
    DisIoStatInfo* disIoStatInfo;
    uint64 diskCount;
    uint64 diskDetailReportTime;
    char diskLogPath[MAX_PATH_LEN];
} DiskStatInfo;

typedef struct {
    MemoryStatInfo memoryStatInfo;
    CpuStatInfo cpuStatInfo;
    DiskStatInfo diskStatInfo;
} SystemStatInfo;

void GetDiskNameByDataPath(const char* datadir, char* devicename, uint32 nameLen);
void DatanodeStatusReport(void);
void fenced_UDF_status_check_and_report(void);
void etcd_status_check_and_report(void);
void kerberos_status_check_and_report();
void SendResStatReportMsg();
void SendResIsregReportMsg();
void InitIsregCheckVar();
void UpdateIsregCheckList(const uint32 *newCheckList, uint32 newCheckCount);

void* ETCDStatusCheckMain(void* arg);
void* ETCDConnectionStatusCheckMain(void *arg);
void* DNStatusCheckMain(void *arg);
void* WRFloatIpCheckMain(void *arg);
void* DNConnectionStatusCheckMain(void *arg);

void* KerberosStatusCheckMain(void *arg);
void *ResourceStatusCheckMain(void *arg);
void *ResourceIsregCheckMain(void *arg);
void CheckResourceState(OneNodeResourceStatus *nodeStat);
void InitResStatCommInfo(OneNodeResourceStatus *nodeStat);

int CreateCheckNodeStatusThread(void);
int CreateCheckSysStatusThread(void);
void *VotingDiskMain(void *arg);

#ifdef ENABLE_XALARMD
#ifdef __cplusplus
extern "C" {
#endif
#include <xalarm/register_xalarm.h>
#ifdef __cplusplus
}
#endif
/*
 * Handle xalarm call back
 */
void HandleXalarm(struct alarm_info *param);
#endif

#endif
