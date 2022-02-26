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
#define FILE_CPUSTAT "/proc/stat"
#define FILE_DISKSTAT "/proc/diskstats"
#define FILE_MOUNTS "/proc/mounts"
#define FILE_MEMINFO "/proc/meminfo"

#define ETCD_NODE_UNHEALTH_FRE 15
#define CHECK_INVALID_ETCD_TIMES 15

/* when report_interval has changed to bigger ,this number 3 will also change */
#define CHECK_DUMMY_STATE_TIMES 3
#define PERCENT (100)

typedef struct IoStat {
    uint64 idle;
    uint64 tot_ticks;
    uint64 uptime;
} IoStat;

void DatanodeStatusReport(void);
void fenced_UDF_status_check_and_report(void);
void etcd_status_check_and_report(void);
void kerberos_status_check_and_report();
void CheckDiskForDNDataPathAndReport(int logLevel);

void* ETCDStatusCheckMain(void* arg);
void* ETCDConnectionStatusCheckMain(void * const arg);
void* DNStatusCheckMain(void * const arg);
void* DNConnectionStatusCheckMain(void * const arg);

void* KerberosStatusCheckMain(void * const arg);
void InitResourceGlobalVal(void);
void *ResourceStatusCheckMain(void * const arg);
void CheckResourceState(cma_resource_status_msg *report_msg);

int CreateCheckNodeStatusThread(void);

#endif