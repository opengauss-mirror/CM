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
 * cma_alarm.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_alarm.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "securec.h"
#include "alarm/alarm.h"
#include "cm/cm_elog.h"
#include "cm_ddb_adapter.h"
#include "cma_main.h"
#include "cma_global_params.h"
#include "cma_common.h"
#include "cma_alarm.h"

Alarm* g_startupAlarmList = NULL;
int g_startupAlarmListSize = 0;

Alarm* g_abnormalAlarmList = NULL;
int g_abnormalAlarmListSize = 0;

Alarm* g_abnormalCmaConnAlarmList = NULL;
int g_abnormalCmaConnAlarmListSize;

Alarm* g_abnormalBuildAlarmList = NULL;
Alarm* g_abnormalDataInstDiskAlarmList = NULL;
Alarm* g_networkIsolatedAlarmList = NULL;
Alarm* g_diskDamagedAlarmList = NULL;
Alarm* g_slowDiskAlarmList = NULL;
Alarm* g_missingDataDirAlarmList = NULL;
Alarm* g_dataDirOverloadAlarmList = NULL;
int g_datanodeAbnormalAlarmListSize;
int g_datanodeBuildFailedAlarmListSize;
int g_networkIsolatedAlarmListSize;
int g_diskDamagedAlarmListSize;
int g_slowDiskAlarmListSize;
int g_missingDataDirAlarmListSize;
int g_dataDirOverloadAlarmListSize;

Alarm* g_pgxcNodeMismatchAlarm = NULL;
Alarm* g_streamingDRAlarmList = NULL;

static THR_LOCAL Alarm* StorageScalingAlarmList = NULL;

/* init alarm info  for coordinate, datanode, gtm, cmserver */
void StartupAlarmItemInitialize(const staticNodeConfig* currentNode)
{
    g_startupAlarmListSize =
        (int)(currentNode->datanodeCount + currentNode->gtm + currentNode->coordinate + currentNode->cmServerLevel);

    if (g_startupAlarmListSize <= 0) {
        return;
    }

    g_startupAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_startupAlarmListSize);
    if (g_startupAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: StartupAlarmItemInitialize failed.\n");
        exit(1);
    }

    int alarmIndex = g_startupAlarmListSize - 1;
    if (currentNode->gtm == 1) {
        /* ALM_AI_AbnormalGTMProcess */
        AlarmItemInitialize(&(g_startupAlarmList[alarmIndex]), ALM_AI_AbnormalGTMProcess, ALM_AS_Init, NULL);

        --alarmIndex;
    }
    if (currentNode->coordinate == 1) {
        /* ALM_AI_AbnormalCoordinatorProcess */
        AlarmItemInitialize(&(g_startupAlarmList[alarmIndex]), ALM_AI_AbnormalCoordinatorProcess, ALM_AS_Init, NULL);

        --alarmIndex;
    }
    if (currentNode->cmServerLevel == 1) {
        /* ALM_AI_AbnormalCMSProcess */
        AlarmItemInitialize(&(g_startupAlarmList[alarmIndex]), ALM_AI_AbnormalCMSProcess, ALM_AS_Init, NULL);

        --alarmIndex;
    }
    for (; alarmIndex >= 0; --alarmIndex) {
        /* ALM_AI_AbnormalDatanodeProcess */
        AlarmItemInitialize(&(g_startupAlarmList[alarmIndex]), ALM_AI_AbnormalDatanodeProcess, ALM_AS_Init, NULL);
    }
}

/* init alarm info for datanode and gtm */
void AbnormalAlarmItemInitialize(const staticNodeConfig* currentNode)
{
    int alarmIndex;
    g_abnormalAlarmListSize = (int)(currentNode->datanodeCount + currentNode->gtm);

    if (g_abnormalAlarmListSize <= 0) {
        return;
    }

    if (g_single_node_cluster) {
        return;
    }

    g_abnormalAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_abnormalAlarmListSize);
    if (g_abnormalAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: AbnormalAlarmItemInitialize failed.\n");
        exit(1);
    }

    alarmIndex = g_abnormalAlarmListSize - 1;
    if (currentNode->gtm == 1) {
        /* ALM_AI_AbnormalGTMInst */
        AlarmItemInitialize(&(g_abnormalAlarmList[alarmIndex]), ALM_AI_AbnormalGTMInst, ALM_AS_Init, NULL);
        --alarmIndex;
    }
    for (; alarmIndex >= 0; --alarmIndex) {
        /* ALM_AI_AbnormalDatanodeInst */
        AlarmItemInitialize(&(g_abnormalAlarmList[alarmIndex]), ALM_AI_AbnormalDatanodeInst, ALM_AS_Init, NULL);
    }
}

void MissingDataDirAlarmItemInitialize()
{
    g_missingDataDirAlarmListSize = (int) (g_currentNode->datanodeCount);
    g_dataDirOverloadAlarmListSize = (int) (g_currentNode->datanodeCount);
    if (g_missingDataDirAlarmListSize == 0 || g_dataDirOverloadAlarmListSize == 0) {
        return;
    }
    g_missingDataDirAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_missingDataDirAlarmListSize);
    g_dataDirOverloadAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_dataDirOverloadAlarmListSize);
    errno_t rc = memset_s(g_missingDataDirAlarmList, sizeof(Alarm) * (size_t)g_missingDataDirAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_missingDataDirAlarmListSize);
    securec_check_errno(rc, (void)rc);
    rc = memset_s(g_dataDirOverloadAlarmList, sizeof(Alarm) * (size_t)g_dataDirOverloadAlarmListSize, 0,
                  sizeof(Alarm) * (size_t)g_dataDirOverloadAlarmListSize);
    securec_check_errno(rc, (void)rc);
    if (g_missingDataDirAlarmList == NULL || g_dataDirOverloadAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: MissingDataDirAlarmItemInitialize failed.\n");
        exit(1);
    }
    int alarmIndex = g_missingDataDirAlarmListSize - 1;
    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(
            &(g_missingDataDirAlarmList[alarmIndex]), ALM_AI_MissingDataInstDataDir, ALM_AS_Init, NULL);
        AlarmItemInitialize(
            &(g_dataDirOverloadAlarmList[alarmIndex]), ALM_AI_DataDirectoryAccumulate, ALM_AS_Init, NULL);
    }
}

void SlowDiskAlarmItemInitialize()
{
    g_slowDiskAlarmListSize = (int) (g_currentNode->datanodeCount + g_currentNode->coordinate);
    if (g_slowDiskAlarmListSize == 0) {
        return;
    }
    g_slowDiskAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_slowDiskAlarmListSize);
    if (g_slowDiskAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: SlowDiskAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_slowDiskAlarmList, sizeof(Alarm) * (size_t)g_slowDiskAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_slowDiskAlarmListSize);
    securec_check_errno(rc, (void)rc);
    int alarmIndex = g_slowDiskAlarmListSize - 1;
    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(&(g_slowDiskAlarmList[alarmIndex]), ALM_AI_SlowDisk, ALM_AS_Init, NULL);
    }
}

void DatanodeBuildFailedAlarmItemInitialize(const staticNodeConfig* currentNode)
{
    int alarmIndex;
    g_datanodeBuildFailedAlarmListSize = (int) currentNode->datanodeCount + currentNode->coordinate;
    if (g_datanodeBuildFailedAlarmListSize == 0) {
        return;
    }
    g_abnormalBuildAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_datanodeBuildFailedAlarmListSize);
    if (g_abnormalBuildAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: DatanodeBuildFailedAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_abnormalBuildAlarmList, sizeof(Alarm) * (size_t)g_datanodeBuildFailedAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_datanodeBuildFailedAlarmListSize);
    securec_check_errno(rc, (void)rc);

    alarmIndex = g_datanodeBuildFailedAlarmListSize - 1;
    if (currentNode->coordinate == 1) {
        AlarmItemInitialize(&(g_abnormalBuildAlarmList[alarmIndex]), ALM_AI_AbnormalBuild, ALM_AS_Init, NULL);
        --alarmIndex;
    }
    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(&(g_abnormalBuildAlarmList[alarmIndex]), ALM_AI_AbnormalBuild, ALM_AS_Init, NULL);
    }
}

/* init alarm info  for datanode */
void DatanodeAbnormalAlarmItemInitialize(const staticNodeConfig* currentNode)
{
    int alarmIndex;
    g_datanodeAbnormalAlarmListSize = (int)currentNode->datanodeCount;
    if (g_datanodeAbnormalAlarmListSize == 0) {
        return;
    }

    g_abnormalDataInstDiskAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_datanodeAbnormalAlarmListSize);
    if (g_abnormalDataInstDiskAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: DatanodeAbnormalAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_abnormalDataInstDiskAlarmList, sizeof(Alarm) * (size_t)g_datanodeAbnormalAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_datanodeAbnormalAlarmListSize);
    securec_check_errno(rc, (void)rc);

    alarmIndex = g_datanodeAbnormalAlarmListSize - 1;

    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(
            &(g_abnormalDataInstDiskAlarmList[alarmIndex]), ALM_AI_AbnormalDataInstDisk, ALM_AS_Init, NULL);
    }
}

void DiskDamagedAlarmItemInitialize()
{
    int alarmIndex;
    g_diskDamagedAlarmListSize = (int)g_currentNode->datanodeCount;
    if (g_diskDamagedAlarmListSize == 0) {
        return;
    }
    g_diskDamagedAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_diskDamagedAlarmListSize);
    if (g_diskDamagedAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: DiskDamagedAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_diskDamagedAlarmList, sizeof(Alarm) * (size_t)g_diskDamagedAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_diskDamagedAlarmListSize);
    securec_check_errno(rc, (void)rc);
    alarmIndex = g_diskDamagedAlarmListSize - 1;
    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(
            &(g_diskDamagedAlarmList[alarmIndex]), ALM_AI_DiskDamage, ALM_AS_Init, NULL);
    }
}

/* init alarm info  for datanode, coordinate, gtm */
void AbnormalCmaConnAlarmItemInitialize(const staticNodeConfig* currentNode)
{
    g_abnormalCmaConnAlarmListSize = (int)(currentNode->datanodeCount + currentNode->gtm + currentNode->coordinate);

    if (g_abnormalCmaConnAlarmListSize <= 0) {
        return;
    }

    g_abnormalCmaConnAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_abnormalCmaConnAlarmListSize);
    if (g_abnormalCmaConnAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: AbnormalCmaConnAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_abnormalCmaConnAlarmList, sizeof(Alarm) * (size_t)g_abnormalCmaConnAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_abnormalCmaConnAlarmListSize);
    securec_check_errno(rc, (void)rc);

    int alarmIndex = g_abnormalCmaConnAlarmListSize - 1;

    for (unsigned int i = 0; i < currentNode->gtm; i++) {
        /* ALM_AI_AbnormalGTMProcess */
        AlarmItemInitialize(&(g_abnormalCmaConnAlarmList[alarmIndex]), ALM_AI_AbnormalCmaConnFail, ALM_AS_Init, NULL);

        --alarmIndex;
    }

    for (unsigned int i = 0; i < currentNode->coordinate; i++) {
        /* ALM_AI_AbnormalCoordinatorProcess */
        AlarmItemInitialize(&(g_abnormalCmaConnAlarmList[alarmIndex]), ALM_AI_AbnormalCmaConnFail, ALM_AS_Init, NULL);

        --alarmIndex;
    }

    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(&(g_abnormalCmaConnAlarmList[alarmIndex]), ALM_AI_AbnormalCmaConnFail, ALM_AS_Init, NULL);
    }
}

void report_build_fail_alarm(AlarmType alarmType, const char *instanceName, int alarmIndex)
{
    if (alarmIndex >= g_datanodeBuildFailedAlarmListSize) {
        return;
    }
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam, instanceName, "", "", "", &(g_abnormalBuildAlarmList[alarmIndex]),
        alarmType, instanceName);
    /* report the alarm */
    AlarmReporter(&(g_abnormalBuildAlarmList[alarmIndex]), alarmType, &tempAdditionalParam);
}

void report_dn_disk_alarm(AlarmType alarmType, const char *instanceName, int alarmIndex, const char *data_path)
{
    if (alarmIndex >= g_datanodeAbnormalAlarmListSize) {
        return;
    }
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
        instanceName,
        "",
        "",
        "",
        &(g_abnormalDataInstDiskAlarmList[alarmIndex]),
        alarmType,
        instanceName,
        data_path);
    /* report the alarm */
    AlarmReporter(&(g_abnormalDataInstDiskAlarmList[alarmIndex]), alarmType, &tempAdditionalParam);
}

Alarm *GetDdbAlarm(int index, DDB_TYPE dbType)
{
    DdbInitConfig config;
    errno_t rc = memset_s(&config, sizeof(DdbInitConfig), 0, sizeof(config));
    securec_check_errno(rc, (void)rc);
    config.type = dbType;

    DdbDriver* drv = InitDdbDrv(&config);
    if (drv == NULL) {
        write_runlog(ERROR, "InitDdbDrv failed");
        return NULL;
    }

    return DdbGetAlarm(drv, index);
}

void report_ddb_fail_alarm(AlarmType alarmType, const char *instanceName, int alarmIndex, DDB_TYPE dbType)
{
    Alarm* alarm = GetDdbAlarm(alarmIndex, dbType);
    if (alarm == NULL) {
        return;
    }

    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, instanceName, "", "", "", alarm, alarmType, instanceName);
    /* report the alarm */
    AlarmReporter(alarm, alarmType, &tempAdditionalParam);
}

void InitializeAlarmItem(const staticNodeConfig* currentNode)
{
    /* init alarm check, check ALM_AS_Reported state of ALM_AI_AbnormalGTMInst, ALM_AI_AbnormalDatanodeInst */
    AbnormalAlarmItemInitialize(currentNode);
    /* init alarm check, check ALM_AS_Reported state of ALM_AI_AbnormalCmaConnFail */
    AbnormalCmaConnAlarmItemInitialize(currentNode);
    /* init alarm check, check ALM_AS_Reported state of ALM_AI_AbnormalDataInstDisk */
    DatanodeAbnormalAlarmItemInitialize(currentNode);
    /* ALM_AI_AbnormalBuild */
    DatanodeBuildFailedAlarmItemInitialize(currentNode);
    /* init alarm check, check ALM_AI_PgxcNodeMismatch */
    PgxcNodeMismatchAlarmItemInitialize();
    /* init alarm check, check ALM_AI_StreamingDRCnDisconnected, ALM_AI_StreamingDRDnDisconnected */
    StreamingDRAlarmItemInitialize();
    /* init alarm check, check ALM_AI_DatanodeNetworkIsolated */
    DatanodeNetworkIsolatedAlarmItemInitialize();
    /* init alarm check, check ALM_AI_DiskDamage */
    DiskDamagedAlarmItemInitialize();
    /* init alarm check, check ALM_AI_SlowDisk */
    SlowDiskAlarmItemInitialize();
    /* init alarm check, check ALM_AI_MissingDataInstDataDir */
    MissingDataDirAlarmItemInitialize();
}

void DatanodeNetworkIsolatedAlarmItemInitialize()
{
    int alarmIndex;
    g_networkIsolatedAlarmListSize = (int)g_currentNode->datanodeCount;
    if (g_networkIsolatedAlarmListSize == 0) {
        return;
    }
    g_networkIsolatedAlarmList = (Alarm*)malloc(sizeof(Alarm) * (size_t)g_networkIsolatedAlarmListSize);
    if (g_networkIsolatedAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: DatanodeNetworkIsolatedAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_networkIsolatedAlarmList, sizeof(Alarm) * (size_t)g_networkIsolatedAlarmListSize, 0,
                          sizeof(Alarm) * (size_t)g_networkIsolatedAlarmListSize);
    securec_check_errno(rc, (void)rc);
    alarmIndex = g_networkIsolatedAlarmListSize - 1;
    for (; alarmIndex >= 0; --alarmIndex) {
        AlarmItemInitialize(
            &(g_networkIsolatedAlarmList[alarmIndex]), ALM_AI_DatanodeNetworkIsolated, ALM_AS_Init, NULL);
    }
}

void StorageScalingAlarmItemInitialize(void)
{
    static const int StorageScalingAlarmListSize = 2;
    StorageScalingAlarmList = (Alarm*)malloc(sizeof(Alarm) * StorageScalingAlarmListSize);
    if (StorageScalingAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memort: StorageScalingAlarmList failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(StorageScalingAlarmList, sizeof(Alarm) * StorageScalingAlarmListSize, 0,
                          sizeof(Alarm) * StorageScalingAlarmListSize);
    securec_check_errno(rc, (void)rc);

    AlarmItemInitialize(&(StorageScalingAlarmList[0]), ALM_AI_StorageDilatationAlarmNotice, ALM_AS_Init, NULL);
    AlarmItemInitialize(&(StorageScalingAlarmList[1]), ALM_AI_StorageDilatationAlarmMajor, ALM_AS_Init, NULL);
}

void ReportStorageScalingAlarm(AlarmType alarmType, const char* instanceName, int alarmIndex, const char *info)
{
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message. */
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, instanceName, "", "", "", &(StorageScalingAlarmList[alarmIndex]), alarmType, info);
    /* report the alarm. */
    AlarmReporter(&(StorageScalingAlarmList[alarmIndex]), alarmType, &tempAdditionalParam);
}

void ReportPgxcNodeMismatchAlarm(AlarmType alarmType, const char* instanceName)
{
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, instanceName, "", "", "", g_pgxcNodeMismatchAlarm, alarmType, instanceName);
    /* report the alarm */
    AlarmReporter(g_pgxcNodeMismatchAlarm, alarmType, &tempAdditionalParam);
}

void PgxcNodeMismatchAlarmItemInitialize()
{
    g_pgxcNodeMismatchAlarm = (Alarm*)malloc(sizeof(Alarm));
    if (g_pgxcNodeMismatchAlarm == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: PgxcNodeMismatchAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_pgxcNodeMismatchAlarm, sizeof(Alarm), 0, sizeof(Alarm));
    securec_check_errno(rc, (void)rc);

    AlarmItemInitialize(g_pgxcNodeMismatchAlarm, ALM_AI_PgxcNodeMismatch, ALM_AS_Init, NULL);
}

void ReportStreamingDRAlarm(AlarmType alarmType, const char *instanceName, int alarmIndex, const char *info)
{
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(
        &tempAdditionalParam, instanceName, "", "", "", &(g_streamingDRAlarmList[alarmIndex]), alarmType, info);
    /* report the alarm */
    AlarmReporter(&(g_streamingDRAlarmList[alarmIndex]), alarmType, &tempAdditionalParam);
}

void StreamingDRAlarmItemInitialize(void)
{
    int32 alarmIndex;
    uint32 streamingDRAlarmListSize = g_currentNode->datanodeCount + g_currentNode->coordinate;
    if (streamingDRAlarmListSize == 0) {
        return;
    }
    g_streamingDRAlarmList = (Alarm*)malloc(sizeof(Alarm) * streamingDRAlarmListSize);
    if (g_streamingDRAlarmList == NULL) {
        AlarmLog(ALM_LOG, "Out of memory: AbnormalAlarmItemInitialize failed.\n");
        exit(1);
    }
    errno_t rc = memset_s(g_streamingDRAlarmList, sizeof(Alarm) * streamingDRAlarmListSize, 0,
                          sizeof(Alarm) * streamingDRAlarmListSize);
    securec_check_errno(rc, (void)rc);
    alarmIndex = (int32)(streamingDRAlarmListSize - 1);
    if (g_currentNode->coordinate == 1) {
        /* ALM_AI_AbnormalGTMInst */
        AlarmItemInitialize(&(g_streamingDRAlarmList[alarmIndex]), ALM_AI_StreamingDisasterRecoveryCnDisconnected,
            ALM_AS_Init, NULL);
        --alarmIndex;
    }
    for (; alarmIndex >= 0; --alarmIndex) {
        /* ALM_AI_AbnormalDatanodeInst */
        AlarmItemInitialize(&(g_streamingDRAlarmList[alarmIndex]), ALM_AI_StreamingDisasterRecoveryDnDisconnected,
            ALM_AS_Init, NULL);
    }
}

void ReportMemoryAbnormalAlarm(int sysMemUsed, int appMemUsed, int threshold)
{
    Alarm memoryAlarm[1];
    AlarmAdditionalParam tempAdditionalParam;
    // Initialize the alarm item
    AlarmItemInitialize(memoryAlarm, ALM_AI_MemoryUsageAbnormal, ALM_AS_Init, NULL);
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             memoryAlarm,
                             ALM_AT_Event,
                             sysMemUsed,
                             appMemUsed,
                             threshold,
                             g_myHostName);
    /* report the alarm */
    AlarmReporter(memoryAlarm, ALM_AT_Event, &tempAdditionalParam);
}

void ReportCpuAbnormalAlarm(int cpuUsed, int threshold)
{
    Alarm cpuAlarm[1];
    AlarmAdditionalParam tempAdditionalParam;
    // Initialize the alarm item
    AlarmItemInitialize(cpuAlarm, ALM_AI_CpuUsageAbnormal, ALM_AS_Init, NULL);
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             cpuAlarm,
                             ALM_AT_Event,
                             cpuUsed,
                             threshold,
                             g_myHostName);
    /* report the alarm */
    AlarmReporter(cpuAlarm, ALM_AT_Event, &tempAdditionalParam);
}

void ReportDiskIOAbnormalAlarm(const char* diskName, int ioUsed, int threshold)
{
    Alarm diskIOAlarm[1];
    AlarmAdditionalParam tempAdditionalParam;
    // Initialize the alarm item
    AlarmItemInitialize(diskIOAlarm, ALM_AI_DiskIOAbnormal, ALM_AS_Init, NULL);
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             diskIOAlarm,
                             ALM_AT_Event,
                             diskName,
                             ioUsed,
                             threshold,
                             g_myHostName);
    /* report the alarm */
    AlarmReporter(diskIOAlarm, ALM_AT_Event, &tempAdditionalParam);
}


void ReportDNDisconnectAlarm(AlarmType alarmType, const char *instanceName, int alarmIndex)
{
    if (alarmIndex >= g_networkIsolatedAlarmListSize) {
        return;
    }
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             instanceName,
                             "",
                             "",
                             "",
                             &(g_networkIsolatedAlarmList[alarmIndex]),
                             alarmType,
                             instanceName);
    /* report the alarm */
    AlarmReporter(&(g_networkIsolatedAlarmList[alarmIndex]), alarmType, &tempAdditionalParam);
}

void ReportDiskUsageAbnormalAlarm(const char* diskName, int diskUsed, int threshold)
{
    Alarm diskUsageAlarm[1];
    AlarmAdditionalParam tempAdditionalParam;
    // Initialize the alarm item
    AlarmItemInitialize(diskUsageAlarm, ALM_AI_DiskUsageAbnormal, ALM_AS_Init, NULL);
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             diskUsageAlarm,
                             ALM_AT_Event,
                             diskName,
                             diskUsed,
                             threshold,
                             g_myHostName);
    /* report the alarm */
    AlarmReporter(diskUsageAlarm, ALM_AT_Event, &tempAdditionalParam);
}

void ReportDiskDamageAlarm(AlarmType alarmType, const char *instanceName, int alarmIndex, const char* details)
{
    if (alarmIndex >= g_diskDamagedAlarmListSize) {
        return;
    }
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             instanceName,
                             "",
                             "",
                             "",
                             &(g_diskDamagedAlarmList[alarmIndex]),
                             alarmType,
                             instanceName,
                             details);
    /* report the alarm */
    AlarmReporter(&(g_diskDamagedAlarmList[alarmIndex]), alarmType, &tempAdditionalParam);
}

void ReportSlowDiskAlarm(const char* diskName, AlarmType alarmType, int index, const char* details)
{
    if (index >= g_slowDiskAlarmListSize) {
        return;
    }
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             &(g_slowDiskAlarmList[index]),
                             alarmType,
                             diskName,
                             details);
    /* report the alarm */
    AlarmReporter(&(g_slowDiskAlarmList[index]), alarmType, &tempAdditionalParam);
}

void ReportDiskHangAlarm(Alarm* alarm, AlarmType alarmType, const char* diskName, uint64 costTime, uint64 timeout)
{
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    char details[MAX_PATH_LEN] = {0};
    errno_t rc = snprintf_s(details, MAX_PATH_LEN, MAX_PATH_LEN - 1, "disk(%s) read/write costTime=%lu, timeout=%lu",
        diskName, costTime, timeout);
    securec_check_intval(rc, (void)rc);
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             alarm,
                             alarmType,
                             g_currentNode->nodeName,
                             details);
    /* report the alarm */
    AlarmReporter(alarm, alarmType, &tempAdditionalParam);
}

void ReportDiskSlowAlarm(Alarm* alarm, AlarmType alarmType, uint64 costTime, uint64 threshold, const char* dirPath)
{
    AlarmAdditionalParam tempAdditionalParam;
    /* fill the alarm message */
    char details[MAX_PATH_LEN] = {0};
    errno_t rc = snprintf_s(details, MAX_PATH_LEN, MAX_PATH_LEN - 1, "data path(%s) read/write costTime=%lu, "
        "threshold=%lu", dirPath, costTime, threshold);
    securec_check_intval(rc, (void)rc);
    WriteAlarmAdditionalInfo(&tempAdditionalParam,
                             "",
                             "",
                             "",
                             "",
                             alarm,
                             alarmType,
                             g_currentNode->nodeName,
                             details);
    /* report the alarm */
    AlarmReporter(alarm, alarmType, &tempAdditionalParam);
}