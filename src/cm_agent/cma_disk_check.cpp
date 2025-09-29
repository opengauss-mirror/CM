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
 * cma_disk_check.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_disk_check.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cm/cm_util.h"
#include "cm/cm_msg.h"
#include "cma_status_check.h"
#include "cma_global_params.h"
#include "cma_disk_check.h"
#include "alarm/alarm.h"
#ifdef ENABLE_XALARMD
#ifdef __cplusplus
extern "C" {
#endif
#include <xalarm/register_xalarm.h>
#ifdef __cplusplus
}
#endif
#include "cjson/cJSON.h"
int g_xalarmClientId = -1;
static const uint64 XALARM_SLOW_DISK_ID = 1002;
#endif

static uint32 g_diskCheckTimeout = DISK_CHECK_TIMEOUT_DEFAULT;
static uint32 g_diskCheckInterval = DISK_CHECK_INTERVAL_DEFAULT;
static uint32 g_diskCheckBufferSize = DISK_CHECK_BUFFER_SIZE_DEFAULT;
static char* g_diskCheckBuffer = NULL;
static pthread_rwlock_t g_diskCheckBufferLock = PTHREAD_RWLOCK_INITIALIZER;

static DiskHealth g_diskHealth = {0};

void LoadDiskCheckConfig(const char *configFile)
{
    g_diskCheckTimeout = get_uint32_value_from_config(configFile, "disk_check_timeout", DISK_CHECK_TIMEOUT_DEFAULT);
    g_diskCheckInterval = get_uint32_value_from_config(configFile, "disk_check_interval", DISK_CHECK_INTERVAL_DEFAULT);
    g_diskCheckBufferSize = get_uint32_value_from_config(configFile, "disk_check_buffer_size",
                                                         DISK_CHECK_BUFFER_SIZE_DEFAULT);
    char enableXalarmStr[MAXPGPATH] = {0};
    if (get_config_param(configFile, "enable_xalarmd_slow_disk_check", enableXalarmStr, sizeof(enableXalarmStr)) >= 0) {
        g_enableXalarmdFeature = IsBoolCmParamTrue(enableXalarmStr);
        write_runlog(LOG, "enable_xalarmd_slow_disk_check is set to %s\n", g_enableXalarmdFeature ? "true" : "false");
    }
    pthread_rwlock_wrlock(&g_diskCheckBufferLock);
    FREE_AND_RESET(g_diskCheckBuffer);
    if (g_diskCheckBufferSize <= 0) {
        write_runlog(ERROR, "Invalid disk check buffer size %d, use default size %d", g_diskCheckBufferSize,
                     DISK_CHECK_BUFFER_SIZE_DEFAULT);
        g_diskCheckBufferSize = DISK_CHECK_BUFFER_SIZE_DEFAULT;
    }
    g_diskCheckBuffer = (char *) malloc(sizeof(char) * g_diskCheckBufferSize);
    if (g_diskCheckBuffer == NULL) {
        write_runlog(ERROR, "Out of memory, use default check buffer size %d", DISK_CHECK_BUFFER_SIZE_DEFAULT);
        g_diskCheckBufferSize = DISK_CHECK_BUFFER_SIZE_DEFAULT;
        g_diskCheckBuffer = (char *) CmMalloc(sizeof(char) * g_diskCheckBufferSize);
    } else {
        errno_t rc = memset_s(g_diskCheckBuffer, sizeof(g_diskCheckBuffer), 0, sizeof(g_diskCheckBuffer));
        securec_check_errno(rc, (void)rc);
    }
    pthread_rwlock_unlock(&g_diskCheckBufferLock);
}

bool IsDiskNameSame(const char *diskName, uint32 diskCount, char* const* diskNames)
{
    for (uint32 i = 0; i < diskCount; ++i) {
        if (strcmp(diskName, diskNames[i]) == 0) {
            return true;
        }
    }
    return false;
}

bool IsDiskHasDir(const char *dirPath, const char* diskName)
{
    char name[MAX_DEVICE_DIR] = {0};
    GetDiskNameByDataPath(dirPath, name, MAX_DEVICE_DIR);
    if (name[0] == '\0') {
        return false;
    }
    return strcmp(name, diskName) == 0;
}

uint32 GetDirCountByDisk(const char *diskName)
{
    uint32 dirCount = 0;
    if (IsDiskHasDir(g_logBasePath, diskName)) {
        dirCount++;
    }
    if (IsDiskHasDir(g_currentNode->cmDataPath, diskName)) {
        dirCount++;
    }
    for (uint32 i = 0; i < g_currentNode->datanodeCount; i++) {
        if (IsDiskHasDir(g_currentNode->datanode[i].datanodeLocalDataPath, diskName)) {
            dirCount++;
        }
    }
    return dirCount;
}

status_t GetDiskNameByPath(const char *diskPath, uint32* diskCount, char** diskNames)
{
    char tmpName[MAX_DEVICE_DIR] = {0};
    GetDiskNameByDataPath(diskPath, tmpName, MAX_DEVICE_DIR);
    if (tmpName[0] == '\0') {
        write_runlog(ERROR, "Failed to get disk name by path %s\n", diskPath);
        return CM_ERROR;
    }
    if (!IsDiskNameSame(tmpName, *diskCount, diskNames)) {
        diskNames[*diskCount] = strdup(tmpName);
        if (diskNames[*diskCount] == NULL) {
            write_runlog(ERROR, "Out of memory, get path(%s) disk name failed.\n", diskPath);
            return CM_ERROR;
        }
        ++(*diskCount);
    }
    return CM_SUCCESS;
}

char** GetAllDiskNames(uint32 *diskCount)
{
    // log and cmData
    const uint32 other = 2;
    (*diskCount) = 0;
    char** diskNames = (char**) CmMalloc(sizeof(char *) * (g_currentNode->datanodeCount + other));
    for (uint32 i = 0; i < g_currentNode->datanodeCount; i++) {
        if (GetDiskNameByPath(g_currentNode->datanode[i].datanodeLocalDataPath, diskCount, diskNames) != CM_SUCCESS) {
            FreePtr2Ptr(diskNames, (*diskCount));
            (*diskCount) = 0;
            return NULL;
        }
    }
    if (GetDiskNameByPath(g_logBasePath, diskCount, diskNames)!= CM_SUCCESS) {
        FreePtr2Ptr(diskNames, (*diskCount));
        (*diskCount) = 0;
        return NULL;
    }
    if (GetDiskNameByPath(g_currentNode->cmDataPath, diskCount, diskNames)!= CM_SUCCESS) {
        FreePtr2Ptr(diskNames, (*diskCount));
        (*diskCount) = 0;
        return NULL;
    }
    return diskNames;
}

void InitDirAlarmItem(DirHealth* dirHealth)
{
    for (uint32 i = 0; i < dirHealth->dirCount; ++i) {
        Alarm* diskAlarm = dirHealth->dir[i].diskAlarm;
        AlarmItemInitialize(&(diskAlarm[DISK_ALARM_READ_WRITE_SLOW]), ALM_AI_DiskReadWriteSlow, ALM_AS_Init, NULL);
        AlarmItemInitialize(&(diskAlarm[DISK_ALARM_HUNG]), ALM_AI_DiskHang, ALM_AS_Init, NULL);
    }
}

status_t InitDirHealthCtx(const char* diskName, DirHealth* dirHealth)
{
    uint32 dirCount = GetDirCountByDisk(diskName);
    dirHealth->dirCount = dirCount;
    if (dirCount == 0) {
        write_runlog(ERROR, "No dir in disk(%s).\n", diskName);
        return CM_ERROR;
    }
    dirHealth->dir = (DirHealthItem*)malloc(sizeof(DirHealthItem) * dirCount);
    if (dirHealth->dir == NULL) {
        write_runlog(ERROR, "[%s] Out of memory, get disk(%s) dir info failed.\n", __FUNCTION__, diskName);
        return CM_ERROR;
    }
    errno_t rc = memset_s(dirHealth->dir, sizeof(DirHealthItem) * dirCount, 0, sizeof(DirHealthItem) * dirCount);
    securec_check_errno(rc, (void)rc);

    uint32 index = 0;
    for (uint32 i = 0; i < g_currentNode->datanodeCount; i++) {
        const char* dnDataPath = g_currentNode->datanode[i].datanodeLocalDataPath;
        if (IsDiskHasDir(dnDataPath, diskName)) {
            dirHealth->dir[index].instanceType = INSTANCE_TYPE_DATANODE;
            dirHealth->dir[index].diskAlarm = (Alarm*) CmMalloc(sizeof(Alarm) * DISK_ALARM_CEIL);
            rc = strcpy_s(dirHealth->dir[index].path, MAX_DEVICE_DIR, dnDataPath);
            securec_check_errno(rc, (void)rc);
            index++;
        }
    }
    if (IsDiskHasDir(g_logBasePath, diskName)) {
        dirHealth->dir[index].instanceType = INSTANCE_TYPE_LOG;
        dirHealth->dir[index].diskAlarm = (Alarm*) CmMalloc(sizeof(Alarm) * DISK_ALARM_CEIL);
        rc = strcpy_s(dirHealth->dir[index].path, MAX_DEVICE_DIR, g_logBasePath);
        securec_check_errno(rc, (void)rc);
        index++;
    }
    if (IsDiskHasDir(g_currentNode->cmDataPath, diskName)) {
        dirHealth->dir[index].instanceType = INSTANCE_TYPE_CM;
        dirHealth->dir[index].diskAlarm = (Alarm*) CmMalloc(sizeof(Alarm) * DISK_ALARM_CEIL);
        rc = strcpy_s(dirHealth->dir[index].path, MAX_DEVICE_DIR, g_currentNode->cmDataPath);
        securec_check_errno(rc, (void)rc);
        index++;
    }
    InitDirAlarmItem(dirHealth);
    return CM_SUCCESS;
}

status_t InitDiskHealthCtx()
{
    uint32 diskCount = 0;
    char** diskNames = GetAllDiskNames(&diskCount);
    if (diskNames == NULL || diskCount == 0) {
        write_runlog(ERROR, "[%s] Failed to get disk info.\n", __FUNCTION__);
        return CM_ERROR;
    }
    g_diskHealth.diskCount = diskCount;
    g_diskHealth.disk = (DiskHealthItem*)malloc(sizeof(DiskHealthItem) * diskCount);
    if (g_diskHealth.disk == NULL) {
        write_runlog(ERROR, "[%s] Out of memory, get disk info failed.\n", __FUNCTION__);
        FreePtr2Ptr(diskNames, diskCount);
        return CM_ERROR;
    }
    errno_t rc = memset_s(g_diskHealth.disk, sizeof(DiskHealthItem) * diskCount, 0, sizeof(DiskHealthItem) * diskCount);
    securec_check_errno(rc, (void)rc);
    for (uint32 i = 0; i < diskCount; i++) {
        rc = strcpy_s(g_diskHealth.disk[i].diskName, MAX_DEVICE_DIR, diskNames[i]);
        securec_check_errno(rc, (void)rc);
        if (InitDirHealthCtx(g_diskHealth.disk[i].diskName, &g_diskHealth.disk[i].dirHealth)!= CM_SUCCESS) {
            write_runlog(ERROR, "Failed to init dir health check context.\n");
            FreePtr2Ptr(diskNames, diskCount);
            return CM_ERROR;
        }
        for (uint32 j = 0; j < g_diskHealth.disk[i].dirHealth.dirCount; ++j) {
            g_diskHealth.disk[i].dirHealth.dir[j].latestIoTime = GetMonotonicTimeMs();
        }
    }
    FreePtr2Ptr(diskNames, diskCount);
    return CM_SUCCESS;
}

char* DirStatusToString(DirStatus status)
{
    switch (status) {
        case DIR_STAT_INIT:
            return "init";
        case DIR_STAT_UNKNOWN:
            return "unknown";
        case DIR_STAT_NORMAL:
            return "normal";
        case DIR_STAT_NOT_EXIST:
            return "not exist";
        case DIR_STAT_NOT_DIR:
            return "not dir";
        case DIR_STAT_PERMISSION_DENIED:
            return "permission denied";
        default:
            break;
    }
    return "error";
}

char* DiskStatusToString(DiskStatus status)
{
    switch (status) {
        case DISK_STAT_INIT:
            return "init";
        case DISK_STAT_UNKNOWN:
            return "unknown";
        case DISK_STAT_NORMAL:
            return "normal";
        case DISK_STAT_HUNG:
            return "hung";
        default:
            break;
    }
    return "error";
}

void SetDirStatus(DirStatus* currentStatus, DirStatus newStatus, const char* dirPath)
{
    if ((*currentStatus) != newStatus) {
        write_runlog(LOG, "The status of dir(%s) changed from %s to %s.\n", dirPath, DirStatusToString(*currentStatus),
                     DirStatusToString(newStatus));
        *currentStatus = newStatus;
    }
}

void SetDiskStatus(DiskStatus* currentStatus, DiskStatus newStatus, const char* dirPath)
{
    if ((*currentStatus)!= newStatus) {
        write_runlog(LOG, "The status of disk(%s) changed from %s to %s.\n", dirPath,
            DiskStatusToString(*currentStatus), DiskStatusToString(newStatus));
        *currentStatus = newStatus;
    }
}

void CheckDirHealth(DirHealthItem* dirHealthItem)
{
    struct stat statBuf = {0};
    if (stat(dirHealthItem->path, &statBuf) != 0) {
        SetDirStatus(&dirHealthItem->dirStatus, DIR_STAT_NOT_EXIST, dirHealthItem->path);
        return;
    }
    if (!S_ISDIR(statBuf.st_mode)) {
        SetDirStatus(&dirHealthItem->dirStatus, DIR_STAT_NOT_DIR, dirHealthItem->path);
        return;
    }
    if ((statBuf.st_mode & S_IRWXU) != S_IRWXU) {
        SetDirStatus(&dirHealthItem->dirStatus, DIR_STAT_PERMISSION_DENIED, dirHealthItem->path);
        return;
    }
    SetDirStatus(&dirHealthItem->dirStatus, DIR_STAT_NORMAL, dirHealthItem->path);
}

DiskStatus GetDiskStatusByErrno(int err)
{
    switch (err) {
        case 0:
            return DISK_STAT_NORMAL;
        case EROFS:
            return DISK_STAT_READONLY;
        case EIO:
            return DISK_STAT_IO_ERROR;
        case ENOSPC:
            return DISK_STAT_NO_SPACE;
        default:
            break;
    }
    return DISK_STAT_BROKEN;
}

void CheckDiskRWSlow(DirHealthItem* dirHealthItem)
{
    uint64 costTime = (GetMonotonicTimeMs() - dirHealthItem->latestIoTime);
    AlarmType alarmType = ALM_AT_Resume;
    uint64 threshold = (g_diskCheckTimeout == 1) ? 1 : g_diskCheckTimeout * 4 / 5;
    if (threshold > 0 && costTime > threshold) {
        alarmType = ALM_AT_Fault;
    }
    ReportDiskSlowAlarm(&(dirHealthItem->diskAlarm[DISK_ALARM_READ_WRITE_SLOW]),
                        alarmType, costTime, threshold, dirHealthItem->path);
}

FILE* CheckOpenTestFile(const char* testFile, const char* openType, DirHealthItem* dirHealthItem, int* saveErrno)
{
    errno = 0;
    dirHealthItem->latestIoTime = GetMonotonicTimeMs();
    FILE* fp = fopen(testFile, openType);
    if (fp == NULL) {
        int tempErrno = errno;
        if (tempErrno == EMFILE) {
            write_runlog(ERROR, "To many files open, cma will exit.\n");
            _exit(1);
        }
        write_runlog(LOG, "Failed to open file(%s) with errno(%d).\n", testFile, tempErrno);
        if (tempErrno == EACCES || tempErrno == EROFS || tempErrno == EIO || tempErrno == ENOSPC
            || tempErrno == ENOENT) {
            (*saveErrno) = tempErrno;
            return NULL;
        }
    }
    (*saveErrno) = 0;
    return fp;
}

int CheckWriteTestFile(FILE* fp, uint32* writeSize, DirHealthItem* dirHealthItem)
{
    errno = 0;
    (void)pthread_rwlock_rdlock(&g_diskCheckBufferLock);
    dirHealthItem->latestIoTime = GetMonotonicTimeMs();
    size_t len = fwrite(g_diskCheckBuffer, g_diskCheckBufferSize, 1, fp);
    CheckDiskRWSlow(dirHealthItem);
    dirHealthItem->latestIoTime = GetMonotonicTimeMs();
    int saveErrno = errno;
    (*writeSize) = g_diskCheckBufferSize;
    (void)pthread_rwlock_unlock(&g_diskCheckBufferLock);
    if (len != 1) {
        write_runlog(ERROR, "Failed to write test file with errno(%d).\n", saveErrno);
        if (saveErrno == EROFS || saveErrno == EIO) {
            return saveErrno;
        } else {
            return 0;
        }
    }
    if (fsync(fileno(fp)) != 0) {
        write_runlog(ERROR, "Failed to fsync test file with errno(%d).\n", errno);
        return errno;
    }
    return 0;
}

int CheckReadTestFile(FILE* fp, uint32 readSize, DirHealthItem* dirHealthItem)
{
    char* readBuf = (char*)CmMalloc(sizeof(char) * readSize);
    errno = 0;
    dirHealthItem->latestIoTime = GetMonotonicTimeMs();
    size_t len = fread(readBuf, readSize, 1, fp);
    CheckDiskRWSlow(dirHealthItem);
    dirHealthItem->latestIoTime = GetMonotonicTimeMs();
    int saveErrno = errno;
    FREE_AND_RESET(readBuf);
    if (len != 1) {
        write_runlog(LOG, "Failed to read test file with errno(%d).\n", saveErrno);
        if (saveErrno == ENOSPC || saveErrno == EACCES) {
            return saveErrno;
        }
    }
    return 0;
}

int CheckWriteAndReadTestFile(const char* testFile, DirHealthItem* dirHealthItem)
{
    uint32 writeSize = 0;
    int err = 0;
    FILE* fp = CheckOpenTestFile(testFile, "we", dirHealthItem, &err);
    if (fp == NULL) {
        return err;
    }
    err = CheckWriteTestFile(fp, &writeSize, dirHealthItem);
    (void)fclose(fp);
    if (err != 0) {
        return err;
    }
    FILE* readFp = CheckOpenTestFile(testFile, "re", dirHealthItem, &err);
    if (readFp == NULL) {
        return err;
    }
    err = CheckReadTestFile(readFp, writeSize, dirHealthItem);
    (void)fclose(readFp);
    (void)remove(testFile);
    return err;
}

void CheckDiskHealth(DirHealthItem* dirHealthItem)
{
    if (dirHealthItem->dirStatus!= DIR_STAT_NORMAL) {
        write_runlog(DEBUG5, "Dir(%s) status(%d) is not normal, skip disk health check.\n",
            dirHealthItem->path, dirHealthItem->dirStatus);
        if (dirHealthItem->diskStatus == DISK_STAT_NORMAL) {
            SetDiskStatus(&dirHealthItem->diskStatus, DISK_STAT_UNKNOWN, dirHealthItem->path);
        }
        return;
    }
    char testFile[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(testFile, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s", dirHealthItem->path, DISK_TEST_FILENAME);
    securec_check_intval(ret, (void)ret);
    if (testFile[0] != '\0') {
        int err = CheckWriteAndReadTestFile(testFile, dirHealthItem);
        SetDiskStatus(&dirHealthItem->diskStatus, GetDiskStatusByErrno(err), dirHealthItem->path);
    } else {
        write_runlog(ERROR, "The test file of (%s) is empty, skip disk health check.\n", dirHealthItem->path);
    }
}

void* DiskItemHealthCheckMain(void* arg)
{
    DiskHealthItem* diskHealthItem = (DiskHealthItem*)arg;
    write_runlog(LOG, "Disk health check for disk(%s) started.\n", diskHealthItem->diskName);
    while (true) {
        if (g_shutdownRequest || g_exitFlag) {
            break;
        }
        if (g_diskCheckInterval == 0 || g_diskCheckBufferSize == 0 || g_diskCheckBuffer == NULL) {
            write_runlog(DEBUG5, "Disk health check config is invalid, g_diskCheckInterval=%u, "
                                 "g_diskCheckBufferSize=%u.\n", g_diskCheckInterval, g_diskCheckBufferSize);
            cm_sleep(1);
            continue;
        }
        for (uint32 i = 0; i < diskHealthItem->dirHealth.dirCount; i++) {
            CheckDirHealth(&diskHealthItem->dirHealth.dir[i]);
            CheckDiskHealth(&diskHealthItem->dirHealth.dir[i]);
        }
        cm_sleep(g_diskCheckInterval);
    }
    write_runlog(LOG, "Disk health check thread for disk(%s) exit.\n", diskHealthItem->diskName);
    return NULL;
}

status_t CreateDiskItemHealthCheckThread(DiskHealthItem* diskHealthItem)
{
    pthread_t threadId;
    if (pthread_create(&threadId, NULL, DiskItemHealthCheckMain, diskHealthItem) != 0) {
        write_runlog(ERROR, "Failed to create disk health check thread for disk(%s).\n", diskHealthItem->diskName);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

void CheckOneDiskHung(uint32 i, uint32 j)
{
    uint64 curTime = GetMonotonicTimeMs();
    uint64 costTime = (curTime - g_diskHealth.disk[i].dirHealth.dir[j].latestIoTime);
    AlarmType alarmType = ALM_AT_Resume;
    if (costTime > g_diskCheckTimeout) {
        g_diskHealth.disk[i].dirHealth.dir[j].diskStatus = DISK_STAT_HUNG;
        alarmType = ALM_AT_Fault;
    }
    ReportDiskHangAlarm(&(g_diskHealth.disk[i].dirHealth.dir[j].diskAlarm[DISK_ALARM_HUNG]), alarmType,
        g_diskHealth.disk[i].diskName, costTime, g_diskCheckTimeout);
}

void* DiskHealthCheckMonitorMain(void* arg)
{
    write_runlog(LOG, "Disk health check monitor thread started.\n");
    while (true) {
        if (g_shutdownRequest || g_exitFlag) {
            break;
        }
        for (uint32 i = 0; i < g_diskHealth.diskCount; i++) {
            for (uint32 j = 0; j < g_diskHealth.disk[i].dirHealth.dirCount; j++) {
                CheckOneDiskHung(i, j);
            }
        }
        cm_sleep(1);
    }
    write_runlog(LOG, "Disk health check monitor thread exit.\n");
    return NULL;
}

status_t CreateDiskHealthCheckMonitorThread()
{
    pthread_t threadId;
    if (pthread_create(&threadId, NULL, DiskHealthCheckMonitorMain, NULL)!= 0) {
        write_runlog(ERROR, "Failed to create disk health check monitor thread.\n");
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

void CreateDiskHealthCheckThread()
{
#ifdef ENABLE_XALARMD
    if (g_enableXalarmdFeature) {
        struct alarm_subscription_info id_filter;
        id_filter.id_list[0] = XALARM_SLOW_DISK_ID;
        id_filter.len = 1;
        g_xalarmClientId = xalarm_Register(HandleXalarm, id_filter);
        if (g_xalarmClientId < 0) {
            write_runlog(ERROR, "Failed to xalarm register, please check the status of xalarmd\n");
        } else {
            write_runlog(LOG, "xalarm register success, client id is %d\n", g_xalarmClientId);
        }
        return;
    } else {
        write_runlog(LOG, "Xalarm feature is disabled by configuration, use traditional disk check.\n");
    }
#else
    write_runlog(LOG, "Xalarm feature is disabled in compile time, use traditional disk check.\n");
#endif
    if (InitDiskHealthCtx() != CM_SUCCESS) {
        write_runlog(FATAL, "Failed to init disk health check context.\n");
        exit(1);
    }
    uint32 totalDir = 0;
    for (uint32 i = 0; i < g_diskHealth.diskCount; i++) {
        write_runlog(LOG, "Start disk health check thread for disk(%s).\n", g_diskHealth.disk[i].diskName);
        if (CreateDiskItemHealthCheckThread(&g_diskHealth.disk[i])!= CM_SUCCESS) {
            write_runlog(FATAL, "Failed to create disk health check thread for disk(%s).\n",
                         g_diskHealth.disk[i].diskName);
            exit(1);
        }
        totalDir += g_diskHealth.disk[i].dirHealth.dirCount;
    }
    if (totalDir > 0) {
        write_runlog(LOG, "Start disk health check monitor thread for %u dirs.\n", totalDir);
        if (CreateDiskHealthCheckMonitorThread() != CM_SUCCESS) {
            write_runlog(FATAL, "Failed to create disk health check monitor thread.\n");
            exit(1);
        }
    }
}

uint32 GetDiskCheckTimeout()
{
    return g_diskCheckTimeout;
}

uint32 GetDiskCheckInterval()
{
    return g_diskCheckInterval;
}

uint32 GetDiskCheckBufferSize()
{
    return g_diskCheckBufferSize;
}