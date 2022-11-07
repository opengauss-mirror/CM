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
 * cm_ddb_sharedisk.cpp
 *
 * IDENTIFICATION
 *    src/cm_adapter/cm_sharedisk_adapter/cm_ddb_sharedisk.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm/cm_c.h"
#include "cm/cm_elog.h"
#include "cm_disk_rw.h"
#include "cm_ddb_sharedisk_cmd.h"
#include "cm_ddb_sharedisk.h"

uint32 g_cmSdServerNum = 0;
static diskLrwHandler g_cmsArbitrateDiskHandler;
static pthread_rwlock_t g_notifySdLock;
static DDB_ROLE g_notifySd = DDB_ROLE_UNKNOWN;
static DDB_ROLE g_dbRole = DDB_ROLE_FOLLOWER;
static uint32 g_cmServerNum = 0;
static int64 g_waitForTime = 0;
static volatile int64 g_waitForChangeTime = 0;
const uint32 ONE_PRIMARY_ONE_STANDBY = 2;

static status_t SdLoadApi(const DrvApiInfo *apiInfo);

static DdbDriver g_drvSd = {PTHREAD_RWLOCK_INITIALIZER, false, DB_SHAREDISK, "sharedisk conn", SdLoadApi};

status_t DrvSdGetValue(const DrvCon_t session, DrvText *key, DrvText *value, const DrvGetOption *option)
{
    status_t res = DiskCacheRead(key->data, value->data, value->len);
    if (res != CM_SUCCESS) {
        write_runlog(DEBUG1, "DrvSdGetValue: failed to get value of key %s.\n", key->data);
        return CM_ERROR;
    }

    write_runlog(DEBUG1,
        "DrvSdGetValue: success to get keyValue[%s:%u, %s:%u].\n",
        key->data,
        key->len,
        value->data,
        value->len);
    return CM_SUCCESS;
}

status_t DrvSdGetAllKV(
    const DrvCon_t session, DrvText *key, DrvKeyValue *keyValue, uint32 length, const DrvGetOption *option)
{
    char kvBuff[DDB_MAX_KEY_VALUE_LEN] = {0};
    status_t res = DiskCacheRead(key->data, kvBuff, DDB_MAX_KEY_VALUE_LEN, true);
    if (res != CM_SUCCESS) {
        write_runlog(DEBUG1, "DrvSdGetValue: failed to get all value of key %s.\n", key->data);
        return CM_ERROR;
    }
    write_runlog(DEBUG1, "DrvSdGetAllKV: get all values, key is %s, result_key_value is %s.\n", key->data, kvBuff);

    errno_t rc;
    char *pLeft = NULL;
    char *pKey = strtok_r(kvBuff, ",", &pLeft);
    char *pValue = strtok_r(NULL, ",", &pLeft);
    uint32 i = 0;
    while (pKey && pValue) {
        rc = snprintf_s(keyValue[i].key, DDB_KEY_LEN, DDB_KEY_LEN - 1, "%s", pKey);
        securec_check_intval(rc, (void)rc);
        rc = snprintf_s(keyValue[i].value, DDB_VALUE_LEN, DDB_VALUE_LEN - 1, "%s", pValue);
        securec_check_intval(rc, (void)rc);
        if (++i >= length) {
            break;
        }
        pKey = strtok_r(NULL, ",", &pLeft);
        pValue = strtok_r(NULL, ",", &pLeft);
    }
    if (i == 0) {
        write_runlog(
            ERROR, "DrvSdGetAllKV: get all values is empty, key is %s result_key_value is %s.\n", key->data, kvBuff);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

status_t DrvSdSetKV(const DrvCon_t session, DrvText *key, DrvText *value, DrvSetOption *option)
{
    // key->len % 512 and value->len % 512 must equal to 0
    write_runlog(DEBUG1, "DrvSdSetKV: set key %s to value %s.\n", key->data, value->data);
    status_t res;
    if (option != NULL) {
        update_option updateOption;
        updateOption.preValue = option->preValue;
        updateOption.len = option->len;
        res = DiskCacheWrite(key->data, key->len, value->data, value->len, &updateOption);
    } else {
        res = DiskCacheWrite(key->data, key->len, value->data, value->len, NULL);
    }
    if (res != CM_SUCCESS) {
        write_runlog(ERROR, "DrvSdSetKV: set key %s to value %s failed.\n", key->data, value->data);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

status_t DrvSdDelKV(const DrvCon_t session, DrvText *key)
{
    write_runlog(DEBUG1, "DrvSdDelKV: begin to del key %s.\n", key->data);
    status_t res = DiskCacheDelete(key->data);
    if (res != CM_SUCCESS) {
        write_runlog(ERROR, "DrvSdDelKV: del key %s failed.\n", key->data);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

status_t GetNotifyRole(DrvCon_t session, char *memberName, DdbNodeState *nodeState)
{
    return CM_SUCCESS;
}

status_t InitShareDiskManager(const DrvApiInfo *apiInfo)
{
    g_cmServerNum = apiInfo->nodeNum;
    g_waitForTime = apiInfo->sdConfig.waitTime;
    int64 instId = apiInfo->sdConfig.instanceId;
    uint32 offset = apiInfo->sdConfig.offset + DISK_ARBITRATE_LOCK_SPACE + DISK_RESERVED_LEN_AFTER_CMSLOCK;
    CM_RETURN_IFERR(InitDiskData(apiInfo->sdConfig.devPath, offset, instId));
    return CM_SUCCESS;
}

status_t InitSdManagerLock(const DrvApiInfo *apiInfo)
{
    CM_RETURN_IFERR(InitShareDiskManager(apiInfo));

    return CM_SUCCESS;
}

const char *DrvSdLastError(void)
{
    return GetDiskRwError();
}

status_t DrvSdAllocConn(DrvCon_t *session, const DrvApiInfo *apiInfo)
{
    return CM_SUCCESS;
}

status_t DrvSdFreeConn(DrvCon_t *session)
{
    return CM_SUCCESS;
}

static uint32 DrvSdHealthCount(int timeOut)
{
    return g_cmSdServerNum;
}

static bool IsDrvSdHeal(DDB_CHECK_MOD checkMod, int timeOut)
{
    return true;
}

static void DrvSdFreeNodeInfo(void)
{
    return;
}

static void DrvNotifySd(DDB_ROLE dbRole)
{
    DdbNotifyStatusFunc ddbNotiStatusFun = GetDdbStatusFunc();
    if (ddbNotiStatusFun == NULL) {
        write_runlog(ERROR,
            "DrvNotifySd: ddb callback statuc func is null.\n");
        return;
    }

    if (g_dbRole != dbRole && g_cmServerNum > ONE_PRIMARY_ONE_STANDBY) {
        if (dbRole == DDB_ROLE_FOLLOWER) {
            (void)pthread_rwlock_wrlock(&g_notifySdLock);
            g_waitForChangeTime = g_waitForTime;
            g_notifySd = DDB_ROLE_FOLLOWER;
            ddbNotiStatusFun(DDB_ROLE_FOLLOWER);
            (void)pthread_rwlock_unlock(&g_notifySdLock);
        }
        write_runlog(LOG, "receive notify msg, it has set ddb role, dbRole is [%d: %d], g_waitForTime is %ld, "
            "g_cmServerNum is %u.\n", (int32)dbRole, (int32)g_dbRole, g_waitForTime, g_cmServerNum);
    }
}

static void DrvSdSetMinority(bool isMinority)
{
    return;
}
static status_t DrvSdSaveAllKV(const DrvCon_t session, const DrvText *key, DrvSaveOption *option)
{
    return CM_SUCCESS;
}

Alarm *DrvSdGetAlarm(int alarmIndex)
{
    return NULL;
}
status_t DrvSdLeaderNodeId(NodeIdInfo *idInfo, const char *azName)
{
    return CM_SUCCESS;
}
status_t DrvSdSetParam(const char *key, const char *value)
{
    if (key == NULL || value == NULL) {
        write_runlog(ERROR, "failed to set dcc param, because key or value is null.\n");
        return CM_ERROR;
    }
    return CM_SUCCESS;
}
status_t DrvSdRestConn(DrvCon_t sess, int32 timeOut)
{
    return CM_SUCCESS;
}

static void NotifyDdbRole(DDB_ROLE *lastDdbRole)
{
    DdbNotifyStatusFunc ddbNotiSta = GetDdbStatusFunc();
    if (ddbNotiSta == NULL) {
        write_runlog(ERROR,
            "NotifyDdbRole: ddb callback statuc func is null.\n");
        return ;
    }

    if (g_dbRole != (*lastDdbRole)) {
        write_runlog(LOG,
            "NotifyDdbRole: current ddbRole is %d, last ddbRole is %d.\n",
            (int)g_dbRole,
            (int)(*lastDdbRole));
        ddbNotiSta(g_dbRole);
    }

    write_runlog(DEBUG1, "NotifyDdbRole: current ddbRole is %d.\n", (int)g_dbRole);
    *lastDdbRole = g_dbRole;
}

static void *GetShareDiskLockMain(void *arg)
{
    thread_name = "GetShareDiskLockMain";
    write_runlog(LOG, "Starting get share disk lock thread.\n");
    char cmd[MAX_PATH_LEN] = {0};
    errno_t rc = snprintf_s(cmd,
        MAX_PATH_LEN,
        MAX_PATH_LEN - 1,
        "cm_persist %s %ld %lu > /dev/null 2>&1",
        g_cmsArbitrateDiskHandler.scsiDev,
        g_cmsArbitrateDiskHandler.instId,
        g_cmsArbitrateDiskHandler.offset);
    securec_check_intval(rc, (void)rc);
    int st;
    DDB_ROLE lastDdbRole = DDB_ROLE_UNKNOWN;

    for (;;) {
        if (g_cmServerNum > ONE_PRIMARY_ONE_STANDBY) {
            (void)pthread_rwlock_wrlock(&g_notifySdLock);
            if (g_notifySd == DDB_ROLE_FOLLOWER && g_waitForChangeTime > 0) {
                g_dbRole = DDB_ROLE_FOLLOWER;
                NotifyDdbRole(&lastDdbRole);
                g_waitForChangeTime--;
                (void)pthread_rwlock_unlock(&g_notifySdLock);
                write_runlog(DEBUG1,
                    "GetShareDiskLockMain: current ddbRole is %d, g_waitForChangeTime is %ld.\n",
                    (int)g_dbRole,
                    g_waitForChangeTime);
                (void)sleep(1);
                continue;
            }

            g_notifySd = DDB_ROLE_UNKNOWN;
            (void)pthread_rwlock_unlock(&g_notifySdLock);
        }

        st = system(cmd);
        g_dbRole = (st == 0) ? DDB_ROLE_LEADER : DDB_ROLE_FOLLOWER;
        NotifyDdbRole(&lastDdbRole);

        (void)sleep(1);
    }
    return NULL;
}
status_t InitDiskLockHandle(diskLrwHandler *sdLrwHandler, const DrvApiInfo *apiInfo)
{
    int32 ret = strcpy_s(sdLrwHandler->scsiDev, MAX_PATH_LENGTH, apiInfo->sdConfig.devPath);
    if (ret != 0) {
        write_runlog(ERROR, "InitDiskLockHandle: copy string %s failed\n", apiInfo->sdConfig.devPath);
        return CM_ERROR;
    }
    sdLrwHandler->instId = apiInfo->sdConfig.instanceId;
    sdLrwHandler->offset = apiInfo->sdConfig.offset;
    return CM_SUCCESS;
}

static status_t CreateShareDiskThread(const DrvApiInfo *apiInfo)
{
    if (InitDiskLockHandle(&g_cmsArbitrateDiskHandler, apiInfo) != CM_SUCCESS) {
        write_runlog(ERROR, "Failed to start get share disk lock thread.\n");
        return CM_ERROR;
    }
    pthread_t thrId;
    int32 res = pthread_create(&thrId, NULL, GetShareDiskLockMain, NULL);
    if (res != 0) {
        write_runlog(ERROR, "Failed to create share disk lock thread.\n");
        return CM_ERROR;
    }
    return CM_SUCCESS;
}
static status_t DrvSdExecCmd(DrvCon_t session, char *cmdLine, char *output, int *outputLen, uint32 maxBufLen)
{
    return ExecuteDdbCmd(cmdLine, output, outputLen, maxBufLen);
}

static status_t SdLoadApi(const DrvApiInfo *apiInfo)
{
    DdbDriver *drv = DrvSdGet();
    drv->allocConn = DrvSdAllocConn;
    drv->freeConn = DrvSdFreeConn;
    drv->getValue = DrvSdGetValue;
    drv->getAllKV = DrvSdGetAllKV;
    drv->saveAllKV = DrvSdSaveAllKV;
    drv->setKV = DrvSdSetKV;
    drv->delKV = DrvSdDelKV;
    drv->execCmd = DrvSdExecCmd;
    drv->drvNodeState = GetNotifyRole;
    drv->lastError = DrvSdLastError;
    drv->isHealth = IsDrvSdHeal;
    drv->healCount = DrvSdHealthCount;
    drv->freeNodeInfo = DrvSdFreeNodeInfo;
    drv->notifyDdb = DrvNotifySd;
    drv->setMinority = DrvSdSetMinority;
    drv->getAlarm = DrvSdGetAlarm;
    drv->leaderNodeId = DrvSdLeaderNodeId;
    drv->restConn = DrvSdRestConn;
    drv->setParam = DrvSdSetParam;
    g_cmSdServerNum = apiInfo->nodeNum;
    status_t st = InitSdManagerLock(apiInfo);
    if (st != CM_SUCCESS) {
        return st;
    }
    st = CreateShareDiskThread(apiInfo);
    return st;
}

DdbDriver *DrvSdGet(void)
{
    return &g_drvSd;
}
