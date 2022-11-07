/*
* Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
* cms_process_messages_agent.cpp
*
*
* IDENTIFICATION
*    src/cm_server/cms_process_messages_agent.cpp
*
* -------------------------------------------------------------------------
*/
#include "cms_conn.h"
#include "cms_common_res.h"
#include "cms_ddb_adapter.h"
#include "cms_global_params.h"
#include "cms_process_messages.h"

typedef struct ResStatReportInfoSt {
    uint32 nodeId;
    uint32 reportInter;
    MaxClusterResStatus isAvail;  // 0:res inst unavailable, 1:res inst available
} ResStatReportInfo;

static ResStatReportInfo *g_resNodeStat = NULL;

void ProcessReportResChangedMsg(bool notifyClient, const OneResStatList &status)
{
    CmsReportResStatList sendMsg = {0};
    sendMsg.msgType = notifyClient ? (int)MSG_CM_AGENT_RES_STATUS_CHANGED : (int)MSG_CM_AGENT_RES_STATUS_LIST;
    errno_t rc = memcpy_s(&sendMsg.resList, sizeof(OneResStatList), &status, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);

    write_runlog(LOG, "[CLIENT] res(%s), version=%llu, status:\n", status.resName, status.version);
    for (uint32 i = 0; i < status.instanceCount; ++i) {
        write_runlog(LOG, "[CLIENT] nodeId=%u, cmInstId=%u, resInstId=%u, isWork=%u, status=%u;\n",
            status.resStat[i].nodeId,
            status.resStat[i].cmInstanceId,
            status.resStat[i].resInstanceId,
            status.resStat[i].isWorkMember,
            status.resStat[i].status);
    }

    (void)BroadcastMsg('S', (char *)(&sendMsg), sizeof(CmsReportResStatList));
}

static bool IsResStatusChanged(uint32 cmInstId, uint32 newStat, CmResStatList *oldResStat)
{
    for (uint32 i = 0; i < oldResStat->status.instanceCount; ++i) {
        if (cmInstId != oldResStat->status.resStat[i].cmInstanceId) {
            continue;
        }
        if (newStat != oldResStat->status.resStat[i].status) {
            oldResStat->status.resStat[i].status = newStat;
            return true;
        }
        break;
    }
    return false;
}

static inline int ReportResStatInterCmp(const void *arg1, const void *arg2)
{
    return (int)((((const ResStatReportInfo *)arg1)->nodeId) - (((const ResStatReportInfo *)arg2)->nodeId));
}

status_t InitNodeReportResStatInter()
{
    g_resNodeStat = (ResStatReportInfo*)malloc(sizeof(ResStatReportInfo) * g_node_num);
    if (g_resNodeStat == NULL) {
        write_runlog(ERROR, "out of memory, InitNodeReportResStatInter.\n");
        return CM_ERROR;
    }
    errno_t rc = memset_s(g_resNodeStat, sizeof(ResStatReportInfo), 0, sizeof(ResStatReportInfo));
    securec_check_errno(rc, (void)rc);
    for (uint32 i = 0; i < g_node_num; ++i) {
        g_resNodeStat[i].nodeId = g_node[i].node;
        g_resNodeStat[i].isAvail = MAX_CLUSTER_STATUS_INIT;
    }
#undef qsort
    qsort(g_resNodeStat, g_node_num, sizeof(ResStatReportInfo), ReportResStatInterCmp);
    return CM_SUCCESS;
}

static uint32 FindNodeReportResInterByNodeId(uint32 nodeId)
{
    if (nodeId >= g_node_num || nodeId == 0) {
        for (int i = (int)(g_node_num - 1); i >= 0; --i) {
            if (g_resNodeStat[i].nodeId == nodeId) {
                return (uint32)i;
            }
        }
        return g_node_num;
    }
    // In most cases, the position is nodeId - 1.
    uint32 comInd = nodeId - 1;
    if (g_resNodeStat[comInd].nodeId == nodeId) {
        return comInd;
    }
    if (g_resNodeStat[comInd].nodeId > nodeId) {
        for (int i = (int)(g_node_num - 1); i >= 0; --i) {
            if (g_resNodeStat[i].nodeId == nodeId) {
                return (uint32)i;
            }
        }
        return g_node_num;
    }
    // if g_resNodeStat[comInd].nodeId < nodeId
    for (uint32 i = comInd + 1; i < g_node_num; ++i) {
        if (g_resNodeStat[i].nodeId == nodeId) {
            return i;
        }
    }
    return g_node_num;
}

static MaxClusterResStatus GetResNodeStatByReport(uint32 stat)
{
    if (stat == RES_INST_WORK_STATUS_UNAVAIL) {
        return MAX_CLUSTER_STATUS_UNAVAIL;
    } else if (stat == RES_INST_WORK_STATUS_AVAIL) {
        return MAX_CLUSTER_STATUS_AVAIL;
    } else if (stat == RES_INST_WORK_STATUS_UNKNOWN) {
        return MAX_CLUSTER_STATUS_UNKNOWN;
    } else {
        write_runlog(LOG, "recv unknown status %u.\n", stat);
    }

    return MAX_CLUSTER_STATUS_UNKNOWN;
}

static MaxClusterResStatus IsAllNodeResInstAvail(const OneNodeResourceStatus *nodeStat, MaxClusterResStatus oldStat)
{
    for (uint32 i = 0; i < nodeStat->count; ++i) {
        MaxClusterResStatus newStat = GetResNodeStatByReport(nodeStat->status[i].workStatus);
        if (newStat != oldStat) {
            write_runlog(LOG, "recv cus_res inst(%u): new work_status(%d), old work_status(%d).\n",
                nodeStat->status[i].cmInstanceId, (int)newStat, (int)oldStat);
        }
        if (newStat != MAX_CLUSTER_STATUS_AVAIL) {
            return newStat;
        }
    }

    return MAX_CLUSTER_STATUS_AVAIL;
}

static inline void WriteGetResNodeStatErrLog(uint32 nodeId)
{
    write_runlog(ERROR, "can't find nodeId(%u) in g_resNodeStat.\n", nodeId);
    for (uint32 i = 0; i < g_node_num; ++i) {
        write_runlog(ERROR, "g_resNodeStat[%u].nodeId = %u.\n", i, g_resNodeStat[i].nodeId);
    }
}

static bool IsCusResStatValid(const OneNodeResourceStatus *nodeStat)
{
    for (uint32 i = 0; i < nodeStat->count; ++i) {
        if (nodeStat->status[i].status != CUS_RES_CHECK_STAT_ONLINE &&
            nodeStat->status[i].status != CUS_RES_CHECK_STAT_OFFLINE &&
            nodeStat->status[i].status != CUS_RES_CHECK_STAT_TIMEOUT) {
            return false;
        }
    }
    return true;
}

static void RecordResStatReport(const OneNodeResourceStatus *nodeStat)
{
    if (g_resNodeStat == NULL) {
        write_runlog(ERROR, "[CLIENT] g_resNodeStat is null.\n");
        return;
    }

    uint32 ind = FindNodeReportResInterByNodeId(nodeStat->node);
    if (ind < g_node_num) {
        if (IsCusResStatValid(nodeStat)) {
            g_resNodeStat[ind].reportInter = 0;
        }
        g_resNodeStat[ind].isAvail = IsAllNodeResInstAvail(nodeStat, g_resNodeStat[ind].isAvail);
    } else {
        WriteGetResNodeStatErrLog(nodeStat->node);
    }
}

static inline bool IsReportTimeout(uint32 reportInter)
{
    return (reportInter >= g_agentNetworkTimeout);
}

static const char* GetClusterResStatStr(MaxClusterResStatus stat)
{
    switch (stat) {
        case MAX_CLUSTER_STATUS_INIT:
            return "init";
        case MAX_CLUSTER_STATUS_UNKNOWN:
            return "unknown";
        case MAX_CLUSTER_STATUS_AVAIL:
            return "avail";
        case MAX_CLUSTER_STATUS_UNAVAIL:
            return "unavail";
        case MAX_CLUSTER_STATUS_CEIL:
            break;
    }

    return "";
}

MaxClusterResStatus GetResNodeStat(uint32 nodeId)
{
    uint32 ind = FindNodeReportResInterByNodeId(nodeId);
    if (ind < g_node_num) {
        if (IsReportTimeout(g_resNodeStat[ind].reportInter)) {
            write_runlog(LOG, "recv node(%u) agent report res status msg timeout.\n", nodeId);
            return MAX_CLUSTER_STATUS_UNAVAIL;
        }
        if (g_resNodeStat[ind].isAvail != MAX_CLUSTER_STATUS_AVAIL) {
            write_runlog(LOG, "res node(%u) stat is (%s).\n", nodeId, GetClusterResStatStr(g_resNodeStat[ind].isAvail));
        }
        return g_resNodeStat[ind].isAvail;
    } else {
        WriteGetResNodeStatErrLog(nodeId);
    }

    return MAX_CLUSTER_STATUS_UNKNOWN;
}

void SetResStatReportInter(uint32 nodeId)
{
    if (g_resNodeStat == NULL) {
        return;
    }
    uint32 ind = FindNodeReportResInterByNodeId(nodeId);
    if (ind < g_node_num) {
        ++g_resNodeStat[ind].reportInter;
    } else {
        WriteGetResNodeStatErrLog(nodeId);
    }
}

uint32 GetResStatReportInter(uint32 nodeId)
{
    if (g_resNodeStat == NULL) {
        return 0;
    }
    uint32 ind = FindNodeReportResInterByNodeId(nodeId);
    if (ind < g_node_num) {
        return g_resNodeStat[ind].reportInter;
    }
    WriteGetResNodeStatErrLog(nodeId);
    return 0;
}

static uint32 GetResInstStat(uint32 recvStat)
{
    if (recvStat == CUS_RES_CHECK_STAT_ONLINE) {
        return (uint32)CM_RES_STAT_ONLINE;
    }
    if (recvStat == CUS_RES_CHECK_STAT_OFFLINE) {
        return (uint32)CM_RES_STAT_OFFLINE;
    }

    return (uint32)CM_RES_STAT_UNKNOWN;
}

static void ProcessOneResInstStatReport(CmResourceStatus *stat)
{
    uint32 index = 0;
    stat->resName[CM_MAX_RES_NAME - 1] = '\0';
    if (GetGlobalResStatusIndex(stat->resName, index) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] %s, unknown the resName(%s).\n", __func__, stat->resName);
        return;
    }

    if (stat->workStatus == RES_INST_WORK_STATUS_UNKNOWN) {
        write_runlog(LOG, "[CLIENT] node(%u) had restart.\n", stat->nodeId);
    }

    uint32 newInstStat = GetResInstStat(stat->status);

    CmResStatList *resStat = &g_resStatus[index];
    (void)pthread_rwlock_wrlock(&(resStat->rwlock));
    bool isChanged = IsResStatusChanged(stat->cmInstanceId, newInstStat, resStat);
    if (isChanged) {
        ++resStat->status.version;
        ProcessReportResChangedMsg(true, resStat->status);
    }
    (void)pthread_rwlock_unlock(&(resStat->rwlock));

    if (isChanged) {
        (void)pthread_rwlock_rdlock(&(resStat->rwlock));
        SaveOneResStatusToDdb(&resStat->status);
        (void)pthread_rwlock_unlock(&(resStat->rwlock));
        write_runlog(LOG, "[CLIENT] [%u:%u] res(%s) changed\n", stat->nodeId, stat->cmInstanceId, stat->resName);
    }
}

void ProcessAgent2CmResStatReportMsg(ReportResStatus *resStatusPtr)
{
    RecordResStatReport(&resStatusPtr->nodeStat);

    for (uint32 i = 0; i < resStatusPtr->nodeStat.count; ++i) {
        ProcessOneResInstStatReport(&resStatusPtr->nodeStat.status[i]);
    }
}

void ProcessRequestResStatusListMsg(MsgRecvInfo* recvMsgInfo)
{
    CmsReportResStatList sendMsg = {0};

    sendMsg.msgType = (int)MSG_CM_AGENT_RES_STATUS_LIST;

    for (uint32 i = 0; i < CusResCount(); ++i) {
        (void)pthread_rwlock_rdlock(&(g_resStatus[i].rwlock));
        errno_t rc = memcpy_s(&sendMsg.resList, sizeof(OneResStatList), &g_resStatus[i].status, sizeof(OneResStatList));
        (void)pthread_rwlock_unlock(&(g_resStatus[i].rwlock));
        securec_check_errno(rc, (void)rc);
        (void)RespondMsg(recvMsgInfo, 'S', (char*)(&sendMsg), sizeof(CmsReportResStatList));
    }
}

static inline void GetResLockDdbKey(char *key, uint32 keyLen, const char *resName, const char *lockName)
{
    int ret = snprintf_s(key, keyLen, keyLen - 1, "/%s/CM/LockOwner/%s/%s", pw->pw_name, resName, lockName);
    securec_check_intval(ret, (void)ret);
}

static status_t GetLockOwner(const char *resName, const char *lockName, uint32 &curLockOwner)
{
    char key[MAX_PATH_LEN] = {0};
    GetResLockDdbKey(key, MAX_PATH_LEN, resName, lockName);

    char lockValue[MAX_PATH_LEN] = {0};
    DDB_RESULT ddbResult = SUCCESS_GET_VALUE;
    status_t st = GetKVFromDDb(key, MAX_PATH_LEN, lockValue, MAX_PATH_LEN, &ddbResult);
    if (st != CM_SUCCESS) {
        if (ddbResult != CAN_NOT_FIND_THE_KEY) {
            write_runlog(ERROR, "[CLIENT] failed to get value of key(%s).\n", key);
            return CM_ERROR;
        }
        curLockOwner = 0;
        return CM_SUCCESS;
    }
    if (is_digit_string(lockValue) != 1) {
        write_runlog(ERROR, "[CLIENT] the value(%s) of key(%s) is not digit, delete it.\n", lockValue, key);
        if (DelKeyInDdb(key, (uint32)strlen(key)) != CM_SUCCESS) {
            write_runlog(ERROR, "[CLIENT] ddb del failed. key=%s.\n", key);
            return CM_ERROR;
        }
        curLockOwner = 0;
        return CM_SUCCESS;
    }

    int owner = CmAtoi(lockValue, 1);
    if (!IsResInstIdValid(owner)) {
        write_runlog(LOG, "[CLIENT] cur res(%s) (%s)lock owner(%d) is invalid, delete it.\n", resName, lockName, owner);
        if (DelKeyInDdb(key, (uint32)strlen(key)) != CM_SUCCESS) {
            write_runlog(ERROR, "[CLIENT] ddb del failed. key=%s.\n", key);
            return CM_ERROR;
        }
        curLockOwner = 0;
        return CM_SUCCESS;
    }
    curLockOwner = (uint32)owner;

    return CM_SUCCESS;
}

static status_t SetNewLockOwner(const char *resName, const char *lockName, uint32 curLockOwner, uint32 resInstId)
{
    char key[MAX_PATH_LEN] = {0};
    GetResLockDdbKey(key, MAX_PATH_LEN, resName, lockName);

    char value[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(value, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%u", resInstId);
    securec_check_intval(ret, (void)ret);

    DrvSetOption opt = {0};
    char preValue[MAX_PATH_LEN] = {0};
    if (curLockOwner != 0) {
        ret = snprintf_s(preValue, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%u", curLockOwner);
        securec_check_intval(ret, (void)ret);
        opt.preValue = preValue;
        opt.len = (uint32)strlen(preValue);
    }
    if (SetKV2Ddb(key, MAX_PATH_LEN, value, MAX_PATH_LEN, &opt) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] ddb set failed. key=%s, value=%s.\n", key, value);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

static status_t DeleteLockKey(const char *resName, const char *lockName)
{
    char key[MAX_PATH_LEN];
    GetResLockDdbKey(key, MAX_PATH_LEN, resName, lockName);

    if (DelKeyInDdb(key, (uint32)strlen(key)) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] ddb del failed. key=%s.\n", key);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

void ReleaseResLockOwner(const char *resName, uint32 instId)
{
    // get all kv need known kv count, dynamic arrays need to be added.
    const uint32 kvCount = 10;
    DrvKeyValue kvs[kvCount];
    errno_t rc = memset_s(kvs, sizeof(DrvKeyValue) * kvCount, 0, sizeof(DrvKeyValue) * kvCount);
    securec_check_errno(rc, (void)rc);
    char key[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(key, MAX_PATH_LEN, MAX_PATH_LEN - 1, "/%s/CM/LockOwner/%s", pw->pw_name, resName);
    securec_check_intval(ret, (void)ret);
    DDB_RESULT dbResult = SUCCESS_GET_VALUE;
    if (GetAllKVFromDDb(key, MAX_PATH_LEN, kvs, kvCount, &dbResult) != CM_SUCCESS) {
        if (dbResult != CAN_NOT_FIND_THE_KEY) {
            write_runlog(ERROR, "[CLIENT] res(%s) release lock owner failed, get kvs fail.\n", resName);
        }
        return;
    }

    bool isSuccess = true;
    for (uint32 i = 0; i < kvCount; ++i) {
        if (kvs[i].key[0] == '\0' || kvs[i].value[0] == '\0') {
            break;
        }
        if ((uint32)CmAtol(kvs[i].value, 0) != instId) {
            continue;
        }
        if (DelKeyInDdb(kvs[i].key, (uint32)strlen(kvs[i].key)) != CM_SUCCESS) {
            write_runlog(ERROR, "[CLIENT] ddb del failed. key=%s, value=%s.\n", kvs[i].key, kvs[i].value);
            isSuccess = false;
        } else {
            write_runlog(LOG, "[CLIENT] ddb del success. key=%s, value=%s.\n", kvs[i].key, kvs[i].value);
        }
    }

    if (isSuccess) {
        write_runlog(LOG, "[CLIENT] res(%s) inst(%u) release all lock success.\n", resName, instId);
    } else {
        write_runlog(ERROR, "[CLIENT] res(%s) inst(%u) release all lock failed.\n", resName, instId);
    }
}

static ClientError CmResLock(const CmaToCmsResLock *lockMsg)
{
    if (!IsResInstIdValid((int)lockMsg->cmInstId)) {
        write_runlog(ERROR, "[CLIENT] res(%s) (%s)lock new owner (%u) is invalid.\n",
            lockMsg->resName, lockMsg->lockName, lockMsg->cmInstId);
        return CM_RES_CLIENT_CANNOT_DO;
    }
    if (!IsOneResInstWork(lockMsg->resName, lockMsg->cmInstId)) {
        write_runlog(ERROR, "[CLIENT] res(%s) inst(%u) has been get out of cluster, can't do lock.\n",
            lockMsg->resName, lockMsg->cmInstId);
        return CM_RES_CLIENT_CANNOT_DO;
    }
    uint32 curLockOwner;
    if (GetLockOwner(lockMsg->resName, lockMsg->lockName, curLockOwner) != CM_SUCCESS) {
        write_runlog(LOG, "[CLIENT] get (%s)lock owner failed, res(%s) inst(%u) can't lock.\n",
            lockMsg->lockName, lockMsg->resName, lockMsg->cmInstId);
        return CM_RES_CLIENT_DDB_ERR;
    }
    if (curLockOwner == lockMsg->cmInstId) {
        write_runlog(LOG, "[CLIENT] res(%s) (%s)lock owner(%u) is same with lock candidate, can't lock again.\n",
            lockMsg->resName, lockMsg->lockName, curLockOwner);
        return CM_RES_CLIENT_CANNOT_DO;
    }
    if (curLockOwner != 0) {
        write_runlog(LOG, "[CLIENT] res(%s) (%s)lock owner is inst(%u), inst(%u) can't lock.\n",
            lockMsg->resName, lockMsg->lockName, curLockOwner, lockMsg->cmInstId);
        return CM_RES_CLIENT_CANNOT_DO;
    }
    if (SetNewLockOwner(lockMsg->resName, lockMsg->lockName, curLockOwner, lockMsg->cmInstId) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] res(%s) instance(%u) (%s)lock failed.\n",
            lockMsg->resName, lockMsg->cmInstId, lockMsg->lockName);
        return CM_RES_CLIENT_DDB_ERR;
    }
    write_runlog(LOG, "[CLIENT] res(%s) instance(%u) (%s)lock success.\n",
        lockMsg->resName, lockMsg->cmInstId, lockMsg->lockName);

    return CM_RES_CLIENT_SUCCESS;
}

static ClientError CmResUnlock(const CmaToCmsResLock *lockMsg)
{
    uint32 curLockOwner = 0;
    if (GetLockOwner(lockMsg->resName, lockMsg->lockName, curLockOwner) != CM_SUCCESS) {
        write_runlog(LOG, "[CLIENT] get cur lock owner failed, res(%s) lockName(%s) inst(%u) can't unlock.\n",
            lockMsg->resName, lockMsg->lockName, lockMsg->cmInstId);
        return CM_RES_CLIENT_DDB_ERR;
    }
    if (curLockOwner == 0) {
        write_runlog(LOG, "[CLIENT] cur lock owner is NULL, res(%s) lockName(%s) inst(%u) can't unlock.\n",
            lockMsg->resName, lockMsg->lockName, lockMsg->cmInstId);
        return CM_RES_CLIENT_CANNOT_DO;
    }
    if (curLockOwner != lockMsg->cmInstId) {
        write_runlog(LOG, "[CLIENT] res(%s) lockName(%s) lock owner is (%u) not inst(%u), can't unlock.\n",
            lockMsg->resName, lockMsg->lockName, curLockOwner, lockMsg->cmInstId);
        return CM_RES_CLIENT_CANNOT_DO;
    }

    if (DeleteLockKey(lockMsg->resName, lockMsg->lockName) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] res(%s) inst(%u) unlock failed, because ddb del lock owner failed.\n",
            lockMsg->resName, lockMsg->cmInstId);
        return CM_RES_CLIENT_DDB_ERR;
    }
    write_runlog(LOG, "[CLIENT] res(%s) instance(%u) unlock success.\n", lockMsg->resName, lockMsg->cmInstId);

    return CM_RES_CLIENT_SUCCESS;
}

static ClientError ResGetLockOwner(const char *resName, const char *lockName, uint32 &lockOwner)
{
    if (GetLockOwner(resName, lockName, lockOwner) != CM_SUCCESS) {
        write_runlog(LOG, "[CLIENT] get res(%s) (%s)lock owner failed.\n", resName, lockName);
        return CM_RES_CLIENT_DDB_ERR;
    }
    if (lockOwner == 0) {
        write_runlog(LOG, "[CLIENT] get res(%s) (%s)lock no owner.\n", resName, lockName);
        return CM_RES_CLIENT_NO_LOCK_OWNER;
    }

    return CM_RES_CLIENT_SUCCESS;
}

static ClientError TransLockOwner(const CmaToCmsResLock *lockMsg)
{
    const char *resName = lockMsg->resName;
    const char *lockName = lockMsg->lockName;
    uint32 resInstId = lockMsg->cmInstId;
    uint32 newLockOwner = lockMsg->transInstId;
    uint32 curLockOwner = 0;
    if (GetLockOwner(resName, lockName, curLockOwner) != CM_SUCCESS) {
        write_runlog(LOG, "[CLIENT] get (%s)lock owner failed, res(%s) can't trans lock.\n", lockName, resName);
        return CM_RES_CLIENT_DDB_ERR;
    }
    if (curLockOwner != resInstId) {
        write_runlog(LOG, "[CLIENT] res(%s) (%s)lock owner is inst(%u), inst(%u) can't trans lock.\n",
            resName, lockName, curLockOwner, resInstId);
        return CM_RES_CLIENT_CANNOT_DO;
    }
    if (!IsOneResInstWork(resName, newLockOwner)) {
        write_runlog(LOG, "[CLIENT] res(%s) inst(%u) get out of cluster, can't be lockOwner.\n", resName, newLockOwner);
        return CM_RES_CLIENT_CANNOT_DO;
    }

    if (SetNewLockOwner(resName, lockName, curLockOwner, newLockOwner) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] res(%s) inst(%u) trans to inst(%u) failed, cause ddb failed.\n",
            resName, resInstId, newLockOwner);
        return CM_RES_CLIENT_DDB_ERR;
    }
    write_runlog(LOG, "[CLIENT] res(%s) inst(%u) trans to inst(%u) success.\n", resName, resInstId, newLockOwner);

    return CM_RES_CLIENT_SUCCESS;
}

void ProcessCmResLock(MsgRecvInfo* recvMsgInfo, CmaToCmsResLock *lockMsg)
{
    lockMsg->resName[CM_MAX_RES_NAME - 1] = '\0';
    lockMsg->lockName[CM_MAX_LOCK_NAME - 1] = '\0';

    CmsReportLockResult ackMsg = {0};
    ackMsg.msgType = (int)MSG_CM_RES_LOCK_ACK;
    ackMsg.conId = lockMsg->conId;
    ackMsg.lockOpt = lockMsg->lockOpt;
    ackMsg.lockOwner = 0;
    errno_t rc = strcpy_s(ackMsg.lockName, CM_MAX_LOCK_NAME, lockMsg->lockName);
    securec_check_errno(rc, (void)rc);

    switch (lockMsg->lockOpt) {
        case (uint32)CM_RES_LOCK: {
            ackMsg.error = (uint32)CmResLock(lockMsg);
            break;
        }
        case (uint32)CM_RES_UNLOCK: {
            ackMsg.error = (uint32)CmResUnlock(lockMsg);
            break;
        }
        case (uint32)CM_RES_GET_LOCK_OWNER: {
            ackMsg.error = (uint32)ResGetLockOwner(lockMsg->resName, lockMsg->lockName, ackMsg.lockOwner);
            break;
        }
        case (uint32)CM_RES_LOCK_TRANS: {
            ackMsg.error = (uint32)TransLockOwner(lockMsg);
            break;
        }
        default: {
            write_runlog(ERROR, "[CLIENT] unknown lockOpt(%u).\n", lockMsg->lockOpt);
            ackMsg.error = (uint32)CM_RES_CLIENT_CANNOT_DO;
            break;
        }
    }

    if (RespondMsg(recvMsgInfo, 'S', (char *)(&ackMsg), sizeof(CmsReportLockResult), DEBUG5) != 0) {
        write_runlog(ERROR, "[CLIENT] send lock ack msg failed.\n");
    }
}

static inline void CopyResStatusToSendMsg(OneResStatList *sendStat, CmResStatList *saveStat)
{
    (void)pthread_rwlock_rdlock(&saveStat->rwlock);
    errno_t rc = memcpy_s(sendStat, sizeof(OneResStatList), &saveStat->status, sizeof(OneResStatList));
    (void)pthread_rwlock_unlock(&saveStat->rwlock);
    securec_check_errno(rc, (void)rc);
}

void ProcessResInstanceStatusMsg(MsgRecvInfo* recvMsgInfo, const CmsToCtlGroupResStatus *queryStatusPtr)
{
    CmsToCtlGroupResStatus instStatMsg = {0};
    instStatMsg.msgType = (int)MSG_CM_QUERY_INSTANCE_STATUS;

    if (queryStatusPtr->msgStep == QUERY_RES_STATUS_STEP) {
        for (uint32 i = 0; i < CusResCount(); ++i) {
            instStatMsg.msgStep = QUERY_RES_STATUS_STEP_ACK;
            CopyResStatusToSendMsg(&instStatMsg.oneResStat, &g_resStatus[i]);
            (void)RespondMsg(recvMsgInfo, 'S', (char*)&(instStatMsg), sizeof(instStatMsg), DEBUG5);
        }

        instStatMsg.msgStep = QUERY_RES_STATUS_STEP_ACK_END;
        (void)RespondMsg(recvMsgInfo, 'S', (char*)&(instStatMsg), sizeof(instStatMsg), DEBUG5);
    }
}

void ProcessQueryOneResInst(MsgRecvInfo* recvMsgInfo, const QueryOneResInstStat *queryMsg)
{
    CmsToCtlOneResInstStat ackMsg = {0};
    ackMsg.msgType = (int)MSG_CM_CTL_QUERY_RES_INST_ACK;

    uint32 destInstId = queryMsg->instId;
    for (uint32 i = 0; i < CusResCount(); ++i) {
        (void)pthread_rwlock_rdlock(&g_resStatus[i].rwlock);
        for (uint32 j = 0; j < g_resStatus[i].status.instanceCount; ++j) {
            if (g_resStatus[i].status.resStat[j].cmInstanceId != destInstId) {
                continue;
            }
            errno_t rc = memcpy_s(&ackMsg.instStat, sizeof(CmResStatInfo),
                &g_resStatus[i].status.resStat[j], sizeof(CmResStatInfo));
            (void)pthread_rwlock_unlock(&g_resStatus[i].rwlock);
            securec_check_errno(rc, (void)rc);
            (void)RespondMsg(recvMsgInfo, 'S', (char*)&(ackMsg), sizeof(ackMsg), DEBUG5);
            return;
        }
        (void)pthread_rwlock_unlock(&g_resStatus[i].rwlock);
    }
    write_runlog(ERROR, "unknown res instId(%u).\n", destInstId);
}

static bool IsregIsNotUnknown(ResInstIsreg *isregList, uint32 isregCount)
{
    for (uint32 i = 0; i < isregCount; ++i) {
        if (!IsRecvIsregStatValid(isregList[i].isreg)) {
            write_runlog(ERROR, "recv inst(%u) isreg stat(%d) invalid.\n", isregList[i].cmInstId, isregList[i].isreg);
            return false;
        }
        if (isregList[i].isreg == (int)CM_RES_ISREG_UNKNOWN) {
            write_runlog(LOG, "recv inst(%u) isreg(%s).\n", isregList[i].cmInstId, GetIsregStatus(isregList[i].isreg));
            return false;
        }
    }
    return true;
}

void ProcessResIsregMsg(MsgRecvInfo *recvMsgInfo, CmaToCmsIsregMsg *isreg)
{
    if (isreg->isregCount > CM_MAX_RES_INST_COUNT) {
        write_runlog(ERROR, "recv isreg list count(%u) invalid, max(%d).\n", isreg->isregCount, CM_MAX_RES_INST_COUNT);
        return;
    }

    bool needUpdateAgentCheckList = false;
    UpdateResIsregStatusList(isreg->nodeId, isreg->isregList, isreg->isregCount, &needUpdateAgentCheckList);

    if (needUpdateAgentCheckList) {
        write_runlog(LOG, "recv check list is not right.\n");
        CmsFlushIsregCheckList sendMsg = {0};
        sendMsg.msgType = (int)MSG_CM_AGENT_ISREG_CHECK_LIST_CHANGED;
        GetCheckListByNodeId(isreg->nodeId, sendMsg.checkList, &sendMsg.checkCount);
        (void)RespondMsg(recvMsgInfo, 'S', (char*)&(sendMsg), sizeof(sendMsg), LOG);
    } else {
        if (IsregIsNotUnknown(isreg->isregList, isreg->isregCount)) {
            CleanReportInter(isreg->nodeId);
        }
    }
}

void ResetResNodeStat()
{
    if (g_resNodeStat == NULL) {
        return;
    }
    for (uint32 i = 0; i < g_node_num; ++i) {
        g_resNodeStat[i].isAvail = MAX_CLUSTER_STATUS_INIT;
    }
}
