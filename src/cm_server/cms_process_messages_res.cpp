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
#include "cms_ddb_adapter.h"
#include "cms_global_params.h"
#include "cms_process_messages.h"

typedef struct ResStatReportInfoSt {
    uint32 nodeId;
    uint32 reportInter;
} ResStatReportInfo;

static ResStatReportInfo *g_nodeReportResStatInter = NULL;

void ProcessReportResChangedMsg(bool notifyClient, const OneResStatList &status)
{
    CmsReportResStatList sendMsg = {0};
    sendMsg.msgType = notifyClient ? (int)MSG_CM_AGENT_RES_STATUS_CHANGED : (int)MSG_CM_AGENT_RES_STATUS_LIST;
    errno_t rc = memcpy_s(&sendMsg.resList, sizeof(OneResStatList), &status, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);

    write_runlog(LOG, "[CLIENT] res(%s) statList changed, version=%llu.\n", status.resName, status.version);
    for (uint32 j = 0; j < status.instanceCount; ++j) {
        write_runlog(LOG, "nodeId=%u, instanceId=%u, status=%u.\n",
            status.resStat[j].nodeId, status.resStat[j].cmInstanceId, status.resStat[j].status);
    }
}

static inline int ReportResStatInterCmp(const void *arg1, const void *arg2)
{
    return (int)((((const ResStatReportInfo *)arg1)->nodeId) - (((const ResStatReportInfo *)arg2)->nodeId));
}

status_t InitNodeReportResStatInter()
{
    g_nodeReportResStatInter = (ResStatReportInfo*)malloc(sizeof(ResStatReportInfo) * g_node_num);
    if (g_nodeReportResStatInter == NULL) {
        write_runlog(ERROR, "out of memory, InitNodeReportResStatInter.\n");
        return CM_ERROR;
    }
    errno_t rc = memset_s(g_nodeReportResStatInter, sizeof(ResStatReportInfo), 0, sizeof(ResStatReportInfo));
    securec_check_errno(rc, (void)rc);
    for (uint32 i = 0; i < g_node_num; ++i) {
        g_nodeReportResStatInter[i].nodeId = g_node[i].node;
    }
#undef qsort
    qsort(g_nodeReportResStatInter, g_node_num, sizeof(ResStatReportInfo), ReportResStatInterCmp);
    return CM_SUCCESS;
}

static uint32 FindNodeReportResInterByNodeId(uint32 nodeId)
{
    if (nodeId >= g_node_num) {
        for (int i = (int)(g_node_num - 1); i >= 0; --i) {
            if (g_nodeReportResStatInter[i].nodeId == nodeId) {
                return (uint32)i;
            }
        }
        return g_node_num;
    }
    // In most cases, the position is nodeId - 1.
    uint32 comInd = nodeId - 1;
    if (g_nodeReportResStatInter[comInd].nodeId == nodeId) {
        return comInd;
    }
    if (g_nodeReportResStatInter[comInd].nodeId > nodeId) {
        for (int i = (int)(g_node_num - 1); i >= 0; --i) {
            if (g_nodeReportResStatInter[i].nodeId == nodeId) {
                return (uint32)i;
            }
        }
        return g_node_num;
    }
    // if g_nodeReportResStatInter[comInd].nodeId < nodeId
    for (uint32 i = comInd + 1; i < g_node_num; ++i) {
        if (g_nodeReportResStatInter[i].nodeId == nodeId) {
            return i;
        }
    }
    return g_node_num;
}

static void RecordResStatReport(uint32 nodeId)
{
    if (g_nodeReportResStatInter == NULL) {
        if (InitNodeReportResStatInter() != CM_SUCCESS) {
            return;
        }
    }

    uint32 ind = FindNodeReportResInterByNodeId(nodeId);
    if (ind < g_node_num) {
        g_nodeReportResStatInter[ind].reportInter = 0;
    } else {
        write_runlog(ERROR, "can't find nodeId(%u) in g_nodeReportResStatInter.\n", nodeId);
        for (uint32 i = 0; i < g_node_num; ++i) {
            write_runlog(ERROR, "g_nodeReportResStatInter[%u].nodeId = %u.\n", i, g_nodeReportResStatInter[i].nodeId);
        }
    }
}

void SetResStatReportInter(uint32 nodeId)
{
    if (g_nodeReportResStatInter == NULL) {
        return;
    }
    uint32 ind = FindNodeReportResInterByNodeId(nodeId);
    if (ind < g_node_num) {
        ++g_nodeReportResStatInter[ind].reportInter;
    } else {
        write_runlog(ERROR, "can't find nodeId(%u) in g_nodeReportResStatInter.\n", nodeId);
        for (uint32 i = 0; i < g_node_num; ++i) {
            write_runlog(ERROR, "g_nodeReportResStatInter[%u].nodeId = %u.\n", i, g_nodeReportResStatInter[i].nodeId);
        }
    }
}

uint32 GetResStatReportInter(uint32 nodeId)
{
    if (g_nodeReportResStatInter == NULL) {
        return 0;
    }
    uint32 ind = FindNodeReportResInterByNodeId(nodeId);
    if (ind < g_node_num) {
        return g_nodeReportResStatInter[ind].reportInter;
    }
    write_runlog(ERROR, "can't find nodeId(%u) in g_nodeReportResStatInter.\n", nodeId);
    for (uint32 i = 0; i < g_node_num; ++i) {
        write_runlog(ERROR, "g_nodeReportResStatInter[%u].nodeId = %u.\n", i, g_nodeReportResStatInter[i].nodeId);
    }
    return 0;
}

static bool IsResStatusChanged(const CmResourceStatus *newInstStat, CmResStatList *oldResStat)
{
    for (uint32 i = 0; i < oldResStat->status.instanceCount; ++i) {
        if (newInstStat->cmInstanceId != oldResStat->status.resStat[i].cmInstanceId) {
            continue;
        }
        uint32 oldStat = oldResStat->status.resStat[i].status;
        uint32 newStat = newInstStat->status;
        bool needNotifyClient = true;
        if (newStat != oldStat || newInstStat->isWorkMember != oldResStat->status.resStat[i].isWorkMember) {
            oldResStat->status.resStat[i].status = newStat;
            oldResStat->status.resStat[i].isWorkMember = newInstStat->isWorkMember;
            if (oldStat == (uint32)CM_RES_STAT_UNKNOWN || newStat == (uint32)CM_RES_STAT_UNKNOWN) {
                needNotifyClient = false;
            }
            if (newInstStat->isWorkMember == 0) {
                write_runlog(LOG, "[CLIENT] res(%s) inst(%u) get out, need release lock.\n",
                    newInstStat->resName, newInstStat->cmInstanceId);
            }
            ProcessReportResChangedMsg(needNotifyClient, oldResStat->status);
            return true;
        }
    }
    return false;
}

void ProcessAgent2CmResStatReportMsg(const ReportResStatus *resStatusPtr)
{
    uint32 index = 0;
    if (GetGlobalResStatusIndex(resStatusPtr->stat.resName, index) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] %s, unknown the resName(%s).\n", __func__, resStatusPtr->stat.resName);
        return;
    }

    RecordResStatReport(resStatusPtr->stat.nodeId);

    (void)pthread_rwlock_wrlock(&(g_resStatus[index].rwlock));
    if (IsResStatusChanged(&resStatusPtr->stat, &g_resStatus[index])) {
        ++g_resStatus[index].status.version;
        write_runlog(LOG, "[CLIENT] res(%s) status has changed, new version=%llu.\n",
                     g_resStatus[index].status.resName, g_resStatus[index].status.version);
    }
    (void)pthread_rwlock_unlock(&(g_resStatus[index].rwlock));
}

void ProcessRequestResStatusListMsg(CM_Connection *con)
{
    CmsReportResStatList sendMsg = {0};

    sendMsg.msgType = (int)MSG_CM_AGENT_RES_STATUS_LIST;

    for (uint32 i = 0; i < g_resStatus.size(); ++i) {
        (void)pthread_rwlock_wrlock(&(g_resStatus[i].rwlock));
        errno_t rc = memcpy_s(&sendMsg.resList, sizeof(OneResStatList), &g_resStatus[i].status, sizeof(OneResStatList));
        securec_check_errno(rc, (void)rc);
        (void)pthread_rwlock_unlock(&(g_resStatus[i].rwlock));
        (void)cm_server_send_msg(con, 'S', (char*)(&sendMsg), sizeof(CmsReportResStatList));
    }
}

static void CopyResStatusToSendMsg(CmsToCtlGroupResStatus *sendStat)
{
    for (uint32 i = 0; i < (uint32)g_resStatus.size(); ++i) {
        (void)pthread_rwlock_wrlock(&(g_resStatus[i].rwlock));
        for (uint32 j = 0; j < g_resStatus[i].status.instanceCount; ++j) {
            uint32 index = g_resStatus[i].status.resStat[j].nodeId - 1;
            sendStat->group_status[index].node = g_resStatus[i].status.resStat[j].nodeId;
            sendStat->group_status[index].status[sendStat->group_status[index].count].status =
                    g_resStatus[i].status.resStat[j].status;
            sendStat->group_status[index].status[sendStat->group_status[index].count].cmInstanceId =
                    g_resStatus[i].status.resStat[j].cmInstanceId;
            ++sendStat->group_status[index].count;
        }
        (void)pthread_rwlock_unlock(&(g_resStatus[i].rwlock));
    }
}

void ProcessResInstanceStatusMsg(CM_Connection *con, const CmsToCtlGroupResStatus *queryStatusPtr)
{
    CmsToCtlGroupResStatus instStatMsg = {0};

    if (queryStatusPtr->msg_step == QUERY_STATUS_CMSERVER_STEP) {
        instStatMsg.msg_type = (int)MSG_CM_QUERY_INSTANCE_STATUS;
        instStatMsg.msg_step = QUERY_STATUS_CMSERVER_STEP;
        instStatMsg.instance_type = queryStatusPtr->instance_type;
        if (queryStatusPtr->instance_type == PROCESS_RESOURCE) {
            instStatMsg.instance_type = PROCESS_RESOURCE;
            CopyResStatusToSendMsg(&instStatMsg);
        } else {
            write_runlog(ERROR, "unknown instance type %u for query instance status.\n", queryStatusPtr->instance_type);
        }
        (void)cm_server_send_msg(con, 'S', (char*)&(instStatMsg), sizeof(instStatMsg), DEBUG5);
    }
}
