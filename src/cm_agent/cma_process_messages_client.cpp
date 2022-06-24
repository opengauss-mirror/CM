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
 * cma_process_messages_client.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_process_messages_client.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm/cm_elog.h"
#include "cma_connect.h"
#include "cma_global_params.h"
#include "cma_connect_client.h"
#include "cma_common.h"
#include "cma_process_messages_client.h"

static void SendHeartbeatAckToClient(uint32 conId)
{
    MsgHead *hbAck = (MsgHead*)malloc(sizeof(MsgHead));
    if (hbAck == NULL) {
        write_runlog(LOG, "[CLIENT] malloc failed, SendHeartbeatAckToClient.\n");
        return;
    }
    errno_t rc = memset_s(hbAck, sizeof(MsgHead), 0, sizeof(MsgHead));
    securec_check_errno(rc, (void)rc);
    hbAck->msgType = MSG_AGENT_CLIENT_HEARTBEAT_ACK;

    PushMsgToClientSendQue((char*)hbAck, sizeof(MsgHead), conId);
    write_runlog(DEBUG5, "[CLIENT] push client msg to send queue, hb ack msg conId(%u).\n", conId);
}

static void SendStatusListToClient(CmResStatList &statList, uint32 conId, bool isNotifyChange)
{
    AgentToClientResList *sendList = (AgentToClientResList*)malloc(sizeof(AgentToClientResList));
    if (sendList == NULL) {
        write_runlog(LOG, "[CLIENT] malloc failed, SendStatusListToClient.\n");
        return;
    }
    errno_t rc = memset_s(sendList, sizeof(AgentToClientResList), 0, sizeof(AgentToClientResList));
    securec_check_errno(rc, (void)rc);
    if (isNotifyChange) {
        sendList->head.msgType = MSG_AGENT_CLIENT_RES_STATUS_CHANGE;
    } else {
        sendList->head.msgType = MSG_AGENT_CLIENT_RES_STATUS_LIST;
    }

    (void)pthread_rwlock_wrlock(&(statList.rwlock));
    rc = memcpy_s(&sendList->resStatusList, sizeof(OneResStatList), &statList.status, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);
    (void)pthread_rwlock_unlock(&(statList.rwlock));

    write_runlog(LOG, "[CLIENT] send status list to res(%s), version=%llu.\n",
        sendList->resStatusList.resName, sendList->resStatusList.version);
    for (uint32 i = 0; i < sendList->resStatusList.instanceCount; ++i) {
        write_runlog(LOG, "nodeId=%u,instanceId=%u,status=%u,isWork=%u\n",
            sendList->resStatusList.resStat[i].nodeId,
            sendList->resStatusList.resStat[i].cmInstanceId,
            sendList->resStatusList.resStat[i].status,
            sendList->resStatusList.resStat[i].isWorkMember);
    }

    PushMsgToClientSendQue((char*)sendList, sizeof(AgentToClientResList), conId);
    write_runlog(DEBUG5, "[CLIENT] push client msg to queue, res(%s) stat list.\n", sendList->resStatusList.resName);
}

static void ProcessClientHeartbeat(const ClientHbMsg &hbMsg)
{
    uint32 index = 0;
    ClientConn *clientCon = GetClientConnect();
    if (GetGlobalResStatusIndex(clientCon[hbMsg.head.conId].resName, index) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] ProcessClientHeartbeat, unknown the resName(%s) of client.\n",
            clientCon[hbMsg.head.conId].resName);
        return;
    }

    (void)pthread_rwlock_wrlock(&(g_resStatus[index].rwlock));
    if (hbMsg.version == g_resStatus[index].status.version) {
        (void)pthread_rwlock_unlock(&(g_resStatus[index].rwlock));
        SendHeartbeatAckToClient(hbMsg.head.conId);
        return;
    }
    (void)pthread_rwlock_unlock(&(g_resStatus[index].rwlock));
    SendStatusListToClient(g_resStatus[index], hbMsg.head.conId, false);
}

static void ProcessInitMsg(const ClientInitMsg &initData)
{
    AgentToClientInitResult *sendMsg = (AgentToClientInitResult*)malloc(sizeof(AgentToClientInitResult));
    if (sendMsg == NULL) {
        write_runlog(LOG, "[CLIENT] malloc failed, ProcessInitMsg.\n");
        return;
    }
    errno_t rc = memset_s(sendMsg, sizeof(AgentToClientInitResult), 0, sizeof(AgentToClientInitResult));
    securec_check_errno(rc, (void)rc);

    sendMsg->head.msgType = (uint32)MSG_AGENT_CLIENT_INIT_ACK;
    sendMsg->head.conId = initData.head.conId;
    sendMsg->result.isSuccess = false;

    ClientConn *clientCon = GetClientConnect();
    for (const CmResConfList &resInfo : g_resConf) {
        if ((strcmp(initData.resInfo.resName, resInfo.resName) == 0) && (g_currentNode->node == resInfo.nodeId) &&
            initData.resInfo.resInstanceId == resInfo.resInstanceId) {
            clientCon[initData.head.conId].cmInstanceId = resInfo.cmInstanceId;
            clientCon[initData.head.conId].resInstanceId = resInfo.resInstanceId;
            rc = strcpy_s(clientCon[initData.head.conId].resName, CM_MAX_RES_NAME, initData.resInfo.resName);
            securec_check_errno(rc, (void)rc);
            sendMsg->result.isSuccess = true;
            break;
        }
    }

    if (sendMsg->result.isSuccess) {
        write_runlog(LOG, "[CLIENT] res(%s) init success.\n", initData.resInfo.resName);
    } else {
        write_runlog(LOG, "[CLIENT] res(%s) init failed, init cfg: nodeId(%u), resInstId(%u).\n",
            initData.resInfo.resName, g_currentNode->node, initData.resInfo.resInstanceId);
    }

    PushMsgToClientSendQue((char*)sendMsg, sizeof(AgentToClientInitResult), initData.head.conId);
    write_runlog(DEBUG5, "[CLIENT] push client msg to send queue, res(%s) init result.\n", initData.resInfo.resName);
}

static void ProcessClientMsg(char *recvMsg)
{
    const MsgHead *head = (MsgHead *)recvMsg;

    switch (head->msgType) {
        case MSG_CLIENT_AGENT_INIT_DATA: {
            ClientInitMsg *initMsg = (ClientInitMsg *)recvMsg;
            ProcessInitMsg(*initMsg);
            break;
        }
        case MSG_CLIENT_AGENT_HEARTBEAT: {
            ClientHbMsg *hbMsg = (ClientHbMsg *)recvMsg;
            ProcessClientHeartbeat(*hbMsg);
            break;
        }
        default:
            write_runlog(LOG, "[CLIENT] agent get unknown msg from client\n");
            break;
    }
}

void* ProcessMessageMain(void * const arg)
{
    for (;;) {
        (void)pthread_mutex_lock(&g_recvQueue.lock);
        while (g_recvQueue.msg.empty()) {
            (void)pthread_cond_wait(&g_recvQueue.cond, &g_recvQueue.lock);
        }
        char *msg = g_recvQueue.msg.front().msgPtr;
        g_recvQueue.msg.pop();
        (void)pthread_mutex_unlock(&g_recvQueue.lock);
        ProcessClientMsg(msg);
        FREE_AND_RESET(msg);
    }
    return NULL;
}

void ProcessResStatusList(const CmsReportResStatList *msg)
{
    uint32 index = 0;
    if (GetGlobalResStatusIndex(msg->resList.resName, index) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] ProcessResStatusList, unknown the res(%s) of client.\n", msg->resList.resName);
        return;
    }
    (void)pthread_rwlock_wrlock(&(g_resStatus[index].rwlock));
    errno_t rc = memcpy_s(&g_resStatus[index].status, sizeof(OneResStatList), &msg->resList, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);
    (void)pthread_rwlock_unlock(&(g_resStatus[index].rwlock));

    write_runlog(LOG, "[CLIENT] res(%s) statList changed, version=%llu.\n", msg->resList.resName, msg->resList.version);
    for (uint32 j = 0; j < msg->resList.instanceCount; ++j) {
        if (j == CM_MAX_INSTANCES) {
            break;
        }
        write_runlog(LOG, "nodeId=%u,instanceId=%u,status=%u,isWork=%u\n",
            g_resStatus[index].status.resStat[j].nodeId,
            g_resStatus[index].status.resStat[j].cmInstanceId,
            g_resStatus[index].status.resStat[j].status,
            g_resStatus[index].status.resStat[j].isWorkMember);
    }
}

void ProcessResStatusChanged(const CmsReportResStatList *msg)
{
    ProcessResStatusList(msg);
    uint32 index = 0;
    if (GetGlobalResStatusIndex(msg->resList.resName, index) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] ProcessResStatusChanged, unknown the res(%s) of client.\n", msg->resList.resName);
        return;
    }
    ClientConn *clientCon = GetClientConnect();
    for (uint32 i = 0; i < MAX_RES_NUM; ++i) {
        if (clientCon[i].isClosed || strcmp(clientCon[i].resName, msg->resList.resName) != 0) {
            continue;
        }
        SendStatusListToClient(g_resStatus[index], i, true);
    }
}
