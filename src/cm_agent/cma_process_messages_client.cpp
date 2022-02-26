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
#include <vector>
#include "cm/cm_elog.h"
#include "cma_connect.h"
#include "cma_global_params.h"
#include "cma_connect_client.h"
#include "cma_process_messages_client.h"

using namespace std;

static void ProcessClientHeartbeat(const ClientHbMsg &hbMsg)
{
    MsgHead *hbAck = NULL;
    MsgHead *sendList = NULL;
    ClientSendQueue &sendQueue = GetSendQueueApi();
    CmResStatList &statusList = GetResStatusListApi();

    if (hbMsg.version == statusList.version) {
        hbAck = (MsgHead*) malloc(sizeof(MsgHead));
        if (hbAck == NULL) {
            write_runlog(LOG, "[agent] Out of memory: hbAck failed.\n");
            return;
        }
        hbAck->conId = hbMsg.head.conId;
        hbAck->msgType = MSG_AGENT_CLIENT_HEARTBEAT_ACK;

        (void)pthread_mutex_lock(&sendQueue.lock);
        sendQueue.queue.push((char*)hbAck);

        goto unlock;
    }

    sendList = (MsgHead*) malloc(sizeof(MsgHead));
    if (sendList == NULL) {
        write_runlog(LOG, "[agent] Out of memory: sendList failed.\n");
        return;
    }
    sendList->conId = hbMsg.head.conId;
    sendList->msgType = MSG_AGENT_CLIENT_RES_STATUS_LIST;

    (void)pthread_mutex_lock(&sendQueue.lock);
    sendQueue.queue.push((char*)sendList);

    goto unlock;

unlock:
    (void)pthread_mutex_unlock(&sendQueue.lock);
    (void)pthread_cond_signal(&sendQueue.cond);
}

static void ProcessInitMsg(const ClientInitMsg &initData)
{
    errno_t rc;
    vector<ClientConn> &clientCon = GetClientConnect();

    for (ResourceListInfo resInfo : g_res_list) {
        if ((strcmp(initData.resInfo.resName, resInfo.resName) == 0) &&
            ((g_nodeId + 1) == resInfo.nodeId) &&
            initData.resInfo.resInstanceId == resInfo.resInstanceId) {
            clientCon[initData.head.conId].cmInstanceId = resInfo.cmInstanceId;
            clientCon[initData.head.conId].resInstanceId = resInfo.resInstanceId;
            rc = strcpy_s(clientCon[initData.head.conId].resName, CM_MAX_RES_NAME, initData.resInfo.resName);
            securec_check_errno(rc, (void)rc);
            return;
        }
    }
}

static void ProcessSetInstanceData(const uint32 &conId, const int64 &data)
{
    int ret;
    ReportSetInstanceData sendMsg;
    vector<ClientConn> &clientCon = GetClientConnect();

    sendMsg.msgType = (int)MSG_AGENT_CM_SET_INSTANCE_DATA;
    sendMsg.data = data;
    sendMsg.nodeId = g_nodeId + 1;
    sendMsg.instanceId = clientCon[conId].cmInstanceId;

    write_runlog(LOG, "set instanceData, instanceId %u, nodeId %u!\n", clientCon[conId].cmInstanceId, g_nodeId);
    ret = cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&sendMsg, sizeof(ReportSetInstanceData));
    if (ret != 0) {
        write_runlog(ERROR, "send agent set res data to cms failed!\n");
        CloseConnToCmserver();
    }
}

static void ProcessSetResData(const ClientSetResDataMsg &recvMsg)
{
    int ret;
    errno_t rc;
    ReportSetResData sendMsg;
    vector<ClientConn> &clientCon = GetClientConnect();

    sendMsg.msgType = (int)MSG_AGENT_CM_SET_RES_DATA;
    sendMsg.conId = recvMsg.head.conId;

    rc = strcpy_s(sendMsg.resName, CM_MAX_RES_NAME, clientCon[recvMsg.head.conId].resName);
    securec_check_errno(rc, (void)rc);

    rc = memcpy_s(&sendMsg.resData, sizeof(ResData), &recvMsg.data, sizeof(ResData));
    securec_check_errno(rc, (void)rc);

    write_runlog(LOG, "set res data, slotId=%lu.\n", recvMsg.data.slotId);
    ret = cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&sendMsg, sizeof(ReportSetResData));
    if (ret != 0) {
        write_runlog(ERROR, "send agent set res data to cms failed!\n");
        CloseConnToCmserver();
    }
}

static void ProcessGetResData(const uint32 &conId, const uint32 &slotId)
{
    int ret;
    errno_t rc;
    RequestGetResData sendMsg;
    vector<ClientConn> &clientCon = GetClientConnect();

    sendMsg.msgType = (int)MSG_AGENT_CM_GET_RES_DATA;
    sendMsg.slotId = slotId;
    sendMsg.conId = conId;
    rc = strcpy_s(sendMsg.resName, CM_MAX_RES_NAME, clientCon[conId].resName);
    securec_check_errno(rc, (void)rc);

    ret = cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&sendMsg, sizeof(RequestGetResData));
    if (ret != 0) {
        write_runlog(ERROR, "send agent set res data to cms failed!\n");
        CloseConnToCmserver();
    }
}

static void ProcessClientMsg(char *recvMsg)
{
    const MsgHead *head = reinterpret_cast<const MsgHead *>(reinterpret_cast<const void *>(recvMsg));

    switch (head->msgType) {
        case MSG_CLIENT_AGENT_HEARTBEAT: {
            ClientHbMsg *hbMsg = reinterpret_cast<ClientHbMsg *>(reinterpret_cast<void *>(recvMsg));
            ProcessClientHeartbeat(*hbMsg);
            break;
            }
        case MSG_CLIENT_AGENT_INIT_DATA: {
            ClientInitMsg *initMsg = reinterpret_cast<ClientInitMsg *>(reinterpret_cast<void *>(recvMsg));
            ProcessInitMsg(*initMsg);
            break;
            }
        case MSG_CLIENT_AGENT_SET_DATA: {
            ClientSetDataMsg *setDataMsg = reinterpret_cast<ClientSetDataMsg *>(reinterpret_cast<void *>(recvMsg));
            ProcessSetInstanceData(setDataMsg->head.conId, setDataMsg->instanceData);
            break;
            }
        case MSG_CLIENT_AGENT_SET_RES_DATA: {
            ClientSetResDataMsg *setResDataMsg =
                reinterpret_cast<ClientSetResDataMsg *>(reinterpret_cast<void *>(recvMsg));
            ProcessSetResData(*setResDataMsg);
            break;
            }
        case MSG_CLIENT_AGENT_GET_RES_DATA: {
            ClientGetResDataMsg *getResDataMsg =
                reinterpret_cast<ClientGetResDataMsg *>(reinterpret_cast<void *>(recvMsg));
            ProcessGetResData(getResDataMsg->head.conId, getResDataMsg->slotId);
            break;
        }
        default:
            write_runlog(LOG, "agent get unknown msg from client\n");
            break;
    }
}

void* ProcessMessageMain(void * const arg)
{
    char *msg = NULL;
    ClientRecvQueue &recvQueue = GetRecvQueueApi();

    for (;;) {
        (void)pthread_mutex_lock(&recvQueue.lock);
        while (recvQueue.queue.empty()) {
            (void)pthread_cond_wait(&recvQueue.cond, &recvQueue.lock);
        }
        msg = recvQueue.queue.front();
        recvQueue.queue.pop();
        (void)pthread_mutex_unlock(&recvQueue.lock);

        ProcessClientMsg(msg);
        free(msg);
        msg = NULL;
    }
    return NULL;
}

void ProcessResStatusChanged(const CmsReportResStatList *msg)
{
    ClientSendQueue &sendQueue = GetSendQueueApi();
    vector<ClientConn> &clientCon = GetClientConnect();

    ProcessResStatusList(msg);

    for (uint32 i = 0; i < MAX_RES_NUM; ++i) {
        if (clientCon[i].isClosed) {
            continue;
        }

        MsgHead *sendList = NULL;
        sendList = (MsgHead*) malloc(sizeof(MsgHead));
        if (sendList == NULL) {
            write_runlog(LOG, "Out of memory: sendList failed.\n");
            return;
        }
        sendList->conId = i;
        sendList->msgType = MSG_AGENT_CLIENT_RES_STATUS_CHANGE;

        (void)pthread_mutex_lock(&sendQueue.lock);
        sendQueue.queue.push((char*)sendList);

        (void)pthread_mutex_unlock(&sendQueue.lock);
        (void)pthread_cond_signal(&sendQueue.cond);
    }
}

void ProcessResDataSetResult(const CmsReportSetDataResult *msg)
{
    AgentToClientSetDataResult *sendMsg = NULL;
    ClientSendQueue &sendQueue = GetSendQueueApi();

    sendMsg = (AgentToClientSetDataResult*) malloc(sizeof(AgentToClientSetDataResult));
    if (sendMsg == NULL) {
        write_runlog(LOG, "[agent] Out of memory: AgentToClientSetDataResult failed.\n");
        return;
    }

    sendMsg->head.msgType = MSG_AGENT_CLIENT_SET_RES_DATA_STATUS;
    sendMsg->head.conId = msg->conId;
    sendMsg->result.isSetSuccess = msg->isSetSuccess;

    write_runlog(LOG, "set slot %lu, state=%d\n", msg->slotId, (int)msg->isSetSuccess);

    (void)pthread_mutex_lock(&sendQueue.lock);
    sendQueue.queue.push((char*)sendMsg);

    (void)pthread_mutex_unlock(&sendQueue.lock);
    (void)pthread_cond_signal(&sendQueue.cond);
}

void ProcessResDataFromCms(const CmsReportResData *msg)
{
    errno_t rc;
    AgentToClientResData *sendData = NULL;
    ClientSendQueue &sendQueue = GetSendQueueApi();

    sendData = (AgentToClientResData*) malloc(sizeof(AgentToClientResData));
    if (sendData == NULL) {
        write_runlog(LOG, "[agent] Out of memory: ResData failed.\n");
        return;
    }

    rc = memcpy_s(&sendData->msgData, sizeof(ResData), &msg->resData, sizeof(ResData));
    securec_check_errno(rc, (void)rc);

    sendData->head.msgType = MSG_AGENT_CLIENT_REPORT_RES_DATA;
    sendData->head.conId = msg->conId;

    (void)pthread_mutex_lock(&sendQueue.lock);
    sendQueue.queue.push((char*)sendData);

    (void)pthread_mutex_unlock(&sendQueue.lock);
    (void)pthread_cond_signal(&sendQueue.cond);
}
