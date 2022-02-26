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
 * cm_client.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_client/cm_client.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <pthread.h>
#include <sys/un.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/unistd.h>
#include "securec.h"
#include "c.h"
#include "cm/cm_elog.h"
#include "cm_client.h"

SetDataFlag g_setData;
GetDataFlag g_resData;
SendMsgQueue g_sendMsg;
OneResStatList g_clientStatusList;

static ConnAgent *g_agentConnect = NULL;

static char g_resName[CM_MAX_RES_NAME];

SetDataFlag &GetSetDataVector()
{
    return g_setData;
}

GetDataFlag &GetResDataVector()
{
    return g_resData;
}

OneResStatList &GetClientStatusList()
{
    return g_clientStatusList;
}

char *GetResName()
{
    return g_resName;
}

void SendMsgApi(char *msg)
{
    (void)pthread_mutex_lock(&g_sendMsg.lock);
    g_sendMsg.sendQueue.push(msg);
    (void)pthread_mutex_unlock(&g_sendMsg.lock);
}

static inline void ConnectSetTimeout(const ConnAgent *con)
{
    struct timeval tv = { 0, 0 };

    tv.tv_sec = CLIENT_TCP_TIMEOUT;
    (void)setsockopt(con->sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv, sizeof(tv));
    (void)setsockopt(con->sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv));
}

static ConnAgent* ConnectEmptyInit()
{
    ConnAgent *con = NULL;

    con = (ConnAgent*) malloc(sizeof(ConnAgent));
    if (con == NULL) {
        write_runlog(LOG, "Res:%s Line:%d, Out of memory for client to get connect with agent!\n",
                     g_resName, __LINE__);
        return con;
    }

    con->sock = CLIENT_INVALID_SOCKET;
    con->isClosed = true;

    return con;
}

static void ConnectClose(ConnAgent *con)
{
    if (con->isClosed) {
        return;
    }

    (void)cm_close_socket((int)con->sock);
    con->sock = CLIENT_INVALID_SOCKET;
    con->isClosed = true;
}

static void ConnectCreate(ConnAgent *con)
{
    int ret;
    char homePath[CM_MAX_PATH_LEN] = {0};
    char serverPath[CM_MAX_PATH_LEN] = {0};
    errno_t rc;
    SockAddr remoteSock{};

    if (!con->isClosed) {
        write_runlog(LOG, "Res:%s Line:%d, Create connect failed, because connect has been created.\n",
                     g_resName, __LINE__);
        return;
    }
    if (GetHomePath(homePath, sizeof(homePath)) != 0) {
        return;
    }
    rc = snprintf_s(serverPath, CM_MAX_PATH_LEN, CM_MAX_PATH_LEN - 1, "%s/bin/%s", homePath, CM_DOMAIN_SOCKET);
    securec_check_intval(rc, (void)rc);

    con->sock = (int)socket(AF_UNIX, SOCK_STREAM, 0);
    if (CLIENT_INVALID_SOCKET == con->sock) {
        write_runlog(ERROR, "Res:%s Line:%d, Creat connect socket failed.\n", g_resName, __LINE__);
        return;
    }

    ConnectSetTimeout(con);

    remoteSock.addrLen = sizeof(remoteSock.addr);
    rc = memset_s(&remoteSock.addr, remoteSock.addrLen, 0, remoteSock.addrLen);
    securec_check_errno(rc, (void)rc);

    remoteSock.addr.sun_family = AF_UNIX;
    rc = strcpy_s(remoteSock.addr.sun_path, sizeof(remoteSock.addr.sun_path), serverPath);
    securec_check_errno(rc, (void)rc);

    ret = connect(con->sock, (struct sockaddr *)&remoteSock.addr, remoteSock.addrLen);
    if (ret < 0) {
        write_runlog(LOG, "Res:%s Line:%d, Client connect to agent error.\n", g_resName, __LINE__);
        cm_close_socket(con->sock);
        con->sock = CLIENT_INVALID_SOCKET;
        return;
    }
    con->isClosed = false;
}

static status_t ConnectSendInitMsg()
{
    errno_t rc;
    ClientInitMsg *initMsg = NULL;

    initMsg = (ClientInitMsg*) malloc(sizeof(ClientInitMsg));
    if (initMsg == NULL) {
        write_runlog(ERROR, "Res:%s Line:%d, Out of memory for client to create init msg!\n", g_resName, __LINE__);
        return CM_ERROR;
    }

    initMsg->head.msgVer = CM_CLIENT_MSG_VER;
    initMsg->head.msgType = MSG_CLIENT_AGENT_INIT_DATA;
    initMsg->resInfo.resInstanceId = g_agentConnect->resInstanceId;
    rc = strcpy_s(initMsg->resInfo.resName, CM_MAX_RES_NAME, g_resName);
    securec_check_errno(rc, (void)rc);

    SendMsgApi((char*)initMsg);

    return CM_SUCCESS;
}

void *ConnectAgentMain(void *arg)
{
    for (;;) {
        if (g_agentConnect->isClosed) {
            write_runlog(LOG, "Res:%s Line:%d, cm_client connect to cm_agent start.\n", g_resName, __LINE__);
            ConnectCreate(g_agentConnect);
            if (g_agentConnect->isClosed) {
                write_runlog(LOG, "Res:%s Line:%d, cm_client connect to cm_agent fail errno = %d, retry.\n",
                             g_resName, __LINE__, errno);
                cm_usleep(CLIENT_CHECK_CONN_INTERVAL);
                continue;
            }
            write_runlog(LOG, "Res:%s Line:%d, cm_client connect to cm_agent success.\n", g_resName, __LINE__);
            if (ConnectSendInitMsg() != CM_SUCCESS) {
                write_runlog(DEBUG1, "Res:%s Line:%d, push init msg in send queue failed, close the connect!\n",
                             g_resName, __LINE__);
                ConnectClose(g_agentConnect);
            }
        }
        cm_usleep(CLIENT_CHECK_CONN_INTERVAL);
    }
    return NULL;
}

status_t CreateConnectAgentThread()
{
    int err;
    pthread_t thrId;

    if ((err = pthread_create(&thrId, NULL, ConnectAgentMain, NULL) != 0)) {
        write_runlog(LOG, "Res:%s Line:%d, Failed to create new thread: error %d\n", g_resName, __LINE__, err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static status_t TcpSendMsg(const ConnAgent *con, const char *buf, int remainSize)
{
    int sentSize;
    uint32 offset = 0;

    if (con->isClosed) {
        return CM_ERROR;
    }

    while (remainSize > 0) {
        sentSize = send(con->sock, buf + offset, remainSize, 0);
        if (sentSize == 0) {
            write_runlog(LOG, "Res:%s Line:%d, [tcp] Client disconnect with agent, errno=%d.",
                         g_resName, __LINE__, errno);
            return CM_ERROR;
        }
        if (sentSize < 0) {
            if (errno == EINTR) {
                continue;
            }
            write_runlog(LOG, "Res:%s Line:%d, [tcp] Client can't send msg to agent with errno=%d\n",
                         g_resName, __LINE__, errno);
            return CM_ERROR;
        }
        offset += sentSize;
        remainSize -= sentSize;
    }

    return CM_SUCCESS;
}

static void SendHeartbeat()
{
    status_t ret;
    ClientHbMsg heartbeatMsg;

    if (g_agentConnect->isClosed) {
        return;
    }

    heartbeatMsg.head.msgVer = CM_CLIENT_MSG_VER;
    heartbeatMsg.head.msgType = MSG_CLIENT_AGENT_HEARTBEAT;
    heartbeatMsg.version = g_clientStatusList.version;

    ret = TcpSendMsg(g_agentConnect, (char*)&heartbeatMsg, sizeof(ClientHbMsg));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "Res:%s Line:%d, cm_client send heartbeat failed!\n", g_resName, __LINE__);
        ConnectClose(g_agentConnect);
        return;
    }
}

static status_t SendMsgToAgent(const ConnAgent *con, const char *buf)
{
    size_t msgLen;
    errno_t rc;
    MsgHead head;
    status_t ret;

    rc = memcpy_s(&head, sizeof(MsgHead), buf, sizeof(MsgHead));
    securec_check_errno(rc, (void)rc);

    switch (head.msgType) {
        case MSG_CLIENT_AGENT_INIT_DATA:
            msgLen = sizeof(ClientInitMsg);
            break;
        case MSG_CLIENT_AGENT_SET_DATA:
            msgLen = sizeof(ClientSetDataMsg);
            break;
        case MSG_CLIENT_AGENT_SET_RES_DATA:
            msgLen = sizeof(ClientSetResDataMsg);
            break;
        case MSG_CLIENT_AGENT_GET_RES_DATA:
            msgLen = sizeof(ClientGetResDataMsg);
            break;
        default:
            write_runlog(LOG, "Line:%d cm_client will send unknown msg, blocking.\n", __LINE__);
            msgLen = 0;
            break;
    }
    ret = TcpSendMsg(con, buf, (int32)msgLen);

    return ret;
}

void *SendMsgToAgentMain(void *arg)
{
    status_t ret;
    struct timespec currentTime = { 0, 0 };
    struct timespec lastReportTime = { 0, 0 };

    (void)clock_gettime(CLOCK_MONOTONIC, &lastReportTime);
    for (;;) {
        if (g_agentConnect->isClosed) {
            cm_usleep(CLIENT_SEND_CHECK_INTERVAL);
            continue;
        }

        (void)pthread_mutex_lock(&g_sendMsg.lock);
        if (!g_sendMsg.sendQueue.empty()) {
            ret = SendMsgToAgent(g_agentConnect, g_sendMsg.sendQueue.front());
            if (ret != CM_SUCCESS) {
                write_runlog(LOG, "Res:%s Line:%d, Client send msg to agent failed!\n", g_resName, __LINE__);
                ConnectClose(g_agentConnect);
            } else {
                free(g_sendMsg.sendQueue.front());
                g_sendMsg.sendQueue.pop();
            }
            (void)pthread_mutex_unlock(&g_sendMsg.lock);
        } else {
            (void)pthread_mutex_unlock(&g_sendMsg.lock);
            cm_usleep(CLIENT_SEND_CHECK_INTERVAL);
        }
        (void)clock_gettime(CLOCK_MONOTONIC, &currentTime);
        if ((currentTime.tv_sec - lastReportTime.tv_sec) >= HEARTBEAT_SEND_INTERVAL) {
            SendHeartbeat();
            (void)clock_gettime(CLOCK_MONOTONIC, &lastReportTime);
        }
    }
    return NULL;
}

status_t CreateSendMsgThread()
{
    int err;

    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, SendMsgToAgentMain, NULL) != 0)) {
        write_runlog(LOG, "Res:%s, Failed to create new thread: error %d\n", g_resName, err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static status_t TcpRecvMsg(const ConnAgent *con, char *buf, int remainSize)
{
    int recvSize;
    uint32 offset = 0;

    if (con->isClosed) {
        return CM_ERROR;
    }

    while (remainSize > 0) {
        recvSize = recv(con->sock, buf + offset, remainSize, 0);
        if (recvSize == 0) {
            write_runlog(LOG, "Res:%s Line:%d, [tcp] Client disconnect with agent.\n", g_resName, __LINE__);
            return CM_ERROR;
        }
        if (recvSize < 0) {
            if (errno == EINTR) {
                continue;
            }
            write_runlog(LOG, "Res:%s Line:%d, [tcp] Can't receive msg from agent with errno=(%d)\n",
                         g_resName, __LINE__, errno);
            return CM_ERROR;
        }
        remainSize -= recvSize;
        offset += (uint32)recvSize;
    }

    return CM_SUCCESS;
}

static inline void RecvHeartbeatAckProcess()
{
    write_runlog(DEBUG1, "Res:%s Line:%d, Recv heartbeat ack from agent\n", g_resName, __LINE__);
}

static void RecvResStatusListProcess(uint32 isNotifyChange)
{
    status_t ret;

    ret = TcpRecvMsg(g_agentConnect, (char*)&g_clientStatusList, sizeof(OneResStatList));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "Res:%s Line:%d, Recv status_t from agent list fail\n", g_resName, __LINE__);
        return;
    }

    if (isNotifyChange) {
        g_agentConnect->callback();
    }
}

static void RecvSetResultProcess()
{
    SetResult result;
    errno_t rc = memset_s(&result, sizeof(SetResult), 0, sizeof(SetResult));
    securec_check_errno(rc, (void)rc);

    status_t ret = TcpRecvMsg(g_agentConnect, (char*)&result, sizeof(SetResult));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "Res:%s Line:%d, cm_client recv res data from agent fail or timeout.\n", g_resName, __LINE__);
        return;
    }

    (void)pthread_mutex_lock(&g_setData.lock);
    g_setData.isSetSuccess = result.isSetSuccess;
    (void)pthread_mutex_unlock(&g_setData.lock);

    (void)pthread_cond_signal(&g_setData.cond);
}

static void RecvResDataProcess()
{
    ResData resData = {0};

    status_t ret = TcpRecvMsg(g_agentConnect, (char*)&resData, sizeof(ResData));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "Res:%s Line:%d, cm_client recv res data from agent fail or timeout.\n", g_resName, __LINE__);
        return;
    }
    (void)pthread_mutex_lock(&g_resData.lock);
    errno_t rc = memcpy_s(&g_resData.resData, sizeof(resData), &resData, sizeof(resData));
    securec_check_errno(rc, (void)rc);
    (void)pthread_mutex_unlock(&g_resData.lock);

    (void)pthread_cond_signal(&g_resData.cond);
}

void *RecvMsgFromAgentMain(void *arg)
{
    status_t ret;
    MsgHead msgHead = {0};

    for (;;) {
        if (g_agentConnect->isClosed) {
            cm_usleep(CLIENT_CHECK_CONN_INTERVAL);
            continue;
        }

        ret = TcpRecvMsg(g_agentConnect, (char*)&msgHead, sizeof(MsgHead));
        if (ret != CM_SUCCESS) {
            write_runlog(LOG, "Res:%s Line:%d, cm_client recv msg from agent fail or timeout.\n", g_resName, __LINE__);
            ConnectClose(g_agentConnect);
            cm_usleep(CLIENT_RECV_CHECK_INTERVAL);
            continue;
        }

        switch (msgHead.msgType) {
            case MSG_AGENT_CLIENT_HEARTBEAT_ACK:
                RecvHeartbeatAckProcess();
                break;
            case MSG_AGENT_CLIENT_RES_STATUS_LIST:
                RecvResStatusListProcess(NO_STAT_CHANGED);
                break;
            case MSG_AGENT_CLIENT_RES_STATUS_CHANGE:
                RecvResStatusListProcess(STAT_CHANGED);
                break;
            case MSG_AGENT_CLIENT_SET_RES_DATA_STATUS:
                RecvSetResultProcess();
                break;
            case MSG_AGENT_CLIENT_REPORT_RES_DATA:
                RecvResDataProcess();
                break;
            default:
                write_runlog(LOG, "Res:%s Line:%d, client recv unknown msg from agent, msgType(%d).\n",
                             g_resName, __LINE__, (int)msgHead.msgType);
                ConnectClose(g_agentConnect);
                break;
        }
    }
    return NULL;
}

status_t CreateRecvMsgThread()
{
    int err;

    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, RecvMsgFromAgentMain, NULL) != 0)) {
        write_runlog(ERROR, "Res:%s, Failed to create new thread: error %d\n", g_resName, err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static status_t InitLogFile()
{
    int rc;
    char logPath[CM_MAX_PATH_LEN] = {0};

    char sysLogPath[CM_MAX_PATH_LEN] = {0};

    prefix_name = "cm_client";

    rc = cm_getenv("GAUSSLOG", logPath, sizeof(logPath));
    if (rc != EOK) {
        printf("cm_client get gausslog dir failed\n");
        return CM_ERROR;
    }
    check_input_for_security(logPath);
    rc = snprintf_s(sysLogPath, CM_MAX_PATH_LEN, CM_MAX_PATH_LEN - 1, "%s/cm/%s", logPath, "cm_client");
    securec_check_intval(rc, (void)rc);

    (void)mkdir(sysLogPath, S_IRWXU);

    if (SetLogFilePath(sysLogPath) == -1) {
        return CM_ERROR;
    }

    write_runlog(LOG, "Res:%s Line:%d, init cm_client log file success.\n", g_resName, __LINE__);
    return CM_SUCCESS;
}

inline void EmptyCallback()
{
    write_runlog(LOG, "Res:%s Line:%d, client init null call back func.\n", g_resName, __LINE__);
}

status_t PreInit(uint32 instanceId, const char *resName, cm_notify_func_t func)
{
    errno_t rc;

    (void)syscalllockInit(&g_cmEnvLock);

    if (InitLogFile() != CM_SUCCESS) {
        return CM_ERROR;
    }

    g_agentConnect = ConnectEmptyInit();
    if (g_agentConnect == NULL) {
        return CM_ERROR;
    }

    if (func == NULL) {
        g_agentConnect->callback = EmptyCallback;
    } else {
        g_agentConnect->callback = func;
    }

    g_agentConnect->resInstanceId = instanceId;
    rc = strcpy_s(g_resName, CM_MAX_RES_NAME, resName);
    securec_check_errno(rc, (void)rc);

    rc = memset_s(&g_clientStatusList, sizeof(OneResStatList), 0, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);

    return CM_SUCCESS;
}
