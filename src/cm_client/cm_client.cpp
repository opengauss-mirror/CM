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
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/unistd.h>
#include "securec.h"
#include "c.h"
#include "cm/cm_elog.h"
#include "cm_client.h"

InitFlag g_initFlag;
bool g_isClientInit = false;

static char g_resName[CM_MAX_RES_NAME] = {0};
static ConnAgent g_agentConnect = {0};
static bool g_shutdownClient = false;
static SendMsgQueue g_sendMsg;
static OneResStatList g_clientStatusList;

bool &GetIsClientInit()
{
    return g_isClientInit;
}

OneResStatList &GetClientStatusList()
{
    return g_clientStatusList;
}

void SendMsgApi(char *msgPtr, size_t msgLen)
{
    MsgPackage msg = {msgPtr, msgLen};
    (void)pthread_mutex_lock(&g_sendMsg.lock);
    g_sendMsg.sendQueue.push(msg);
    (void)pthread_mutex_unlock(&g_sendMsg.lock);
    (void)pthread_cond_signal(&g_sendMsg.cond);
}

status_t SendInitMsg(uint32 instanceId, const char *resName)
{
    ClientInitMsg *initMsg = (ClientInitMsg*) malloc(sizeof(ClientInitMsg));
    if (initMsg == NULL) {
        write_runlog(ERROR, "Out of memory, client create init msg!\n");
        return CM_ERROR;
    }
    errno_t rc = memset_s(initMsg, sizeof(ClientInitMsg), 0, sizeof(ClientInitMsg));
    securec_check_errno(rc, (void)rc);
    initMsg->head.msgVer = CM_CLIENT_MSG_VER;
    initMsg->head.msgType = (int)MSG_CLIENT_AGENT_INIT_DATA;
    initMsg->resInfo.resInstanceId = instanceId;
    rc = strcpy_s(initMsg->resInfo.resName, CM_MAX_RES_NAME, resName);
    securec_check_errno(rc, (void)rc);

    SendMsgApi((char*)initMsg, sizeof(ClientInitMsg));

    return CM_SUCCESS;
}

void SendHeartBeatMsg()
{
    ClientHbMsg *hbMsg = (ClientHbMsg*)malloc(sizeof(ClientHbMsg));
    if (hbMsg == NULL) {
        write_runlog(ERROR, "out of memory, SendHeartBeatMsg.\n");
        return;
    }
    errno_t rc = memset_s(hbMsg, sizeof(ClientHbMsg), 0, sizeof(ClientHbMsg));
    securec_check_errno(rc, (void)rc);
    hbMsg->head.msgVer = CM_CLIENT_MSG_VER;
    hbMsg->head.msgType = MSG_CLIENT_AGENT_HEARTBEAT;
    hbMsg->version = g_clientStatusList.version;

    SendMsgApi((char*)hbMsg, sizeof(ClientHbMsg));
}

static inline void ConnectSetTimeout(const ConnAgent *con)
{
    struct timeval tv = { 0, 0 };

    tv.tv_sec = CLIENT_TCP_TIMEOUT;
    (void)setsockopt(con->sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv, sizeof(tv));
    (void)setsockopt(con->sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv));
}

static void ConnectClose()
{
    if (g_agentConnect.isClosed) {
        return;
    }

    (void)close(g_agentConnect.sock);
    g_agentConnect.sock = CLIENT_INVALID_SOCKET;
    g_agentConnect.isClosed = true;

    (void)pthread_mutex_lock(&g_sendMsg.lock);
    while (!g_sendMsg.sendQueue.empty()) {
        free(g_sendMsg.sendQueue.front().msgPtr);
        g_sendMsg.sendQueue.pop();
    }
    (void)pthread_mutex_unlock(&g_sendMsg.lock);
    (void)pthread_mutex_lock(&g_initFlag.lock);
    g_initFlag.initSuccess = false;
    (void)pthread_mutex_unlock(&g_initFlag.lock);

    write_runlog(LOG, "client close connect with cm agent.\n");
}

static void ConnectCreate(ConnAgent *con)
{
    char homePath[MAX_PATH_LEN] = {0};
    char serverPath[MAX_PATH_LEN] = {0};
    SockAddr remoteSock{};

    if (!con->isClosed) {
        write_runlog(LOG, "Create connect failed, because connect has been created.\n");
        return;
    }
    if (GetHomePath(homePath, sizeof(homePath)) != 0) {
        return;
    }
    errno_t rc = snprintf_s(serverPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/%s", homePath, CM_DOMAIN_SOCKET);
    securec_check_intval(rc, (void)rc);

    con->sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (CLIENT_INVALID_SOCKET == con->sock) {
        write_runlog(ERROR, "Creat connect socket failed.\n");
        return;
    }

    ConnectSetTimeout(con);

    remoteSock.addrLen = sizeof(remoteSock.addr);
    rc = memset_s(&remoteSock.addr, remoteSock.addrLen, 0, remoteSock.addrLen);
    securec_check_errno(rc, (void)rc);

    remoteSock.addr.sun_family = AF_UNIX;
    rc = strcpy_s(remoteSock.addr.sun_path, sizeof(remoteSock.addr.sun_path), serverPath);
    securec_check_errno(rc, (void)rc);

    int ret = connect(con->sock, (struct sockaddr *)&remoteSock.addr, remoteSock.addrLen);
    if (ret < 0) {
        write_runlog(LOG, "Client connect to agent error, ret=%d.\n", ret);
        close(con->sock);
        con->sock = CLIENT_INVALID_SOCKET;
        return;
    }
    // create connect success
    con->isClosed = false;
}

void *ConnectAgentMain(void *arg)
{
    struct timespec currentTime = { 0, 0 };
    struct timespec lastReportTime = { 0, 0 };
    for (;;) {
        if (g_shutdownClient) {
            write_runlog(LOG, "exit conn agent thread.\n");
            break;
        }
        if (g_agentConnect.isClosed) {
            write_runlog(LOG, "cm_client connect to cm_agent start.\n");
            ConnectCreate(&g_agentConnect);
            if (g_agentConnect.isClosed) {
                write_runlog(LOG, "cm_client connect to cm_agent fail errno = %d, retry.\n", errno);
                cm_usleep(CLIENT_CHECK_CONN_INTERVAL);
                continue;
            }
            (void)clock_gettime(CLOCK_MONOTONIC, &lastReportTime);
            if (!g_isClientInit) {
                write_runlog(LOG, "cm_client connect to cm_agent success.\n");
                continue;
            }
            bool isSuccess = SendInitMsgAndGetResult(g_resName, g_agentConnect.resInstanceId);
            if (!isSuccess) {
                write_runlog(ERROR, "cm_client init failed, close the new connect.\n");
                ConnectClose();
            }
        } else {
            (void)clock_gettime(CLOCK_MONOTONIC, &currentTime);
            if ((currentTime.tv_sec - lastReportTime.tv_sec) >= 1 && g_initFlag.initSuccess) {
                SendHeartBeatMsg();
                (void)clock_gettime(CLOCK_MONOTONIC, &lastReportTime);
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
        write_runlog(LOG, "fail to create connect agent thread, err=%d.\n", err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

void *SendMsgToAgentMain(void *arg)
{
    for (;;) {
        if (g_shutdownClient) {
            write_runlog(LOG, "exit send msg thread.\n");
            break;
        }
        if (g_agentConnect.isClosed) {
            cm_usleep(CLIENT_SEND_CHECK_INTERVAL);
            continue;
        }

        (void)pthread_mutex_lock(&g_sendMsg.lock);
        while (g_sendMsg.sendQueue.empty()) {
            (void)pthread_cond_wait(&g_sendMsg.cond, &g_sendMsg.lock);
        }
        MsgPackage msgPkg = g_sendMsg.sendQueue.front();
        g_sendMsg.sendQueue.pop();
        (void)pthread_mutex_unlock(&g_sendMsg.lock);

        if (TcpSendMsg(g_agentConnect.sock, msgPkg.msgPtr, msgPkg.msgLen) != CM_SUCCESS) {
            write_runlog(LOG, "client send msg to agent failed, close connect!\n");
            ConnectClose();
        }
        free(msgPkg.msgPtr);
    }

    return NULL;
}

status_t CreateSendMsgThread()
{
    int err;
    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, SendMsgToAgentMain, NULL) != 0)) {
        write_runlog(LOG, "failed to create send msg thread, err=%d\n", err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static void RecvInitAckProcess()
{
    InitResult result = {0};

    if (g_initFlag.initSuccess) {
        write_runlog(LOG, "client has init, can't process init ack again.\n");
        return;
    }
    if (TcpRecvMsg(g_agentConnect.sock, (char*)&result, sizeof(InitResult)) != CM_SUCCESS) {
        write_runlog(LOG, "cm_client recv init ack from agent fail or timeout.\n");
        return;
    }
    if (result.isSuccess) {
        write_runlog(LOG, "client init success.\n");
    } else {
        write_runlog(ERROR, "client init fail.\n");
    }
    (void)pthread_mutex_lock(&g_initFlag.lock);
    g_initFlag.initSuccess = result.isSuccess;
    (void)pthread_mutex_unlock(&g_initFlag.lock);
    (void)pthread_cond_signal(&g_initFlag.cond);
}

static inline void RecvHeartbeatAckProcess()
{
    write_runlog(DEBUG1, "recv heartbeat ack from agent.\n");
}

static void RecvResStatusListProcess(int isNotifyChange)
{
    OneResStatList tmpStatList = {0};

    if (TcpRecvMsg(g_agentConnect.sock, (char*)&tmpStatList, sizeof(OneResStatList)) != CM_SUCCESS) {
        write_runlog(LOG, "recv status list from agent fail.\n");
        return;
    }
    if (!g_initFlag.initSuccess) {
        write_runlog(LOG, "client has not init, can't refresh status list.\n");
        return;
    }

    if (g_clientStatusList.version == tmpStatList.version) {
        write_runlog(DEBUG1, "same version(%llu).\n", g_clientStatusList.version);
        return;
    }

    errno_t rc = memcpy_s(&g_clientStatusList, sizeof(OneResStatList), &tmpStatList, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);
    if (isNotifyChange == STAT_CHANGED) {
        g_agentConnect.callback();
    }
    write_runlog(LOG, "version=%llu\n", g_clientStatusList.version);
    for (uint32 i = 0; i < g_clientStatusList.instanceCount; ++i) {
        write_runlog(LOG, "resName(%s):nodeId(%u),instanceId=%u,status=%u\n",
            g_clientStatusList.resName,
            g_clientStatusList.resStat[i].nodeId,
            g_clientStatusList.resStat[i].cmInstanceId,
            g_clientStatusList.resStat[i].status);
    }
}

void *RecvMsgFromAgentMain(void *arg)
{
    for (;;) {
        if (g_shutdownClient) {
            write_runlog(LOG, "exit recv msg thread.\n");
            break;
        }
        if (g_agentConnect.isClosed) {
            cm_usleep(CLIENT_CHECK_CONN_INTERVAL);
            continue;
        }
        MsgHead msgHead = {0};
        if (TcpRecvMsg(g_agentConnect.sock, (char*)&msgHead, sizeof(MsgHead)) != CM_SUCCESS) {
            write_runlog(LOG, "client recv msg from agent fail or timeout.\n");
            ConnectClose();
            cm_usleep(CLIENT_RECV_CHECK_INTERVAL);
            continue;
        }

        switch (msgHead.msgType) {
            case MSG_AGENT_CLIENT_INIT_ACK:
                RecvInitAckProcess();
                break;
            case MSG_AGENT_CLIENT_HEARTBEAT_ACK:
                RecvHeartbeatAckProcess();
                break;
            case MSG_AGENT_CLIENT_RES_STATUS_LIST:
                RecvResStatusListProcess(NO_STAT_CHANGED);
                break;
            case MSG_AGENT_CLIENT_RES_STATUS_CHANGE:
                RecvResStatusListProcess(STAT_CHANGED);
                break;
            default:
                write_runlog(ERROR, "recv unknown msg, msgType(%u).\n", msgHead.msgType);
                ConnectClose();
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
        write_runlog(ERROR, "failed to create recv msg thread, error=%d.\n", err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static status_t InitLogFile(const char *resName)
{
    char logPath[MAX_PATH_LEN] = {0};
    char clientLogPath[MAX_PATH_LEN] = {0};

    prefix_name = resName;

    (void)syscalllockInit(&g_cmEnvLock);
    if (cm_getenv("GAUSSLOG", logPath, sizeof(logPath)) != EOK) {
        (void)printf(_("cm_client get GAUSSLOG dir failed\n"));
        return CM_ERROR;
    }
    check_input_for_security(logPath);
    int ret = snprintf_s(clientLogPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/cm/cm_client", logPath);
    securec_check_intval(ret, (void)ret);
    if (access(clientLogPath, F_OK) != 0) {
        (void)mkdir(clientLogPath, S_IRWXU);
    }

    if (SetLogFilePath(clientLogPath) == -1) {
        return CM_ERROR;
    }

    write_runlog(LOG, "init cm_client log file (%s) success.\n", clientLogPath);

    return CM_SUCCESS;
}

inline void EmptyCallback()
{
    write_runlog(LOG, "client init null call back func.\n");
}

static inline void InitAgentConnect(uint32 instanceId, CmNotifyFunc func)
{
    errno_t rc = memset_s(&g_agentConnect, sizeof(ConnAgent), 0, sizeof(ConnAgent));
    securec_check_errno(rc, (void)rc);
    g_agentConnect.sock = CLIENT_INVALID_SOCKET;
    g_agentConnect.isClosed = true;
    g_agentConnect.resInstanceId = instanceId;
    g_agentConnect.callback = (func == NULL) ? EmptyCallback : func;
}

static void InitGlobalVariable(const char *resName)
{
    errno_t rc = strcpy_s(g_resName, CM_MAX_RES_NAME, resName);
    securec_check_errno(rc, (void)rc);

    rc = memset_s(&g_clientStatusList, sizeof(OneResStatList), 0, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);

    g_initFlag.initSuccess = false;
    (void)pthread_mutex_init(&g_initFlag.lock, NULL);
    (void)pthread_cond_init(&g_initFlag.cond, NULL);

    while (!g_sendMsg.sendQueue.empty()) {
        g_sendMsg.sendQueue.pop();
    }
    (void)pthread_mutex_init(&g_sendMsg.lock, NULL);
    (void)pthread_cond_init(&g_sendMsg.cond, NULL);
}

status_t PreInit(uint32 instanceId, const char *resName, CmNotifyFunc func, bool *isFirstInit)
{
    if (isFirstInit) {
        CM_RETURN_IFERR(InitLogFile(resName));
        InitGlobalVariable(resName);
        *isFirstInit = false;
    }
    InitAgentConnect(instanceId, func);
    g_shutdownClient = false;

    return CM_SUCCESS;
}

void ShutdownClient()
{
    g_shutdownClient = true;
    // weak up send msg thread
    (void)pthread_cond_signal(&g_sendMsg.cond);
    ConnectClose();
}

bool SendInitMsgAndGetResult(const char *resName, uint32 instId)
{
    struct timespec releaseTime;
    struct timeval tv = { 0, 0 };

    (void)pthread_mutex_lock(&g_initFlag.lock);
    g_initFlag.initSuccess = false;
    if (SendInitMsg(instId, resName) != CM_SUCCESS) {
        (void)pthread_mutex_unlock(&g_initFlag.lock);
        return false;
    }
    (void)gettimeofday(&tv, NULL);
    releaseTime.tv_sec = tv.tv_sec + CLIENT_RES_DATA_TIMEOUT;
    releaseTime.tv_nsec = tv.tv_usec * CLIENT_USEC_TO_NSEC;
    (void)pthread_cond_timedwait(&g_initFlag.cond, &g_initFlag.lock, &releaseTime);
    bool result = g_initFlag.initSuccess;
    (void)pthread_mutex_unlock(&g_initFlag.lock);

    return result;
}
