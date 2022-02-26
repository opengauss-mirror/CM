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
 * cma_connect_client.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_connect_client.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <vector>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/un.h>
#include <sys/unistd.h>
#include "securec.h"
#include "cm_misc.h"
#include "cm_msg.h"
#include "cm_defs.h"
#include "cma_main.h"
#include "cma_common.h"
#include "cma_connect.h"
#include "cma_global_params.h"
#include "cma_connect_client.h"

using namespace std;

vector<ClientConn> g_clientConnect(MAX_RES_NUM);

vector<ClientConn> &GetClientConnect()
{
    return g_clientConnect;
}

static inline void SetConIdOfMsgHead(MsgHead &head, const MsgHead &recvHead, const uint32 &conId)
{
    errno_t rc;

    rc = memcpy_s(&head, sizeof(MsgHead), &recvHead, sizeof(MsgHead));
    securec_check_errno(rc, (void)rc);

    head.conId = conId;
}

static void ConnectClose(ClientConn *con)
{
    error_t rc;
    if (con->isClosed) {
        return;
    }

    (void)cm_close_socket((int)con->sock);
    con->sock = AGENT_INVALID_SOCKET;
    con->isClosed = true;
    con->cmInstanceId = 0;
    con->resInstanceId = 0;
    rc = strcpy_s(con->resName, CM_MAX_RES_NAME, "unknown");
    securec_check_errno(rc, (void)rc);
}

static status_t EpollEventAdd(int epollfd, int sock)
{
    struct epoll_event ev = {0};

    ev.events = EPOLLIN;
    ev.data.fd = sock;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock, &ev) < 0) {
        write_runlog(LOG, "Event Add failed [fd=%d], eventType[%04X]: errno=%d.\n", sock, EPOLLIN, errno);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static void EpollEventDel(int epollfd, int sock)
{
    struct epoll_event ev = {0};

    if (epoll_ctl(epollfd, EPOLL_CTL_DEL, sock, &ev) < 0) {
        write_runlog(LOG, "EPOLL_CTL_DEL failed [fd=%d]: errno=%d.\n", sock, errno);
    }
    cm_close_socket(sock);
}

static status_t TcpRecvMsg(const ClientConn *con, char *buf, int remainSize)
{
    int recvSize;
    uint32 offset = 0;

    if (con->isClosed) {
        return CM_ERROR;
    }

    while (remainSize > 0) {
        recvSize = recv(con->sock, buf + offset, remainSize, 0);
        if (recvSize == 0) {
            write_runlog(ERROR, "Agent disconnect with client.\n");
            return CM_ERROR;
        }
        if (recvSize < 0) {
            if (errno == EINTR) {
                continue;
            }
            write_runlog(ERROR, "Agent can't receive msg from client with errno=(%d).\n", errno);
            return CM_ERROR;
        }
        remainSize -= recvSize;
        offset += recvSize;
    }

    return CM_SUCCESS;
}

static status_t TcpSendMsg(const ClientConn *con, const char *buf, int remainSize)
{
    int sentSize;
    uint32 offset = 0;

    if (con->isClosed) {
        return CM_ERROR;
    }

    while (remainSize > 0) {
        sentSize = send(con->sock, buf + offset, remainSize, 0);
        if (sentSize == 0) {
            write_runlog(ERROR, "Agent disconnect with client.\n");
            return CM_ERROR;
        }
        if (sentSize < 0) {
            if (errno == EINTR) {
                continue;
            }
            write_runlog(ERROR, "agent can't send msg to client with errno=(%d).\n", errno);
            return CM_ERROR;
        }
        remainSize -= sentSize;
        offset += sentSize;
    }

    return CM_SUCCESS;
}

static void ConnectInit()
{
    errno_t rc;

    for (ClientConn &con : g_clientConnect) {
        con.sock = AGENT_INVALID_SOCKET;
        con.isClosed = true;
        rc = strcpy_s(con.resName, CM_MAX_RES_NAME, "unknown");
        securec_check_errno(rc, (void)rc);
        con.recvTime = {0, 0};
        con.cmInstanceId = 10001;
        con.resInstanceId = 0;
    }
}

static inline void ConnectSetTimeout(const ClientConn *con)
{
    struct timeval tv = { 0, 0 };

    tv.tv_sec = CM_TCP_TIMEOUT;
    (void)setsockopt(con->sock, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv, sizeof(tv));
    (void)setsockopt(con->sock, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(tv));
}

static void ConnectAccept(int listenSock, ClientConn *con)
{
    con->addr.addrLen = sizeof(con->addr.addr);

    con->sock = (int)accept(listenSock, (struct sockaddr *)&con->addr.addr, &con->addr.addrLen);
    if (con->sock == AGENT_INVALID_SOCKET) {
        write_runlog(ERROR, "[agent_listenfd] Accept new connection from client failed, errno=%d.\n", errno);
        return;
    }
    con->isClosed = false;
    ConnectSetTimeout(con);
    write_runlog(LOG, "[agent_listenfd] Create connect success.\n");
}

static void CreateListenSocket(ListenPort *listenfd)
{
    int ret;
    error_t rc;
    char homePath[MAX_PATH_LEN] = {0};
    char socketPath[MAX_PATH_LEN] = {0};

    if (GetHomePath(homePath, sizeof(homePath)) != 0) {
        return;
    }
    rc = snprintf_s(socketPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/%s", homePath, CM_DOMAIN_SOCKET);
    securec_check_intval(rc, (void)rc)
    write_runlog(DEBUG1, "Create domain socket failed.\n");

    listenfd->sock = (int)socket(AF_UNIX, SOCK_STREAM, 0);
    if (AGENT_INVALID_SOCKET == listenfd->sock) {
        write_runlog(DEBUG1, "Create connect socket failed.\n");
        return;
    }

    listenfd->addr.addrLen = sizeof(listenfd->addr.addr);
    rc = memset_s(&listenfd->addr.addr, listenfd->addr.addrLen, 0, listenfd->addr.addrLen);
    securec_check_errno(rc, (void)rc);
    listenfd->addr.addr.sun_family = AF_UNIX;
    rc = strcpy_s(listenfd->addr.addr.sun_path, MAX_PATH_LENGTH, socketPath);
    securec_check_errno(rc, (void)rc);

    (void)unlink(socketPath);
    ret = bind(listenfd->sock, (struct sockaddr *)&listenfd->addr.addr, listenfd->addr.addrLen);
    if (ret != 0) {
        write_runlog(DEBUG1, "Create bind failed.\n");
        goto err;
    }

    ret = listen(listenfd->sock, MAX_CONNECTIONS);
    if (ret != 0) {
        write_runlog(DEBUG1, "Create listen failed.\n");
        goto err;
    }

    (void)chmod(socketPath, DOMAIN_SOCKET_PERMISSION);

    return;

err:
    (void)unlink(socketPath);
    cm_close_socket(listenfd->sock);
    listenfd->sock = AGENT_INVALID_SOCKET;
}

static status_t RecvListenEvent(int listenSock, int epollfd)
{
    errno_t rc;
    status_t ret = CM_SUCCESS;
    ClientConn con;
    rc = memset_s(&con, sizeof(ClientConn), 0, sizeof(ClientConn));
    securec_check_errno(rc, (void)rc);

    ConnectAccept(listenSock, &con);
    if (con.isClosed) {
        return CM_ERROR;
    }
    (void)clock_gettime(CLOCK_MONOTONIC, &con.recvTime);

    for (uint64 i = 0; i < MAX_RES_NUM; ++i) {
        if (g_clientConnect[i].isClosed) {
            rc = memcpy_s(&g_clientConnect[i], sizeof(ClientConn), &con, sizeof(ClientConn));
            securec_check_errno(rc, (void)rc);
            ret = EpollEventAdd(epollfd, g_clientConnect[i].sock);
            return ret;
        }
    }
    ConnectClose(&con);
    write_runlog(ERROR, "connection is no free slot.\n");
    return CM_ERROR;
}

static inline void PushRecvToQueue(char *recvMsg)
{
    ClientRecvQueue &recvQueue = GetRecvQueueApi();

    (void)pthread_mutex_lock(&recvQueue.lock);
    recvQueue.queue.push(recvMsg);
    (void)pthread_mutex_unlock(&recvQueue.lock);

    (void)pthread_cond_signal(&recvQueue.cond);
}

static void RecvHeartBeatProcess(const MsgHead &head, const uint32 &conId, int epollfd)
{
    status_t ret;
    ClientHbMsg *recvMsg = NULL;

    recvMsg = (ClientHbMsg*) malloc(sizeof(ClientHbMsg));
    if (recvMsg == NULL) {
        write_runlog(LOG, "Out of memory: recv heartbeat Msg failed.\n");
        return;
    }

    ret = TcpRecvMsg(&g_clientConnect[conId], (char*)&recvMsg->version, sizeof(uint64));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "(client) Recv heartbeat Msg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[conId].sock);
        ConnectClose(&g_clientConnect[conId]);
    }

    SetConIdOfMsgHead(recvMsg->head, head, conId);

    PushRecvToQueue((char*)recvMsg);
}

static void RecvInitDataProcess(const MsgHead &head, const uint32 &conId, int epollfd)
{
    status_t ret;
    ClientInitMsg *recvMsg = NULL;

    recvMsg = (ClientInitMsg*) malloc(sizeof(ClientInitMsg));
    if (recvMsg == NULL) {
        write_runlog(LOG, "Out of memory: ClientInitMsg failed.\n");
        return;
    }

    ret = TcpRecvMsg(&g_clientConnect[conId], (char*)&recvMsg->resInfo, sizeof(ResInfo));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "(client) Recv InitMsg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[conId].sock);
        ConnectClose(&g_clientConnect[conId]);
    }

    SetConIdOfMsgHead(recvMsg->head, head, conId);

    PushRecvToQueue((char*)recvMsg);
}

static void RecvSetDataProcess(const MsgHead &head, const uint32 &conId, int epollfd)
{
    status_t ret;
    ClientSetDataMsg *recvMsg = NULL;

    recvMsg = (ClientSetDataMsg*) malloc(sizeof(ClientSetDataMsg));
    if (recvMsg == NULL) {
        write_runlog(LOG, "[agent] Out of memory: recvSetInstanceDataMsg failed.\n");
        return;
    }

    ret = TcpRecvMsg(&g_clientConnect[conId], (char*)&recvMsg->instanceData, sizeof(int64));

    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "(client) Recv SetInstanceDataMsg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[conId].sock);
        ConnectClose(&g_clientConnect[conId]);
    }

    SetConIdOfMsgHead(recvMsg->head, head, conId);

    PushRecvToQueue((char*)recvMsg);
}

static void RecvSetResDataProcess(const MsgHead &head, const uint32 &conId, int epollfd)
{
    status_t ret;
    ClientSetResDataMsg *recvMsg = NULL;

    recvMsg = (ClientSetResDataMsg*) malloc(sizeof(ClientSetResDataMsg));
    if (recvMsg == NULL) {
        write_runlog(LOG, "Out of memory: ClientSetResDataMsg failed.\n");
        return;
    }

    ret = TcpRecvMsg(&g_clientConnect[conId], (char*)&recvMsg->data, sizeof(ResData));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "(client) Recv SetResDataMsg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[conId].sock);
        ConnectClose(&g_clientConnect[conId]);
    }

    SetConIdOfMsgHead(recvMsg->head, head, conId);

    PushRecvToQueue((char*)recvMsg);
}

static void RecvGetResDataProcess(const MsgHead &head, const uint32 &conId, int epollfd)
{
    status_t ret;
    ClientGetResDataMsg *recvMsg = NULL;

    recvMsg = (ClientGetResDataMsg*) malloc(sizeof(ClientGetResDataMsg));
    if (recvMsg == NULL) {
        write_runlog(LOG, "Out of memory: ClientGetResDataMsg failed.\n");
        return;
    }

    ret = TcpRecvMsg(&g_clientConnect[conId], (char*)&recvMsg->slotId, sizeof(uint64));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "(client) Recv ClientGetResDataMsg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[conId].sock);
        ConnectClose(&g_clientConnect[conId]);
    }

    SetConIdOfMsgHead(recvMsg->head, head, conId);

    PushRecvToQueue((char*)recvMsg);
}

static void RecvClientMessage(const uint32 &conId, int epollfd)
{
    MsgHead head = {0};

    status_t ret = TcpRecvMsg(&g_clientConnect[conId], (char*)&head, sizeof(MsgHead));
    if (ret != CM_SUCCESS) {
        write_runlog(LOG, "(client) Recv msg type failed, close the connect.\n");
        goto err;
    }
    switch (head.msgType) {
        case MSG_CLIENT_AGENT_HEARTBEAT:
            RecvHeartBeatProcess(head, conId, epollfd);
            break;
        case MSG_CLIENT_AGENT_INIT_DATA:
            RecvInitDataProcess(head, conId, epollfd);
            break;
        case MSG_CLIENT_AGENT_SET_DATA:
            RecvSetDataProcess(head, conId, epollfd);
            break;
        case MSG_CLIENT_AGENT_SET_RES_DATA:
            RecvSetResDataProcess(head, conId, epollfd);
            break;
        case MSG_CLIENT_AGENT_GET_RES_DATA:
            RecvGetResDataProcess(head, conId, epollfd);
            break;
        default:
            write_runlog(ERROR, "(client) Recv unknown msg.\n");
            goto err;
    }
    (void)clock_gettime(CLOCK_MONOTONIC, &g_clientConnect[conId].recvTime);
    return;

err:
    EpollEventDel(epollfd, g_clientConnect[conId].sock);
    ConnectClose(&g_clientConnect[conId]);
}

static void RecvClientMessageMain(
    int epollfd, int eventNums, const ListenPort *listenfd, const struct epoll_event *events)
{
    uint32 conId;
    status_t ret;
    struct timespec currentTime = { 0, 0 };

    for (int i = 0; i < eventNums; ++i) {
        if (events[i].data.fd == listenfd->sock) {
            ret = RecvListenEvent(listenfd->sock, epollfd);
            if (ret != CM_SUCCESS) {
                write_runlog(LOG, "[agent_listenfd] Process listenfd event failed.\n");
            }
            continue;
        }
        for (conId = 0; conId <= MAX_RES_NUM; ++conId) {
            if (conId == MAX_RES_NUM) {
                EpollEventDel(epollfd, events[i].data.fd);
                break;
            }
            if (events[i].data.fd == g_clientConnect[conId].sock && !g_clientConnect[conId].isClosed) {
                RecvClientMessage(conId, epollfd);
                break;
            }
        }
    }

    // Check whether the client loses heartbeat
    for (uint64 i = 0; i < MAX_RES_NUM; ++i) {
        if (g_clientConnect[i].isClosed) {
            continue;
        }
        (void)clock_gettime(CLOCK_MONOTONIC, &currentTime);
        if ((currentTime.tv_sec - g_clientConnect[i].recvTime.tv_sec) > HEARTBEAT_TIMEOUT) {
            write_runlog(LOG, "Agent rec no heartbeat from %s client more than 5s.\n", g_clientConnect[i].resName);
            EpollEventDel(epollfd, g_clientConnect[i].sock);
            ConnectClose(&g_clientConnect[i]);
            continue;
        }
    }
}

void* RecvClientEventsMain(void * const arg)
{
    int epollfd;
    status_t ret;
    ListenPort listenfd;
    struct epoll_event events[MAX_EVENTS];

    ConnectInit();

    CreateListenSocket(&listenfd);
    if (listenfd.sock == AGENT_INVALID_SOCKET) {
        write_runlog(ERROR, "(client) agent create listen socket failed.\n");
        exit(1);
    }

    epollfd = epoll_create(MAX_EVENTS);
    if (epollfd < 0) {
        write_runlog(ERROR, "(client) agent create epoll failed %d.\n", epollfd);
        exit(1);
    }

    ret = EpollEventAdd(epollfd, listenfd.sock);
    if (ret != CM_SUCCESS) {
        write_runlog(ERROR, "(client) Agent add listen socket (fd=%d) failed.\n", listenfd.sock);
        exit(1);
    }
    write_runlog(LOG, "(client) Agent add listen socket (fd=%d) success.\n", listenfd.sock);

    // agent recv client event loop
    for (;;) {
        int eventNums = epoll_wait(epollfd, events, MAX_EVENTS, EPOLL_WAIT_TIMEOUT);
        if (eventNums < 0) {
            if (errno != EINTR && errno != EWOULDBLOCK) {
                write_runlog(ERROR, "(client) epoll_wait error, RecvClientMessageMain thread exit.\n");
                break;
            }
        }
        RecvClientMessageMain(epollfd, eventNums, &listenfd, events);
    }
    close(epollfd);

    return NULL;
}

void SendHeartBeatAck(const uint32 &conId)
{
    status_t ret;
    AgentToClientHbAck hbAck;

    hbAck.head.msgType = MSG_AGENT_CLIENT_HEARTBEAT_ACK;

    ret = TcpSendMsg(&g_clientConnect[conId], (char*)&hbAck, sizeof(AgentToClientHbAck));
    if (ret != CM_SUCCESS) {
        ConnectClose(&g_clientConnect[conId]);
    }
}

void SendResStatusList(const uint32 &conId, bool isNotifyChange)
{
    errno_t rc;
    status_t ret;
    uint32 resInstanceCount;
    AgentToClientResList sendList;
    CmResStatList &statList = GetResStatusListApi();

    if (isNotifyChange) {
        sendList.head.msgType = MSG_AGENT_CLIENT_RES_STATUS_CHANGE;
    } else {
        sendList.head.msgType = MSG_AGENT_CLIENT_RES_STATUS_LIST;
    }

    sendList.resStatusList.masterNodeId = 1;
    sendList.resStatusList.version = statList.version;
    resInstanceCount = 0;
    for (uint32 i = 0; i < CM_MAX_RES_NODE_COUNT; ++i) {
        for (uint32 j = 0; j < statList.nodeStatus[i].count; j++) {
            cm_resource_status resStatus = statList.nodeStatus[i].status[j];
            if (strcmp(g_clientConnect[conId].resName, resStatus.resName) != 0) {
                continue;
            }
            sendList.resStatusList.resStat[resInstanceCount].nodeId = i + 1;
            sendList.resStatusList.resStat[resInstanceCount].stat = (ResStatus)resStatus.status;
            sendList.resStatusList.resStat[resInstanceCount].cmInstanceId = resStatus.instanceId;
            sendList.resStatusList.resStat[resInstanceCount].resInstanceId = resStatus.resInstanceId;
            sendList.resStatusList.resStat[resInstanceCount].instanceData = resStatus.instanceData;
            rc = strcpy_s(sendList.resStatusList.resStat[resInstanceCount].resName, CM_MAX_RES_NAME, resStatus.resName);
            securec_check_errno(rc, (void)rc);

            if (resStatus.isMaster) {
                sendList.resStatusList.masterNodeId = i + 1;
            }

            resInstanceCount++;
        }
    }
    sendList.resStatusList.instanceCount = resInstanceCount;

    ret = TcpSendMsg(&g_clientConnect[conId], (char*)&sendList, sizeof(AgentToClientResList));
    if (ret != CM_SUCCESS) {
        ConnectClose(&g_clientConnect[conId]);
    }
}

void SendResDataSetResult(const uint32 &conId, const char *sendData)
{
    status_t ret;

    ret = TcpSendMsg(&g_clientConnect[conId], sendData, sizeof(AgentToClientSetDataResult));
    if (ret != CM_SUCCESS) {
        ConnectClose(&g_clientConnect[conId]);
    }
}

void SendResData(const uint32 &conId, const char *sendMsg)
{
    status_t ret;

    ret = TcpSendMsg(&g_clientConnect[conId], sendMsg, sizeof(AgentToClientResData));
    if (ret != CM_SUCCESS) {
        ConnectClose(&g_clientConnect[conId]);
    }

    const AgentToClientResData *msg =
        reinterpret_cast<const AgentToClientResData *>(reinterpret_cast<const void *>(sendMsg));
    write_runlog(LOG, "(client) send res data, soldId=%lu.\n", msg->msgData.slotId);
}

void* SendMessageToClientMain(void * const arg)
{
    MsgHead* sendHead = NULL;
    ClientSendQueue &sendQueue = GetSendQueueApi();

    for (;;) {
        (void)pthread_mutex_lock(&sendQueue.lock);

        while (sendQueue.queue.empty()) {
            (void)pthread_cond_wait(&sendQueue.cond, &sendQueue.lock);
        }

        sendHead = reinterpret_cast<MsgHead *>(reinterpret_cast<void *>(sendQueue.queue.front()));
        sendQueue.queue.pop();
        (void)pthread_mutex_unlock(&sendQueue.lock);

        if (sendHead->conId >= MAX_RES_NUM || g_clientConnect[sendHead->conId].isClosed) {
            sendHead->msgType = 0;
        }

        switch (sendHead->msgType) {
            case MSG_AGENT_CLIENT_HEARTBEAT_ACK:
                SendHeartBeatAck(sendHead->conId);
                break;
            case MSG_AGENT_CLIENT_RES_STATUS_LIST:
                SendResStatusList(sendHead->conId, false);
                break;
            case MSG_AGENT_CLIENT_RES_STATUS_CHANGE:
                SendResStatusList(sendHead->conId, true);
                break;
            case MSG_AGENT_CLIENT_SET_RES_DATA_STATUS:
                SendResDataSetResult(sendHead->conId, reinterpret_cast<char *>(sendHead));
                break;
            case MSG_AGENT_CLIENT_REPORT_RES_DATA:
                SendResData(sendHead->conId, reinterpret_cast<char *>(sendHead));
                break;
            default:
                write_runlog(LOG, "(client) Agent send unknown msg to client.\n");
                break;
        }

        free(sendHead);
        sendHead = NULL;
    }

    return NULL;
}
