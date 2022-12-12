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
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/time.h>
#include <sys/unistd.h>
#include "securec.h"
#include "cm_misc.h"
#include "cm_msg.h"
#include "cm_defs.h"
#include "cma_common.h"
#include "cma_connect_client.h"
#include "cma_global_params.h"

ClientConn g_clientConnect[MAX_RES_NUM];

ClientConn *GetClientConnect()
{
    return g_clientConnect;
}

static void ConnectClose(ClientConn *con)
{
    if (con->isClosed) {
        return;
    }

    (void)close((int)con->sock);
    con->sock = AGENT_INVALID_SOCKET;
    con->isClosed = true;
    con->cmInstanceId = 0;
    con->resInstanceId = 0;
    error_t rc = strcpy_s(con->resName, CM_MAX_RES_NAME, "unknown");
    securec_check_errno(rc, (void)rc);
}

static status_t EpollEventAdd(int epollfd, int sock)
{
    struct epoll_event ev = {0};

    ev.events = EPOLLIN;
    ev.data.fd = sock;

    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sock, &ev) < 0) {
        write_runlog(LOG, "[CLIENT] Event Add failed [fd=%d], eventType[%04X]: errno=%d.\n", sock, EPOLLIN, errno);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static void EpollEventDel(int epollfd, int sock)
{
    struct epoll_event ev = {0};

    if (epoll_ctl(epollfd, EPOLL_CTL_DEL, sock, &ev) < 0) {
        write_runlog(LOG, "[CLIENT] EPOLL_CTL_DEL failed [fd=%d]: errno=%d.\n", sock, errno);
    }
    (void)close(sock);
}

static void ConnectInit()
{
    for (int i = 0; i < MAX_RES_NUM; ++i) {
        g_clientConnect[i].sock = AGENT_INVALID_SOCKET;
        g_clientConnect[i].isClosed = true;
        g_clientConnect[i].recvTime = {0, 0};
        g_clientConnect[i].cmInstanceId = 0;
        g_clientConnect[i].resInstanceId = 0;
        errno_t rc = strcpy_s(g_clientConnect[i].resName, CM_MAX_RES_NAME, "unknown");
        securec_check_errno(rc, (void)rc);
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
        write_runlog(ERROR, "[CLIENT] Accept new connection from client failed, errno=%d.\n", errno);
        return;
    }
    con->isClosed = false;
    ConnectSetTimeout(con);
    write_runlog(LOG, "[CLIENT] Create connect success.\n");
}

static void CreateListenSocket(ListenPort *listenfd)
{
    char homePath[MAX_PATH_LEN] = {0};
    char socketPath[MAX_PATH_LEN] = {0};

    if (GetHomePath(homePath, sizeof(homePath)) != 0) {
        return;
    }
    error_t rc = snprintf_s(socketPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/%s", homePath, CM_DOMAIN_SOCKET);
    securec_check_intval(rc, (void)rc)

    listenfd->sock = (int)socket(AF_UNIX, SOCK_STREAM, 0);
    if (AGENT_INVALID_SOCKET == listenfd->sock) {
        write_runlog(ERROR, "[CLIENT] Create connect socket failed.\n");
        return;
    }

    listenfd->addr.addrLen = sizeof(listenfd->addr.addr);
    rc = memset_s(&listenfd->addr.addr, listenfd->addr.addrLen, 0, listenfd->addr.addrLen);
    securec_check_errno(rc, (void)rc);
    listenfd->addr.addr.sun_family = AF_UNIX;
    rc = strcpy_s(listenfd->addr.addr.sun_path, MAX_PATH_LENGTH, socketPath);
    securec_check_errno(rc, (void)rc);

    (void)unlink(socketPath);
    int ret = bind(listenfd->sock, (struct sockaddr *)&listenfd->addr.addr, listenfd->addr.addrLen);
    if (ret != 0) {
        write_runlog(ERROR, "[CLIENT] bind failed, socketPath=\'%s\', ret=%d.\n", socketPath, ret);
        (void)unlink(socketPath);
        close(listenfd->sock);
        listenfd->sock = AGENT_INVALID_SOCKET;
        return;
    }

    ret = listen(listenfd->sock, MAX_CONNECTIONS);
    if (ret != 0) {
        write_runlog(ERROR, "[CLIENT] Create listen failed, sock=%d, ret=%d.\n", listenfd->sock, ret);
        (void)unlink(socketPath);
        close(listenfd->sock);
        listenfd->sock = AGENT_INVALID_SOCKET;
        return;
    }

    (void)chmod(socketPath, DOMAIN_SOCKET_PERMISSION);

    return;
}

static status_t RecvListenEvent(int listenSock, int epollfd)
{
    ClientConn con;
    errno_t rc = memset_s(&con, sizeof(ClientConn), 0, sizeof(ClientConn));
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
            return EpollEventAdd(epollfd, g_clientConnect[i].sock);
        }
    }
    ConnectClose(&con);
    write_runlog(ERROR, "[CLIENT] g_clientConnect has no memory to save new connection.\n");

    return CM_ERROR;
}

static void RecvHeartBeatProcess(const MsgHead &head, int epollfd)
{
    ClientHbMsg *recvMsg = (ClientHbMsg*) malloc(sizeof(ClientHbMsg));
    if (recvMsg == NULL) {
        write_runlog(ERROR, "[CLIENT] malloc failed, RecvHeartBeatProcess.\n");
        return;
    }

    if (TcpRecvMsg(g_clientConnect[head.conId].sock, (char*)&recvMsg->version, sizeof(uint64)) != CM_SUCCESS) {
        write_runlog(LOG, "[CLIENT] Recv heartbeat Msg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[head.conId].sock);
        ConnectClose(&g_clientConnect[head.conId]);
        CleanClientMsgQueue(head.conId);
        return;
    }

    errno_t rc = memcpy_s(&recvMsg->head, sizeof(MsgHead), &head, sizeof(MsgHead));
    securec_check_errno(rc, (void)rc);
    PushMsgToClientRecvQue((char*)recvMsg, sizeof(ClientHbMsg));
}

static void RecvInitDataProcess(const MsgHead &head, int epollfd)
{
    ClientInitMsg *recvMsg = (ClientInitMsg*) malloc(sizeof(ClientInitMsg));
    if (recvMsg == NULL) {
        write_runlog(LOG, "[CLIENT] malloc failed, RecvInitDataProcess.\n");
        return;
    }

    if (TcpRecvMsg(g_clientConnect[head.conId].sock, (char*)&recvMsg->resInfo, sizeof(ResInfo)) != CM_SUCCESS) {
        write_runlog(LOG, "[CLIENT] Recv InitMsg failed, close the connect.\n");
        EpollEventDel(epollfd, g_clientConnect[head.conId].sock);
        ConnectClose(&g_clientConnect[head.conId]);
        CleanClientMsgQueue(head.conId);
        return;
    }

    errno_t rc = memcpy_s(&recvMsg->head, sizeof(MsgHead), &head, sizeof(MsgHead));
    securec_check_errno(rc, (void)rc);
    PushMsgToClientRecvQue((char*)recvMsg, sizeof(ClientInitMsg));
}

static void RecvClientMessage(const uint32 &conId, int epollfd)
{
    MsgHead head = {0};

    if (TcpRecvMsg(g_clientConnect[conId].sock, (char*)&head, sizeof(MsgHead)) != CM_SUCCESS) {
        EpollEventDel(epollfd, g_clientConnect[conId].sock);
        ConnectClose(&g_clientConnect[conId]);
        CleanClientMsgQueue(conId);
        write_runlog(LOG, "[CLIENT] Recv msg type failed, close the connect.\n");
        return;
    }
    head.conId = conId;
    switch (head.msgType) {
        case MSG_CLIENT_AGENT_INIT_DATA:
            RecvInitDataProcess(head, epollfd);
            break;
        case MSG_CLIENT_AGENT_HEARTBEAT:
            RecvHeartBeatProcess(head, epollfd);
            break;
        default:
            EpollEventDel(epollfd, g_clientConnect[conId].sock);
            ConnectClose(&g_clientConnect[conId]);
            CleanClientMsgQueue(conId);
            write_runlog(ERROR, "[CLIENT] Recv unknown msg, %u.\n", head.msgType);
            return;
    }
    (void)clock_gettime(CLOCK_MONOTONIC, &g_clientConnect[conId].recvTime);

    return;
}

static void RecvClientMsgMain(int epollfd, int eventNums, const ListenPort *listenfd, const struct epoll_event *events)
{
    uint32 conId;
    struct timespec currentTime = { 0, 0 };

    for (int i = 0; i < eventNums; ++i) {
        if (events[i].data.fd == listenfd->sock) {
            if (RecvListenEvent(listenfd->sock, epollfd) != CM_SUCCESS) {
                write_runlog(ERROR, "[CLIENT] Process listenfd event failed.\n");
            }
            continue;
        }
        for (conId = 0; conId < MAX_RES_NUM; ++conId) {
            if (events[i].data.fd == g_clientConnect[conId].sock && !g_clientConnect[conId].isClosed) {
                RecvClientMessage(conId, epollfd);
                break;
            }
        }
        if (conId == MAX_RES_NUM) {
            EpollEventDel(epollfd, events[i].data.fd);
        }
    }

    // Check whether the client loses heartbeat
    for (uint64 i = 0; i < MAX_RES_NUM; ++i) {
        if (g_clientConnect[i].isClosed) {
            continue;
        }
        (void)clock_gettime(CLOCK_MONOTONIC, &currentTime);
        if ((currentTime.tv_sec - g_clientConnect[i].recvTime.tv_sec) > HEARTBEAT_TIMEOUT) {
            write_runlog(ERROR, "[CLIENT] Agent rec no hb from %s client more than 5s.\n", g_clientConnect[i].resName);
            EpollEventDel(epollfd, g_clientConnect[i].sock);
            ConnectClose(&g_clientConnect[i]);
            CleanClientMsgQueue((uint32)i);
            continue;
        }
    }
}

void* RecvClientEventsMain(void * const arg)
{
    int epollfd;
    ListenPort listenfd;
    struct epoll_event events[MAX_EVENTS];

    ConnectInit();

    CreateListenSocket(&listenfd);
    if (listenfd.sock == AGENT_INVALID_SOCKET) {
        write_runlog(ERROR, "[CLIENT] agent create listen socket failed.\n");
        exit(1);
    }

    epollfd = epoll_create(MAX_EVENTS);
    if (epollfd < 0) {
        write_runlog(ERROR, "[CLIENT] agent create epoll failed %d.\n", epollfd);
        exit(1);
    }

    if (EpollEventAdd(epollfd, listenfd.sock) != CM_SUCCESS) {
        write_runlog(ERROR, "[CLIENT] Agent add listen socket (fd=%d) failed.\n", listenfd.sock);
        exit(1);
    }
    write_runlog(LOG, "[CLIENT] Agent add listen socket (fd=%d) success.\n", listenfd.sock);

    // agent recv client event loop
    for (;;) {
        int eventNums = epoll_wait(epollfd, events, MAX_EVENTS, EPOLL_WAIT_TIMEOUT);
        if (eventNums < 0) {
            if (errno != EINTR && errno != EWOULDBLOCK) {
                write_runlog(ERROR, "[CLIENT] epoll_wait error, RecvClientMsgMain thread exit.\n");
                break;
            }
        }
        RecvClientMsgMain(epollfd, eventNums, &listenfd, events);
    }
    close(epollfd);

    return NULL;
}

void* SendMessageToClientMain(void * const arg)
{
    for (;;) {
        (void)pthread_mutex_lock(&g_sendQueue.lock);
        while (g_sendQueue.msg.empty()) {
            (void)pthread_cond_wait(&g_sendQueue.cond, &g_sendQueue.lock);
        }
        AgentMsgPkg sendMsg = g_sendQueue.msg.front();
        g_sendQueue.msg.pop();
        (void)pthread_mutex_unlock(&g_sendQueue.lock);

        if (sendMsg.conId >= MAX_RES_NUM || g_clientConnect[sendMsg.conId].isClosed) {
            write_runlog(ERROR, "[CLIENT] invalid conId(%u).\n", sendMsg.conId);
            FREE_AND_RESET(sendMsg.msgPtr);
            continue;
        }

        if (TcpSendMsg(g_clientConnect[sendMsg.conId].sock, sendMsg.msgPtr, sendMsg.msgLen) != CM_SUCCESS) {
            ConnectClose(&g_clientConnect[sendMsg.conId]);
            CleanClientMsgQueue(sendMsg.conId);
        }
        FREE_AND_RESET(sendMsg.msgPtr);
    }

    return NULL;
}
