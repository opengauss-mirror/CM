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
 * cms_conn.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_server/cms_conn.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "sys/epoll.h"
#include "cm/cm_elog.h"
#include "cms_conn.h"
#include "cms_common.h"
#include "cms_global_params.h"
#ifdef KRB5
#include "gssapi/gssapi_krb5.h"
#endif

int ServerListenSocket[MAXLISTEN] = {0};

void CloseHAConnection(CM_Connection* con)
{
    if (con != NULL && con->port != NULL) {
        StreamClose(con->port->sock);
        FREE_AND_RESET(con->port->node_name);
        FREE_AND_RESET(con->port->remote_host);

        ConnFree(con->port);
        con->port = NULL;
    }
}

/*
 * ConnCreate -- create a local connection data structure
 */
Port* ConnCreate(int serverFd)
{
    Port* port = NULL;

    if ((port = (Port*)calloc(1, sizeof(Port))) == NULL) {
        write_runlog(ERROR, "out of memory\n");
        FreeNotifyMsg();
        exit(1);
    }

    if (StreamConnection(serverFd, port) != STATUS_OK) {
        if (port->sock >= 0) {
            StreamClose(port->sock);
        }
        ConnFree(port);
        port = NULL;
    }

    return port;
}

void set_socket_timeout(const Port* my_port, int timeout)
{
    struct timeval t = {timeout, 0};
    socklen_t len = sizeof(struct timeval);
    if (setsockopt(my_port->sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&t, len) < 0) {
        write_runlog(LOG, "setsockopt set SO_RCVTIMEO=%d failed.", timeout);
    }
}

void ConnCloseAndFree(CM_Connection* con)
{
    if (con == NULL) {
        write_runlog(DEBUG1, "The input connection pointer is NULL: Function:%s.\n", "ConnCloseAndFree");
        return;
    } else if (con->port == NULL) {
        write_runlog(DEBUG1, "The input connection port pointer is NULL: Function:%s.\n", "ConnCloseAndFree");
    } else {
        if (con->port->remote_type == CM_CTL || g_node_num < con->port->node_id) {
            write_runlog(DEBUG1,
                "close connection sock [fd=%d], type is %d, nodeid %u.\n",
                con->port->sock,
                con->port->remote_type,
                con->port->node_id);
        } else {
            write_runlog(LOG,
                "close connection sock [fd=%d], type is %d, nodeid %u.\n",
                con->port->sock,
                con->port->remote_type,
                con->port->node_id);
        }

        // prevent the sock will be closed again
        CsDisconnect(&(con->port->pipe), con->port->remote_type, &(con->port->sock));
        if (con->port->sock >= 0) {
            StreamClose(con->port->sock);
        }
        FREE_AND_RESET(con->port->user_name);

        FREE_AND_RESET(con->port->node_name);

        FREE_AND_RESET(con->port->remote_host);

        FREE_AND_RESET(con->port);
    }

    con->fd = INVALIDFD;

    if (con->inBuffer != NULL) {
        CM_freeStringInfo(con->inBuffer);
        FREE_AND_RESET(con->inBuffer);
    }
}
void RemoveCMAgentConnection(CM_Connection* con)
{
    if (con == NULL) {
        return;
    }

    /* for check cma and cms conn */
    if (con->port == NULL || con->port->node_id == CM_MAX_CONNECTIONS * 2) {
        ConnCloseAndFree(con);
        return;
    }

    if (con->port->remote_type == CM_AGENT) {
        (void)pthread_rwlock_wrlock(&gConns.lock);

        Assert(con->port->node_id < CM_MAX_CONNECTIONS);

        if (con == gConns.connections[con->port->node_id]) {
            gConns.connections[con->port->node_id] = NULL;
            gConns.count--;
            if (g_preAgentCon.conFlag[con->port->node_id] == 1) {
                --g_preAgentCon.connCount;
                g_preAgentCon.conFlag[con->port->node_id] = 0;
            }
        }

        (void)pthread_rwlock_unlock(&gConns.lock);
    }
    ConnCloseAndFree(con);
}

void AddCMAgentConnection(CM_Connection *con)
{
    Assert(con != NULL);
    (void)pthread_rwlock_wrlock(&gConns.lock);
    if (gConns.connections[con->port->node_id] != NULL) {
        write_runlog(ERROR, "A same cm_agent connected from nodeId %u.\n", con->port->node_id);
        // if free old cm_agent conn will core for other thread maybe use this conn now.
        // old cm_agent conn will be free by other thread when read or send msg from this conn failed.
        gConns.count--;
    }

    gConns.connections[con->port->node_id] = con;
    gConns.count++;
    write_runlog(LOG, "cm_agent connected from nodeId %u, conn count=%u.\n", con->port->node_id, gConns.count);
    if (gConns.count == 1) {
        write_runlog(LOG, "pre conn count reset when add conn.\n");
        g_preAgentCon.connCount = 0;
        errno_t rc = memset_s(g_preAgentCon.conFlag, sizeof(g_preAgentCon.conFlag), 0, sizeof(g_preAgentCon.conFlag));
        securec_check_errno(rc, (void)pthread_rwlock_unlock(&gConns.lock));
    }
    (void)pthread_rwlock_unlock(&gConns.lock);
    con->notifyCn = setNotifyCnFlagByNodeId(con->port->node_id);
}

int cm_server_flush_msg(CM_Connection* con)
{
    int ret = 0;
    if (con != NULL && con->fd >= 0) {
        ret = pq_flush(con->port);
        if (ret != 0) {
            write_runlog(ERROR, "pq_flush failed, return ret=%d\n", ret);

            if (con->port->remote_type != CM_SERVER) {
                EventDel(con->epHandle, con);
                RemoveCMAgentConnection(con);
            }
        }
    }
    return ret;
}
int CMHandleCheckAuth(CM_Connection* con)
{
    int cmAuth;
#ifdef KRB5
    if (con->gss_check == true) {
        return 0;
    }
#endif // KRB5
    char envPath[MAX_PATH_LEN] = {0};
    if (con->port == NULL) {
        write_runlog(ERROR, "port is null.\n");
        return -1;
    }

    /* 2. Prepare gss environment. */
    if (cmserver_getenv("KRB5_KTNAME", envPath, (uint32)sizeof(envPath), DEBUG5) != EOK) {
        /* check whether set guc parameter gtm_krb_server_keyfile or not */
        if (cm_krb_server_keyfile == NULL || strlen(cm_krb_server_keyfile) == 0) {
            write_runlog(ERROR, "out of memory, failed to malloc memory.\n");
            return -1;
        }
        int rc = memset_s(envPath, MAX_PATH_LEN, 0, MAX_PATH_LEN);
        securec_check_errno(rc, (void)rc);
        rc = snprintf_s(envPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "KRB5_KTNAME=%s", cm_krb_server_keyfile);
        securec_check_intval(rc, (void)rc);
        rc = putenv(envPath);
        if (rc != 0) {
            write_runlog(ERROR, "failed to putenv 'KRB5_KTNAME', return value: %d.\n", rc);
            return -1;
        }
        write_runlog(DEBUG1, "Set KRB5_KTNAME to %s.\n", envPath);
    }

#ifdef KRB5
    /* 3. Handle client GSS authentication message. */
    con->gss_ctx = GSS_C_NO_CONTEXT;
    con->gss_cred = GSS_C_NO_CREDENTIAL;
    OM_uint32 maj_stat = 0;
    OM_uint32 min_stat = 0;
    OM_uint32 lmin_s = 0;
    OM_uint32 gflags = 0;
    gss_buffer_desc gss_buf;
    char* krbconfig = NULL;

    do {
        /* Get the actual GSS token */
        if (pq_getmessage(con->port, con->inBuffer, CM_MAX_AUTH_TOKEN_LENGTH)) {
            return -1;
        }
        /* Map to GSSAPI style buffer */
        gss_buf.length = con->inBuffer->len;
        gss_buf.value = con->inBuffer->data;
        /* Clean the config cache and ticket cache set by hadoop remote read. */
        krb5_clean_cache_profile_path();
        /* Krb5 config file priority : setpath > env(MPPDB_KRB5_FILE_PATH) > default(/etc/krb5.conf). */
        if (NULL != (krbconfig = gs_getenv_r("MPPDB_KRB5_FILE_PATH"))) {
            krb5_set_profile_path(krbconfig);
        }
        maj_stat = gss_accept_sec_context(&min_stat,
            &con->gss_ctx,
            con->gss_cred,
            &gss_buf,
            GSS_C_NO_CHANNEL_BINDINGS,
            &con->gss_name,
            NULL,
            &con->gss_outbuf,
            &gflags,
            NULL,
            NULL);

        /* Negotiation generated data to be sent to the client. */
        if (con->gss_outbuf.length > 0) {
            int ret = 0;
            ret = cm_server_send_msg(con, 'P', (char*)con->gss_outbuf.value, con->gss_outbuf.length);
            if (ret == 0) {
                ret = cm_server_flush_msg(con);
            }
            if (ret != 0) {
                (void)gss_release_cred(&min_stat, &con->gss_cred);
                (void)gss_delete_sec_context(&lmin_s, &con->gss_ctx, GSS_C_NO_BUFFER);
                (void)gss_release_name(&lmin_s, &con->gss_name);
                if (con->gss_outbuf.value != NULL) {
                    FREE_AND_RESET(con->gss_outbuf.value);
                }
                write_runlog(ERROR, "line %d: accepting GSS security context failed.\n", __LINE__);
                return -1;
            }
        }

        /* Wrong status, report error here */
        if (maj_stat != GSS_S_COMPLETE && maj_stat != GSS_S_CONTINUE_NEEDED) {
            (void)gss_release_cred(&lmin_s, &con->gss_cred);
            (void)gss_delete_sec_context(&lmin_s, &con->gss_ctx, GSS_C_NO_BUFFER);
            (void)gss_release_name(&lmin_s, &con->gss_name);
            if (con->gss_outbuf.value != NULL) {
                FREE_AND_RESET(con->gss_outbuf.value);
            }
            write_runlog(ERROR, "line %d: accepting GSS security context failed.\n", __LINE__);
            return -1;
        }
    } while (maj_stat == GSS_S_CONTINUE_NEEDED); /* GSS_S_COMPLETE now */

    /* Release gss security credential */
    (void)gss_release_cred(&min_stat, &con->gss_cred);
    /* Release gss security context and name after server authentication finished */
    (void)gss_delete_sec_context(&min_stat, &con->gss_ctx, GSS_C_NO_BUFFER);
    /* Release gss_name and gss_buf */
    (void)gss_release_name(&min_stat, &con->gss_name);
    (void)gss_release_buffer(&lmin_s, &con->gss_outbuf);
#endif // KRB5
    /* Authentication succeed, send GTM_AUTH_REQ_OK */
    cmAuth = (int)htonl(CM_AUTH_REQ_OK);
    (void)cm_server_send_msg(con, 'R', (char*)&cmAuth, sizeof(cmAuth));
    (void)cm_server_flush_msg(con);

    return 0;
}

/*
 * Initialise the masks for select() for the ports we are listening on.
 * Return the number of sockets to listen on.
 */
int initMasks(const int* listenSocket, fd_set* rmask)
{
    int maxSock = -1;
    int i;
    int fd;

    FD_ZERO(rmask);

    for (i = 0; i < MAXLISTEN; i++) {
        fd = listenSocket[i];
        if (fd == -1) {
            break;
        }
        FD_SET(fd, rmask);
        if (fd > maxSock) {
            maxSock = fd;
        }
    }

    return maxSock + 1;
}

/*
 * ConnFree -- free a local connection data structure
 */
void ConnFree(Port* conn)
{
    free(conn);
}


static void CloseAllConnections(int epollHandle)
{
    CM_Connection* con = NULL;

    if (got_conns_close == true) {
        /* left some time, other thread maybe use the mem of conn. */
        cm_sleep(1);
        bool findepollHandle = false;
        (void)pthread_rwlock_wrlock(&gConns.lock);
        write_runlog(LOG, "receive signal to close all the agent connections now, conn count is %u.\n", gConns.count);
        for (int i = 0; i < CM_MAX_CONNECTIONS; i++) {
            con = gConns.connections[i];
            if (con != NULL && epollHandle == con->epHandle) {
                Assert(con->port->remote_type == CM_AGENT);

                EventDel(con->epHandle, con);
                Assert(con->port->node_id < CM_MAX_CONNECTIONS);
                gConns.connections[con->port->node_id] = NULL;
                gConns.count--;

                ConnCloseAndFree(con);
                FREE_AND_RESET(con);
                findepollHandle = true;
            }
        }
        if (gConns.count == 0 || g_HA_status->local_role == CM_SERVER_PRIMARY) {
            got_conns_close = false;
            write_runlog(LOG, "reset close conn flag.\n");
        }
        (void)pthread_rwlock_unlock(&gConns.lock);
        if (!findepollHandle) {
            write_runlog(LOG, "can't get epollHandle %d.\n", epollHandle);
        }
    }
}

void setBlockSigMask(sigset_t* block_signal)
{
    (void)sigfillset(block_signal);

#ifdef SIGTRAP
    (void)sigdelset(block_signal, SIGTRAP);
#endif
#ifdef SIGABRT
    (void)sigdelset(block_signal, SIGABRT);
#endif
#ifdef SIGILL
    (void)sigdelset(block_signal, SIGILL);
#endif
#ifdef SIGFPE
    (void)sigdelset(block_signal, SIGFPE);
#endif
#ifdef SIGSEGV
    (void)sigdelset(block_signal, SIGSEGV);
#endif
#ifdef SIGBUS
    (void)sigdelset(block_signal, SIGBUS);
#endif
#ifdef SIGSYS
    (void)sigdelset(block_signal, SIGSYS);
#endif
}

void* CM_ThreadMain(void* argp)
{
    int epollHandle;
    struct epoll_event events[MAX_EVENTS];
    sigset_t block_sig_set;

    CM_Thread* thrinfo = (CM_Thread*)argp;

    thread_name = (thrinfo->type == CM_AGENT) ? "CM_AGENT" : "CM_CTL";

    epollHandle = thrinfo->epHandle;

    (void)pthread_detach(pthread_self());

    setBlockSigMask(&block_sig_set);

    write_runlog(LOG, "cmserver pool thread %lu starting, epollfd is %d.\n", thrinfo->tid, epollHandle);

    uint32 msgCount = 0;

    for (;;) {
        if (got_stop == true) {
            write_runlog(LOG, "receive exit request in cm arbitrate.\n");
            cm_sleep(1);
            continue;
        }

        CloseAllConnections(epollHandle);
        thrinfo->isBusy = false;

        /* wait for events to happen, 5s timeout */
        int fds = epoll_pwait(epollHandle, events, MAX_EVENTS, 5000, &block_sig_set);
        if (fds < 0) {
            if (errno != EINTR && errno != EWOULDBLOCK) {
                write_runlog(ERROR, "epoll_wait fd %d error :%m, agent thread exit.\n", epollHandle);
                break;
            }
        }

        thrinfo->isBusy = true;
        for (int i = 0; i < fds; i++) {
            CM_Connection* con = (CM_Connection*)events[i].data.ptr;
            write_runlog(DEBUG5, "epoll event type %d.\n", events[i].events);
            /* read event */
            if (events[i].events & EPOLLIN) {
                if ((con != NULL) && (con->port != NULL)) {
                    con->callback(epollHandle, events[i].events, con->arg);
                    msgCount++;
                }
            }
        }

        if (msgCount % (MSG_COUNT_FOR_LOG) == 0 && msgCount >= (MSG_COUNT_FOR_LOG)) {
            write_runlog(DEBUG1, "the thread has deal 300 msg at this time.\n");
            msgCount = 0;
        }
    }

    close(epollHandle);
    return thrinfo;
}

/**
 * @brief add/mod an event to epoll
 * 
 * @param  epoll_handle     My Param doc
 * @param  events           My Param doc
 * @param  con              My Param doc
 * @return int 
 */
int EventAdd(int epoll_handle, int events, CM_Connection* con)
{
    struct epoll_event epv = {0};
    epv.data.ptr = con;
    con->events = events;
    epv.events = (uint32)events;

    if (epoll_ctl(epoll_handle, EPOLL_CTL_ADD, con->fd, &epv) < 0) {
        write_runlog(LOG, "Event Add failed [fd=%d], evnets[%04X]: %m\n", con->fd, events);
        return -1;
    }

    return 0;
}

/**
 * @brief delete an event from epoll
 * 
 * @param  epollFd          My Param doc
 * @param  con              My Param doc
 */
void EventDel(int epollFd, CM_Connection* con)
{
    struct epoll_event epv = {0};

    epv.data.ptr = con;

    if (epoll_ctl(epollFd, EPOLL_CTL_DEL, con->fd, &epv) < 0) {
        write_runlog(LOG, "EPOLL_CTL_DEL failed [fd=%d]: %m\n", con->fd);
    }
}

/**
 * @brief ReadCommand reads a command from either the frontend or
 *      standard input, places it in inBuf, and returns the
 *      message type code (first byte of the message).
 *      EOF is returned if end of file.
 * 
 * @param  myport           My Param doc
 * @param  inBuf            My Param doc
 * @return int 
 */
int ReadCommand(Port* myport, CM_StringInfo inBuf)
{
    int qtype;
    int ret;

    if (myport == NULL || inBuf == NULL) {
        write_runlog(ERROR, "input param is null.\n");
        return -1;
    }

    if ((inBuf->msglen != 0) && (inBuf->msglen == inBuf->len)) {
        return inBuf->qtype;
    }
    /*
     * Get message type code from the frontend.
     */
    if (inBuf->qtype == 0) {
        qtype = pq_getbyte(myport);
        /* frontend disconnected */
        if (qtype < 0) {
            return qtype;
        }

        switch (qtype) {
            case 'A':
            case 'C':
            case 'X':
            case 'p':
                break;
            default:
                write_runlog(ERROR, "invalid frontend message type %d, in ReadCommand\n", qtype);
                return EOF;
        }
        inBuf->qtype = qtype;
    }
    /*
     * In protocol version 3, all frontend messages have a length word next
     * after the type code; we can read the message contents independently of
     * the type.
     */
    ret = pq_getmessage(myport, inBuf, 0);
    if (ret != 0) {
        return ret; /* suitable message already logged */
    }

    if ((inBuf->msglen != 0) && (inBuf->msglen == inBuf->len)) {
        return inBuf->qtype;
    } else {
        return 0;
    }
}

/*
 * GtmHandleTrustAuth
 *     handles trust authentication between gtm client and gtm server.
 *
 * @param (in) thrinfo: CreateRlsPolicyStmt describes the policy to create.
 * @return: void
 */
static void CMHandleTrustAuth(CM_Connection* con)
{
    /*
     * Send a dummy authentication request message 'R' as the client
     * expects that in the current protocol
     */
    int cmAuth = (int)htonl(CM_AUTH_REQ_OK);
    (void)cm_server_send_msg(con, 'R', (char*)&cmAuth, sizeof(cmAuth));
    (void)cm_server_flush_msg(con);
    CM_resetStringInfo(con->inBuffer);
}

#ifdef KRB5
static void CMHandleGssAuth(CM_Connection* con)
{
    /* 1. Send authentication request message GTM_AUTH_REQ_GSS to client */
    int cmAuth = (int)htonl(CM_AUTH_REQ_GSS);
    (void)cm_server_send_msg(con, 'R', (char*)&cmAuth, sizeof(cmAuth));
    (void)cm_server_flush_msg(con);
    CM_resetStringInfo(con->inBuffer);
}
#endif // KRB5

/*
 * GtmPerformAuthentication -- gtm server authenticate a remote client
 *
 * returns: nothing.  Will not return at all if there's any failure.
 */
void CMPerformAuthentication(CM_Connection *con)
{
    if (cm_auth_method == CM_AUTH_TRUST) {
        CMHandleTrustAuth(con);
        return;
    }
#ifdef KRB5
    if (cm_auth_method == CM_AUTH_GSS) {
        CMHandleGssAuth(con);
        return;
    }
#endif  // KRB5
    if (cm_auth_method == CM_AUTH_REJECT) {
        write_runlog(ERROR, "CM server reject any client connection.\n");
        return;
    }

    write_runlog(ERROR, "Invalid authentication method for CM server.\n");
    return;
}

#define BUF_LEN 1024
int get_authentication_type(const char* config_file)
{
    char buf[BUF_LEN];
    FILE* fd = NULL;
    int type = CM_AUTH_TRUST;

    if (config_file == NULL) {
        return CM_AUTH_TRUST;  /* default level */
    }

    fd = fopen(config_file, "r");
    if (fd == NULL) {
        printf("can not open config file: %s errno:%s\n", config_file, strerror(errno));
        exit(1);
    }

    while (!feof(fd)) {
        errno_t rc;
        rc = memset_s(buf, BUF_LEN, 0, BUF_LEN);
        securec_check_errno(rc, (void)rc);
        (void)fgets(buf, BUF_LEN, fd);

        if (is_comment_line(buf) == 1) {
            continue;  /* skip  # comment */
        }

        if (strstr(buf, "cm_auth_method") != NULL) {
            /* check all lines */
            if (strstr(buf, "trust") != NULL) {
                type = CM_AUTH_TRUST;
            }

#ifdef KRB5
            if (strstr(buf, "gss") != NULL) {
                type = CM_AUTH_GSS;
            }
#endif // KRB5
        }
    }

    fclose(fd);
    return type;
}
