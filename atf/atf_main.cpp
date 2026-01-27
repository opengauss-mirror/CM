/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
 *
 * ATF is licensed under Mulan PSL v2.
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
 * atf_main.cpp
 *    ATF SSL server core implementation (main entry, event loop, connection management and signal handling)
 *
 * IDENTIFICATION
 *    ATF/atf_main.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "atf.h"
using json = nlohmann::json;

namespace atf {

const std::string JSON_FIELD_COMMAND = "command";
const std::string JSON_FIELD_TYPE = "type";
const std::string JSON_FIELD_DATA = "data";
const std::string JSON_FIELD_IP = "ip";
const std::string JSON_FIELD_ROLE = "role";

const std::string COMMAND_VALUE_QUERY = "query";

const std::string RESPONSE_TYPE_ERROR_FROM_ATF = "ERROR FROM ATF";
const std::string RESPONSE_TYPE_QUERY_FROM_ATF = "QUERY FROM ATF";

const std::string NODE_ROLE_PRIMARY = "primary";
const std::string NODE_ROLE_STANDBY = "standby";

const std::string CLUSTER_STATE_NORMAL = "Normal";
const std::string CLUSTER_STATE_DEGRADED = "Degraded";

static const std::string ATF_ERROR_RESPONSE_JSON = json({
    {JSON_FIELD_COMMAND, COMMAND_VALUE_QUERY},
    {JSON_FIELD_TYPE, RESPONSE_TYPE_ERROR_FROM_ATF},
    {JSON_FIELD_DATA, {{JSON_FIELD_IP, nullptr}, {JSON_FIELD_ROLE, nullptr}}}
}).dump();

static const int CONN_CLEANUP_CHECK_INTERVAL_SECONDS = 30;

static std::string ValidateQueryCommand(const json& request)
{
    if (!request.contains(JSON_FIELD_COMMAND)) {
        return ATF_ERROR_RESPONSE_JSON;
    }

    const auto& commandVal = request[JSON_FIELD_COMMAND];
    if (!commandVal.is_string()) {
        return ATF_ERROR_RESPONSE_JSON;
    }
    std::string command = commandVal.get<std::string>();
    std::transform(command.begin(), command.end(), command.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (command != COMMAND_VALUE_QUERY) {
        return ATF_ERROR_RESPONSE_JSON;
    }
    return std::string();
}

static std::pair<std::string, std::string> GetTargetRole(const json& request)
{
    std::string role = NODE_ROLE_PRIMARY;
    if (request.contains(JSON_FIELD_DATA)
        && request[JSON_FIELD_DATA].contains(JSON_FIELD_ROLE)
        && !request[JSON_FIELD_DATA][JSON_FIELD_ROLE].is_null()) {
        role = request[JSON_FIELD_DATA][JSON_FIELD_ROLE].get<std::string>();
        if (role != NODE_ROLE_PRIMARY && role != NODE_ROLE_STANDBY) {
            return {"", ATF_ERROR_RESPONSE_JSON};
        }
    }
    return {role, ""};
}

static std::string FindRoleIp(const std::map<std::string, std::string>& ipToRole,
                              const std::string& role)
{
    for (const auto& [ip, nodeRole] : ipToRole) {
        if (nodeRole == role) {
            return ip;
        }
    }
    return "";
}

static std::string GetTargetIpResponse(const std::string& role, const std::shared_ptr<ATFClientContext>& ctx)
{
    ATFClusterState clusterState;
    std::map<std::string, std::string> ipToRole;
    int availableCount;
    GetClusterStatus(clusterState, ipToRole, availableCount);
    if (clusterState != ATFClusterState::Normal || availableCount == 0) {
        ATFLogMessage("WARN",
                      "Cluster state abnormal or no available nodes, cannot query role: "
                      + role, ctx->connId);
        return ATF_ERROR_RESPONSE_JSON;
    }
    std::string targetIp = FindRoleIp(ipToRole, role);
    if (targetIp.empty()) {
        ATFLogMessage("WARN",
                      "No node with role "
                      + role + " found (available nodes: "
                      + std::to_string(availableCount)
                      + ")", ctx->connId);
        return ATF_ERROR_RESPONSE_JSON;
    }
    ctx->lastActiveTime = time(nullptr);
    return json({
        {JSON_FIELD_COMMAND, COMMAND_VALUE_QUERY},
        {JSON_FIELD_TYPE, RESPONSE_TYPE_QUERY_FROM_ATF},
        {JSON_FIELD_DATA, {{JSON_FIELD_IP, targetIp}, {JSON_FIELD_ROLE, role}}}
    }).dump();
}

std::string ProcessJsonRequest(const std::string& jsonStr, const std::shared_ptr<ATFClientContext>& ctx)
{
    try {
        json request = json::parse(jsonStr);
        std::string validateErr = ValidateQueryCommand(request);
        if (!validateErr.empty()) {
            return validateErr;
        }
        auto [role, roleErr] = GetTargetRole(request);
        if (!roleErr.empty()) {
            return roleErr;
        }
        return GetTargetIpResponse(role, ctx);
    } catch (const std::exception& e) {
        ATFLogMessage("ERROR", "Failed to process JSON request: "
                      + std::string(e.what()), ctx->connId);
        return ATF_ERROR_RESPONSE_JSON;
    }
}

/* Handle client data */
static bool SendClientResponse(const std::shared_ptr<ATFClientContext>& ctx,
                               const std::string& response, std::unique_lock<std::mutex>& lock)
{
    std::string respWithNewline = response + "\r\n";
    ssize_t bytesSent = SSL_write(ctx->ssl, respWithNewline.c_str(),
                                  respWithNewline.size());
    const char* errStr = ERR_reason_error_string(ERR_get_error());
    if (bytesSent <= 0) {
        ATFLogMessage("ERROR", "Failed to send response: "
                      + std::string(errStr ? errStr : "Unknown error"), ctx->connId);
        CloseConnection(ctx, lock);
        return false;
    }
    ATFLogMessage("INFO", "Response sent successfully", ctx->connId);
    ctx->bufferLen = 0;
    memset(ctx->buffer, 0, sizeof(ctx->buffer));
    return true;
}

static bool HandleSslReadOk(const std::shared_ptr<ATFClientContext>& ctx,
                            ssize_t bytesRead, std::unique_lock<std::mutex>& lock)
{
    ctx->bufferLen += bytesRead;
    ctx->buffer[ctx->bufferLen] = '\0';
    ATFLogMessage("INFO", "Received message: "
                  + std::string(ctx->buffer, ctx->bufferLen), ctx->connId);
    std::string response = ProcessJsonRequest(std::string(ctx->buffer, ctx->bufferLen), ctx);
    return SendClientResponse(ctx, response, lock);
}

static bool HandleSslReadErr(const std::shared_ptr<ATFClientContext>& ctx,
                             ssize_t bytesRead, std::unique_lock<std::mutex>& lock)
{
    int sslErr = SSL_get_error(ctx->ssl, bytesRead);
    std::string errMsg;
    if (sslErr == SSL_ERROR_SYSCALL) {
        errMsg = (errno != 0) ? std::string(strerror(errno)) :
                 "Unknown syscall error (errno=0)";
    } else {
        unsigned long errCode = ERR_get_error();
        errMsg = ERR_reason_error_string(errCode) ?
                 ERR_reason_error_string(errCode) : "Unknown SSL error";
    }
    if (sslErr == SSL_ERROR_WANT_READ) {
        return false;
    } else if (sslErr == SSL_ERROR_WANT_WRITE) {
        struct epoll_event event = {EPOLLOUT | EPOLLET | EPOLLRDHUP,
                                    {.ptr = ctx.get()}};
        if (epoll_ctl(g_epollFd, EPOLL_CTL_MOD, ctx->fd, &event) < 0) {
            ATFLogMessage("WARN", "Failed to modify epoll event: "
                          + std::string(strerror(errno)), ctx->connId);
            CloseConnection(ctx, lock);
        }
        return false;
    }
    ATFLogMessage("ERROR", "SSL read failed: " + errMsg, ctx->connId);
    CloseConnection(ctx, lock);
    return false;
}

void HandleClientData(const std::shared_ptr<ATFClientContext>& ctx)
{
    std::unique_lock<std::mutex> lock(ctx->ctxMutex);
    if (!ctx->isValid) {
        return;
    }

    ATFTimer reqTimer;
    int handshakeRet = HandleSslHandshake(ctx);
    bool handshakeOk = (handshakeRet == 0);
    int needEvent = handshakeRet;
    if (!handshakeOk) {
        if (needEvent == -1) {
            ATFLogMessage("INFO", "Connection closed: SSL handshake failed", ctx->connId);
            CloseConnection(ctx, lock);
        } else if (needEvent != 0) {
            struct epoll_event event = {needEvent | EPOLLET | EPOLLRDHUP,
                                        {.ptr = ctx.get()}};
            if (epoll_ctl(g_epollFd, EPOLL_CTL_MOD, ctx->fd, &event) < 0) {
                ATFLogMessage("WARN", "Failed to modify epoll event: "
                           + std::string(strerror(errno)), ctx->connId);
                CloseConnection(ctx, lock);
            }
        }
        return;
    }

    bool hasMoreData = true;
    while (hasMoreData && ctx->isValid) {
        ssize_t bytesRead = SSL_read(ctx->ssl, ctx->buffer + ctx->bufferLen,
                                     sizeof(ctx->buffer) - 1 - ctx->bufferLen);
        if (bytesRead > 0) {
            hasMoreData = HandleSslReadOk(ctx, bytesRead, lock);
        } else if (bytesRead == 0) {
            ATFLogMessage("INFO", "Connection closed: client disconnected", ctx->connId);
            CloseConnection(ctx, lock);
            hasMoreData = false;
        } else {
            hasMoreData = HandleSslReadErr(ctx, bytesRead, lock);
        }
    }
    ATFLogMessage("INFO", "Request processed, time: "
               + std::to_string(reqTimer.ElapsedMs()) + " ms", ctx->connId);
}


/* Handle SSL handshake */
int HandleSslHandshake(const std::shared_ptr<ATFClientContext>& ctx)
{
    if (ctx->sslHandshaked) {
        return 0;
    }
    int ret = SSL_accept(ctx->ssl);
    if (ret > 0) {
        ctx->sslHandshaked = true;
        ctx->lastActiveTime = time(nullptr);
        const char* cipher = SSL_get_cipher(ctx->ssl);
        std::string cipherStr = cipher ? cipher : "Unknown cipher suite";
        ATFLogMessage("INFO", "SSL handshake successful, protocol: " + cipherStr, ctx->connId);
        return 0;
    }
    int sslErr = SSL_get_error(ctx->ssl, ret);
    const char* errCstr = ERR_reason_error_string(ERR_get_error());
    std::string errMsg = errCstr ? errCstr : "Unknown SSL error";
    if (sslErr == SSL_ERROR_WANT_READ) {
        return EPOLLIN;
    } else if (sslErr == SSL_ERROR_WANT_WRITE) {
        return EPOLLOUT;
    } else {
        ATFLogMessage("ERROR", "SSL handshake failed: " + errMsg, ctx->connId);
        return -1;
    }
}

/* Handle new connections */
void HandleNewConnection()
{
    sockaddr_in clientAddr;
    int clientFd;
    while (g_running) {
        socklen_t clientAddrLen = sizeof(clientAddr);
        errno = 0;
        ATFLogMessage("DEBUG",
                      "Before accept | clientAddrLen=" + std::to_string(clientAddrLen)
                      + " | g_running=" + std::to_string(g_running));
        clientFd = accept(g_serverFd, (struct sockaddr*)&clientAddr, &clientAddrLen);
        if (clientFd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                ATFLogMessage("DEBUG", "No more new connections, exit accept loop");
            } else {
                ATFLogMessage("ERROR", "accept failed: " + std::string(strerror(errno)));
            }
            break;
        }
        if (!SetNonblocking(clientFd)) {
            ATFLogMessage("ERROR", "!SetNonblocking ");
            close(clientFd);
            continue;
        }
        std::string clientIp = inet_ntoa(clientAddr.sin_addr);
        auto ctx = std::make_shared<ATFClientContext>(clientFd, clientIp);
        if (!ctx->InitSsl()) {
            ATFLogMessage("ERROR", "Failed to create SSL object, closing connection", ctx->connId);
            close(clientFd);
            continue;
        }
        ATFLogMessage("DEBUG", "New conn thread " +
                      std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) +
                      " try to lock g_connListMutex");
        {
            std::lock_guard<std::mutex> listLock(g_connListMutex);
            g_activeConns.emplace(clientFd, ctx);
        }
        ATFLogMessage("INFO", "New connection established (current active connections: "
                      + std::to_string(g_activeConns.size()) + ")", ctx->connId);
        
        struct epoll_event event;
        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        event.data.ptr = ctx.get();
        if (epoll_ctl(g_epollFd, EPOLL_CTL_ADD, clientFd, &event) < 0) {
            ATFLogMessage("ERROR", "Failed to add to epoll: "
                          + std::string(strerror(errno)), ctx->connId);
            std::unique_lock<std::mutex> lock(ctx->ctxMutex);
            CloseConnection(ctx, lock);
        }
    }
}

/* Connection timeout cleanup thread */
static std::vector<std::shared_ptr<ATFClientContext>> CollectExpiredConnections(time_t now)
{
    std::vector<std::shared_ptr<ATFClientContext>> toClean;
    
    /* Optimization: Copy all shared_ptrs at once instead of just FDs.
       This increases the reference count of each context, ensuring they stay alive
       during the check, and allows us to iterate without touching the global lock again
       until we actually need to remove something. */
    std::vector<std::shared_ptr<ATFClientContext>> currentConns;
    
    /* Step 1: Acquire global lock briefly to take a snapshot of all active connections */
    {
        ATFLogMessage("DEBUG", "Cleanup thread "
                      + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()))
                      + " try to lock g_connListMutex");
        std::lock_guard<std::mutex> listLock(g_connListMutex);
        currentConns.reserve(g_activeConns.size());
        for (const auto& [fd, ctx] : g_activeConns) {
            currentConns.push_back(ctx);
        }
    }

    /* Step 2: Iterate through the snapshot without global lock */
    for (const auto& ctx : currentConns) {
        if (!ctx) continue;

        /* Check expiration using local lock only */
        bool needClean = false;
        {
            std::lock_guard<std::mutex> ctxLock(ctx->ctxMutex);
            needClean = !ctx->isValid || (g_config.connIdleTimeoutSeconds > 0 &&
                        (now - ctx->lastActiveTime) > g_config.connIdleTimeoutSeconds);
        }

        /* Step 3: If expired, acquire global lock to remove from the authoritative list */
        if (needClean) {
            ATFLogMessage("DEBUG", "Cleanup thread " +
                          std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id())) +
                          " try to lock g_connListMutex");
            std::lock_guard<std::mutex> listLock(g_connListMutex);
            
            /* Double check: make sure the connection is still in the map and matches our instance */
            auto it = g_activeConns.find(ctx->fd);
            if (it != g_activeConns.end() && it->second == ctx) {
                toClean.push_back(ctx);
                g_activeConns.erase(it);
            }
        }
    }
    return toClean;
}

static void CleanupCollectedConnections(const std::vector<std::shared_ptr<ATFClientContext>>& toClean)
{
    if (!toClean.empty()) {
        ATFLogMessage("INFO", "Starting cleanup of " + std::to_string(toClean.size())
                      + " invalid/timeout connections");
    }
    for (const auto& ctx : toClean) {
        std::lock_guard<std::mutex> ctxLock(ctx->ctxMutex);
        if (!ctx->isValid) {
            continue;
        }
        std::string reason = !ctx->isValid ? "Context already invalid" :
                             ("Idle timeout (exceeded "
                              + std::to_string(g_config.connIdleTimeoutSeconds)
                              + " seconds)");
        if (epoll_ctl(g_epollFd, EPOLL_CTL_DEL, ctx->fd, nullptr) < 0) {
            ATFLogMessage("WARN", "Failed to remove connection from epoll ("
                          + reason + "): "
                          + std::string(strerror(errno)), ctx->connId);
        }
        ctx->isValid = false;
        ATFLogMessage("INFO", "Connection closed: " + reason, ctx->connId);
    }
}

void ConnCleanupLoop()
{
    ATFLogMessage("INFO", "Connection cleanup thread started, checking every 30 seconds");

    while (g_cleanupRunning) {
        std::this_thread::sleep_for(std::chrono::seconds(CONN_CLEANUP_CHECK_INTERVAL_SECONDS));
        time_t now = time(nullptr);
        std::vector<std::shared_ptr<ATFClientContext>> toClean = CollectExpiredConnections(now);
        CleanupCollectedConnections(toClean);
    }

    ATFLogMessage("INFO", "Connection cleanup thread stopped");
}

/* Unified connection closing function */
void CloseConnection(const std::shared_ptr<ATFClientContext>& ctx, std::unique_lock<std::mutex>& lock)
{
    if (!ctx) return;

    /* Note: Caller MUST hold ctx->ctxMutex */
    if (!lock.owns_lock()) return;
    
    if (!ctx->isValid) return;
    
    int fd = ctx->fd;
    ctx->isValid = false;
    ctx->fd = -1; /* Prevent double close in destructor */

    /* Optimize: Unlock early to minimize critical section and avoid nested locks */
    lock.unlock();

    /* Process epoll (lock-free operation) */
    if (epoll_ctl(g_epollFd, EPOLL_CTL_DEL, fd, nullptr) < 0) {
        ATFLogMessage("WARN", "Failed to remove connection from epoll (fd=" +
                      std::to_string(fd) + "): " +
                      std::string(strerror(errno)), ctx->connId);
    }

    /* Acquire the global lock to process the active list. */
    {
        std::lock_guard<std::mutex> listLock(g_connListMutex);
        auto it = g_activeConns.find(fd);
        if (it != g_activeConns.end() && it->second == ctx) {
            g_activeConns.erase(it);
        }
    }

    close(fd);
}

/* Signal handler with self-pipe wakeup */
void SignalHandler(int signum)
{
    if (signum != SIGINT && signum != SIGTERM) {
        return;
    }

    g_running = false;
    g_cleanupRunning = false;

    /* Write to self-pipe to wake epoll_wait */
    char c = 1;
    if (write(wakeupPipe[1], &c, 1) < 0) {
        /* Ignore error in signal handler */
    }
}

/* Initialize epoll with self-pipe */
bool InitEpoll()
{
    g_epollFd = epoll_create1(0);
    if (g_epollFd < 0) {
        ATFLogMessage("ERROR", "Failed to create epoll: " + std::string(strerror(errno)));
        return false;
    }
    
    /* Add self-pipe read end to epoll */
    struct epoll_event wakeupEvent;
    wakeupEvent.events = EPOLLIN | EPOLLET;
    wakeupEvent.data.fd = wakeupPipe[0];
    if (epoll_ctl(g_epollFd, EPOLL_CTL_ADD, wakeupPipe[0], &wakeupEvent) < 0) {
        ATFLogMessage("ERROR", "Failed to add self-pipe to epoll: " + std::string(strerror(errno)));
        close(g_epollFd);
        g_epollFd = -1;
        return false;
    }
    
    /* Add server socket to epoll */
    struct epoll_event serverEvent;
    serverEvent.events = EPOLLIN | EPOLLET;
    serverEvent.data.fd = g_serverFd;
    if (epoll_ctl(g_epollFd, EPOLL_CTL_ADD, g_serverFd, &serverEvent) < 0) {
        ATFLogMessage("ERROR", "Failed to add server socket to epoll: " + std::string(strerror(errno)));
        close(g_epollFd);
        g_epollFd = -1;
        return false;
    }
    
    ATFLogMessage("INFO", "Epoll initialized successfully");
    return true;
}

/* Initialize server with self-pipe creation */
static bool InitSelfPipe()
{
    if (pipe(wakeupPipe) < 0) {
        ATFLogMessage("ERROR", "Failed to create self-pipe: "
                      + std::string(strerror(errno)));
        return false;
    }
    if (!SetNonblocking(wakeupPipe[0]) || !SetNonblocking(wakeupPipe[1])) {
        close(wakeupPipe[0]);
        close(wakeupPipe[1]);
        return false;
    }
    return true;
}

static bool InitServerSocket()
{
    g_serverFd = socket(AF_INET, SOCK_STREAM, 0);
    if (g_serverFd < 0) {
        ATFLogMessage("ERROR", "Failed to create socket: "
                      + std::string(strerror(errno)));
        return false;
    }
    int opt = 1;
    if (setsockopt(g_serverFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        ATFLogMessage("ERROR", "Failed to set socket options: "
                      + std::string(strerror(errno)));
        close(g_serverFd);
        g_serverFd = -1;
        return false;
    }
    if (!SetNonblocking(g_serverFd)) {
        close(g_serverFd);
        g_serverFd = -1;
        return false;
    }
    sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_addr.s_addr = INADDR_ANY;
    serverAddr.sin_port = htons(g_config.port);
    if (bind(g_serverFd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        ATFLogMessage("ERROR", "Failed to bind to port "
                      + std::to_string(g_config.port) + ": "
                      + std::string(strerror(errno)));
        close(g_serverFd);
        g_serverFd = -1;
        return false;
    }
    if (listen(g_serverFd, g_config.halfOpenConnQueueSize) < 0) {
        ATFLogMessage("ERROR", "Failed to listen on port "
                      + std::to_string(g_config.port) + ": "
                      + std::string(strerror(errno)));
        close(g_serverFd);
        g_serverFd = -1;
        return false;
    }
    return true;
}

static bool InitThreadPoolAndCleanup()
{
    try {
        g_threadPool = std::make_unique<ThreadPool>();
    } catch (const std::bad_alloc&) {
        ATFLogMessage("ERROR", "Failed to allocate memory for thread pool");
        return false;
    } catch (const std::exception& e) {
        ATFLogMessage("ERROR", "Failed to initialize thread pool: " + std::string(e.what()));
        return false;
    } catch (...) {
        ATFLogMessage("ERROR", "Failed to initialize thread pool due to unknown error");
        return false;
    }
    if (!g_threadPool) {
        ATFLogMessage("ERROR", "Failed to initialize thread pool");
        return false;
    }
    g_cleanupRunning = true;
    g_connCleanupThread = std::thread(ConnCleanupLoop);
    return true;
}

static void CleanupResources()
{
    /* 1. Stop threads first */
    g_cleanupRunning = false;
    /* 清理线程通过检查标志位优雅退出，无需条件变量唤醒 */
    if (g_connCleanupThread.joinable()) {
        g_connCleanupThread.join();
    }
    
    if (g_threadPool) {
        g_threadPool->Stop();
        g_threadPool.reset();
    }

    /* 2. Close all active connections */
    std::vector<std::shared_ptr<ATFClientContext>> connectionsToClose;
    {
        std::lock_guard<std::mutex> listLock(g_connListMutex);
        for (const auto& [fd, ctx] : g_activeConns) {
            connectionsToClose.push_back(ctx);
        }
        g_activeConns.clear();
    }

    for (const auto& ctx : connectionsToClose) {
        std::unique_lock<std::mutex> ctxLock(ctx->ctxMutex);
        if (ctx->isValid) {
            ATFLogMessage("INFO", "Connection closed: service exiting", ctx->connId);
            CloseConnection(ctx, ctxLock);
        }
    }

    /* 3. Release system resources */
    if (g_epollFd != -1) {
        close(g_epollFd);
        g_epollFd = -1;
    }

    if (g_serverFd != -1) {
        close(g_serverFd);
        g_serverFd = -1;
    }

    close(wakeupPipe[0]);
    close(wakeupPipe[1]);

    CleanupSsl();
    RemovePidFile();

    ATFLogMessage("INFO", "Total cm_ctl command calls: " + std::to_string(g_cmCtlCallCount));
    ATFLogMessage("INFO", "SSL server resource cleanup completed");
}

bool InitServer()
{
    if (!InitSelfPipe()) {
        CleanupResources();
        return false;
    }
    if (!InitSslContext()) {
        CleanupResources();
        return false;
    }
    if (!InitServerSocket()) {
        CleanupResources();
        return false;
    }
    if (!InitEpoll()) {
        CleanupResources();
        return false;
    }
    if (!InitThreadPoolAndCleanup()) {
        CleanupResources();
        return false;
    }
    ATFLogMessage("INFO", "ATF SSL server started, listening on port "
                  + std::to_string(g_config.port)
                  + " (supports TLSv1.2/TLSv1.3, long connection mode)...");
    return true;
}

/* Server main loop with self-pipe handling */
/*
 * Reads from the self-pipe to clear the signal notification.
 *
 * Why use self-pipe?
 * The epoll_wait() call blocks the thread until an event occurs. If we receive
 * a signal (like SIGINT/SIGTERM) to stop the server, the signal handler runs,
 * sets g_running = false, and returns. However, if there are no network events,
 * epoll_wait() will remain blocked, and the main loop won't have a chance to
 * check g_running and exit.
 *
 * By writing to this pipe in the signal handler, we artificially create a
 * readable event. This wakes up epoll_wait(), causing it to return. We then
 * read the data here to empty the pipe buffer. The loop then proceeds to the
 * next iteration, checks g_running, and exits gracefully.
 */
static void HandleWakeupEvent()
{
    char buf[1024];
    /* Drain the pipe to prevent buffer overflow, though we usually just write 1 byte */
    read(wakeupPipe[0], buf, sizeof(buf));
}

static void ProcessClientErrorCtx(const std::shared_ptr<ATFClientContext>& ctx)
{
    std::unique_lock<std::mutex> lock(ctx->ctxMutex);
    if (ctx->isValid) {
        ATFLogMessage("INFO", "Connection closed: abnormal (EPOLLERR/EPOLLHUP)", ctx->connId);
        CloseConnection(ctx, lock);
    }
}

static std::shared_ptr<ATFClientContext> GetClientCtxByFd(int fd)
{
    std::lock_guard<std::mutex> listLock(g_connListMutex);
    auto it = g_activeConns.find(fd);
    return (it != g_activeConns.end()) ? it->second : nullptr;
}

static void HandleClientErrorEvent(const struct epoll_event& event)
{
    ATFClientContext* rawCtx = static_cast<ATFClientContext*>(event.data.ptr);
    if (!rawCtx) {
        ATFLogMessage("WARN", "Received null pointer error event, ignoring");
        return;
    }
    std::shared_ptr<ATFClientContext> ctx = GetClientCtxByFd(rawCtx->fd);
    if (ctx) {
        ProcessClientErrorCtx(ctx);
    } else {
        ATFLogMessage("WARN", "Error event for invalid connection (fd="
                      + std::to_string(rawCtx->fd) + "), ignored");
    }
}

/* Handle client readable event (supplementary definition) */
static void HandleClientReadableEvent(const struct epoll_event& event)
{
    ATFClientContext* rawCtx = static_cast<ATFClientContext*>(event.data.ptr);
    if (!rawCtx) {
        ATFLogMessage("WARN", "Received null pointer read event, ignoring");
        return;
    }
    std::shared_ptr<ATFClientContext> ctx = GetClientCtxByFd(rawCtx->fd);
    if (ctx) {
        g_threadPool->Submit([ctx]() { HandleClientData(ctx); });
    } else {
        ATFLogMessage("WARN", "Read event for invalid connection (fd="
                      + std::to_string(rawCtx->fd) + "), ignored");
    }
}

void ServerLoop()
{
    struct epoll_event events[g_config.epollMaxEvents];
    
    while (g_running) {
        int numEvents = epoll_wait(g_epollFd, events, g_config.epollMaxEvents, g_config.epollTimeout);
        if (numEvents < 0) {
            if (errno == EINTR) {
                continue;
            }
            ATFLogMessage("ERROR", "epoll_wait failed: " + std::string(strerror(errno)));
            break;
        } else if (numEvents == 0) {
            continue;
        }
        for (int i = 0; i < numEvents; ++i) {
            if (events[i].data.fd == wakeupPipe[0]) {
                /* Wake up from epoll_wait to check g_running flag for graceful shutdown */
                HandleWakeupEvent();
            } else if (events[i].data.fd == g_serverFd) {
                HandleNewConnection();
            } else if (events[i].events & (EPOLLIN | EPOLLPRI | EPOLLRDHUP)) {
                HandleClientReadableEvent(events[i]);
            } else if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                HandleClientErrorEvent(events[i]);
            }
        }
    }
}

static bool CheckArguments(int argc, char* argv[])
{
    if (argc != 1) {
        std::cerr << "Error: Incorrect number of arguments!" << std::endl
                  << "Correct usage: " << argv[0]
                  << " (start directly without additional parameters)"
                  << std::endl;
        return false;
    }
    return true;
}

static void LoadConfig()
{
    if (ParseConfig("conf/atf_config.conf", &g_config) != 0) {
        std::cerr << "[WARNING] Continuing with default configuration" << std::endl;
    }
}

static void RegisterSignals()
{
    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);
    signal(SIGPIPE, SIG_IGN);
}

static bool PrepareEnvironment()
{
    return WritePidFile();
}

}

/* Main function */
int main(int argc, char* argv[])
{
    if (!atf::CheckArguments(argc, argv)) {
        return EXIT_FAILURE;
    }
    atf::LoadConfig();
    atf::RegisterSignals();
    if (!atf::CreateLogDir()) {
        std::cerr << "[FATAL] Failed to create log directory, exit" << std::endl;
        return EXIT_FAILURE;
    }
    if (!atf::PrepareEnvironment()) {
        return EXIT_FAILURE;
    }

    atf::ATFLogMessage("INFO", "ATF SSL server starting (foreground mode only)");
    if (!atf::InitServer()) {
        atf::CleanupResources();
        return EXIT_FAILURE;
    }
    atf::ServerLoop();
    atf::CleanupResources();
    return EXIT_SUCCESS;
}