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
 * atf.h
 *    Header files, constants, structures, and function declarations used by the ATF component
 *
 * IDENTIFICATION
 *    ATF/atf.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ATF_H
#define ATF_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <functional>
#include <csignal>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>
#include <algorithm>
#include <sys/epoll.h>
#include <pthread.h>
#include <sys/sysinfo.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <ctype.h>
#include <atomic>
#include <map>
#include <chrono>
#include <sys/wait.h>
#include <memory>
#include <unordered_map>
#include "nlohmann/json.hpp"
#include <filesystem>
#include "threadpool.h"

#define ENABLE_LOGGING

namespace atf {

constexpr int CONFIG_STR_MAX_LENGTH = 256;
constexpr int ATF_CLIENT_IO_BUFFER_MAX_SIZE = 4096;
constexpr int DEFAULT_N_AVAILABLE_NODES = 0;
constexpr int CLUSTER_CACHE_AVAILABLE_NODE_COUNT_INIT = 0;
constexpr time_t CLUSTER_CACHE_UPDATE_TIME_UNINITIALIZED = 0;
constexpr int WAKEUPPIPE = 2;
constexpr int EXEC_COMMAND_DEFAULT_TIMEOUT_SECONDS = 10;

/* High-precision timer */
struct ATFTimer {
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = std::chrono::time_point<Clock>;

    TimePoint start;
    ATFTimer() : start(Clock::now()) {}

    double ElapsedMs() const;
    double ElapsedUs() const;
};

/* Configuration structure */
struct ATFConfig {
    int port;                                   /* Listening port */
    char distDir[CONFIG_STR_MAX_LENGTH];    /* Output directory */
    char pidFile[CONFIG_STR_MAX_LENGTH];    /* PID file path */
    char logFile[CONFIG_STR_MAX_LENGTH];    /* Log file path */
    char sslPemFile[CONFIG_STR_MAX_LENGTH]; /* SSL pem file path */
    char sslKeyFile[CONFIG_STR_MAX_LENGTH]; /* SSL private key file path */
    int maxThreads;                             /* Maximum number of threads */
    int epollMaxEvents;                         /* Maximum epoll events */
    int epollTimeout;                           /* Epoll timeout (milliseconds) */
    int halfOpenConnQueueSize;                  /* Listen queue size */
    int cacheExpireSeconds;                     /* Cache expiration time (seconds) */
    int connIdleTimeoutSeconds;                 /* Connection idle timeout (seconds) */
    int unavailableRetryCount;                  /* Maximum retry count for Unavailable state */
};

/* Cluster state enumeration */
enum class ATFClusterState : uint8_t {
    Normal,      /* Normal state */
    Unavailable  /* Unavailable state */
};

/* Cluster parsing result structure */
struct ATFClusterParseResult {
    ATFClusterState clusterState;                /* Cluster state */
    std::map<std::string, std::string> ipToRole; /* IP->role mapping */
    int availableNodeCount{DEFAULT_N_AVAILABLE_NODES}; /* Number of available nodes */
};

/* Cluster state cache */
struct ATFClusterCache {
    std::map<std::string, std::string> ipToRole; /* IP->role mapping */
    int availableNodeCount{CLUSTER_CACHE_AVAILABLE_NODE_COUNT_INIT}; /* Total available nodes */
    ATFClusterState clusterState = ATFClusterState::Unavailable; /* Cluster state */
    time_t updateTime{CLUSTER_CACHE_UPDATE_TIME_UNINITIALIZED}; /* Last update timestamp */
    std::mutex cacheMutex;                        /* Cache mutex */
    std::condition_variable cv;                   /* Cache condition variable */
    bool isUpdating = false;                      /* Whether update is in progress */
};

/* Client connection context (complete definition) */
struct ATFClientContext {
    int fd;
    std::string ip;
    std::string connId;
    SSL* ssl;
    bool sslHandshaked;
    char buffer[ATF_CLIENT_IO_BUFFER_MAX_SIZE];
    ssize_t bufferLen;
    std::mutex ctxMutex;  /* Protects is_valid, last_active_time, etc. */
    bool isValid;
    time_t lastActiveTime;
    
    ATFClientContext(int fd, const std::string& ip);
    ~ATFClientContext();
    bool InitSsl();  /* SSL initialization function declaration */
};

/* Global configuration */
extern ATFConfig g_config;

/* cm_ctl command call counter */
extern std::atomic<int> g_cmCtlCallCount;

/* Global cluster state cache */
extern ATFClusterCache g_clusterCache;

/* Active connections storage */
extern std::unordered_map<int, std::shared_ptr<ATFClientContext>> g_activeConns;
extern std::mutex g_connListMutex;
extern std::thread g_connCleanupThread;
extern std::atomic<bool> g_cleanupRunning;

/* Global variables */
extern bool g_running;
extern int wakeupPipe[WAKEUPPIPE];  /* Self-pipe for waking epoll_wait */
#ifdef ENABLE_LOGGING
extern std::mutex g_logMutex;
#endif
extern int g_serverFd;
extern int g_epollFd;
extern SSL_CTX* g_sslCtx;
extern std::mutex g_sslMutex;
extern std::unique_ptr<ThreadPool> g_threadPool;

/* Create Log Dir*/
bool CreateLogDir();

/* Log function declaration (default parameter only in declaration) */
void ATFLogMessage(const std::string& level, const std::string& content,
                   const std::string& connId = "");

/* Configuration parsing function */
int ParseConfig(const char* filename, ATFConfig* cfg);

/* Utility function declarations */
void TrimInplace(char *str);

/* Get the installation directory */
std::string GetInstallDir();

bool CreateDistDir();
bool WritePidFile();
void RemovePidFile();
bool SetNonblocking(int fd);
std::string ExecuteCommand(const std::string& command,
                           int timeoutSeconds = EXEC_COMMAND_DEFAULT_TIMEOUT_SECONDS);
ATFClusterParseResult ParseClusterInfo(const std::string& rawOutput);
bool InitSslContext();
void CleanupSsl();
void RefreshClusterCache();
void GetClusterStatus(ATFClusterState& state,
                      std::map<std::string, std::string>& ipToRole,
                      int& availableCount);
std::string ProcessJsonRequest(const std::string& jsonStr,
                               const std::string& connId = "");
int HandleSslHandshake(const std::shared_ptr<ATFClientContext>& ctx);
void HandleClientData(const std::shared_ptr<ATFClientContext>& ctx);
void HandleNewConnection();
void ConnCleanupLoop();
void CloseConnection(const std::shared_ptr<ATFClientContext>& ctx, std::unique_lock<std::mutex>& lock);

}

#endif // ATF_H