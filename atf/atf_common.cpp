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
 * atf_common.cpp
 *    ATF common utility implementation
 *    (config parse, logging, SSL, thread pool, cluster cache and ATFClientContext helper functions)
 *
 * IDENTIFICATION
 *    ATF/atf_common.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "atf.h"
using json = nlohmann::json;

namespace atf {
    
static constexpr int DEFAULT_LISTEN_PORT = 12345;
const int DEFAULT_MAX_THREADS = 128;
const int DEFAULT_EPOLL_MAX_EVENTS = 1024;
const int DEFAULT_HALF_OPEN_CONN_QUEUE_SIZE = 1024;
const int DEFAULT_EPOLL_TIMEOUT_MS = 500;
const int DEFAULT_CACHE_EXPIRE_SECONDS = 5;
const int DEFAULT_CONN_IDLE_TIMEOUT_SECONDS = 300;
const int DEFAULT_UNAVAILABLE_RETRY_COUNT = 3;
const mode_t DEFAULT_DIR_PERMISSIONS = 0755;
const long WAIT_INTERVAL_NS = 50000000;
const int MIN_DATANODE_FIELD_COUNT = 7;
const int DATANODE_FIELD_IP_INDEX = 2;
const long DEFAULT_SSL_SESSION_CACHE_SIZE = 1024;
const int MAX_CLUSTER_CACHE_WAIT_SECONDS = 100;
const int THREAD_POOL_CORE_COUNT_MULTIPLIER = 2;
const int SSL_SHUTDOWN_RETRY_WAIT_US = 10;
static const int DATANODE_STATE_FIELD_INDEX = 6;
const int maxStableRetry = 3;
const int ATFCONFIG_PARSE_SUCCESS = 0;
const int ATFCONFIG_PARSE_FILE_NOT_FOUND = -1;
const int ATFCONFIG_PARSE_JSON_ERROR = -2;
const int ATFCONFIG_PARSE_READ_ERROR = -3;

/* ATFTimer implementations */
double ATFTimer::ElapsedMs() const
{
    auto end = Clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

double ATFTimer::ElapsedUs() const
{
    auto end = Clock::now();
    return std::chrono::duration<double, std::micro>(end - start).count();
}

/* Global variable definitions */
ATFConfig g_config;
std::atomic<int> g_cmCtlCallCount{0};
ATFClusterCache g_clusterCache;
std::unordered_map<int, std::shared_ptr<ATFClientContext>> g_activeConns;
std::mutex g_connListMutex;
std::thread g_connCleanupThread;
std::atomic<bool> g_cleanupRunning(false);
bool g_running = true;
int wakeupPipe[WAKEUPPIPE];
#ifdef ENABLE_LOGGING
std::mutex g_logMutex;
#endif
int g_serverFd = -1;
int g_epollFd = -1;
SSL_CTX* g_sslCtx = nullptr;
std::mutex g_sslMutex;
std::unique_ptr<ThreadPool> g_threadPool;

/* Trim whitespace from string in-place */
void TrimInplace(char *str)
{
    if (!str) {
        return;
    }
    char *start = str;
    while (isspace((unsigned char)*start)) {
        start++;
    }
    char *end = str + strlen(str) - 1;
    while (end > start && isspace((unsigned char)*end)) {
        end--;
    }
    size_t len = end - start + 1;
    if (start != str) {
        memmove(str, start, len);
    }
    str[len] = '\0';
}

/* Get install directory */
std::string GetInstallDir()
{
    char exePath[PATH_MAX] = {0};
    /*
     * Read the absolute path of the current executable file
     * (Linux-specific: /proc/self/exe is a symbolic link)
     */
    ssize_t len = readlink("/proc/self/exe", exePath, sizeof(exePath) - 1);
    if (len == -1) {
        ATFLogMessage("ERROR", "Failed to read executable path: "
                      + std::string(strerror(errno)));
        return "";
    }
    exePath[len] = '\0';

    /* Parse the path of the executable file to obtain the installation directory */
    std::filesystem::path exePathObj = exePath;
    std::filesystem::path binDir = exePathObj.parent_path();
    std::filesystem::path installDir = binDir.parent_path();

    std::string installDirStr = installDir.string();
    ATFLogMessage("INFO", "Successfully get install directory: " + installDirStr);
    return installDirStr;
}

/* Set default values of initial configuration */
static void InitDefaultConfig(ATFConfig* cfg)
{
    cfg->port = DEFAULT_LISTEN_PORT;
    strncpy(cfg->distDir, "dist", sizeof(cfg->distDir) - 1);
    strncpy(cfg->pidFile, "atf.pid", sizeof(cfg->pidFile) - 1);
    strncpy(cfg->logFile, "/var/log/atf/atf_server.log", sizeof(cfg->logFile) - 1);
    strncpy(cfg->sslPemFile, "", sizeof(cfg->sslPemFile) - 1);
    strncpy(cfg->sslKeyFile, "", sizeof(cfg->sslKeyFile) - 1);

    cfg->maxThreads = DEFAULT_MAX_THREADS;
    cfg->epollMaxEvents = DEFAULT_EPOLL_MAX_EVENTS;
    cfg->epollTimeout = DEFAULT_EPOLL_TIMEOUT_MS;
    cfg->halfOpenConnQueueSize = DEFAULT_HALF_OPEN_CONN_QUEUE_SIZE;
    cfg->cacheExpireSeconds = DEFAULT_CACHE_EXPIRE_SECONDS;
    cfg->connIdleTimeoutSeconds = DEFAULT_CONN_IDLE_TIMEOUT_SECONDS;
    cfg->unavailableRetryCount = DEFAULT_UNAVAILABLE_RETRY_COUNT;
}

std::string to_lowercase(const std::string& str)
{
    std::string lowerStr = str;
    std::transform(lowerStr.begin(), lowerStr.end(), lowerStr.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return lowerStr;
}

json json_keys_to_lower(const json& originalJson)
{
    json lowerKeyJson;
    if (!originalJson.is_object()) {
        return originalJson;
    }
    for (auto it = originalJson.begin(); it != originalJson.end(); ++it) {
        std::string lowerKey = to_lowercase(it.key());
        lowerKeyJson[lowerKey] = it.value();
    }
    return lowerKeyJson;
}

/* Parse configuration file */
int ParseConfig(const char* filename, ATFConfig* cfg)
{
    InitDefaultConfig(cfg);
    std::ifstream jsonFile(filename);
    if (!jsonFile.is_open()) {
        std::cerr << "[WARNING] Configuration file " << filename
                  << " does not exist, using default configuration"
                  << std::endl;
        return ATFCONFIG_PARSE_FILE_NOT_FOUND;
    }
    try {
        json configJson = json::parse(jsonFile);
        json lowerKeyConfigJson = json_keys_to_lower(configJson);

        cfg->port = lowerKeyConfigJson.value(
            to_lowercase("PORT"), cfg->port);
        cfg->maxThreads = lowerKeyConfigJson.value(
            to_lowercase("MAX_THREADS"), cfg->maxThreads);
        cfg->epollMaxEvents = lowerKeyConfigJson.value(
            to_lowercase("EPOLL_MAX_EVENTS"), cfg->epollMaxEvents);
        cfg->epollTimeout = lowerKeyConfigJson.value(
            to_lowercase("EPOLL_TIMEOUT"), cfg->epollTimeout);
        cfg->halfOpenConnQueueSize = lowerKeyConfigJson.value(
            to_lowercase("HALF_OPEN_CONN_QUEUE_SIZE"), cfg->halfOpenConnQueueSize);
        cfg->cacheExpireSeconds = lowerKeyConfigJson.value(
            to_lowercase("CACHE_EXPIRE_SECONDS"), cfg->cacheExpireSeconds);
        cfg->connIdleTimeoutSeconds = lowerKeyConfigJson.value(
            to_lowercase("CONN_IDLE_TIMEOUT"), cfg->connIdleTimeoutSeconds);
        cfg->unavailableRetryCount = lowerKeyConfigJson.value(
            to_lowercase("UNAVAILABLE_RETRY_COUNT"), cfg->unavailableRetryCount);

        std::string distDir = lowerKeyConfigJson.value(to_lowercase("DIST_DIR"), std::string(cfg->distDir));
        strncpy(cfg->distDir, distDir.c_str(), sizeof(cfg->distDir) - 1);

        std::string pidFile = lowerKeyConfigJson.value(to_lowercase("PID_FILE"), std::string(cfg->pidFile));
        strncpy(cfg->pidFile, pidFile.c_str(), sizeof(cfg->pidFile) - 1);

        std::string logFile = lowerKeyConfigJson.value(to_lowercase("LOG_FILE"), std::string(cfg->logFile));
        strncpy(cfg->logFile, logFile.c_str(), sizeof(cfg->logFile) - 1);

        std::string sslPemFile = lowerKeyConfigJson.value(to_lowercase("SSL_PEM_FILE"), std::string(cfg->sslPemFile));
        strncpy(cfg->sslPemFile, sslPemFile.c_str(), sizeof(cfg->sslPemFile) - 1);

        std::string sslKeyFile = lowerKeyConfigJson.value(to_lowercase("SSL_KEY_FILE"), std::string(cfg->sslKeyFile));
        strncpy(cfg->sslKeyFile, sslKeyFile.c_str(), sizeof(cfg->sslKeyFile) - 1);
    } catch (const json::parse_error& e) {
        std::cerr << "[ERROR] Failed to parse JSON config file: " << e.what() << std::endl;
        jsonFile.close();
        return ATFCONFIG_PARSE_JSON_ERROR;
    } catch (const std::exception& e) {
        std::cerr << "[ERROR] Error when reading config file: " << e.what() << std::endl;
        jsonFile.close();
        return ATFCONFIG_PARSE_READ_ERROR;
    }
    jsonFile.close();
    std::cout << "[INFO] Successfully parsed JSON config file: " << filename << std::endl;
    return ATFCONFIG_PARSE_SUCCESS;
}

bool CreateLogDir()
{
    std::string logFilePath = g_config.logFile;
    if (logFilePath.empty()) {
        ATFLogMessage("INFO", "Log file path is empty, skip log directory creation");
        return true;
    }

    try {
        std::filesystem::path logPathObj = logFilePath;
        std::filesystem::path logDir = logPathObj.parent_path();
        if (!std::filesystem::exists(logDir)) {
            std::error_code ec;
            /* create directory */
            std::filesystem::create_directories(logDir, ec);
            if (ec) {
                std::cerr << "[ERROR] Failed to create log directory: "
                          << logDir.string() << " - " << ec.message() << std::endl;
                return false;
            }
            /* Set directory permissions */
            std::filesystem::permissions(
                logDir,
                std::filesystem::perms(DEFAULT_DIR_PERMISSIONS),
                std::filesystem::perm_options::replace,
                ec);
            if (ec) {
                std::cerr << "[WARNING] Failed to set log directory permissions: "
                          << logDir.string() << " - " << ec.message() << std::endl;
            }
            std::cerr << "[INFO] Successfully created log directory: " << logDir.string() << std::endl;
        }
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "[ERROR] Failed to check/create log directory: " << e.what() << std::endl;
        return false;
    }
    return true;
}

/* Logging implementation */
#ifdef ENABLE_LOGGING
void ATFLogMessage(const std::string& level, const std::string& content, const std::string& connId)
{
    std::lock_guard<std::mutex> lock(g_logMutex);

    time_t now = time(nullptr);
    struct tm* tmInfo = localtime(&now);
    char timeBuf[20];
    strftime(timeBuf, sizeof(timeBuf), "%Y-%m-%d %H:%M:%S", tmInfo);
    std::string connPart = connId.empty() ? "" : "[conn: " + connId + "] ";
    std::string logLine = "[" + std::string(timeBuf) + "] [" + level + "] " + connPart + content;

    /* Output to terminal */
    std::cout << logLine << std::endl;

    /* Handle log file writing */
    std::string logFilePath = g_config.logFile;
    if (!logFilePath.empty()) {
        /*
         * Open file in append mode (ios::app) to ensure
         * logs are appended to the end without overwriting
         */
        std::ofstream logFile(logFilePath, std::ios::app | std::ios::out);
        if (logFile.is_open()) {
            logFile << logLine << std::endl;
            logFile.close();
        } else {
            /* If opening log file fails: output error only to terminal */
            std::cerr << "[" << timeBuf << "] [ERROR] Failed to open log file: " << logFilePath
                      << " - " << strerror(errno) << std::endl;
        }
    }
}
#else
inline void ATFLogMessage(const std::string& level, const std::string& content, const std::string& connId) {}
#endif

/* Create distribution directory */
bool CreateDistDir()
{
    struct stat st;
    if (stat(g_config.distDir, &st) == -1) {
        if (mkdir(g_config.distDir, DEFAULT_DIR_PERMISSIONS) == -1) {
            ATFLogMessage("ERROR", "Failed to create dist directory: " + std::string(strerror(errno)));
            return false;
        }
        ATFLogMessage("INFO", "Successfully created dist directory");
    }
    return true;
}

/* Write PID file */
bool WritePidFile()
{
    if (!CreateDistDir()) {
        return false;
    }
    std::string pidPath = std::string(g_config.distDir) + "/" + g_config.pidFile;
    std::ofstream pidFile(pidPath);
    if (!pidFile) {
        ATFLogMessage("ERROR", "Unable to create PID file: " + pidPath
                      + ", error: " + std::string(strerror(errno)));
        return false;
    }
    pidFile << getpid() << std::endl;
    pidFile.close();
    return true;
}

/* Remove PID file */
void RemovePidFile()
{
    std::string pidPath = std::string(g_config.distDir) + "/" + g_config.pidFile;
    if (unlink(pidPath.c_str()) != 0 && errno != ENOENT) {
        ATFLogMessage("ERROR", "Failed to delete PID file: " + pidPath
                      + ", error: " + std::string(strerror(errno)));
    }
}

/* Set file descriptor to non-blocking mode */
bool SetNonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        ATFLogMessage("ERROR", "Failed to get socket flags: " + std::string(strerror(errno)));
        return false;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        ATFLogMessage("ERROR", "Failed to set socket to non-blocking: " + std::string(strerror(errno)));
        return false;
    }
    return true;
}

/* Execute command with timeout - Child process execution */
static void ChildProcessExec(int pipeWriteFd, const std::string& command)
{
    dup2(pipeWriteFd, STDOUT_FILENO);
    dup2(pipeWriteFd, STDERR_FILENO);
    close(pipeWriteFd);

    std::vector<std::string> cmdParts;
    std::stringstream ss(command);
    std::string part;
    while (ss >> part) {
        cmdParts.push_back(part);
    }
    if (cmdParts.empty()) {
        exit(EXIT_FAILURE);
    }

    std::vector<const char*> argv;
    for (const auto& p : cmdParts) {
        argv.push_back(p.c_str());
    }
    argv.push_back(nullptr);

    execvp(argv[0], const_cast<char* const*>(argv.data()));

    exit(EXIT_FAILURE);
}

static std::string HandleParentProcess(int pipeFd[2], pid_t pid, int timeoutSeconds)
{
    close(pipeFd[1]);

    fd_set readSet;
    FD_ZERO(&readSet);
    FD_SET(pipeFd[0], &readSet);
    struct timeval timeout = {timeoutSeconds, 0};
    int selectRet = select(pipeFd[0] + 1, &readSet, nullptr, nullptr, &timeout);
    if (selectRet == 0 || selectRet < 0) {
        kill(pid, SIGTERM);
        struct timespec waitTimeout = {2, 0};
        pid_t waitRet = 0;
        do {
            waitRet = waitpid(pid, nullptr, WNOHANG);
            if (waitRet != 0) {
                break;
            }
            nanosleep(&waitTimeout, nullptr);
        } while (waitRet == 0);

        close(pipeFd[0]);
        return (selectRet == 0)
            ? "[ERROR] Command timed out after " + std::to_string(timeoutSeconds) + "s"
            : "[ERROR] Select failed: " + std::string(strerror(errno));
    }

    std::string result;
    char buffer[1024];
    ssize_t bytesRead;
    while ((bytesRead = read(pipeFd[0], buffer, sizeof(buffer) - 1)) > 0) {
        buffer[bytesRead] = '\0';
        result += buffer;
    }
    close(pipeFd[0]);

    int status;
    waitpid(pid, &status, 0);
    if (WIFEXITED(status) && WEXITSTATUS(status) != 0) {
        result = "[ERROR] Command exited with code " + std::to_string(WEXITSTATUS(status)) + ": " + result;
    }
    return result;
}

std::string ExecuteCommand(const std::string& command, int timeoutSeconds)
{
    int pipeFd[2];
    if (pipe(pipeFd) < 0) {
        return "[ERROR] Failed to create pipe: " + std::string(strerror(errno));
    }
    pid_t pid = fork();
    if (pid < 0) {
        close(pipeFd[0]);
        close(pipeFd[1]);
        return "[ERROR] Failed to fork child process: " + std::string(strerror(errno));
    }
    if (pid == 0) {
        ChildProcessExec(pipeFd[1], command);
    } else {
        return HandleParentProcess(pipeFd, pid, timeoutSeconds);
    }
    return "";
}

/* Parse single datanode line for cluster info */
static void ParseSingleNodeLine(const std::string& line, ATFClusterParseResult& result)
{
    std::istringstream lineIss(line);
    std::vector<std::string> parts;
    std::string part;
    while (lineIss >> part) {
        parts.push_back(part);
    }
    if (parts.size() < MIN_DATANODE_FIELD_COUNT || parts[DATANODE_STATE_FIELD_INDEX] == "Down") {
        return;
    }
    std::string role = "down";
    for (const auto& p : parts) {
        if (p == "Primary") {
            role = "primary";
            break;
        } else if (p == "Standby") {
            role = "standby";
            break;
        }
    }
    result.ipToRole[parts[DATANODE_FIELD_IP_INDEX]] = role;
    result.availableNodeCount++;
}

static bool ParseClusterState(std::istringstream& iss, ATFClusterParseResult& result)
{
    std::string line;
    while (std::getline(iss, line)) {
        if (line.find("cluster_state") != std::string::npos) {
            size_t colonPos = line.find(':');
            if (colonPos == std::string::npos) {
                result.clusterState = ATFClusterState::Unavailable;
                return true;
            }
            std::string stateStr = line.substr(colonPos + 1);
            stateStr.erase(0, stateStr.find_first_not_of(" \t"));
            stateStr.erase(stateStr.find_last_not_of(" \t") + 1);
            ATFLogMessage("INFO", "stateStr = " + stateStr);
            result.clusterState = (stateStr == "Normal" || stateStr == "Degraded")
                ? ATFClusterState::Normal
                : ATFClusterState::Unavailable;
            return true;
        }
    }
    return false;
}

static void ParseDatanodeSection(std::istringstream& iss, ATFClusterParseResult& result)
{
    std::string line;
    bool inDatanodeSection = false;
    while (std::getline(iss, line)) {
        if (line == "[  Datanode State   ]") {
            inDatanodeSection = true;
            continue;
        }
        if (inDatanodeSection && line.find("[") == 0 &&
            line.find("]") != std::string::npos) {
            break;
        }
        if (!inDatanodeSection || line.empty() ||
            line.find("---") != std::string::npos ||
            line == "node     node_ip         instance state" ||
            line.find("-----------------------------------------------------")
            != std::string::npos) {
            continue;
        }
        ParseSingleNodeLine(line, result);
    }
}

ATFClusterParseResult ParseClusterInfo(const std::string& rawOutput)
{
    ATFTimer parseTimer;
    ATFLogMessage("DEBUG","Starting to parse cm_ctl output (length: "
                  + std::to_string(rawOutput.size()) + " characters)");

    ATFClusterParseResult result;
    result.clusterState = ATFClusterState::Unavailable;
    std::istringstream iss(rawOutput);

    bool foundClusterState = ParseClusterState(iss, result);
    if (!foundClusterState) {
        ATFLogMessage("DEBUG", "No cluster_state field detected, "
                      "treating as unknown state");
        return result;
    }
    if (result.clusterState == ATFClusterState::Normal) {
        iss.clear();
        iss.seekg(0);
        ParseDatanodeSection(iss, result);
    }

    double elapsed = parseTimer.ElapsedUs();
    ATFLogMessage("DEBUG",
                  "Parsing completed, time taken: " + std::to_string(elapsed)
                  + " microseconds (state: "
                  + (result.clusterState == ATFClusterState::Normal ? "Normal" : "Unavailable")
                  + ", available nodes: " + std::to_string(result.availableNodeCount)
                  + ")");

    return result;
}

/* SSL initialization - Initialize SSL library */
static bool InitSslLibrary()
{
    SSL_library_init();
    OpenSSL_add_all_algorithms();
    SSL_load_error_strings();
    return true;
}

static bool LoadSslPemAndKey(SSL_CTX* ctx, const std::string& pemFile, const std::string& keyFile)
{
    if (SSL_CTX_use_certificate_file(ctx, pemFile.c_str(), SSL_FILETYPE_PEM) <= 0) {
        ATFLogMessage("ERROR", "Failed to load SSL pem: " + pemFile);
        return false;
    }

    if (SSL_CTX_use_PrivateKey_file(ctx, keyFile.c_str(), SSL_FILETYPE_PEM) <= 0) {
        ATFLogMessage("ERROR", "Failed to load SSL private key: " + keyFile);
        return false;
    }

    if (!SSL_CTX_check_private_key(ctx)) {
        ATFLogMessage("ERROR", "SSL private key does not match pem");
        return false;
    }
    return true;
}

bool FileExists(const std::string& path)
{
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

bool InitSslContext()
{
    if (!InitSslLibrary()) {
        ATFLogMessage("ERROR", "SSL library initialization failed");
        return false;
    }

    const SSL_METHOD* method = TLS_server_method();
    if (method == nullptr) {
        ATFLogMessage("ERROR", "Failed to create Transport Layer Security server method");
        return false;
    }

    g_sslCtx = SSL_CTX_new(method);
    if (g_sslCtx == nullptr) {
        ATFLogMessage("ERROR", "Failed to create SSL context");
        return false;
    }

    SSL_CTX_set_session_cache_mode(g_sslCtx, SSL_SESS_CACHE_SERVER);
    SSL_CTX_sess_set_cache_size(g_sslCtx, DEFAULT_SSL_SESSION_CACHE_SIZE);

    std::string pemFile = g_config.sslPemFile;
    std::string keyFile = g_config.sslKeyFile;
    std::string installDir = GetInstallDir();

    if (pemFile.empty() || !FileExists(pemFile)) {
        pemFile = installDir + "/ssl/server.pem";
    }
    if (keyFile.empty() || !FileExists(keyFile)) {
        keyFile = installDir + "/ssl/server.key";
    }

    if (!LoadSslPemAndKey(g_sslCtx, pemFile, keyFile)) {
        SSL_CTX_free(g_sslCtx);
        g_sslCtx = nullptr;
        return false;
    }

    ATFLogMessage("INFO", "SSL context initialized successfully (pem: " + pemFile + ")");
    return true;
}

/* Clean up SSL resources */
void CleanupSsl()
{
    if (g_sslCtx != nullptr) {
        SSL_CTX_free(g_sslCtx);
        g_sslCtx = nullptr;
        ATFLogMessage("INFO", "SSL context released");
    }
    EVP_cleanup();
}

/* Check if cluster cache is being updated */
static bool CheckCacheUpdating(std::unique_lock<std::mutex>& lock)
{
    if (g_clusterCache.isUpdating) {
        g_clusterCache.cv.wait_for(lock, std::chrono::seconds(MAX_CLUSTER_CACHE_WAIT_SECONDS));
        return true;
    }
    g_clusterCache.isUpdating = true;
    return false;
}

static std::string GetStableCmctlOutput()
{
    std::string currentOutput;
    std::string prevOutput;
    int retryCount = 0;
    bool isStable = false;
    while (retryCount < maxStableRetry && !isStable) {
        currentOutput = ExecuteCommand("cm_ctl query -C -v -w -i");
        ATFLogMessage("DEBUG",
                      "cm_ctl execution result (attempt "
                      + std::to_string(retryCount + 1) + "):\n" + currentOutput);
        
        if (retryCount > 0 && currentOutput == prevOutput) {
            isStable = true;
            ATFLogMessage("INFO",
                          "Consecutive identical outputs, stopping retries (count: "
                          + std::to_string(retryCount + 1) + ")");
        } else {
            prevOutput = currentOutput;
            retryCount++;
            if (retryCount < maxStableRetry) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    }
    if (!isStable) {
        ATFLogMessage("ERROR",
                      "Exceeded max stable retry ("
                      + std::to_string(maxStableRetry) + " attempts)");
    }
    return currentOutput;
}

static ATFClusterParseResult HandleUnavailableRetry(const std::string& initialOutput)
{
    ATFClusterParseResult finalResult = ParseClusterInfo(initialOutput);
    const int waitSec = g_config.cacheExpireSeconds + 1;
    int currentRetry = 0;
    bool recovered = false;
    
    while (currentRetry < g_config.unavailableRetryCount && !recovered) {
        std::this_thread::sleep_for(std::chrono::seconds(waitSec));
        std::string retryOutput = ExecuteCommand("cm_ctl query -C -v -w -i");
        ATFLogMessage("DEBUG",
                      "Unavailable retry "
                      + std::to_string(currentRetry)
                      + " output:\n"
                      + retryOutput);
        
        ATFClusterParseResult retryResult = ParseClusterInfo(retryOutput);
        if (retryResult.clusterState == ATFClusterState::Normal) {
            finalResult = retryResult;
            recovered = true;
            ATFLogMessage("INFO",
                          "Retry "
                          + std::to_string(currentRetry)
                          + " detected cluster recovery");
        } else {
            ATFLogMessage("WARN",
                          "Retry "
                          + std::to_string(currentRetry)
                          + " still Unavailable");
            currentRetry++;
        }
    }
    if (!recovered) {
        finalResult.clusterState = ATFClusterState::Unavailable;
    }
    return finalResult;
}

static void UpdateCacheData(const ATFClusterParseResult& result)
{
    if (result.clusterState == ATFClusterState::Normal &&
        result.availableNodeCount > 0) {
        g_clusterCache.clusterState = ATFClusterState::Normal;
        g_clusterCache.ipToRole = result.ipToRole;
        g_clusterCache.availableNodeCount = result.availableNodeCount;
        ATFLogMessage("INFO",
                      "Cache updated (available nodes: "
                      + std::to_string(result.availableNodeCount)
                      + ")");
    } else {
        g_clusterCache.clusterState = ATFClusterState::Unavailable;
        g_clusterCache.ipToRole.clear();
        g_clusterCache.availableNodeCount = 0;
        ATFLogMessage("ERROR", "Cluster crashed, cache cleared");
    }
    g_clusterCache.updateTime = time(nullptr);
    g_clusterCache.isUpdating = false;
    g_clusterCache.cv.notify_all();
}

void RefreshClusterCache()
{
    ATFTimer totalTimer;
    std::unique_lock<std::mutex> lock(g_clusterCache.cacheMutex);
    if (CheckCacheUpdating(lock)) {
        return;
    }
    std::string currentOutput = GetStableCmctlOutput();
    if (currentOutput.empty()) {
        g_clusterCache.isUpdating = false;
        g_clusterCache.cv.notify_all();
        return;
    }

    ATFClusterParseResult finalResult = ParseClusterInfo(currentOutput);
    if (finalResult.clusterState == ATFClusterState::Unavailable) {
        finalResult = HandleUnavailableRetry(currentOutput);
    }

    UpdateCacheData(finalResult);
    ATFLogMessage("INFO",
                  "Cache refresh completed, total time: "
                  + std::to_string(totalTimer.ElapsedMs())
                  + " ms");
}

/* Get cluster status from cache */
void GetClusterStatus(ATFClusterState& state, std::map<std::string, std::string>& ipToRole, int& availableCount)
{
    std::unique_lock<std::mutex> lock(g_clusterCache.cacheMutex);
    time_t now = time(nullptr);
    if (g_clusterCache.updateTime == 0 ||
        (now - g_clusterCache.updateTime) >= g_config.cacheExpireSeconds) {
        lock.unlock();
        RefreshClusterCache();
        lock.lock();
    }

    state = g_clusterCache.clusterState;
    ipToRole = g_clusterCache.ipToRole;
    availableCount = g_clusterCache.availableNodeCount;
}

/* ThreadPool implementations */
ThreadPool::ThreadPool()
    : ThreadPool(std::min(
        get_nprocs() * THREAD_POOL_CORE_COUNT_MULTIPLIER,
        g_config.maxThreads))
{}

ThreadPool::ThreadPool(size_t threadCount) : mRunning(false)
{
    Start(threadCount);
}

ThreadPool::~ThreadPool()
{}

void ThreadPool::Submit(std::function<void()> task)
{
    std::unique_lock<std::mutex> lock(mMutex);
    mTasks.emplace(std::move(task));
    mCondition.notify_one();
}

void ThreadPool::Start(size_t threadCount)
{
    if (mRunning) {
        return;
    }
    mRunning = true;
    mThreads.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i) {
        mThreads.emplace_back(&ThreadPool::Worker, this);
    }
    ATFLogMessage("INFO", "Thread pool started, thread count: " + std::to_string(threadCount));
}

void ThreadPool::Stop()
{
    {
        std::lock_guard<std::mutex> lock(mMutex);
        if (!mRunning) {
            return;
        }
        mRunning = false;
        mTasks = {};
        mCondition.notify_all();  /* Wake all blocking threads */
    }
    
    for (auto& thread : mThreads) {
        if (thread.joinable()) {
            thread.join();  /* Wait for threads to exit */
        }
    }
    ATFLogMessage("INFO", "Thread pool stopped (task queue cleared)");
}

void ThreadPool::Worker()
{
    while (mRunning) {  /* Check exit flag */
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mMutex);
            /* Wait condition: have tasks or received exit signal */
            mCondition.wait(lock, [this]() {
                return !mRunning || !mTasks.empty();
            });
            /* If exiting and no tasks, exit loop directly */
            if (!mRunning) {
                return;
            }
            task = std::move(mTasks.front());
            mTasks.pop();
        }
        try {
            task();
        } catch (const std::exception& e) {
            ATFLogMessage("ERROR", "Thread pool task execution exception: " + std::string(e.what()));
        } catch (...) {
            ATFLogMessage("ERROR", "Thread pool task execution unknown exception");
        }
    }
}

/* SSL initialization function */
bool ATFClientContext::InitSsl()
{
    std::lock_guard<std::mutex> lock(g_sslMutex);
    if (!isValid || ssl != nullptr) {
        return false;
    }
    ssl = SSL_new(g_sslCtx);
    if (ssl) {
        SSL_set_fd(ssl, fd);
        return true;
    }
    return false;
}

/* ATFClientContext implementations */
ATFClientContext::ATFClientContext(int fd, const std::string& ip)
    : fd(fd), ip(ip),
      connId(ip + ":" + std::to_string(fd)),  /* Initialize connection identifier */
      ssl(nullptr), sslHandshaked(false),
      bufferLen(0), isValid(true),
      lastActiveTime(time(nullptr))
{}

static bool HandleSslShutdownError(SSL* ssl, int ret)
{
    if (ret >= 0) {
        return false;
    }

    int sslErr = SSL_get_error(ssl, ret);
    if (sslErr != SSL_ERROR_WANT_READ && sslErr != SSL_ERROR_WANT_WRITE) {
        unsigned long errCode = ERR_get_error();
        const char* errStr = ERR_reason_error_string(errCode);
        ATFLogMessage("ERROR", "SSL shutdown failed: " + std::string(errStr));
        return false;
    }

    return true;
}

static void SafeSslShutdown(SSL* ssl)
{
    int ret;
    do {
        ret = SSL_shutdown(ssl);
        if (ret < 0 && !HandleSslShutdownError(ssl, ret)) {
            break;
        }
    } while (ret < 0);
    SSL_free(ssl);
}

ATFClientContext::~ATFClientContext()
{
    if (ssl) {
        /*
         * If connection is marked invalid (client disconnected),
         * release SSL resources directly without shutdown retry
         */
        if (!this->isValid) {
            SSL_free(ssl);
            ssl = nullptr;
        } else {
            /*
             * When connection is still valid, attempt normal SSL shutdown
             * (retain original retry logic)
             */
            SafeSslShutdown(ssl);
            ssl = nullptr;
        }
    }
    /* Ensure file descriptor is closed */
    if (fd != -1) {
        close(fd);
        fd = -1;
    }
    /* Mark connection invalid (redundant protection) */
    isValid = false;
    ATFLogMessage("DEBUG", "ATFClientContext destructed (reference count zero)", connId);
}

}