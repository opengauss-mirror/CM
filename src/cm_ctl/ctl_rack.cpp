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
 * ctl_rack.cpp
 *      cm_ctl Rack main files
 *
 * IDENTIFICATION
 *    src/cm_ctl/ctl_rack.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <cstdio>
#include <unordered_map>
#include <string>
#include <ostream>
#include <iostream>
#include <fstream>
#include <math.h>
#include <dlfcn.h>
#include "rack.h"
#include "cm/cm_elog.h"
#include "cm/cm_ctl/ctl_rack.h"

MatrixMemFunc g_matrixMemFunc = {0};
static constexpr auto BASE_NID = "";
static constexpr const char* HOST_NAME = "rack_nodeid";
static constexpr const char* MEM_TOTAL = "MemTotal(MB)";
static constexpr const char* MEM_USED = "MemUsed(MB)";
static constexpr const char* MEM_EXPORT = "MemExport(MB)";
static constexpr const char* MEM_IMPORT = "MemImport(MB)";
static constexpr const char* AVAIL_MEM = "AvailMem(MB)";
std::unordered_map<std::string, std::string> g_NodeIds = {};
std::string nodeId;
std::string hostName;
static char* g_matrixMemLibPath = "/usr/local/softbus/ctrlbus/lib/libmemfabric_client.so";

int MaxtrixMemLoadSymbol(char *symbol, void **symLibHandle)
{
    const char *dlsymErr = NULL;

    *symLibHandle = dlsym(g_matrixMemFunc.handle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != NULL) {
        write_runlog(ERROR, "matrix mem load symbol: %s, error: %s", symbol, dlsymErr);
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

int MaxtrixMemOpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == NULL) {
        write_runlog(ERROR, "load matrix mem dynamic lib: %s, error: %s", symbol, dlerror());
        return MATRIX_MEM_ERROR;
    }
    return MATRIX_MEM_SUCCESS;
}

int MatrixMemFuncInit(char *matrixMemLibPath)
{
    SymbolInfo symbols[] = {
        {"RackMemShmLookupShareRegions", (void **)&g_matrixMemFunc.rackMemShmLookupShareRegions},
        {"RackMemLookupClusterStatistic", (void **)&g_matrixMemFunc.rackMemLookupClusterStatistic}
    };

    if (SECUREC_UNLIKELY(MaxtrixMemOpenDl(&g_matrixMemFunc.handle, matrixMemLibPath) != MATRIX_MEM_SUCCESS)) {
        return MATRIX_MEM_ERROR;
    }

    size_t numSymbols = sizeof(symbols) / sizeof(symbols[0]);
    for (size_t i = 0; i < numSymbols; i++) {
        if (SECUREC_UNLIKELY(MaxtrixMemLoadSymbol(symbols[i].symbolName, symbols[i].funcptr) != MATRIX_MEM_SUCCESS)) {
            return MATRIX_MEM_ERROR;
        }
    }

    /* succeeded to load */
    g_matrixMemFunc.inited = true;
    return MATRIX_MEM_SUCCESS;
}

int RackMemShmLookupShareRegions(const char *baseNid, ShmRegionType type, SHMRegions *regions)
{
    return g_matrixMemFunc.rackMemShmLookupShareRegions(baseNid, type, regions);
}
int RackMemLookupClusterStatistic(ClusterInfo *cluster)
{
    return g_matrixMemFunc.rackMemLookupClusterStatistic(cluster);
}

static void RackMemGetNodeInfo()
{
    if (!g_NodeIds.empty()) {
        write_runlog(WARNING, "The RackManager node information has been initialized.\n");
        return;
    }

    int ret;
    SHMRegions regions = SHMRegions();
    ret = RackMemShmLookupShareRegions(BASE_NID, ShmRegionType::INCLUDE_ALL_TYPE, &regions);
    if (ret != 0 || regions.region[0].num <= 0) {
        write_runlog(ERROR, "lookup rack share regions failed, code: [%d], node num: [%d]\n",
                     ret, regions.region[0].num);
        return;
    }
    for (int i = 0; i < regions.num; i++) {
        for (int j = 0; j < regions.region[i].num; j++) {
            write_runlog(DEBUG1, "The share regions [%d] host name: [%s], node id: [%s].\n", i,
                         regions.region[i].hostName[j], regions.region[i].nodeId[j]);
            g_NodeIds[std::string(regions.region[i].hostName[j])] = std::string(regions.region[i].hostName[j]);
        }
    }
    return;
}

static void GetHostName()
{
    if (hostName != "") {
        write_runlog(WARNING, "The RackManager host name [%s] has been initialized.\n", hostName.c_str());
        return;
    }

    const std::string filePath = "/etc/hostname";
    std::ifstream file(filePath);
    if (!file.is_open()) {
        write_runlog(ERROR, "Failed to open /etc/hostname , error: %s\n", strerror(errno));
        return;
    }

    std::string content;
    if (std::getline(file, content)) {
        if (content.length() >= MAX_HOSTNAME_LENGTH) {
            write_runlog(ERROR, "the hostname is too long.");
            file.close();
            return;
        }
        hostName = content;
    } else {
        write_runlog(ERROR, "Unable to read file /etc/hostname");
    }

    file.close();
    write_runlog(DEBUG1, "The RackManager host name is: [%s].\n", hostName.c_str());
    return;
}

static void GetNodeId()
{
    if (nodeId != "") {
        write_runlog(WARNING, "The RackManager node id [%s] has been initialized.\n", nodeId.c_str());
        return;
    }

    GetHostName();
    if (hostName.empty()) {
        write_runlog(ERROR, "Failed to get host name from /etc/hostname.");
        return;
    }
    RackMemGetNodeInfo();
    if (g_NodeIds.empty()) {
        write_runlog(ERROR, "Failed to get Rack node information, hostname: [%s].", hostName.c_str());
        return;
    }
    nodeId = g_NodeIds[hostName];
    if (nodeId.empty()) {
        write_runlog(ERROR, "Failed to get Rack nodeId, hostname: [%s].", hostName.c_str());
        return;
    }
    write_runlog(DEBUG1, "The RackManager node id is: [%s].\n", nodeId.c_str());
}

int updateMaxWidth(int currentValue, int currentMax)
{
    int digits;
    if (currentValue == 0) {
        digits = 1; // Special case for zero
    } else {
        digits = static_cast<int>(log10(abs(currentValue))) + 1;
    }

    if (digits > currentMax) {
        return digits;
    } else {
        return currentMax;
    }
}

MemoryStates calculateHostMemory(const struct HostInfo *host)
{
    MemoryStates stats = {0};
    for (int i = 0; i < host->num; i++) {
        const struct SocketInfo *socket = &host->socket[i];
        stats.totalMemTotal += socket->memTotal;
        stats.totalMemUsed += socket->memUsed;
        stats.totalMemExport += socket->memExport;
        stats.totalMemImport += socket->memImport;
    }
    stats.availableMem = (int)stats.totalMemTotal * MAX_RACK_MEMORY_PERCENT - stats.totalMemExport;
    return stats;
}

void calculateAndPrintClusterInfo(const struct ClusterInfo *clusterInfo)
{
    int maxHostNameLength = strlen(HOST_NAME);
    int maxMemTotalLength = strlen(MEM_TOTAL);
    int maxMemUsedLength = strlen(MEM_USED);
    int maxMemExportLength = strlen(MEM_EXPORT);
    int maxMemImportLength = strlen(MEM_IMPORT);
    int maxAvailMemLength = strlen(AVAIL_MEM);

    MemoryStates *hosMemoryStats = new MemoryStates[clusterInfo->num];

    for (int i = 0; i < clusterInfo->num; i++) {
        const struct HostInfo *currenthost = &clusterInfo->host[i];
        MemoryStates stats = calculateHostMemory(currenthost);
        hosMemoryStats[i] = stats;

        int currentHostNameLength = strlen(currenthost->hostName);
        if (currentHostNameLength > maxHostNameLength) {
            maxHostNameLength = currentHostNameLength;
        }

        maxMemTotalLength = updateMaxWidth(stats.totalMemTotal, maxMemTotalLength);
        maxMemUsedLength = updateMaxWidth(stats.totalMemUsed, maxMemUsedLength);
        maxMemExportLength = updateMaxWidth(stats.totalMemExport, maxMemExportLength);
        maxMemImportLength = updateMaxWidth(stats.totalMemImport, maxMemImportLength);
        maxAvailMemLength = updateMaxWidth(stats.availableMem, maxAvailMemLength);
    }

    int totalWidth = maxHostNameLength + maxMemTotalLength + maxMemUsedLength +
                     maxMemExportLength + maxMemImportLength + maxAvailMemLength + 5;
    std::cout << std::string(totalWidth, '-') << std::endl;

    printf("%-*s %-*s %-*s %-*s %-*s %-*s\n", maxHostNameLength, HOST_NAME,
           maxMemTotalLength, MEM_TOTAL, maxMemUsedLength, MEM_USED,
           maxMemExportLength, MEM_EXPORT, maxMemImportLength, MEM_IMPORT,
           maxAvailMemLength, AVAIL_MEM);
    std::cout << std::string(totalWidth, '-') << std::endl;

    for (int i = 0; i < clusterInfo->num; i++) {
        const struct HostInfo *currenthost = &clusterInfo->host[i];
        MemoryStates stats = hosMemoryStats[i];

        printf("%-*s %-*d %-*d %-*d %-*d %-*d\n", maxHostNameLength, currenthost->hostName,
               maxMemTotalLength, stats.totalMemTotal, maxMemUsedLength, stats.totalMemUsed,
               maxMemExportLength, stats.totalMemExport, maxMemImportLength, stats.totalMemImport,
               maxAvailMemLength, stats.availableMem);
        if (i != clusterInfo->num - 1) {
            printf("\n\n");
        }
    }

    std::cout << std::string(totalWidth, '-') << std::endl;
    printf("NOTE: Available memory refers to the memory that this node can lend to other nodes.\n");
}

int DoRack()
{
    int ret;
    ClusterInfo cluster;
    ret = MatrixMemFuncInit(g_matrixMemLibPath);
    if (ret != MATRIX_MEM_SUCCESS) {
        write_runlog(ERROR, "Failed to initialize matrix memory functions, error code: %d\n."
                            "It may means that you are not on a specific environment", ret);
        return 1;
    }

    GetNodeId();
    if (nodeId.empty()) {
        return 1;
    }

    ret = RackMemLookupClusterStatistic(&cluster);
    if (ret != 0 || cluster.num <= 1) {
        write_runlog(ERROR, "lookup rack cluster statistic failed, code: [%d], node num: [%d]\n", ret, cluster.num);
        return 1;
    }
    calculateAndPrintClusterInfo(&cluster);
    return 0;
}