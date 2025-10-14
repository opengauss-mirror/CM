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
#include <vector>
#include <string>
#include <ostream>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <math.h>
#include <dlfcn.h>
#include "rack.h"
#include "cm/cm_elog.h"
#include "cm/cm_ctl/ctl_rack.h"

MatrixMemFunc g_matrixMemFunc = {0};
static char* g_matrixMemLibPath = "/usr/local/ubs_mem/lib/libubsm_sdk.so";
const int NULL_SIZE = 4;

using namespace std;

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
        {"ubsmem_lookup_cluster_statistic", (void **)&g_matrixMemFunc.ubsmem_lookup_cluster_statistic},
        {"ubsmem_init_attributes", (void **)&g_matrixMemFunc.ubsmem_init_attributes},
        {"ubsmem_initialize", (void **)&g_matrixMemFunc.ubsmem_initialize},
        {"ubsmem_finalize", (void **)&g_matrixMemFunc.ubsmem_finalize}
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

void MatrixMemFuncUnInit()
{
    if (g_matrixMemFunc.inited) {
        (void)dlclose(g_matrixMemFunc.handle);
        g_matrixMemFunc.handle = NULL;
        g_matrixMemFunc.inited = false;
    }
}

int ubsmem_lookup_cluster_statistic(ubsmem_cluster_info_t *info)
{
    return g_matrixMemFunc.ubsmem_lookup_cluster_statistic(info);
}

int ubsmem_init_attributes(ubsmem_options_t *ubsm_shmem_opts)
{
    return g_matrixMemFunc.ubsmem_init_attributes(ubsm_shmem_opts);
}

int ubsmem_initialize(const ubsmem_options_t *ubsm_shmem_opts)
{
    return g_matrixMemFunc.ubsmem_initialize(ubsm_shmem_opts);
}

int ubsmem_finalize(void)
{
    return g_matrixMemFunc.ubsmem_finalize();
}

HostMemoryInfo calculateHostMemory(const ubsmem_host_info_t& hostInfo)
{
    HostMemoryInfo stats = {0};
    for (int i = 0; i < hostInfo.numa_num; i++) {
        const ubsmem_numa_mem_t& numaInfo = hostInfo.numa[i];
        stats.memTotal += numaInfo.mem_total;
        stats.memUsed += numaInfo.mem_total - numaInfo.mem_free;
        stats.memExport += numaInfo.mem_borrow;
        stats.memImport += numaInfo.mem_lend;
        stats.availableMem +=
            (numaInfo.mem_total * numaInfo.mem_lend_ratio / PERCENTAGE_CONVERSION - numaInfo.mem_lend);
    }

    stats.memTotal /= (KILO * KILO);
    stats.memUsed /= (KILO * KILO);
    stats.memExport /= (KILO * KILO);
    stats.memImport /= (KILO * KILO);
    stats.availableMem /= (KILO * KILO);
    return stats;
}

void PrintHostInfo(const char* host_name, const HostMemoryInfo& stats, const vector<int>& columnWidth)
{
    cout << left << setw(columnWidth[0] + OFFSET_ALIGNMENT);
    if (host_name == nullptr || host_name[0] == '\0') {
        cout << "NULL";
    } else {
        cout << host_name;
    }

    cout << left << setw(columnWidth[1] + OFFSET_ALIGNMENT) << stats.memTotal
         << left << setw(columnWidth[2] + OFFSET_ALIGNMENT) << stats.memUsed
         << left << setw(columnWidth[3] + OFFSET_ALIGNMENT) << stats.memExport
         << left << setw(columnWidth[4] + OFFSET_ALIGNMENT) << stats.memImport
         << left << setw(columnWidth[5] + OFFSET_ALIGNMENT) << stats.availableMem << endl;
}

void PrintClusterInfo(ubsmem_cluster_info_t& clusterInfo)
{
    vector<int> columnWidths(COLUMN_SIZE, 0);
    string headers[] = {"NodeId", "MemTotal(MB)", "MemUsed(MB)", "MemExport(MB)", "MemImport(MB)", "AvailMem(MB)"};

    for (int i = 0; i < COLUMN_SIZE; i++) {
        columnWidths[i] = static_cast<int>(headers[i].length());
    }

    vector<HostMemoryInfo> hostMemoryInfos;
    for (int i = 0; i < clusterInfo.host_num; i++) {
        HostMemoryInfo hostInfo = calculateHostMemory(clusterInfo.host[i]);

        columnWidths[0] =
            max(columnWidths[0],
                clusterInfo.host[i].host_name[0] ? static_cast<int>(strlen(clusterInfo.host[i].host_name)) : NULL_SIZE);
        columnWidths[1] = max(columnWidths[1], static_cast<int>(to_string(hostInfo.memTotal).length()));
        columnWidths[2] = max(columnWidths[2], static_cast<int>(to_string(hostInfo.memUsed).length()));
        columnWidths[3] = max(columnWidths[3], static_cast<int>(to_string(hostInfo.memExport).length()));
        columnWidths[4] = max(columnWidths[4], static_cast<int>(to_string(hostInfo.memImport).length()));
        columnWidths[5] = max(columnWidths[5], static_cast<int>(to_string(hostInfo.availableMem).length()));
        hostMemoryInfos.push_back(hostInfo);
    }

    cout << "------------------------------------------------------------------------------------\n";
    for (int i = 0; i < COLUMN_SIZE; i++) {
        cout << left << setw(columnWidths[i] + OFFSET_ALIGNMENT) << headers[i];
    }
    cout << endl;
    cout << "------------------------------------------------------------------------------------\n";

    for (int i = 0; i < clusterInfo.host_num; i++) {
        PrintHostInfo(clusterInfo.host[i].host_name, hostMemoryInfos[i], columnWidths);
        cout << endl;
    }
    cout << "NOTE: Available memory refers to the memory that this node can lend to other nodes.\n";
}

int DoRack()
{
    int ret;
    ubsmem_cluster_info_t cluster;
    ubsmem_options_t ubsm_shmem_opts;
    ret = MatrixMemFuncInit(g_matrixMemLibPath);
    if (ret != MATRIX_MEM_SUCCESS) {
        write_runlog(ERROR, "Failed to initialize matrix memory functions, error code: %d\n."
                            "It may means that you are not on a specific environment", ret);
        return 1;
    }

    ret = ubsmem_init_attributes(&ubsm_shmem_opts);
    if (ret != 0) {
        write_runlog(ERROR, "Failed to initialize ubsmem_attributes, error code: %d\n.", ret);
    }
    ret = ubsmem_initialize(&ubsm_shmem_opts);
    if (ret != 0) {
        write_runlog(ERROR, "Failed to initialize ubsmd, error code: %d\n.", ret);
    }

    ret = ubsmem_lookup_cluster_statistic(&cluster);
    if (ret != 0 || cluster.host_num <= 1) {
        write_runlog(ERROR, "lookup rack cluster statistic failed, code: [%d], node num: [%d]\n", ret,
                     cluster.host_num);
        return 1;
    }
    PrintClusterInfo(cluster);

    ret = ubsmem_finalize();
    if (ret != 0) {
        write_runlog(ERROR, "ubsmem_finalize failed, code: [%d].\n", ret);
        return 1;
    }
    MatrixMemFuncUnInit();
    return 0;
}