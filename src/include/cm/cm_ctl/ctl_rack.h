/*
 * Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 * ctl_rack.h
 *
 * IDENTIFICATION
 *    include/cm/cm_ctl/ctl_rack.h
 *
 * -------------------------------------------------------------------------
 */

constexpr auto MATRIX_MEM_SUCCESS = 0;
constexpr auto MATRIX_MEM_ERROR = -1;
constexpr auto RACK_PRECENT = 25;

typedef struct SymbolInfo {
    char *symbolName;
    void **funcptr;
} SymbolInfo;

typedef struct MatrixMemFunc {
    bool inited;
    void *handle;
    int (*rackMemShmLookupShareRegions)(const char *baseBid, ShmRegionType type, SHMRegions *regions);
    int (*rackMemLookupClusterStatistic)(ClusterInfo *cluster);
} MatrixMemFunc;

typedef struct {
    int totalMemTotal;
    int totalMemUsed;
    int totalMemExport;
    int totalMemImport;
    int availableMem;
} MemoryStates;
