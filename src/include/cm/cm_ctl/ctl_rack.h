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
#ifndef CTL_RACK_H
#define CTL_RACK_H
constexpr auto MATRIX_MEM_SUCCESS = 0;
constexpr auto MATRIX_MEM_ERROR = -1;
constexpr auto PERCENTAGE_CONVERSION = 100;
constexpr auto KILO = 1024;
constexpr auto OFFSET_ALIGNMENT = 2;
constexpr auto COLUMN_SIZE = 6;

typedef struct SymbolInfo {
    char *symbolName;
    void **funcptr;
} SymbolInfo;

typedef struct MatrixMemFunc {
    bool inited;
    void *handle;
    int (*ubsmem_init_attributes)(ubsmem_options_t *ubsm_shmem_opts);
    int (*ubsmem_initialize)(const ubsmem_options_t *ubsm_shmem_opts);
    int (*ubsmem_finalize)(void);
    int (*ubsmem_lookup_cluster_statistic)(ubsmem_cluster_info_t *info);
} MatrixMemFunc;

typedef struct {
    uint64_t memTotal;
    uint64_t memUsed;
    uint64_t memExport;
    uint64_t memImport;
    uint64_t availableMem;
} HostMemoryInfo;
#endif // CTL_RACK_H