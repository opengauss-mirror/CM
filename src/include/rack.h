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
 * rack.h
 *
 * IDENTIFICATION
 *    include/rack.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef RACK_H
#define RACK_H
#define MAX_HOST_NAMEDESC_LENGTH 48
#define MAX_NUMA_RESV_LEN 16
#define MAX_NUMA_NUM 32
#define MAX_HOST_NUM 16

typedef struct {
    // todo
} ubsmem_options_t;

typedef struct {
    uint32_t slot_id;
    uint32_t socket_id;
    uint32_t numa_id;
    uint32_t mem_lend_ratio;
    uint64_t mem_total;
    uint64_t mem_free;
    uint64_t mem_borrow;
    uint64_t mem_lend;
    uint8_t resv[MAX_NUMA_RESV_LEN];   
} ubsmem_numa_mem_t;

typedef struct {
    char host_name[MAX_HOST_NAMEDESC_LENGTH];
    int numa_num;
    ubsmem_numa_mem_t numa[MAX_NUMA_NUM];
} ubsmem_host_info_t;

typedef struct {
    int host_num;
    ubsmem_host_info_t host[MAX_HOST_NUM];
} ubsmem_cluster_info_t;

int ubsmem_lookup_cluster_statistic(ubsmem_cluster_info_t *info);

int ubsmem_init_attributes(ubsmem_options_t *ubsm_shmem_opts);

int ubsmem_initialize(const ubsmem_options_t *ubsm_shmem_opts);

int ubsmem_finalize(void);
#endif // RACK_H