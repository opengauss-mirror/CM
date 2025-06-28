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
constexpr double MAX_RACK_MEMORY_PERCENT = 0.25;

#define MAX_HOSTNAME_LENGTH 48
#define MAX_SOCKET_NUM 2
#define MAX_HOST_NUM 16
#define MAX_REGIONS_NUM 6
#define MEM_INVALID_NODE_ID ("")
#define MEM_TOPOLOGY_MAX_HOSTS 16
#define MEM_TOPOLOGY_MAX_TOTAL_NUMS 16
#define MEM_MAX_ID_LENGTH 48

struct SocketInfo {
    int memTotal;
    int memUsed;
    int memExport;
    int memImport;
};

struct HostInfo {
    char hostName[MAX_HOSTNAME_LENGTH];
    int num;
    SocketInfo socket[MAX_SOCKET_NUM];
};

struct ClusterInfo {
    int num;
    HostInfo host[MAX_HOST_NUM];
};

typedef enum RackMemRegionType {
    ALL2ALL_SHARE = 0,
    ONE2ALL_SHARE,
    INCLUDE_ALL_TYPE,
} ShmRegionType;

typedef enum RackMemPerfLevel {
    L0,
    L1,
    L2
} PerfLevel;

typedef struct TagRackMemSHMRegionDesc {
    PerfLevel perfLevel;
    ShmRegionType type;
    int num;
    char nodeId[MEM_TOPOLOGY_MAX_HOSTS][MEM_MAX_ID_LENGTH];
    char hostName[MEM_TOPOLOGY_MAX_HOSTS][MEM_MAX_ID_LENGTH];
} SHMRegionDesc;

typedef struct TagRackMemSHMRegions {
    int num;
    SHMRegionDesc region[MAX_REGIONS_NUM];
} SHMRegions;

typedef struct TagRackMemSHMRegionInfo {
    int num;
    int64_t info[0];
} SHMRegionInfo;

int RackMemShmLookupShareRegions(const char *baseBid, ShmRegionType type, SHMRegions *regions);
int RackMemLookupClusterStatistic(ClusterInfo *cluster);