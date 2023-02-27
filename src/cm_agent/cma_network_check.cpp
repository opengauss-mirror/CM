/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * cma_network_check.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_network_check.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <netdb.h>
#include <ifaddrs.h>
#include <linux/if.h>
#include "cjson/cJSON.h"
#include "cma_global_params.h"
#include "cm_util.h"
#include "cm_json_config.h"
#include "cma_common.h"
#include "cma_network_check.h"

const uint32 INVALID_PORT = 0xFFFFFFFF;

typedef enum NetworkQuestE {
    NETWORK_QUEST_UNKNOWN = 0,
    NETWORK_QUEST_CHECK,
    NETWORK_QUEST_GET,
    NETWORK_QUEST_CEIL
} NetworkQuest;

typedef struct NetWorkAddrT {
    int32 family;
    const char *ip;
    char netName[NI_MAXHOST];
    char netMask[NI_MAXHOST];
} NetWorkAddr;

typedef struct ArpingCmdResT {
    char cmd[CM_MAX_COMMAND_LONG_LEN];
} ArpingCmdRes;

typedef struct NetworkInfoT {
    bool networkRes;
    NetworkType type;
    NetworkOper oper;
    uint32 port;
    uint32 cnt;
    const char (*ips)[CM_IP_LENGTH];
    NetWorkAddr *netAddr;
    NetworkState *stateCheck;
    NetworkState *stateRecord;
    ArpingCmdRes *arpingCmd; // notify switch command
} NetworkInfo;

typedef struct CmNetworkInfoT {
    uint32 instId;
    NetworkInfo manaIp[NETWORK_TYPE_CEIL];
} CmNetworkInfo;

typedef struct CmNetworkByTypeT {
    uint32 count;
    CmNetworkInfo *cmNetwork;
} CmNetworkByType;

typedef struct NetworkStateOperMapT {
    NetworkState state;
    NetworkOper oper;
} NetworkStateOperMap;

typedef struct NetworkOperStringMapT {
    NetworkOper oper;
    const char *str;
} NetworkOperStringMap;

typedef struct NetworkStateStringMapT {
    NetworkState state;
    const char *str;
} NetworkStateStringMap;

static bool GetNicstatusByAddrs(const struct ifaddrs *ifList, NetworkInfo *netInfo, int32 logLevel = WARNING,
    NetworkQuest quest = NETWORK_QUEST_CHECK);
static void GetNicDownCmd(char *cmd, uint32 cmdLen, const NetworkInfo *netInfo, uint32 index);

static CmNetworkInfo *g_cmNetWorkInfo = NULL;
static uint32 g_instCnt = 0;
static CmNetworkByType g_cmNetworkByType[CM_INSTANCE_TYPE_CEIL] = {{0}};

static NetworkStateOperMap g_stateOperMap[] = {{NETWORK_STATE_UNKNOWN, NETWORK_OPER_UNKNOWN},
    {NETWORK_STATE_UP, NETWORK_OPER_UP},
    {NETWORK_STATE_DOWN, NETWORK_OPER_DOWN},
    {NETWORK_STATE_CEIL, NETWORK_OPER_CEIL}};

static const char *g_ifconfigCmd = "ifconfig";
static const char *IFCONFIG_CMD_DEFAULT = "ifconfig";
static const char *IFCONFIG_CMD_SUSE = "/sbin/ifconfig";
static const char *IFCONFIG_CMD_EULER = "/usr/sbin/ifconfig";
static const char *ARPING_CMD = "arping -w 1 -A -I";
static const char *SHOW_IPV6_CMD = "ip addr show | grep";
static const char *IPV6_TENTATIVE_FLAG = "tentative";
static const char *IPV6_DADFAILED_FLAG = "dadfailed";

static NetworkOperStringMap g_operStringMap[NETWORK_OPER_CEIL] = {
    {NETWORK_OPER_UNKNOWN, "NETWORK_OPER_UNKNOWN"},
    {NETWORK_OPER_UP, "NETWORK_OPER_UP"},
    {NETWORK_OPER_DOWN, "NETWORK_OPER_DOWN"},
};

static NetworkStateStringMap g_stateStringMap[NETWORK_STATE_CEIL] = {
    {NETWORK_STATE_UNKNOWN, "NETWORK_STATE_UNKNOWN"},
    {NETWORK_STATE_UP, "NETWORK_STATE_UP"},
    {NETWORK_STATE_DOWN, "NETWORK_STATE_DOWN"},
};

const char *GetStateMapString(NetworkState state)
{
    for (uint32 i = 0; i < (uint32)NETWORK_STATE_CEIL; ++i) {
        if (g_stateStringMap[i].state == state) {
            return g_stateStringMap[i].str;
        }
    }
    return "unkown_state";
}

const char *GetOperMapString(NetworkOper oper)
{
    for (uint32 i = 0; i < (uint32)NETWORK_OPER_CEIL; ++i) {
        if (g_operStringMap[i].oper == oper) {
            return g_operStringMap[i].str;
        }
    }
    return "unknown_oper";
}

NetworkOper ChangeInt2NetworkOper(int32 oper)
{
    if (oper < 0 || oper >= (int32)NETWORK_OPER_CEIL) {
        return NETWORK_OPER_UNKNOWN;
    }
    return (NetworkOper)oper;
}

NetworkState GetNetworkStateByOper(NetworkOper oper)
{
    size_t len = sizeof(g_stateOperMap) / sizeof(g_stateOperMap[0]);
    for (size_t i = 0; i < len; ++i) {
        if (g_stateOperMap[i].oper == oper) {
            return g_stateOperMap[i].state;
        }
    }
    return NETWORK_STATE_UNKNOWN;
}

NetworkOper GetNetworkOperByState(NetworkState state)
{
    size_t len = sizeof(g_stateOperMap) / sizeof(g_stateOperMap[0]);
    for (size_t i = 0; i < len; ++i) {
        if (g_stateOperMap[i].state == state) {
            return g_stateOperMap[i].oper;
        }
    }
    return NETWORK_OPER_UNKNOWN;
}

static NetworkInfo *GetNetworkInfo(uint32 instId, CmaInstType instType, NetworkType type)
{
    if (instType >= CM_INSTANCE_TYPE_CEIL || type >= NETWORK_TYPE_CEIL) {
        write_runlog(ERROR, "error instType(%d: %d) or networkType(%d: %d).",
            (int32)instType, (int32)CM_INSTANCE_TYPE_CEIL, (int32)type, (int32)NETWORK_TYPE_CEIL);
        return NULL;
    }
    CmNetworkByType *cmNetworkByType = &(g_cmNetworkByType[instType]);
    if (cmNetworkByType->cmNetwork == NULL) {
        write_runlog(ERROR, "cmNetwork is NULL, type is [%d:%d].\n", (int32)instType, (int32)type);
        return NULL;
    }
    for (uint32 i = 0; i < cmNetworkByType->count; ++i) {
        if (cmNetworkByType->cmNetwork[i].instId == instId) {
            return &(cmNetworkByType->cmNetwork[i].manaIp[type]);
        }
    }
    write_runlog(ERROR, "cmNetInfo is NULL, type is [%d:%d].\n", (int32)instType, (int32)type);
    return NULL;
}

bool GetNicStatus(unsigned int instId, CmaInstType instType, NetworkType type)
{
    NetworkInfo *netInfo = GetNetworkInfo(instId, instType, type);
    if (netInfo == NULL) {
        write_runlog(ERROR, "[GetNicStatus] cannot find the NetInfo, instId(%u), instType(%d), networkType(%d).\n",
            instId, (int32)instType, (int32)type);
        return false;
    }
    return netInfo->networkRes;
}

void GetFloatIpNicStatus(uint32 instId, CmaInstType instType, NetworkState *state, uint32 count)
{
    for (uint32 i = 0; i < count; ++i) {
        state[i] = NETWORK_STATE_UNKNOWN;
    }
    NetworkInfo *netInfo = GetNetworkInfo(instId, instType, NETWORK_TYPE_FLOATIP);
    if (netInfo == NULL) {
        write_runlog(ERROR, "[GetNicStatus] cannot find the NetInfo, instId(%u), instType(%d), networkType(%d).\n",
            instId, (int32)instType, (int32)NETWORK_TYPE_FLOATIP);
        return;
    }
    for (uint32 i = 0; i < count; ++i) {
        if (i >= netInfo->cnt) {
            return;
        }
        state[i] = netInfo->stateRecord[i];
    }
}

void SetNicOper(uint32 instId, CmaInstType instType, NetworkType type, NetworkOper oper)
{
    NetworkInfo *netInfo = GetNetworkInfo(instId, instType, type);
    if (netInfo == NULL) {
        write_runlog(ERROR, "[SetNicOper] cannot find the NetInfo, instId(%u), instType(%d), networkType(%d).\n",
            instId, (int32)instType, (int32)type);
        return;
    }
    netInfo->oper = oper;
    return;
}

static void SetCmNetworkByTypeCnt(CmaInstType type, uint32 *count, uint32 instcnt)
{
    if (type >= CM_INSTANCE_TYPE_CEIL) {
        return;
    }
    g_cmNetworkByType[type].count = instcnt;
    (*count) += instcnt;
}

static uint32 GetCurrentNodeInstNum()
{
    uint32 count = 0;
    // cm_agent
    SetCmNetworkByTypeCnt(CM_INSTANCE_TYPE_CMA, &count, 1);

    // cm_server
    if (g_currentNode->cmServerLevel == 1) {
        SetCmNetworkByTypeCnt(CM_INSTANCE_TYPE_CMS, &count, 1);
    }

    // CN
    if (g_currentNode->coordinate == 1) {
        SetCmNetworkByTypeCnt(CM_INSTANCE_TYPE_CN, &count, 1);
    }

    // gtm
    if (g_currentNode->gtm == 1) {
        SetCmNetworkByTypeCnt(CM_INSTANCE_TYPE_GTM, &count, 1);
    }

    // DN
    SetCmNetworkByTypeCnt(CM_INSTANCE_TYPE_DN, &count, g_currentNode->datanodeCount);
    return count;
}

static status_t SetNetWorkInfo(
    NetworkInfo *info, uint32 cnt, const char (*ips)[CM_IP_LENGTH], NetworkType type, uint32 port)
{
    info->networkRes = false;
    info->type = type;
    info->ips = ips;
    uint32 curCnt = cnt;
    if (cnt >= MAX_FLOAT_IP_COUNT) {
        curCnt = MAX_FLOAT_IP_COUNT;
        write_runlog(LOG, "cnt(%u) is more than MAX_FLOAT_IP_COUNT(%u), will set it to MAX_FLOAT_IP_COUNT.\n",
            cnt, MAX_FLOAT_IP_COUNT);
    }
    info->cnt = curCnt;
    info->oper = NETWORK_OPER_UNKNOWN;
    info->port = port;
    if (cnt == 0) {
        info->netAddr = NULL;
    } else {
        if (type == NETWORK_TYPE_LISTEN || type == NETWORK_TYPE_HA) {
            return CM_SUCCESS;
        }
        size_t allSize =
            (sizeof(NetWorkAddr) + sizeof(NetworkState) + sizeof(NetworkState) + sizeof(ArpingCmdRes)) * curCnt;
        char *dynamicStr = (char *)malloc(allSize);
        if (dynamicStr == NULL) {
            write_runlog(ERROR, "failed to malloc memory(%lu).\n", allSize);
            return CM_ERROR;
        }
        errno_t rc = memset_s(dynamicStr, allSize, 0, allSize);
        securec_check_errno(rc, (void)rc);
        size_t curSize = 0;
        info->netAddr = (NetWorkAddr *)GetDynamicMem(dynamicStr, &curSize, sizeof(NetWorkAddr) * curCnt);
        info->stateCheck = (NetworkState *)GetDynamicMem(dynamicStr, &curSize, sizeof(NetworkState) * curCnt);
        info->stateRecord = (NetworkState *)GetDynamicMem(dynamicStr, &curSize, sizeof(NetworkState) * curCnt);
        info->arpingCmd = (ArpingCmdRes *)GetDynamicMem(dynamicStr, &curSize, sizeof(ArpingCmdRes) * curCnt);
        if (curSize != allSize) {
            FREE_AND_RESET(dynamicStr);
            info->netAddr = NULL;
            info->stateCheck = NULL;
            info->stateRecord = NULL;
            info->arpingCmd = NULL;
            write_runlog(ERROR, "falled to alloc memory, curSize is %lu, allSize is %lu.\n", curSize, allSize);
            return CM_ERROR;
        }
    }
    return CM_SUCCESS;
}

static status_t SetCmNetWorkInfoDn(CmaInstType type, uint32 *index, const dataNodeInfo *datanodeInfo, uint32 dnIdx)
{
    const dataNodeInfo *dnInfo = &(datanodeInfo[dnIdx]);
    CmNetworkInfo *cmNetWorkInfo = &(g_cmNetWorkInfo[(*index)]);
    cmNetWorkInfo->instId = dnInfo->datanodeId;
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_LISTEN]), dnInfo->datanodeListenCount,
        dnInfo->datanodeListenIP, NETWORK_TYPE_LISTEN, dnInfo->datanodePort));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_HA]), dnInfo->datanodeLocalHAListenCount,
        dnInfo->datanodeLocalHAIP, NETWORK_TYPE_HA, dnInfo->datanodeLocalHAPort));
    ++(*index);
    if (type >= CM_INSTANCE_TYPE_CEIL) {
        return CM_SUCCESS;
    }
    g_cmNetworkByType[type].cmNetwork = cmNetWorkInfo;
    return CM_SUCCESS;
}

static status_t SetCmNetWorkInfoCms(uint32 *index)
{
    CmNetworkInfo *cmNetWorkInfo = &(g_cmNetWorkInfo[(*index)]);
    cmNetWorkInfo->instId = g_currentNode->cmServerId;
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_LISTEN]), g_currentNode->cmServerListenCount,
        g_currentNode->cmServer, NETWORK_TYPE_LISTEN, g_currentNode->port));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_HA]), g_currentNode->cmServerLocalHAListenCount,
        g_currentNode->cmServerLocalHAIP, NETWORK_TYPE_HA, g_currentNode->cmServerLocalHAPort));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_FLOATIP]), 0,
        NULL, NETWORK_TYPE_FLOATIP, INVALID_PORT));
    ++(*index);
    g_cmNetworkByType[CM_INSTANCE_TYPE_CMS].cmNetwork = cmNetWorkInfo;
    return CM_SUCCESS;
}

static status_t SetCmNetWorkInfoCma(uint32 *index)
{
    CmNetworkInfo *cmNetWorkInfo = &(g_cmNetWorkInfo[(*index)]);
    cmNetWorkInfo->instId = g_currentNode->cmAgentId;
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_LISTEN]), g_currentNode->cmAgentListenCount,
        g_currentNode->cmAgentIP, NETWORK_TYPE_LISTEN,
        INVALID_PORT));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_HA]), 0,
        NULL, NETWORK_TYPE_HA, INVALID_PORT));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_FLOATIP]), 0,
        NULL, NETWORK_TYPE_FLOATIP, INVALID_PORT));
    ++(*index);
    g_cmNetworkByType[CM_INSTANCE_TYPE_CMA].cmNetwork = cmNetWorkInfo;
    return CM_SUCCESS;
}

static status_t SetCmNetWorkInfoCN(uint32 *index)
{
    CmNetworkInfo *cmNetWorkInfo = &(g_cmNetWorkInfo[(*index)]);
    cmNetWorkInfo->instId = g_currentNode->coordinateId;
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_LISTEN]), g_currentNode->coordinateListenCount,
        g_currentNode->coordinateListenIP, NETWORK_TYPE_LISTEN, g_currentNode->coordinatePort));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_HA]), g_currentNode->coordinateListenCount,
        g_currentNode->coordinateListenIP, NETWORK_TYPE_HA, g_currentNode->coordinateHAPort));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_FLOATIP]), 0,
        NULL, NETWORK_TYPE_FLOATIP, INVALID_PORT));
    ++(*index);
    g_cmNetworkByType[CM_INSTANCE_TYPE_CN].cmNetwork = cmNetWorkInfo;
    return CM_SUCCESS;
}

static status_t SetCmNetWorkInfoGTM(uint32 *index)
{
    CmNetworkInfo *cmNetWorkInfo = &(g_cmNetWorkInfo[(*index)]);
    cmNetWorkInfo->instId = g_currentNode->gtmId;
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_LISTEN]), g_currentNode->gtmLocalListenCount,
        g_currentNode->gtmLocalListenIP, NETWORK_TYPE_LISTEN, g_currentNode->gtmLocalport));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_HA]), g_currentNode->gtmLocalHAListenCount,
        g_currentNode->gtmLocalHAIP, NETWORK_TYPE_HA, g_currentNode->gtmLocalHAPort));
    CM_RETURN_IFERR(SetNetWorkInfo(&(cmNetWorkInfo->manaIp[NETWORK_TYPE_FLOATIP]), 0,
        NULL, NETWORK_TYPE_FLOATIP, INVALID_PORT));
    ++(*index);
    g_cmNetworkByType[CM_INSTANCE_TYPE_GTM].cmNetwork = cmNetWorkInfo;
    return CM_SUCCESS;
}

static status_t SetCmNetworkInfo()
{
    uint32 index = 0;
    // agent
    CM_RETURN_IFERR(SetCmNetWorkInfoCma(&index));

    // cm_server
    if (g_currentNode->cmServerLevel == 1) {
        CM_RETURN_IFERR(SetCmNetWorkInfoCms(&index));
    }

    // CN
    if (g_currentNode->coordinate == 1) {
        CM_RETURN_IFERR(SetCmNetWorkInfoCN(&index));
    }

    // GTM
    if (g_currentNode->gtm == 1) {
        CM_RETURN_IFERR(SetCmNetWorkInfoGTM(&index));
    }

    // DN
    for (uint32 i = 0; i < g_currentNode->datanodeCount; ++i) {
        CmaInstType type = (i == 0) ? CM_INSTANCE_TYPE_DN : CM_INSTANCE_TYPE_CEIL;
        CM_RETURN_IFERR(SetCmNetWorkInfoDn(type, &index, g_currentNode->datanode, i));
    }

    // check index
    if (index != g_instCnt) {
        write_runlog(ERROR, "index(%u) is different from instCnt(%u).\n", index, g_instCnt);
        FREE_AND_RESET(g_cmNetWorkInfo);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static void InitIfconfigCmd()
{
    if (CmFileExist(IFCONFIG_CMD_EULER)) {
        g_ifconfigCmd = IFCONFIG_CMD_EULER;
    } else if (CmFileExist(IFCONFIG_CMD_SUSE)) {
        g_ifconfigCmd = IFCONFIG_CMD_SUSE;
    } else {
        g_ifconfigCmd = IFCONFIG_CMD_DEFAULT;
    }
}

static status_t InitCmNetWorkInfo()
{
    InitIfconfigCmd();
    g_instCnt = GetCurrentNodeInstNum();
    write_runlog(LOG, "current node has %u instance.\n", g_instCnt);
    size_t mallocLen = sizeof(CmNetworkInfo) * g_instCnt;
    g_cmNetWorkInfo = (CmNetworkInfo *)malloc(mallocLen);
    if (g_cmNetWorkInfo == NULL) {
        write_runlog(ERROR, "[InitCmNetWorkInfo] failed to malloc %lu memory.\n", mallocLen);
        return CM_ERROR;
    }
    errno_t rc = memset_s(g_cmNetWorkInfo, mallocLen, 0, mallocLen);
    securec_check_errno(rc, (void)rc);
    return SetCmNetworkInfo();
}

static void SetNetworkStatus(NetworkInfo *netWorkInfo, bool networkRes)
{
    if (!networkRes) {
        netWorkInfo->networkRes = false;
    }
}

static void CheckNetworkValidCnt(bool networkRes)
{
    if (networkRes) {
        return;
    }
    for (uint32 i = 0; i < g_instCnt; ++i) {
        for (uint32 j = 0; j < (uint32)NETWORK_TYPE_CEIL; ++j) {
            SetNetworkStatus(&(g_cmNetWorkInfo[i].manaIp[j]), networkRes);
            if (j != (uint32)NETWORK_TYPE_FLOATIP) {
                continue;
            }
            NetworkInfo *networkInfo = &(g_cmNetWorkInfo[i].manaIp[j]);
            for (uint32 k = 0; k < networkInfo->cnt; ++k) {
                networkInfo->stateRecord[k] = NETWORK_STATE_DOWN;
            }
        }
    }
}

static void ResetNetMask(NetWorkAddr *netAddr)
{
    errno_t rc = memset_s(netAddr->netMask, NI_MAXHOST, 0, NI_MAXHOST);
    securec_check_errno(rc, (void)rc);
    rc = memset_s(netAddr->netName, NI_MAXHOST, 0, NI_MAXHOST);
    securec_check_errno(rc, (void)rc);
}

static void ResetAllNetMask(NetworkInfo *netInfo)
{
    for (uint32 i = 0; i < netInfo->cnt; ++i) {
        ResetNetMask(&(netInfo->netAddr[i]));
    }
}

static void GetNetworkAddrNetMask(NetWorkAddr *netAddr, const struct ifaddrs *ifa, const char *ip)
{
    if (netAddr == NULL) {
        return;
    }
    // family
    int32 family = ifa->ifa_addr->sa_family;
    netAddr->family = ifa->ifa_addr->sa_family;
    // netname
    errno_t rc = strcpy_s(netAddr->netName, NI_MAXHOST, ifa->ifa_name);
    securec_check_errno(rc, (void)rc);

    // netMask
    size_t saLen = (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
    if (getnameinfo(ifa->ifa_netmask, saLen, netAddr->netMask, NI_MAXHOST, NULL, 0, NI_NUMERICHOST) != 0) {
        write_runlog(WARNING, "failed to get netMask info, ip is %s.\n", ip);
        ResetNetMask(netAddr);
    }
    write_runlog(LOG, "ip is %s, family is %d, netName is %s, netmask is %s.\n",
        ip, family, netAddr->netName, netAddr->netMask);
}

static bool GetNetworkIp(
    NetworkInfo *netInfo, const char *host, uint32 *index, NetworkQuest quest, const struct ifaddrs *ifa)
{
    if (quest == NETWORK_QUEST_CHECK) {
        for (uint32 i = 0; i < netInfo->cnt; ++i) {
            if (strncmp(netInfo->ips[i], host, NI_MAXHOST) == 0) {
                *index = i;
                return true;
            }
        }
    } else if (quest == NETWORK_QUEST_GET) {
        uint32 i;
        for (i = 0; i < netInfo->cnt; ++i) {
            if (strncmp(netInfo->netAddr[i].ip, host, NI_MAXHOST) == 0) {
                break;
            }
        }
        if (i >= netInfo->cnt) {
            return false;
        }
        *index = i;
        GetNetworkAddrNetMask(&(netInfo->netAddr[(*index)]), ifa, host);
    }
    return false;
}

static bool IsNicAvailable(const struct ifaddrs *ifa, const char *host, int32 logLevel)
{
    if (!(ifa->ifa_flags & IFF_UP)) {
        write_runlog(logLevel, "nic %s related with %s is down, ifa_flags=%u.\n", ifa->ifa_name, host, ifa->ifa_flags);
        return false;
    }

    if (!(ifa->ifa_flags & IFF_RUNNING)) {
        write_runlog(
            logLevel, "nic %s related with %s not running, ifa_flags=%u.\n", ifa->ifa_name, host, ifa->ifa_flags);
        return false;
    }
    return true;
}

static void CheckFloatIpNic(
    NetworkInfo *netInfo, const struct ifaddrs *ifa, const char *host, int32 logLevel, uint32 index)
{
    if (index >= netInfo->cnt) {
        return;
    }
    bool res = IsNicAvailable(ifa, host, logLevel);
    if (!res) {
        netInfo->stateCheck[index] = NETWORK_STATE_DOWN;
    } else {
        netInfo->stateCheck[index] = NETWORK_STATE_UP;
    }
}

static bool GetNicstatusByAddrs(
    const struct ifaddrs *ifList, NetworkInfo *netInfo, int32 logLevel, NetworkQuest quest)
{
    if (netInfo->cnt == 0 || netInfo->ips == NULL) {
        return false;
    }
    char host[NI_MAXHOST] = {0};
    uint32 validIpCount = 0;
    uint32 index = 0;
    for (const struct ifaddrs *ifa = ifList; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL) {
            continue;
        }
        int32 family = ifa->ifa_addr->sa_family;
        if (family != AF_INET && family != AF_INET6) {
            continue;
        }
        size_t saLen = (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
        if (getnameinfo(ifa->ifa_addr, saLen, host, NI_MAXHOST, NULL, 0, NI_NUMERICHOST) != 0) {
            write_runlog(WARNING, "failed to get name info.\n");
            return false;
        }
        if (!GetNetworkIp(netInfo, host, &index, quest, ifa)) {
            continue;
        }
        if (netInfo->type == NETWORK_TYPE_FLOATIP) {
            CheckFloatIpNic(netInfo, ifa, host, logLevel, index);
            continue;
        }
        if (!IsNicAvailable(ifa, host, logLevel)) {
            return false;
        }
        ++validIpCount;
        if (validIpCount == netInfo->cnt) {
            return true;
        }
    }
    if (quest == NETWORK_QUEST_GET || netInfo->type == NETWORK_TYPE_FLOATIP) {
        return true;
    }
    char allListenIp[CM_IP_ALL_NUM_LENGTH] = {0};
    listen_ip_merge(netInfo->cnt, netInfo->ips, allListenIp, CM_IP_ALL_NUM_LENGTH);
    write_runlog(WARNING, "can't find nic related with %s.\n", allListenIp);
    return false;
}

static void CheckNicStatus(struct ifaddrs *ifList, NetworkInfo *netInfo)
{
    int32 logLevel = DEBUG1;
    if (netInfo->type == NETWORK_TYPE_LISTEN || netInfo->type == NETWORK_TYPE_HA) {
        logLevel = WARNING;
    }
    if (netInfo->type == NETWORK_TYPE_FLOATIP) {
        for (uint32 i = 0; i < netInfo->cnt; ++i) {
            netInfo->stateCheck[i] = NETWORK_STATE_DOWN;
        }
    }
    if (GetNicstatusByAddrs(ifList, netInfo, logLevel)) {
        netInfo->networkRes = true;
    } else {
        netInfo->networkRes = false;
    }
    if (netInfo->type != NETWORK_TYPE_FLOATIP) {
        return;
    }
    for (uint32 i = 0; i < netInfo->cnt; ++i) {
        if (!netInfo->networkRes) {
            netInfo->stateRecord[i] = NETWORK_STATE_DOWN;
        } else {
            netInfo->stateRecord[i] = netInfo->stateCheck[i];
        }
    }
}

static bool CheckNetworkStatus()
{
    struct ifaddrs *ifList = NULL;
    if (getifaddrs(&ifList) < 0) {
        write_runlog(WARNING, "failed to get iflist.\n");
        return false;
    }
    for (uint32 i = 0; i < g_instCnt; ++i) {
        for (uint32 j = 0; j < (uint32)NETWORK_TYPE_CEIL; ++j) {
            CheckNicStatus(ifList, &(g_cmNetWorkInfo[i].manaIp[j]));
        }
    }
    freeifaddrs(ifList);
    return true;
}

static bool GetNetworkAddr(NetworkInfo *netInfo, const char *str)
{
    struct ifaddrs *ifList = NULL;
    if (getifaddrs(&ifList) < 0) {
        write_runlog(WARNING, "%s failed to get iflist.\n", str);
        return false;
    }
    bool ret = GetNicstatusByAddrs(ifList, netInfo, DEBUG1, NETWORK_QUEST_GET);
    freeifaddrs(ifList);
    return ret;
}

static bool SetDownIpV6Nic(const NetworkInfo *netInfo, uint32 index, const char *str)
{
    char cmd[CM_MAX_COMMAND_LONG_LEN] = {0};
    GetNicDownCmd(cmd, CM_MAX_COMMAND_LONG_LEN, netInfo, index);
    if (cmd[0] == '\0') {
        return false;
    }
    write_runlog(LOG, "%s Ip: %s oper=[%d: %s], state=[%d: %s], GetNicCmd(%s).\n",
        str, netInfo->ips[index], (int32)netInfo->oper, GetOperMapString(netInfo->oper),
        (int32)netInfo->stateRecord[index], GetStateMapString(netInfo->stateRecord[index]), cmd);
    int32 res = ExecuteSystemCmd(cmd);
    if (res != 0) {
        write_runlog(ERROR, "%s failed to execute the cmd(%s), res=%d, errno is %d.\n", str, cmd, res, errno);
        return false;
    } else {
        write_runlog(LOG, "%s successfully to execute the cmd(%s).\n", str, cmd);
        return true;
    }
}

static bool CheckIpV6Valid(const NetworkInfo *netInfo, uint32 index, const char *str, int32 logLevel)
{
    char cmd[CM_MAX_COMMAND_LONG_LEN] = {0};
    errno_t rc = snprintf_s(cmd, CM_MAX_COMMAND_LONG_LEN, CM_MAX_COMMAND_LONG_LEN - 1, "%s %s |grep %s |grep %s",
        SHOW_IPV6_CMD, netInfo->ips[index], IPV6_DADFAILED_FLAG, IPV6_TENTATIVE_FLAG);
    securec_check_intval(rc, (void)rc);
    write_runlog(logLevel, "%s it will check ipV6 nic, and cmd is %s.\n", str, cmd);
    int32 ret = ExecuteSystemCmd(cmd);
    if (ret == 0) {
        write_runlog(logLevel, "%s ipV6(%s) nic may be faulty.\n", str, netInfo->ips[index]);
        return SetDownIpV6Nic(netInfo, index, str);
    }
    return true;
}

static void GetNicUpCmd(char *cmd, uint32 cmdLen, const NetworkInfo *netInfo, uint32 index)
{
    const NetWorkAddr *netAddr = &(netInfo->netAddr[index]);
    if (netInfo->netAddr[index].netMask[0] == '\0' || netInfo->netAddr[index].netName[0] == '\0') {
        return;
    }
    errno_t rc = 0;
    if (netInfo->netAddr[index].family == AF_INET) {
        rc = snprintf_s(cmd, cmdLen, cmdLen - 1, "%s %s:%u %s netmask %s up", g_ifconfigCmd, netAddr->netName,
            netInfo->port, netInfo->ips[index], netAddr->netMask);
    } else if (netInfo->netAddr[index].family == AF_INET6) {
        if (!CheckIpV6Valid(netInfo, index, "[GetNicUpCmd]", LOG)) {
            return;
        }
        rc = snprintf_s(cmd, cmdLen, cmdLen - 1, "%s %s inet6 add %s/%s", g_ifconfigCmd, netAddr->netName,
            netInfo->ips[index], netAddr->netMask);
    }
    securec_check_intval(rc, (void)rc);
}

static void GetNicDownCmd(char *cmd, uint32 cmdLen, const NetworkInfo *netInfo, uint32 index)
{
    const NetWorkAddr *netAddr = &(netInfo->netAddr[index]);
    if (netInfo->netAddr[index].netMask[0] == '\0' || netInfo->netAddr[index].netName[0] == '\0') {
        return;
    }
    errno_t rc = 0;
    if (netInfo->netAddr[index].family == AF_INET) {
        rc = snprintf_s(cmd, cmdLen, cmdLen - 1, "%s %s:%u down", g_ifconfigCmd, netAddr->netName,
            netInfo->port);
    } else {
        rc = snprintf_s(cmd, cmdLen, cmdLen - 1, "%s %s inet6 del %s/%s", g_ifconfigCmd, netAddr->netName,
            netInfo->ips[index], netAddr->netMask);
    }
    securec_check_intval(rc, (void)rc);
}

static void ExecuteArpingCmd(ArpingCmdRes *arpingCmd, const char *str)
{
    if (arpingCmd == NULL || arpingCmd->cmd[0] == '\0') {
        return;
    }
    write_runlog(LOG, "%s it will notify switch, and cmd is %s.\n", str, arpingCmd->cmd);
    int32 res = ExecuteSystemCmd(arpingCmd->cmd);
    if (res != 0) {
        write_runlog(
            ERROR, "%s failed to execute the cmd(%s), res=%d, errno is %d.\n", str, arpingCmd->cmd, res, errno);
    } else {
        write_runlog(LOG, "%s success to execute the cmd(%s).\n", str, arpingCmd->cmd);
        errno_t rc = memset_s(arpingCmd->cmd, CM_MAX_COMMAND_LONG_LEN, 0, CM_MAX_COMMAND_LONG_LEN);
        securec_check_errno(rc, (void)rc);
    }
}

static void CheckArpingCmdRes(NetworkInfo *netInfo)
{
    if (netInfo->oper != NETWORK_OPER_UP) {
        return;
    }
    for (uint32 i = 0; i < netInfo->cnt; ++i) {
        ExecuteArpingCmd(&(netInfo->arpingCmd[i]), "[CheckArpingCmdRes]");
    }
}

static bool CheckNicStatusMeetsExpect(NetworkInfo *netInfo)
{
    if (netInfo->cnt == 0) {
        return true;
    }
    NetworkState state = GetNetworkStateByOper(netInfo->oper);
    if (state == NETWORK_STATE_UNKNOWN) {
        return true;
    }
    // only float ip is up, it will notify switch
    if (netInfo->oper == NETWORK_OPER_UP) {
        CheckArpingCmdRes(netInfo);
    }
    for (uint32 i = 0; i < netInfo->cnt; ++i) {
        if (netInfo->stateRecord[i] != state) {
            return false;
        }
    }
    return true;
}

static void GenArpingCmdAndExecute(NetworkInfo *netInfo, uint32 index)
{
    if (netInfo->oper != NETWORK_OPER_UP) {
        return;
    }
    errno_t rc = memset_s(netInfo->arpingCmd[index].cmd, CM_MAX_COMMAND_LONG_LEN, 0, CM_MAX_COMMAND_LONG_LEN);
    securec_check_errno(rc, (void)rc);
    if (netInfo->netAddr[index].family == AF_INET) {
        rc = snprintf_s(netInfo->arpingCmd[index].cmd, CM_MAX_COMMAND_LONG_LEN, CM_MAX_COMMAND_LONG_LEN - 1,
            "%s %s %s", ARPING_CMD, netInfo->netAddr[index].netName, netInfo->ips[index]);
        securec_check_intval(rc, (void)rc);
    }
    ExecuteArpingCmd(&(netInfo->arpingCmd[index]), "[GenArpingCmdAndExecute]");
}

static void DoUpOrDownNetworkOper(NetworkInfo *netInfo)
{
    if (CheckNicStatusMeetsExpect(netInfo)) {
        return;
    }
    const char *str = (netInfo->oper == NETWORK_OPER_UP) ? "[DoUpNetworkOper]" : "[DoDownNetworkOper]";
    ResetAllNetMask(netInfo);
    if (!GetNetworkAddr(netInfo, str)) {
        return;
    }
    char cmd[CM_MAX_COMMAND_LONG_LEN];
    int32 res;
    errno_t rc;
    for (uint32 i = 0; i < netInfo->cnt; ++i) {
        rc = memset_s(cmd, CM_MAX_COMMAND_LONG_LEN, 0, CM_MAX_COMMAND_LONG_LEN);
        securec_check_errno(rc, (void)rc);
        if (netInfo->oper == NETWORK_OPER_UP && netInfo->stateRecord[i] != NETWORK_STATE_UP) {
            GetNicUpCmd(cmd, CM_MAX_COMMAND_LONG_LEN, netInfo, i);
        } else if (netInfo->oper == NETWORK_OPER_DOWN && netInfo->stateRecord[i] == NETWORK_STATE_UP) {
            GetNicDownCmd(cmd, CM_MAX_COMMAND_LONG_LEN, netInfo, i);
        }
        if (cmd[0] == '\0') {
            continue;
        }
        write_runlog(LOG, "%s Ip: %s oper=[%d: %s], state=[%d: %s], GetNicCmd(%s).\n", str, netInfo->ips[i],
            (int32)netInfo->oper, GetOperMapString(netInfo->oper), (int32)netInfo->stateRecord[i],
            GetStateMapString(netInfo->stateRecord[i]), cmd);

        if (g_isPauseArbitration) {
            continue;
        }

        res = ExecuteSystemCmd(cmd);
        if (res != 0) {
            write_runlog(ERROR, "%s failed to execute the cmd(%s), res=%d, errno is %d.\n", str, cmd, res, errno);
        } else {
            netInfo->stateRecord[i] = GetNetworkStateByOper(netInfo->oper);
            write_runlog(LOG, "%s successfully to execute the cmd(%s).\n", str, cmd);
            GenArpingCmdAndExecute(netInfo, i);
        }
    }
}

static void DoNetworkOper()
{
    NetworkInfo *manaIp = NULL;
    for (uint32 i = 0; i < g_instCnt; ++i) {
        manaIp = &(g_cmNetWorkInfo[i].manaIp[NETWORK_TYPE_FLOATIP]);
        if (manaIp->oper == NETWORK_OPER_UNKNOWN) {
            continue;
        }
        DoUpOrDownNetworkOper(manaIp);
    }
}

static void ReleaseSource()
{
    if (g_cmNetWorkInfo == NULL) {
        return;
    }
    for (uint32 i = 0; i < g_instCnt; ++i) {
        for (uint32 j = 0; j < (uint32)NETWORK_TYPE_CEIL; ++j) {
            FREE_AND_RESET(g_cmNetWorkInfo[i].manaIp[j].netAddr);
        }
    }
    FREE_AND_RESET(g_cmNetWorkInfo);
}

status_t CreateNetworkResource()
{
    status_t st = InitCmNetWorkInfo();
    if (st != CM_SUCCESS) {
        ReleaseSource();
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

void *CmaCheckNetWorkMain(void *arg)
{
    thread_name = "CheckNetWork";
    write_runlog(LOG, "CmaCheckNetWorkMain will start, and threadId is %llu.\n", (unsigned long long)pthread_self());
    (void)pthread_detach(pthread_self());
    uint32 sleepInterval = 1;
    bool networkRes = false;
    for (;;) {
        if ((g_exitFlag || g_shutdownRequest)) {
            cm_sleep(sleepInterval);
            continue;
        }
        networkRes = CheckNetworkStatus();
        CheckNetworkValidCnt(networkRes);
        DoNetworkOper();
        cm_sleep(sleepInterval);
    }
    ReleaseSource();
    return NULL;
}
