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
* cm_misc_res.cpp
*
*
* IDENTIFICATION
*    src/cm_common/cm_misc_res.cpp
*
* -------------------------------------------------------------------------
 */
#include "cm_json_config.h"
#include "cm_msg.h"
#include "cm_misc.h"
#include "cm_misc_res.h"

bool g_enableSharedStorage = false;
CmResStatList g_resStatus[CM_MAX_RES_COUNT] = {{0}};

static uint32 g_resCount = 0;
static bool8 g_isDnSSMode = CM_FALSE;

typedef enum IpTypeEn {
    IP_TYPE_INIT = 0,
    IP_TYPE_UNKNOWN = 1,
    IP_TYPE_IPV4,
    IP_TYPE_IPV6,
    IP_TYPE_NEITHER,
    IP_TYPE_CEIL,
} IpType;

typedef struct ResConfRangeSt {
    const char *param;
    int min;
    int max;
} ResConfRange;

typedef struct ResConfDefaultSt {
    const char *param;
    const char *defValue;
} ResConfDefault;

static ResConfRange g_resConfRange[] = {
    {"check_interval", 1, 2147483647},
    {"time_out", 1, 2147483647},
    {"abnormal_timeout", 0, 2147483647},
    {"restart_delay", 0, 1800},
    {"restart_period", 0, 3600},
    {"restart_times", -1, 9999},
    {"res_instance_id", 0, 2147483647}
};

static ResConfDefault g_resConfDef[] = {
    {"resources_type", "APP"},
    {"check_interval", "1"},
    {"time_out", "10"},
    {"abnormal_timeout", "30"},
    {"restart_delay", "1"},
    {"restart_period", "1"},
    {"restart_times", "-1"},
    {"res_instance_id", "0"},
    {"one instance res_instance_id", "0"},
    {"is_critical", "true"},
    {"location_type", "local"},
    {"location_attr", ""}
};

typedef status_t (*InitCusRes)(const OneCusResConfJson *oneResJson, OneResStatList *oneResStat);

static status_t InitAllAppResInstStatMain(const OneCusResConfJson *oneResJson, OneResStatList *oneResStat);
static status_t InitAllDnResInstStatMain(const OneCusResConfJson *oneResJson, OneResStatList *oneResStat);

typedef struct InitCusResMapSt {
    CusResType type;
    InitCusRes initFuc;
} InitCusResMap;

static InitCusResMap g_customResourceType[] = {
    {CUSTOM_RESOURCE_APP, InitAllAppResInstStatMain},
    {CUSTOM_RESOURCE_DN, InitAllDnResInstStatMain}
};

typedef struct ResIsregStatusMapSt {
    ResIsregStatus isreg;
    const char isregStr[CM_MAX_RES_NAME];
} ResIsregStatusMap;

const ResIsregStatusMap g_resIsregStatusMap[] = {
    {CM_RES_ISREG_INIT, "init"},
    {CM_RES_ISREG_REG, "reg"},
    {CM_RES_ISREG_UNREG, "unreg"},
    {CM_RES_ISREG_PENDING, "pending"},
    {CM_RES_ISREG_UNKNOWN, "unknown"},
    {CM_RES_ISREG_NOT_SUPPORT, "not_support"},
};

const char *GetIsregStatus(int isreg)
{
    uint32 size = (uint32)(sizeof(g_resIsregStatusMap) / sizeof(g_resIsregStatusMap[0]));
    for (uint32 i = 0; i < size; ++i) {
        if ((int)g_resIsregStatusMap[i].isreg == isreg) {
            return g_resIsregStatusMap[i].isregStr;
        }
    }
    return "";
}

bool IsResConfValid(const char *param, int value)
{
    uint32 arrLen = (sizeof(g_resConfRange) / sizeof(g_resConfRange[0]));
    for (uint32 i = 0; i < arrLen; ++i) {
        if (strcmp(param, g_resConfRange[i].param) != 0) {
            continue;
        }
        return (value >= g_resConfRange[i].min && value <= g_resConfRange[i].max);
    }
    return false;
}

int ResConfMaxValue(const char *param)
{
    uint32 arrLen = (sizeof(g_resConfRange) / sizeof(g_resConfRange[0]));
    for (uint32 i = 0; i < arrLen; ++i) {
        if (strcmp(param, g_resConfRange[i].param) == 0) {
            return g_resConfRange[i].max;
        }
    }
    return INT32_MAX;
}

int ResConfMinValue(const char *param)
{
    uint32 arrLen = (sizeof(g_resConfRange) / sizeof(g_resConfRange[0]));
    for (uint32 i = 0; i < arrLen; ++i) {
        if (strcmp(param, g_resConfRange[i].param) == 0) {
            return g_resConfRange[i].min;
        }
    }
    return INT32_MIN;
}

const char* ResConfDefValue(const char *param)
{
    uint32 arrLen = (sizeof(g_resConfDef) / sizeof(g_resConfDef[0]));
    for (uint32 i = 0; i < arrLen; ++i) {
        if (strcmp(param, g_resConfDef[i].param) == 0) {
            return g_resConfDef[i].defValue;
        }
    }
    return "";
}

static inline bool IsCustomResource(CusResType resType)
{
    uint32 arrLen = (sizeof(g_customResourceType) / sizeof(g_customResourceType[0]));
    for (uint32 i = 0; i < arrLen; ++i) {
        if (resType == g_customResourceType[i].type) {
            return true;
        }
    }
    return false;
}

static inline void PaddingResStat(const CmResStatList *oneRes)
{
    errno_t rc = memcpy_s(&g_resStatus[g_resCount], sizeof(CmResStatList), oneRes, sizeof(CmResStatList));
    securec_check_errno(rc, (void)rc);
    ++g_resCount;
}

static inline void CleanResStat()
{
    errno_t rc = memset_s(g_resStatus, sizeof(CmResStatList) * CM_MAX_RES_COUNT,
        0, sizeof(CmResStatList) * CM_MAX_RES_COUNT);
    securec_check_errno(rc, (void)rc);
    g_resCount = 0;
}

static status_t InitResName(const char *jsonResName, char *initResName, size_t maxResNameLen)
{
    if (strlen(jsonResName) >= maxResNameLen) {
        write_runlog(ERROR, "[InitResStat] res(%s) is longer than %lu.\n", jsonResName, (maxResNameLen - 1));
        return CM_ERROR;
    }
    errno_t rc = strcpy_s(initResName, maxResNameLen, jsonResName);
    securec_check_errno(rc, (void)rc);
    return CM_SUCCESS;
}

static inline uint32 GetResCmInstId()
{
    static uint32 curCmInstId = RES_INSTANCE_ID_MIN;
    return ++curCmInstId;
}

static status_t InitOneAppResInstStat(const char *resName, const CusResInstConf *resInst, CmResStatInfo *instStat)
{
    if (IsNodeIdValid(resInst->nodeId)) {
        instStat->nodeId = (uint32)resInst->nodeId;
    } else {
        write_runlog(ERROR, "[InitResStat] res(%s), nodeId(%d) is invalid.\n", resName, resInst->nodeId);
        return CM_ERROR;
    }

    instStat->cmInstanceId = GetResCmInstId();
    if (!IsResInstIdValid((int)instStat->cmInstanceId)) {
        write_runlog(ERROR, "[InitResStat] res(%s), cmInstId(%u) is invalid.\n", resName, instStat->cmInstanceId);
        return CM_ERROR;
    }

    if (resInst->resInstId >= 0) {
        instStat->resInstanceId = (uint32)resInst->resInstId;
    } else {
        write_runlog(ERROR, "[InitResStat] res(%s), resInstId(%d) is invalid.\n", resName, resInst->resInstId);
        return CM_ERROR;
    }

    instStat->isWorkMember = RES_INST_WORK_STATUS_AVAIL;
    instStat->status = (uint32)CM_RES_STAT_UNKNOWN;

    return CM_SUCCESS;
}

static status_t InitAllAppResInstStat(const AppCusResConfJson *appRes, OneResStatList *oneResStat)
{
    CM_RETURN_IFERR(InitResName(appRes->resName, oneResStat->resName, CM_MAX_RES_NAME));

    if (appRes->instance.count > CM_MAX_RES_INST_COUNT) {
        write_runlog(ERROR, "[InitResStat] res(%s) instance count(%u) invalid, range[0, %d].\n",
            appRes->resName, appRes->instance.count, CM_MAX_RES_INST_COUNT);
        return CM_ERROR;
    }

    oneResStat->instanceCount = appRes->instance.count;
    for (uint32 i = 0; i < oneResStat->instanceCount; ++i) {
        CM_RETURN_IFERR(InitOneAppResInstStat(appRes->resName, &appRes->instance.conf[i], &oneResStat->resStat[i]));
    }

    return CM_SUCCESS;
}

static status_t InitAllAppResInstStatMain(const OneCusResConfJson *oneResJson, OneResStatList *oneResStat)
{
    CM_RETURN_IFERR(InitAllAppResInstStat(&oneResJson->appResConf, oneResStat));
    return CM_SUCCESS;
}

static inline void SetSharedStorageMode(const DnCusResConfJson *dnResJson)
{
    if (!CM_IS_EMPTY_STR(dnResJson->resScript)) {
        g_enableSharedStorage = true;
    }
}

static inline void InitOneDnResInstStatByStaticConfig(const dataNodeInfo *dnInst, CmResStatInfo *instStat)
{
    instStat->cmInstanceId = dnInst->datanodeId;
    instStat->resInstanceId = dnInst->datanodeId;
    instStat->isWorkMember = RES_INST_WORK_STATUS_AVAIL;
    instStat->status = (uint32)CM_RES_STAT_UNKNOWN;
}

static status_t InitAllDnResInstStat(const DnCusResConfJson *dnResJson, OneResStatList *oneStat)
{
    SetSharedStorageMode(dnResJson);
    CM_RETURN_IFERR(InitResName(dnResJson->resName, oneStat->resName, CM_MAX_RES_NAME));

    if (g_dn_replication_num > CM_MAX_RES_INST_COUNT) {
        write_runlog(ERROR, "[InitResStat] res(%s) instance count(%u) is more than max(%d).\n",
            dnResJson->resName, g_dn_replication_num, CM_MAX_RES_INST_COUNT);
        return CM_ERROR;
    }

    oneStat->instanceCount = 0;
    for (uint32 i = 0; i < g_node_num; ++i) {
        for (uint32 k = 0; k < g_node[i].datanodeCount; ++k) {
            InitOneDnResInstStatByStaticConfig(&g_node[i].datanode[k], &oneStat->resStat[oneStat->instanceCount]);
            oneStat->resStat[oneStat->instanceCount].nodeId = g_node[i].node;
            ++oneStat->instanceCount;
        }
    }

    return CM_SUCCESS;
}

static status_t InitAllDnResInstStatMain(const OneCusResConfJson *oneResJson, OneResStatList *oneResStat)
{
    CM_RETURN_IFERR(InitAllDnResInstStat(&oneResJson->dnResConf, oneResStat));
    return CM_SUCCESS;
}

static inline void InitOneResStatZero(OneResStatList *oneResStat)
{
    errno_t rc = memset_s(oneResStat, sizeof(OneResStatList), 0, sizeof(OneResStatList));
    securec_check_errno(rc, (void)rc);
    oneResStat->version = 0;
}

static status_t InitOneResStat(const OneCusResConfJson *oneResJson, OneResStatList *oneResStat)
{
    InitOneResStatZero(oneResStat);

    uint32 arrLen = (sizeof(g_customResourceType) / sizeof(g_customResourceType[0]));
    for (uint32 i = 0; i < arrLen; ++i) {
        if (oneResJson->resType == g_customResourceType[i].type) {
            CM_RETURN_IFERR(g_customResourceType[i].initFuc(oneResJson, oneResStat));
        }
    }

    return CM_SUCCESS;
}

status_t InitAllResStat(int logLevel)
{
    if (IsConfJsonEmpty()) {
        write_runlog(logLevel, "[InitResStat] no custom resource config.\n");
        return CM_SUCCESS;
    }

    for (uint32 i = 0; i < g_confJson->resource.count; ++i) {
        if (!IsCustomResource(g_confJson->resource.conf[i].resType)) {
            continue;
        }
        if (g_confJson->resource.conf[i].resType == CUSTOM_RESOURCE_DN) {
            g_isDnSSMode = CM_TRUE;
        }
        if (g_resCount >= CM_MAX_RES_COUNT) {
            write_runlog(ERROR, "[InitResStat] custom resource count overflow, max:%d.\n", CM_MAX_RES_COUNT);
            CleanResStat();
            return CM_ERROR;
        }
        CmResStatList newRes;
        (void)pthread_rwlock_init(&newRes.rwlock, NULL);
        CM_RETURN_IFERR_EX(InitOneResStat(&g_confJson->resource.conf[i], &newRes.status), CleanResStat());
        PaddingResStat(&newRes);
    }

    return CM_SUCCESS;
}

int ReadCmConfJson(void *logFunc)
{
    char jsonFile[MAX_PATH_LEN] = {0};
    GetCmConfJsonPath(jsonFile, MAX_PATH_LEN);

    SetReadJsonConfWriteLog((CmJsonLogOutput)logFunc);

    return ReadConfJsonFile(jsonFile);
}

void GetCmConfJsonPath(char *path, uint32 pathLen)
{
    int ret = snprintf_s(path, pathLen, pathLen - 1, "%s/cm_agent/cm_resource.json", g_currentNode->cmDataPath);
    securec_check_intval(ret, (void)ret);
}

status_t GetGlobalResStatusIndex(const char *resName, uint32 &index)
{
    for (uint32 i = 0; i < CusResCount(); ++i) {
        (void)pthread_rwlock_rdlock(&(g_resStatus[i].rwlock));
        if (strcmp(g_resStatus[i].status.resName, resName) == 0) {
            (void)pthread_rwlock_unlock(&(g_resStatus[i].rwlock));
            index = i;
            return CM_SUCCESS;
        }
        (void)pthread_rwlock_unlock(&(g_resStatus[i].rwlock));
    }
    return CM_ERROR;
}

bool IsResInstIdValid(int instId)
{
    const int minDnInstId = 6000;
    const int maxDnInstId = 7000;
    if (instId > minDnInstId && instId < maxDnInstId) {
        return true;
    }
    if (instId > RES_INSTANCE_ID_MIN && instId < RES_INSTANCE_ID_MAX) {
        return true;
    }
    return false;
}

bool IsOneResInstWork(const char *resName, uint32 cmInstId)
{
    uint32 index = 0;
    if (GetGlobalResStatusIndex(resName, index) != CM_SUCCESS) {
        write_runlog(ERROR, "%s, unknown resName(%s).\n", __FUNCTION__, resName);
        return false;
    }

    bool isWork = false;
    CmResStatList *resStat = &g_resStatus[index];
    (void)pthread_rwlock_rdlock(&resStat->rwlock);
    for (uint32 i = 0; i < resStat->status.instanceCount; ++i) {
        if (resStat->status.resStat[i].cmInstanceId == cmInstId) {
            isWork = (resStat->status.resStat[i].isWorkMember == RES_INST_WORK_STATUS_AVAIL);
            break;
        }
    }
    (void)pthread_rwlock_unlock(&resStat->rwlock);

    return isWork;
}

bool IsReadConfJsonSuccess(int ret)
{
    return !CM_IS_READ_JSON_FAIL(ret);
}

const char *ReadConfJsonFailStr(int ret)
{
    switch (ret) {
        case CM_JSON_NOT_EXIST:
            return "conf json not exist";
        case CM_JSON_OPEN_ERROR:
            return "open conf json file failed";
        case CM_JSON_GET_LEN_ERROR:
            return "get conf json context's size failed";
        case CM_JSON_OUT_OF_MEMORY:
            return "malloc failed, out of memory";
        case CM_JSON_READ_ERROR:
            return "get conf json context failed";
        default:
            break;
    }

    return "unknown fail reason";
}

status_t GetResNameByCmInstId(uint32 instId, char *resName, uint32 nameLen)
{
    for (uint32 i = 0; i < CusResCount(); ++i) {
        (void)pthread_rwlock_rdlock(&g_resStatus[i].rwlock);
        for (uint32 j = 0; j < g_resStatus[i].status.instanceCount; ++j) {
            if (g_resStatus[i].status.resStat[j].cmInstanceId == instId) {
                errno_t rc = strcpy_s(resName, nameLen, g_resStatus[i].status.resName);
                (void)pthread_rwlock_unlock(&g_resStatus[i].rwlock);
                securec_check_errno(rc, (void)rc);
                return CM_SUCCESS;
            }
        }
        (void)pthread_rwlock_unlock(&g_resStatus[i].rwlock);
    }

    write_runlog(LOG, "unknown cm_inst_id %u.\n", instId);
    return CM_ERROR;
}

static IpType GetIpType(const char *ip)
{
    if (CM_IS_EMPTY_STR(ip)) {
        return IP_TYPE_NEITHER;
    }
    int32 ip4Cnt = 0;
    int32 ip6Cnt = 0;
    const int32 ip4CntTotal = 3;
    const int32 ip6CntMax = 7;
    const int32 ip6CntMin = 2;
    uint32 ipLen = (uint32)strlen(ip);
    for (uint32 i = 0; i < ipLen; ++i) {
        if (ip[i] == '.') {
            ++ip4Cnt;
        } else if (ip[i] == ':') {
            ++ip6Cnt;
        }
    }
    if (ip6Cnt == 0 && ip4Cnt == ip4CntTotal) {
        return IP_TYPE_IPV4;
    }
    if (ip4Cnt == 0 && (ip6Cnt >= ip6CntMin && ip6Cnt <= ip6CntMax)) {
        return IP_TYPE_IPV6;
    }
    write_runlog(ERROR, "ip(%s) is invalid, and ip4Cnt=%d, ip6Cnd=%d.\n", ip, ip4Cnt, ip6Cnt);
    return IP_TYPE_NEITHER;
}

static uint8 CheckIpV4PartlyValid(const char *ipPart)
{
    if (CM_is_str_all_digit(ipPart) != 0) {
        write_runlog(ERROR, "ip(%s) is not digital.\n", ipPart);
        return CM_FALSE;
    }
    const uint32 maxLen = 3;
    uint32 ipLen = (uint32)strlen(ipPart);
    if (ipLen > maxLen) {
        return CM_FALSE;
    }
    if (ipPart[0] == '0' && ipLen > 1) {
        write_runlog(ERROR, "ip(%s) first is 0.\n", ipPart);
        return CM_FALSE;
    }
    const int32 maxValue = 255;
    int32 value = (int32)CmAtoi(ipPart, 0);
    if (value < 0 || value > maxValue) {
        write_runlog(ERROR, "ip(%s) value(%d) is not in [%d: %d].\n", ipPart, value, 0, maxValue);
        return CM_FALSE;
    }
    return CM_TRUE;
}

static uint8 CheckIpV4Valid(char *ip, uint32 ipLen)
{
    if (CM_IS_EMPTY_STR(ip)) {
        return CM_FALSE;
    }
    char baseIp[CM_IP_LENGTH] = {0};
    errno_t rc = strncpy_s(baseIp, CM_IP_LENGTH, ip, ipLen);
    securec_check_errno(rc, (void)rc);

    const char *ipPoint = ".";
    char *savePtr = NULL;
    // first
    char *subStr = strtok_r(ip, ipPoint, &savePtr);
    CM_RETFALSE_IFNOT(CheckIpV4PartlyValid(subStr));

    int cnt = 1;
    while (!CM_IS_EMPTY_STR(savePtr)) {
        subStr = strtok_r(NULL, ipPoint, &savePtr);
        CM_RETFALSE_IFNOT(CheckIpV4PartlyValid(subStr));
        ++cnt;
    }
    const int maxIpv4Part = 4;
    if (cnt != maxIpv4Part) {
        write_runlog(ERROR, "ip(%s) is invalid, cnt=%d.\n", baseIp, cnt);
        return CM_FALSE;
    }

    return CM_TRUE;
}

uint8 CheckIpValid(const char *ip)
{
    char tempIp[CM_IP_LENGTH] = {0};
    errno_t rc = strcpy_s(tempIp, CM_IP_LENGTH, ip);
    securec_check_errno(rc, (void)rc);

    if (GetIpType(tempIp) != IP_TYPE_IPV4) {
        write_runlog(ERROR, "ip(%s) is invalid, not ipV4.\n", ip);
        return CM_FALSE;
    }

    if (CheckIpV4Valid(tempIp, CM_IP_LENGTH) == CM_FALSE) {
        write_runlog(ERROR, "ipV4(%s) is invalid.\n", ip);
        return CM_FALSE;
    }

    return CM_TRUE;
}

uint32 CusResCount()
{
    return g_resCount;
}

bool IsCusResExist()
{
    return (g_resCount > 0);
}

void PrintCusInfoResList(const OneResStatList *status, const char *info)
{
    write_runlog(LOG, "[CUS_RES] [%s] res(%s), version=%llu, status:\n", info, status->resName, status->version);
    for (uint32 i = 0; i < status->instanceCount; ++i) {
        write_runlog(LOG, "[CUS_RES] nodeId=%u, cmInstId=%u, resInstId=%u, status=%u, isWork=%u;\n",
            status->resStat[i].nodeId,
            status->resStat[i].cmInstanceId,
            status->resStat[i].resInstanceId,
            status->resStat[i].status,
            status->resStat[i].isWorkMember);
    }
}

bool8 IsDatanodeSSMode()
{
    return g_isDnSSMode;
}
