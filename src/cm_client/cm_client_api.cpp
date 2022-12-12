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
 * cm_client_api.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_client/cm_client_api.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdlib.h>
#include "cm/cm_elog.h"
#include "cm/cm_msg.h"
#include "cm_client.h"
#include "cm_client_api.h"

static char *g_jsonStrPtr = NULL;

static bool IsStrOverLength(const char *str, uint32 maxLen)
{
    uint32 len = 0;
    while (len < maxLen) {
        if (str[len++] == '\0') {
            return true;
        }
    }
    return false;
}

static bool CanDoCmInit(const char *resName)
{
    if (GetIsClientInit()) {
        write_runlog(LOG, "cm_client has init, can't do init again.\n");
        return false;
    }
    if (resName == NULL) {
        (void)printf(_("resName length is NULL.\n"));
        return false;
    }
    if (!IsStrOverLength(resName, CM_MAX_RES_NAME)) {
        (void)printf(_("resName length >= %d.\n"), CM_MAX_RES_NAME);
        return false;
    }
    return true;
}

#ifdef __cplusplus
extern "C" {
#endif

int CmInit(unsigned int instId, const char *resName, CmNotifyFunc func)
{
    static bool isFirstInit = true;
    if (!CanDoCmInit(resName)) {
        return -1;
    }
    if (PreInit(instId, resName, func, &isFirstInit) != 0) {
        (void)printf(_("resName(%s) instanceId(%u) init cm_client failed.\n"), resName, instId);
        return -1;
    }
    bool &isClientInit = GetIsClientInit();
    isClientInit = false;
    if (CreateConnectAgentThread() != 0 || CreateSendMsgThread() != 0 || CreateRecvMsgThread() != 0) {
        write_runlog(LOG, "cm_client create thread failed.\n");
        ShutdownClient();
        return -1;
    }
    bool isSuccess = SendInitMsgAndGetResult(resName, instId);
    if (!isSuccess) {
        write_runlog(ERROR, "resName(%s) instanceId(%u) init client failed, can check agent.\n", resName, instId);
        ShutdownClient();
        return -1;
    }
    write_runlog(LOG, "resName(%s) instanceId(%u) init cm_client success.\n", resName, instId);
    isClientInit = true;
    return 0;
}

static void GetResStatJsonHead(char *jsonStr, uint32 strLen, const OneResStatList *statList)
{
    int ret = snprintf_s(jsonStr,
         strLen,
         strLen - 1,
         "{\"version\":%llu,\"res_name\":\"%s\",\"inst_count\":%u,\"inst_status\":[",
         statList->version,
         statList->resName,
         statList->instanceCount);
    securec_check_intval(ret, (void)ret);
}

static void GetResStatJsonInst(char *instInfo, uint32 strLen, const CmResStatInfo *instStat, bool isEnd)
{
    int ret = snprintf_s(instInfo,
         strLen,
         strLen - 1,
         "{\"node_id\":%u,\"res_instance_id\":%u,\"is_work_member\":%u,\"status\":%u}",
         instStat->nodeId,
         instStat->resInstanceId,
         instStat->isWorkMember,
         instStat->status);
    securec_check_intval(ret, (void)ret);
    if (!isEnd) {
        errno_t rc = strcat_s(instInfo, MAX_PATH_LEN, ",");
        securec_check_errno(rc, (void)rc);
    }
}

static void ResStatusToJsonStr(const OneResStatList *statList)
{
    const int maxJsonStrLen = 102400;
    char jsonStr[maxJsonStrLen] = {0};

    GetResStatJsonHead(jsonStr, maxJsonStrLen, statList);

    errno_t rc;
    for (uint32 i = 0; i < statList->instanceCount; ++i) {
        char instInfo[MAX_PATH_LEN] = {0};
        GetResStatJsonInst(instInfo, MAX_PATH_LEN, &statList->resStat[i], (i == (statList->instanceCount - 1)));
        rc = strcat_s(jsonStr, maxJsonStrLen, instInfo);
        securec_check_errno(rc, (void)rc);
    }
    rc = strcat_s(jsonStr, maxJsonStrLen, "]}");
    securec_check_errno(rc, (void)rc);

    g_jsonStrPtr = strdup(jsonStr);
}

char *CmGetResStats()
{
    OneResStatList &statusList = GetClientStatusList();
    if (statusList.version == 0) {
        write_runlog(LOG, "version is 0, statList is invalid.\n");
        return NULL;
    }
    ResStatusToJsonStr(&statusList);
    return g_jsonStrPtr;
}

#ifdef __cplusplus
}
#endif
