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
#include <sys/time.h>
#include "cm/cm_elog.h"
#include "cm/cm_msg.h"
#include "cm_client.h"
#include "cm_client_api.h"

#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr)) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)
#endif

bool g_isClientInit = false;

int cm_init(uint32 instanceId, const char *resName, cm_notify_func_t func)
{
    if (strlen(resName) > CM_MAX_RES_NAME) {
        (void)printf(_("resName(%s) length is longer than 32.\n"), resName);
        return -1;
    }
    if (PreInit(instanceId, resName, func) != 0 ||
        CreateConnectAgentThread() != 0 || CreateSendMsgThread() != 0 || CreateRecvMsgThread() != 0) {
        (void)printf(_("resName(%s) instanceId(%u) init cm_client failed.\n"), resName, instanceId);
        return -1;
    }
    write_runlog(LOG, "resName(%s) instanceId(%u) init cm_client success.\n", resName, instanceId);
    g_isClientInit = true;

    return 0;
}

int cm_set_instance_data(long long data)
{
    if (!g_isClientInit) {
        (void)printf(_("cm_client is not alive, please init cm_client first.\n"));
        return -1;
    }

    ClientSetDataMsg *sendMsg = NULL;

    sendMsg = (ClientSetDataMsg*) malloc(sizeof(ClientSetDataMsg));
    if (sendMsg == NULL) {
        write_runlog(ERROR, "Res:%s, Out of memory: clientToAgentSetDataMsg failed.\n", GetResName());
        return -1;
    }
    errno_t rc = memset_s(sendMsg, sizeof(ClientSetDataMsg), 0, sizeof(ClientSetDataMsg));
    securec_check_errno(rc, (void)rc);

    sendMsg->head.msgVer = CM_CLIENT_MSG_VER;
    sendMsg->head.msgType = MSG_CLIENT_AGENT_SET_DATA;
    sendMsg->instanceData = (int64)data;

    SendMsgApi((char*)sendMsg);

    return 0;
}

int cm_get_res_stat_list(cms_res_stat_list_t* statList)
{
    if (!g_isClientInit) {
        (void)printf(_("cm_client is not alive, please init cm_client first.\n"));
        return -1;
    }
    if (statList == NULL) {
        write_runlog(ERROR, "Res:%s, statList ptr is NULL, can't get status list.\n", GetResName());
        return -1;
    }

    errno_t rc;
    OneResStatList &statusList = GetClientStatusList();

    rc = memcpy_s(statList, sizeof(cms_res_stat_list_t), &statusList, sizeof(cms_res_stat_list_t));
    securec_check_errno(rc, (void)rc);

    return 0;
}

inline void ClientGetTimespec(struct timespec &time)
{
    struct timeval tv = { 0, 0 };
    (void)gettimeofday(&tv, NULL);

    time.tv_sec = tv.tv_sec + CLIENT_RES_DATA_TIMEOUT;
    time.tv_nsec = tv.tv_usec * CLIENT_USEC_TO_NSEC;
}

inline int CopyDataToMsg(char *desData, uint32 desLen, const char *srcData, uint32 srcLen)
{
    if (desLen < srcLen) {
        write_runlog(ERROR, "srcData(%s) len(%u) is more than len(%u), cannot copy to msg.\n", srcData, srcLen, desLen);
        return -1;
    }
    errno_t rc = memcpy_s(desData, desLen, srcData, srcLen);
    securec_check_errno(rc, (void)rc);
    return 0;
}

int cm_set_res_data(uint32 slotId, char *data, uint32 size, unsigned long long oldVersion)
{
    if (!g_isClientInit) {
        (void)printf(_("cm_client is not alive, please init cm_client first.\n"));
        return -1;
    }
    if (data == NULL) {
        write_runlog(ERROR, "Res:%s, set data NULL, can't set res data.\n", GetResName());
        return -1;
    }

    errno_t rc;
    struct timespec releaseTime;
    SetDataFlag &setData = GetSetDataVector();
    ClientSetResDataMsg *sendMsg = NULL;

    sendMsg = (ClientSetResDataMsg*) malloc(sizeof(ClientSetResDataMsg));
    if (sendMsg == NULL) {
        write_runlog(ERROR, "Res:%s, Out of memory: clientToAgentSetResDataMsg failed.\n", GetResName());
        return -1;
    }

    rc = memset_s(sendMsg, sizeof(ClientSetResDataMsg), 0, sizeof(ClientSetResDataMsg));
    securec_check_errno(rc, FREE_AND_RESET(sendMsg));

    sendMsg->head.msgVer = CM_CLIENT_MSG_VER;
    sendMsg->head.msgType = MSG_CLIENT_AGENT_SET_RES_DATA;
    sendMsg->data.version = (uint64)oldVersion;
    sendMsg->data.slotId = (uint64)slotId;
    int32 res = CopyDataToMsg(sendMsg->data.data, CM_MAX_RES_DATA_SIZE, data, size);
    if (res != 0) {
        write_runlog(ERROR, "failed to ClientSetResDataMsg .\n");
        FREE_AND_RESET(sendMsg);
        return -1;
    }
    sendMsg->data.size = size;

    (void)pthread_mutex_lock(&setData.lock);

    SendMsgApi((char*)sendMsg);

    setData.isSetSuccess = false;
    ClientGetTimespec(releaseTime);
    (void)pthread_cond_timedwait(&setData.cond, &setData.lock, &releaseTime);
    if (setData.isSetSuccess) {
        (void)pthread_mutex_unlock(&setData.lock);
        return 0;
    }

    (void)pthread_mutex_unlock(&setData.lock);
    return -1;
}

int cm_get_res_data(uint32 slotId, char* data, uint32 maxSize, uint32* size, unsigned long long* newVersion)
{
    if (!g_isClientInit) {
        (void)printf(_("cm_client is not alive, please init cm_client first.\n"));
        return -1;
    }
    if (data == NULL) {
        write_runlog(ERROR, "Res:%s, data ptr is NULL, can't get res data.\n", GetResName());
        return -1;
    }

    errno_t rc;
    struct timespec releaseTime;
    GetDataFlag &resData = GetResDataVector();
    ClientGetResDataMsg *sendMsg = NULL;

    sendMsg = (ClientGetResDataMsg*) malloc(sizeof(ClientGetResDataMsg));
    if (sendMsg == NULL) {
        write_runlog(ERROR, "Res:%s, Out of memory: clientToAgentGetResDataMsg failed.\n", GetResName());
        return -1;
    }
    rc = memset_s(sendMsg, sizeof(ClientGetResDataMsg), 0, sizeof(ClientGetResDataMsg));
    securec_check_errno(rc, FREE_AND_RESET(sendMsg));

    sendMsg->head.msgVer = CM_CLIENT_MSG_VER;
    sendMsg->head.msgType = MSG_CLIENT_AGENT_GET_RES_DATA;
    sendMsg->slotId = (uint64)slotId;

    (void)pthread_mutex_lock(&resData.lock);

    SendMsgApi((char*)sendMsg);

    resData.resData.size = 0;
    ClientGetTimespec(releaseTime);
    (void)pthread_cond_timedwait(&resData.cond, &resData.lock, &releaseTime);
    if (resData.resData.size > maxSize || resData.resData.size == 0) {
        write_runlog(ERROR, "Res:%s, recv data size is (%u), error.\n", GetResName(), resData.resData.size);
        (void)pthread_mutex_unlock(&resData.lock);
        return -1;
    }
    rc = memcpy_s(data, maxSize, resData.resData.data, resData.resData.size);
    securec_check_errno(rc, (void)rc);
    if (size != NULL) {
        *size = resData.resData.size;
    }
    if (newVersion != NULL) {
        *newVersion = (unsigned long long)resData.resData.version;
    }

    (void)pthread_mutex_unlock(&resData.lock);

    return 0;
}
