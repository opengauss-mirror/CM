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
 * cms_process_messages_hadr.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_server/cms_process_messages_hadr.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cms_common.h"
#include "cms_global_params.h"
#include "cms_process_messages.h"

#ifdef ENABLE_MULTIPLE_NODES
static bool CaculateMinRecoveryBarrierCn(uint32 cnGroup, char *recoveryBarrierId, size_t len)
{
    const char *curBarrierId = g_instance_group_report_status_ptr[cnGroup].instance_status.coordinatemember.barrierID;
    if (curBarrierId[0] == 0) {
        return false;
    }

    if (recoveryBarrierId[0] == 0 || strcmp(curBarrierId, recoveryBarrierId) < 0) {
        errno_t rc = snprintf_s(recoveryBarrierId, len, len - 1, "%s", curBarrierId);
        securec_check_intval(rc, (void)rc);
    }

    return true;
}
#endif

static bool CaculateMinRecoveryBarrierDn(uint32 dnGroup, char *recoveryBarrierId, size_t len)
{
    const int half = 2;
    int groupDnNum = g_instance_role_group_ptr[dnGroup].count;
    char tmpRecoveryBarrier[BARRIERLEN] = {0};
    int hasBarrierNum = 0;
    cm_instance_datanode_report_status *dnReportStatus =
        g_instance_group_report_status_ptr[dnGroup].instance_status.data_node_member;

    for (int i = 0; i < groupDnNum; i++) {
        if (dnReportStatus[i].barrierID[0] != 0) {
            hasBarrierNum++;
            if (tmpRecoveryBarrier[0] == 0 || strcmp(dnReportStatus[i].barrierID, tmpRecoveryBarrier) < 0) {
                errno_t rc =
                    snprintf_s(tmpRecoveryBarrier, BARRIERLEN, BARRIERLEN - 1, "%s", dnReportStatus[i].barrierID);
                securec_check_intval(rc, (void)rc);
            }
        }
    }

    // majority
    if (hasBarrierNum > groupDnNum / half) {
        if (recoveryBarrierId[0] == 0 || strcmp(tmpRecoveryBarrier, recoveryBarrierId) < 0) {
            errno_t rc = snprintf_s(recoveryBarrierId, len, len - 1, "%s", tmpRecoveryBarrier);
            securec_check_intval(rc, (void)rc);
        }
    } else {
        return false;
    }

    return true;
}

static void CaculateObsMinRecoveryBarrier(char *recoveryBarrierId, size_t len)
{
    bool hasInvalidBarrier = false;
    for (uint32 groupIdx = 0; groupIdx < g_dynamic_header->relationCount; groupIdx++) {
#ifdef ENABLE_MULTIPLE_NODES
        if (g_instance_role_group_ptr[groupIdx].instanceMember[0].instanceType == INSTANCE_TYPE_COORDINATE &&
            !CaculateMinRecoveryBarrierCn(groupIdx, recoveryBarrierId, len)) {
            hasInvalidBarrier = true;
            break;
        }
#endif
        if (g_instance_role_group_ptr[groupIdx].instanceMember[0].instanceType == INSTANCE_TYPE_DATANODE &&
            !CaculateMinRecoveryBarrierDn(groupIdx, recoveryBarrierId, len)) {
            hasInvalidBarrier = true;
            break;
        }
    }
    if (hasInvalidBarrier) {
        write_runlog2(LOG,
            errmsg(
                "calc min barrier, has invalid barrier, recoveryBarrierId before clean is: [%s].", recoveryBarrierId),
            errmodule(MOD_CMS));
        errno_t rc = memset_s(recoveryBarrierId, len, 0, len);
        securec_check_errno(rc, (void)rc);
    }
    write_runlog2(LOG, errmsg("calc min barrier, now it's [%s].", recoveryBarrierId), errmodule(MOD_CMS));
    return;
}

static void CaculateStandbyClusterMinRecoveryBarrier(char *recoveryBarrierId)
{
    errno_t rc;
    /*
     * Consider the aync operation, it's better wait for some times.
     * Now set the sleep time 2 * interavl_time.
     */
    cm_sleep(2);
    /*
     * Get the global_barrierId from the etcd key `queryvalue`
     */
    rc = strncpy_s(g_global_barrier->globalBarrierInfo.globalBarriers[0].globalBarrierId,
        BARRIERLEN, g_queryBarrier, BARRIERLEN - 1);
    securec_check_errno(rc, (void)rc);
    /*
     * Get the global_recovery_barrierId from the etcd key `targetvalue`
     */
    rc = strncpy_s(g_global_barrier->globalRecoveryBarrierId, BARRIERLEN, g_targetBarrier, BARRIERLEN - 1);
    securec_check_errno(rc, (void)rc);
    rc = strncpy_s(recoveryBarrierId, BARRIERLEN, g_global_barrier->globalRecoveryBarrierId, BARRIERLEN - 1);
    securec_check_errno(rc, (void)rc);
    rc = memset_s(g_global_barrier->globalBarrierInfo.globalBarriers[0].globalAchiveBarrierId,
        BARRIERLEN, 0, BARRIERLEN);
    securec_check_errno(rc, (void)rc);
    return;
}

static void CaculateMinRecoveryBarrier(char *recoveryBarrierId, int msgType)
{
    if (msgType == MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW && g_clusterInstallType == INSTALL_TYPE_STREAMING) {
        /* shared storage install type */
        CaculateStandbyClusterMinRecoveryBarrier(recoveryBarrierId);
    } else {
        CaculateObsMinRecoveryBarrier(recoveryBarrierId, BARRIERLEN);
    }
}

void ProcessCtl2CmOneInstanceBarrierQueryMsg(CM_Connection *con, uint32 node, uint32 instanceId, int instanceType)
{
    uint32 groupIndex = 0;
    int memberIndex = 0;
    int ret;
    cm_to_ctl_instance_barrier_info cm2CtlBarrierContent = {0};
    errno_t rc;

    ret = find_node_in_dynamic_configure(node, instanceId, &groupIndex, &memberIndex);
    if (ret != 0) {
        write_runlog2(
            LOG, errmsg("can't find the instance(node =%u  instanceid =%u)\n", node, instanceId), errmodule(MOD_CMS));
        return;
    }

    cm2CtlBarrierContent.msg_type = (int)MSG_CM_CTL_GLOBAL_BARRIER_DATA;
    cm2CtlBarrierContent.node = node;
    cm2CtlBarrierContent.instanceId = instanceId;
    cm2CtlBarrierContent.instance_type = instanceType;
    if (instanceType == INSTANCE_TYPE_COORDINATE) {
        cm2CtlBarrierContent.archive_LSN =
            g_instance_group_report_status_ptr[groupIndex].instance_status.coordinatemember.archive_LSN;
        cm2CtlBarrierContent.ckpt_redo_point =
            g_instance_group_report_status_ptr[groupIndex].instance_status.coordinatemember.ckpt_redo_point;
        cm2CtlBarrierContent.barrierLSN =
            g_instance_group_report_status_ptr[groupIndex].instance_status.coordinatemember.barrierLSN;
        cm2CtlBarrierContent.flush_LSN =
            g_instance_group_report_status_ptr[groupIndex].instance_status.coordinatemember.flush_LSN;

        rc = memcpy_s(cm2CtlBarrierContent.barrierID,
            BARRIERLEN, g_instance_group_report_status_ptr[groupIndex].instance_status.coordinatemember.barrierID,
            BARRIERLEN);
        securec_check_errno(rc, (void)rc);
    }
    if (instanceType == INSTANCE_TYPE_DATANODE) {
        cm_instance_datanode_report_status *curDn =
            &g_instance_group_report_status_ptr[groupIndex].instance_status.data_node_member[memberIndex];
        cm2CtlBarrierContent.archive_LSN = curDn->archive_LSN;
        cm2CtlBarrierContent.ckpt_redo_point = curDn->ckpt_redo_point;
        cm2CtlBarrierContent.barrierLSN = curDn->barrierLSN;
        cm2CtlBarrierContent.flush_LSN = curDn->flush_LSN;

        rc = memcpy_s(cm2CtlBarrierContent.barrierID, BARRIERLEN, curDn->barrierID, BARRIERLEN);
        securec_check_errno(rc, (void)rc);
    }

    write_runlog2(LOG,
        errmsg("ProcessCtl2CmOneInstanceBarrierQueryMsg %u instance = %u, barrierID = %s, barrierLSN = %llu, "
            "archive_LSN = %llu, ckpt_redo_point = %llu, flush_LSN = %llu\n",
            cm2CtlBarrierContent.node, cm2CtlBarrierContent.instanceId, cm2CtlBarrierContent.barrierID,
            (unsigned long long)cm2CtlBarrierContent.barrierLSN, (unsigned long long)cm2CtlBarrierContent.archive_LSN,
            (unsigned long long)cm2CtlBarrierContent.ckpt_redo_point,
            (unsigned long long)cm2CtlBarrierContent.flush_LSN), errmodule(MOD_CMS));
    (void)cm_server_send_msg(con, 'S', (char *)(&cm2CtlBarrierContent), sizeof(cm_to_ctl_instance_barrier_info));
}

void ProcessCtlToCmQueryGlobalBarrierMsg(CM_Connection *con)
{
    char recoveryBarrierId[BARRIERLEN] = {0};
    cm_to_ctl_cluster_global_barrier_info globalBarrierInfo = {0};
    globalBarrierInfo.msg_type = (int)MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN;
    (void)pthread_rwlock_wrlock(&(g_global_barrier->barrier_lock));
    CaculateMinRecoveryBarrier(recoveryBarrierId, MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN);
    errno_t rc = snprintf_s(globalBarrierInfo.global_barrierId,
        BARRIERLEN,
        BARRIERLEN - 1,
        "%s",
        g_global_barrier->globalBarrierInfo.globalBarriers[0].globalBarrierId);
    securec_check_intval(rc, (void)rc);
    rc = snprintf_s(globalBarrierInfo.global_achive_barrierId,
        BARRIERLEN,
        BARRIERLEN - 1,
        "%s",
        g_global_barrier->globalBarrierInfo.globalBarriers[0].globalAchiveBarrierId);
    securec_check_intval(rc, (void)rc);
    rc = snprintf_s(globalBarrierInfo.globalRecoveryBarrierId, BARRIERLEN, BARRIERLEN - 1, "%s", recoveryBarrierId);

    securec_check_intval(rc, (void)rc);
    write_runlog2(LOG,
        errmsg("ProcessCtlToCmQueryGlobalBarrierMsg, global_barrierId = %s,global_achive_barrierId = %s, "
            "globalRecoveryBarrierId = %s",
            globalBarrierInfo.global_barrierId,
            globalBarrierInfo.global_achive_barrierId,
            recoveryBarrierId), errmodule(MOD_CMS));
    (void)pthread_rwlock_unlock(&(g_global_barrier->barrier_lock));
    (void)cm_server_send_msg(con, 'S', (char *)(&globalBarrierInfo), sizeof(cm_to_ctl_cluster_global_barrier_info));
}

void ProcessCtlToCmQueryBarrierMsg(
    CM_Connection *con, const ctl_to_cm_global_barrier_query *ctl2CmGlobalBarrierQueryPtr)
{
    if (ctl2CmGlobalBarrierQueryPtr->msg_type == MSG_CTL_CM_GLOBAL_BARRIER_QUERY_NEW) {
        ProcessCtl2CmQueryGlobalBarrierMsgNew(con);
    } else {
        ProcessCtlToCmQueryGlobalBarrierMsg(con);
    }
    uint32 i;
    cm_to_ctl_cluster_global_barrier_info globalBarrierInfo;

    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].coordinate == 1) {
            ProcessCtl2CmOneInstanceBarrierQueryMsg(
                con, g_node[i].node, g_node[i].coordinateId, INSTANCE_TYPE_COORDINATE);
        }

        for (uint32 j = 0; j < g_node[i].datanodeCount; j++) {
            ProcessCtl2CmOneInstanceBarrierQueryMsg(
                con, g_node[i].node, g_node[i].datanode[j].datanodeId, INSTANCE_TYPE_DATANODE);
        }
    }
    globalBarrierInfo.msg_type = (int)MSG_CM_CTL_BARRIER_DATA_END;
    (void)cm_server_send_msg(con, 'S', (char *)(&globalBarrierInfo), sizeof(cm_to_ctl_cluster_global_barrier_info));
}

void ProcessCtl2CmQueryGlobalBarrierMsgNew(CM_Connection *con)
{
    char recoveryBarrierId[BARRIERLEN] = {0};
    cm2CtlGlobalBarrierNew globalBarrierMsg = {0};
    globalBarrierMsg.msg_type = (int)MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW;
    (void)pthread_rwlock_wrlock(&(g_global_barrier->barrier_lock));
    CaculateMinRecoveryBarrier(recoveryBarrierId, MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW);
    errno_t rc =
        snprintf_s(globalBarrierMsg.globalRecoveryBarrierId, BARRIERLEN, BARRIERLEN - 1, "%s", recoveryBarrierId);
    securec_check_intval(rc, (void)rc);

    rc = memcpy_s(&globalBarrierMsg.globalStatus,
        sizeof(GlobalBarrierStatus),
        &g_global_barrier->globalBarrierInfo,
        sizeof(GlobalBarrierStatus));
    securec_check_errno(rc, (void)rc);

    (void)pthread_rwlock_unlock(&(g_global_barrier->barrier_lock));
    (void)cm_server_send_msg(con, 'S', (char *)(&globalBarrierMsg), sizeof(cm2CtlGlobalBarrierNew));
}

void ProcessSharedStorageMsg(CM_Connection *con)
{
    errno_t rc;
    CmsSharedStorageInfo sendMsg;

    sendMsg.msg_type = (int)MSG_GET_SHARED_STORAGE_INFO_ACK;

    if (g_doradoIp[0] == '\0') {
        rc = strcpy_s(g_doradoIp, CM_IP_LENGTH, "unknown");
        securec_check_errno(rc, (void)rc);
        (void)cm_server_send_msg(con, 'S', (char *)(&sendMsg), sizeof(sendMsg));
        return;
    }

    rc = strcpy_s(sendMsg.doradoIp, CM_IP_LENGTH, g_doradoIp);
    securec_check_errno(rc, (void)rc);
    (void)cm_server_send_msg(con, 'S', (char *)(&sendMsg), sizeof(sendMsg));

    return;
}
