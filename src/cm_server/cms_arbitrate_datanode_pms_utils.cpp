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
 * cms_arbitrate_datanode_pms_utils.cpp
 *     DN one primary multi standby mode arbitration in cms
 *
 * IDENTIFICATION
 *    src/cm_server/cms_arbitrate_datanode_pms_utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cms_arbitrate_datanode_pms_utils.h"
#include  "cms_az.h"

/**
 * @brief
 *
 * @return true
 * @return false
 */
bool CheckPotentialTermRollback()
{
    for (uint32 i = 0; i < g_dynamic_header->relationCount; i++) {
        for (int j = 0; j < g_instance_role_group_ptr[i].count; j++) {
            if (g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].local_status.term >
                FirstTerm) {
                write_runlog(FATAL, "We are in danger of a term-rollback. Abort this arbitration!\n");
                return true;
            }
        }
    }
    return false;
}

void GroupStatusShow(const char *str, const uint32 groupIndex, const uint32 instanceId,
    const int validCount, const bool finishRedo)
{
    cm_instance_role_group *roleGroup = &g_instance_role_group_ptr[groupIndex];
    cm_instance_role_status *roleMember = roleGroup->instanceMember;
    cm_instance_report_status *reportGrp = &(g_instance_group_report_status_ptr[groupIndex].instance_status);
    cm_instance_datanode_report_status *dnReport = reportGrp->data_node_member;
    cm_instance_arbitrate_status *dnArbi = reportGrp->arbitrate_status_member;
    DnInstInfo instInfo = {{0}};
    GetSyncListStr(reportGrp, &instInfo);
    for (int i = 0; i < roleGroup->count; ++i) {
        write_runlog(LOG,
            "%s: line %d: current report instance is %u, node %u"
            ", instanceId %u, local_static_role %d=%s, local_dynamic_role %d=%s, local_term=%u"
            ", local_last_xlog_location=%X/%X, local_db_state %d=%s, local_sync_state=%d, build_reason %d=%s"
            ", double_restarting=%d, disconn_mode %u=%s, disconn_host=%s, disconn_port=%u, local_host=%s, local_port=%u"
            ", redo_finished=%d, peer_state=%d, sync_mode=%d, current_cluster_az_status=%d, validCount=%d"
            ", finishRedo=%d, group_term=%u, curSyncList is [%s], expectSyncList is [%s], voteAzList is [%s], "
            "arbitrate_time is %u.\n",
            str, __LINE__, instanceId, roleMember[i].node, roleMember[i].instanceId, roleMember[i].role,
            datanode_role_int_to_string(roleMember[i].role), dnReport[i].local_status.local_role,
            datanode_role_int_to_string(dnReport[i].local_status.local_role), dnReport[i].local_status.term,
            (uint32)(dnReport[i].local_status.last_flush_lsn >> 32),
            (uint32)dnReport[i].local_status.last_flush_lsn, dnReport[i].local_status.db_state,
            datanode_dbstate_int_to_string(dnReport[i].local_status.db_state),
            dnReport[i].sender_status[0].sync_state, dnReport[i].local_status.buildReason,
            datanode_rebuild_reason_int_to_string(dnReport[i].local_status.buildReason),
            dnArbi[i].restarting, dnReport[i].local_status.disconn_mode,
            DatanodeLockmodeIntToString(dnReport[i].local_status.disconn_mode),
            dnReport[i].local_status.disconn_host, dnReport[i].local_status.disconn_port,
            dnReport[i].local_status.local_host, dnReport[i].local_status.local_port,
            dnReport[i].local_status.redo_finished, dnReport[i].receive_status.peer_state,
            dnReport[i].sync_standby_mode, current_cluster_az_status,
            validCount, finishRedo, reportGrp->term,
            instInfo.curSl, instInfo.expSl, instInfo.voteL, dnReport[i].arbiTime);
    }
}

bool IsInstanceInCurrentAz(uint32 groupIndex, uint32 memberIndex, int curAzIndex, int az1Index, int az2Index)
{
    uint32 tmpPriority = g_instance_role_group_ptr[groupIndex].instanceMember[memberIndex].azPriority;
    int tmpAzIndex = 0;
    if (tmpPriority >= g_az_master && tmpPriority < g_az_slave) {
        tmpAzIndex = az1Index;
    } else if (tmpPriority >= g_az_slave && tmpPriority < g_az_arbiter) {
        tmpAzIndex = az2Index;
    }
    if (curAzIndex > 0 && curAzIndex != tmpAzIndex) {
        return false;
    }
    return true;
}

bool IsSyncListEmpty(uint32 groupIndex, uint32 instanceId, maintenance_mode mode)
{
    int onePrimaryTowStandby = 3;
    bool isVoteAz = (GetVoteAzIndex() != AZ_ALL_INDEX);
    if (GetAzDeploymentType(isVoteAz) != TWO_AZ_DEPLOYMENT || mode == MAINTENANCE_NODE_UPGRADED_GRAYSCALE ||
        cm_arbitration_mode == MINORITY_ARBITRATION ||
        g_instance_role_group_ptr[groupIndex].count <= onePrimaryTowStandby ||
        (g_isEnableUpdateSyncList != SYNCLIST_THREADS_IN_PROCESS && 
        g_isEnableUpdateSyncList != SYNCLIST_THREADS_IN_DDB_BAD)) {
        return false;
    }

    bool result = false;
    if (g_instance_group_report_status_ptr[groupIndex].instance_status.currentSyncList.count == 0) {
        result = true;
    }

    if (g_instance_group_report_status_ptr[groupIndex].instance_status.exceptSyncList.count == 0) {
        result = true;
    }

    if (result) {
        write_runlog(LOG, "instance(%u): currentSyncList or exceptSyncList in the group is empty, can not arbitrate.\n",
            instanceId);
    }
    return result;
}

bool IsTermLsnValid(uint32 term, XLogRecPtr lsn)
{
    if (TermIsInvalid(term) || XLogRecPtrIsInvalid(lsn)) {
        return false;
    }
    return true;
}

void ClearDnArbiCond(uint32 groupIndex, ClearAribType type)
{
    cm_instance_datanode_report_status *dnReportStatus =
        g_instance_group_report_status_ptr[groupIndex].instance_status.data_node_member;
    int count = g_instance_role_group_ptr[groupIndex].count;
    for (int i = 0; i < count; ++i) {
        if (type == CLEAR_ALL || type == CLEAR_ARBI_TIME) {
            dnReportStatus[i].arbiTime = 0;
        }
        if (type == CLEAR_ALL || type == CLEAR_SEND_FAILOVER_TIMES) {
            dnReportStatus[i].sendFailoverTimes = 0;
        }
    }
}

bool IsInSyncList(uint32 groupIndex, int memberIndex, int reportMemberIndex)
{
    if (memberIndex == reportMemberIndex) {
        return true;
    }
    bool isInSync = false;
    uint32 instanceId = g_instance_role_group_ptr[groupIndex].instanceMember[memberIndex].instanceId;
    isInSync = IsInstanceIdInSyncList(
        instanceId, &(g_instance_group_report_status_ptr[groupIndex].instance_status.currentSyncList));
    if (!isInSync) {
        return false;
    }

    isInSync = IsInstanceIdInSyncList(
        instanceId, &(g_instance_group_report_status_ptr[groupIndex].instance_status.exceptSyncList));
    return isInSync;
}

void CheckDnBuildStatus(uint32 groupIdx, int32 memIdx, DnBuildStatus *buildStatus)
{
    int32 count = GetInstanceCountsInGroup(groupIdx);
    cm_instance_command_status *status = g_instance_group_report_status_ptr[groupIdx].instance_status.command_member;
    cm_instance_datanode_report_status *dnReport =
        g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member;
    cm_instance_role_status *role = g_instance_role_group_ptr[groupIdx].instanceMember;
    for (int32 i = 0; i < count; ++i) {
        if (memIdx != -1 && (!IsInSyncList(groupIdx, i, -1) || role[i].role == INSTANCE_ROLE_CASCADE_STANDBY)) {
            if (i == memIdx) {
                buildStatus->inSyncList = -1;
            }
            continue;
        }
        if (status[i].pengding_command == MSG_CM_AGENT_BUILD &&
            dnReport[i].local_status.local_role == INSTANCE_ROLE_STANDBY) {
            write_runlog(LOG, "instd(%u) CheckDnBuildStatus: instance(%u) is building.\n",
                GetInstanceIdInGroup(groupIdx, memIdx), GetInstanceIdInGroup(groupIdx, i));
            buildStatus->buildCount++;
        }
        if (dnReport[i].local_status.local_role == INSTANCE_ROLE_STANDBY) {
            buildStatus->standbyCount++;
        }
    }
}

int32 GetStaticPrimaryCount(uint32 groupIndex)
{
    int32 count = GetInstanceCountsInGroup(groupIndex);
    int32 staticPrimaryCount = 0;
    cm_instance_role_status *roleGroup = g_instance_role_group_ptr[groupIndex].instanceMember;
    for (int32 i = 0; i < count; ++i) {
        if (roleGroup[i].role == INSTANCE_ROLE_PRIMARY) {
            staticPrimaryCount++;
        }
    }
    return staticPrimaryCount;
}

cm_instance_command_status *GetCommand(uint32 groupIndex, int32 memberIndex)
{
    return &(g_instance_group_report_status_ptr[groupIndex].instance_status.command_member[memberIndex]);
}

cm_instance_report_status *GetReportStatus(uint32 groupIndex)
{
    return &(g_instance_group_report_status_ptr[groupIndex].instance_status);
}

cm_instance_datanode_report_status *GetLocalReportStatus(uint32 groupIndex, int32 memberIndex)
{
    return &(g_instance_group_report_status_ptr[groupIndex].instance_status.data_node_member[memberIndex]);
}

cm_instance_datanode_report_status *GetDnReportStatus(uint32 groupIndex)
{
    return (g_instance_group_report_status_ptr[groupIndex].instance_status.data_node_member);
}

cm_instance_role_status *GetRoleStatus(uint32 groupIndex, int32 memberIndex)
{
    return &(g_instance_role_group_ptr[groupIndex].instanceMember[memberIndex]);
}

uint32 GetMaxTerm(uint32 groupIdx)
{
    int32 count = GetInstanceCountsInGroup(groupIdx);
    uint32 maxTerm = 0;
    cm_instance_datanode_report_status *dnReport = GetDnReportStatus(groupIdx);
    for (int32 i = 0; i < count; ++i) {
        if (dnReport[i].local_status.term > maxTerm) {
            maxTerm = dnReport[i].local_status.term;
        }
    }
    return maxTerm;
}

uint32 GetInstanceTerm(uint32 groupIndex, int memberIndex)
{
    return g_instance_group_report_status_ptr[groupIndex]
        .instance_status.data_node_member[memberIndex]
        .local_status.term;
}

bool IsFinishReduceSyncList(uint32 groupIdx, int32 memIdx, const char *str)
{
    bool res = CompareCurWithExceptSyncList(groupIdx);
    if (res) {
        return true;
    }
    PrintSyncListMsg(groupIdx, memIdx, str);
    return false;
}

void GetCandiInfoBackup(DnArbCtx *ctx, int32 memIdx)
{
    cm_local_replconninfo *localRepl = &(ctx->dnReport[memIdx].local_status);
    ctx->cond.vaildCount++;
    if (ctx->cond.maxMemArbiTime < ctx->dnReport[memIdx].arbiTime) {
        ctx->cond.maxMemArbiTime = ctx->dnReport[memIdx].arbiTime;
    }
    if (localRepl->local_role != INSTANCE_ROLE_UNKNOWN) {
        ctx->cond.onlineCount++;
    }

    if (localRepl->local_role == INSTANCE_ROLE_PRIMARY) {
        ctx->cond.hasDynamicPrimary = true;
        ctx->cond.dyPrimIdx = memIdx;
        if (localRepl->db_state == INSTANCE_HA_STATE_NORMAL) {
            ctx->cond.dyPrimNormalIdx = memIdx;
        }
        if (ctx->roleGroup->instanceMember[memIdx].role == INSTANCE_ROLE_PRIMARY) {
            ctx->cond.isPrimaryValid = true;
            ctx->cond.vaildPrimIdx = memIdx;
            if (localRepl->db_state == INSTANCE_HA_STATE_DEMOTING) {
                ctx->cond.isPrimDemoting = true;
            }
        } else {
            ctx->cond.igPrimaryCount++;
            ctx->cond.igPrimaryIdx = memIdx;
        }
        (void)clock_gettime(CLOCK_MONOTONIC, &ctx->repGroup->finishredo_time);
    }
    if (ctx->roleGroup->instanceMember[memIdx].role == INSTANCE_ROLE_PRIMARY) {
        ctx->cond.staticPriIdx = memIdx;
    }
    if (!ctx->cond.hasDynamicPrimary && ctx->dnReport[memIdx].sendFailoverTimes >= MAX_SEND_FAILOVER_TIMES) {
        return;
    }
    if (XLByteWE_W_TERM(localRepl->term, localRepl->last_flush_lsn, ctx->cond.maxTerm, ctx->cond.maxLsn)) {
        ctx->cond.maxTerm = localRepl->term;
        ctx->cond.maxLsn = localRepl->last_flush_lsn;
    }
    if (localRepl->local_role == INSTANCE_ROLE_STANDBY) {
        if (XLByteWE_W_TERM(localRepl->term, localRepl->last_flush_lsn,
            ctx->cond.standbyMaxTerm, ctx->cond.standbyMaxLsn)) {
            ctx->cond.standbyMaxTerm = localRepl->term;
            ctx->cond.standbyMaxLsn = localRepl->last_flush_lsn;
        }
    }
}

bool CanbeCandicateBackup(const DnArbCtx *ctx, int32 memIdx, const CandicateCond *cadiCond)
{
    /* memIdx index is valid */
    if (memIdx == INVALID_INDEX) {
        return false;
    }
    /* Failover condition */
    if (cadiCond->mode == COS4FAILOVER) {
        /* memIdx failover times archive the most */
        if (ctx->dnReport[memIdx].sendFailoverTimes >= MAX_SEND_FAILOVER_TIMES) {
            return false;
        }
        /* memIdx is standby */
        if (ctx->dnReport[memIdx].local_status.local_role != INSTANCE_ROLE_STANDBY) {
            return false;
        }
    }
    uint32 localTerm = ctx->dnReport[memIdx].local_status.term;
    XLogRecPtr localLsn = ctx->dnReport[memIdx].local_status.last_flush_lsn;
    /* term and lsn is the most */
    if (!XLByteEQ_W_TERM(ctx->cond.standbyMaxTerm, ctx->cond.standbyMaxLsn, localTerm, localLsn)) {
        return false;
    }
    return true;
}

void ChooseCandicateIdxFromOtherBackup(DnArbCtx *ctx, const CandicateCond *cadiCond)
{
    /* the static primary may be the best choice */
    if (ctx->cond.candiIdx != INVALID_INDEX) {
        return;
    }
    int32 candiIdx = INVALID_INDEX;
    for (int32 i = 0; i < ctx->roleGroup->count; ++i) {
        if (!CanbeCandicateBackup(ctx, i, cadiCond)) {
            continue;
        }
        /* the smaller instanceId is the prefer choice */
        if (candiIdx == INVALID_INDEX) {
            candiIdx = i;
            break;
        }
    }
    ctx->cond.candiIdx = candiIdx;
}

void GetCandicateIdxBackup(DnArbCtx *ctx, const CandicateCond *cadiCond)
{
    ctx->cond.candiIdx = -1;
    const char *str = "[GetCandicate]";
    if (cadiCond->mode == COS4FAILOVER && ctx->cond.dyPrimNormalIdx != INVALID_INDEX &&
        ctx->cond.vaildPrimIdx != INVALID_INDEX) {
        write_runlog(DEBUG1, "%s, instanceId(%u), this group has dynamic primary(%d), validPrimIdx is %d, "
            "not need to choose candicate.\n", str, ctx->instId, ctx->cond.dyPrimNormalIdx, ctx->cond.vaildPrimIdx);
        return;
    }
    /* max term and lsn is valid */
    if (!IsTermLsnValid(ctx->cond.standbyMaxTerm, ctx->cond.standbyMaxLsn)) {
        write_runlog(LOG, "%s, instanceId(%u) standbyMaxTerm or standbyMaxLsn is invalid.\n", str, ctx->instId);
        return;
    }
    /* static primary is the first choice */
    if (CanbeCandicateBackup(ctx, ctx->cond.staticPriIdx, cadiCond)) {
        ctx->cond.candiIdx = ctx->cond.staticPriIdx;
        return;
    }
    /* static primary cannot be candicate */
    ChooseCandicateIdxFromOtherBackup(ctx, cadiCond);
}

void GetInstanceInfoStr(const StatusInstances *insInfo, char *logStr, size_t maxLen)
{
    if (maxLen == 0) {
        write_runlog(ERROR, "[GetInstanceInfoStr] maxLen is 0.\n");
        return;
    }
    errno_t rc = 0;
    if (insInfo->count == 0) {
        rc = strcpy_s(logStr, maxLen, "insInfo is empty");
        securec_check_errno(rc, (void)rc);
        return;
    }
    size_t strLen = 0;
    int32 idx = 0;
    for (; idx < insInfo->count; ++idx) {
        strLen = strlen(logStr);
        if (strLen >= (maxLen - 1)) {
            return;
        }
        if (idx != insInfo->count - 1) {
            rc = snprintf_s(logStr + strLen, maxLen - strLen, (maxLen - 1) - strLen, "%d: %u: %u, ",
                insInfo->itStatus[idx].memIdx, insInfo->itStatus[idx].instId, insInfo->itStatus[idx].term);
        } else {
            rc = snprintf_s(logStr + strLen, maxLen - strLen, (maxLen - 1) - strLen, "%d: %u: %u",
                insInfo->itStatus[idx].memIdx, insInfo->itStatus[idx].instId, insInfo->itStatus[idx].term);
        }
        securec_check_intval(rc, (void)rc);
    }
}

void GetSyncListStr(const cm_instance_report_status *repGroup, DnInstInfo *instInfo)
{
    /* covert the sync list to String. */
    GetSyncListString(&(repGroup->currentSyncList), instInfo->curSl, MAX_PATH_LEN);
    GetSyncListString(&(repGroup->exceptSyncList), instInfo->expSl, MAX_PATH_LEN);
    GetDnStatusString(&(repGroup->voteAzInstance), instInfo->voteL, MAX_PATH_LEN);
}

void GetDnIntanceInfo(const DnArbCtx *ctx, DnInstInfo *instInfo)
{
    GetSyncListStr(ctx->repGroup, instInfo);
    GetInstanceInfoStr(&(ctx->staCasCade), instInfo->stCasL, MAX_PATH_LEN);
    GetInstanceInfoStr(&(ctx->dyCascade), instInfo->dyCasL, MAX_PATH_LEN);
}
