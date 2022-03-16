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
 * cms_arbitrate_datanode_pms_utils.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_server/cms_arbitrate_datanode_pms_utils.h
 *
 * -------------------------------------------------------------------------
 */
#include "cms_global_params.h"
#include "cms_arbitrate_datanode_pms.h"
#ifndef CMS_ARBITRATE_DATANODE_PMS_UTILS_H
#define CMS_ARBITRATE_DATANODE_PMS_UTILS_H

typedef enum ClearAribType {
    CLEAR_ALL = 0,
    CLEAR_ARBI_TIME,
    CLEAR_SEND_FAILOVER_TIMES
} ClearAribType;

typedef struct DnBuildStatus {
    int32 buildCount;
    int32 standbyCount;
    int32 inSyncList;
} DnBuildStatus;

extern bool CheckPotentialTermRollback();
extern void GroupStatusShow(const char *str, const uint32 groupIndex, const uint32 instanceId,
    const int validCount, const bool finishRedo);
extern bool IsInstanceInCurrentAz(uint32 groupIndex, uint32 memberIndex, int curAzIndex, int az1Index, int az2Index);
extern bool IsSyncListEmpty(uint32 groupIndex, uint32 instanceId, maintenance_mode mode);
extern bool IsTermLsnValid(uint32 term, XLogRecPtr lsn);
extern void ClearDnArbiCond(uint32 groupIndex, ClearAribType type);
extern bool IsInSyncList(uint32 groupIndex, int memberIndex, int reportMemberIndex);
extern void CheckDnBuildStatus(uint32 groupIdx, int32 memIdx, DnBuildStatus *buildStatus);
int32 GetStaticPrimaryCount(uint32 groupIndex);
cm_instance_command_status *GetCommand(uint32 groupIndex, int32 memberIndex);
cm_instance_report_status *GetReportStatus(uint32 groupIndex);
cm_instance_datanode_report_status *GetLocalReportStatus(uint32 groupIndex, int32 memberIndex);
cm_instance_role_status *GetRoleStatus(uint32 groupIndex, int32 memberIndex);
cm_instance_datanode_report_status *GetDnReportStatus(uint32 groupIndex);
uint32 GetInstanceTerm(uint32 groupIndex, int memberIndex);
uint32 GetMaxTerm(uint32 groupIdx);
bool IsFinishReduceSyncList(uint32 groupIdx, int32 memIdx, const char *str);
void GetCandiInfoBackup(DnArbCtx *ctx, int32 memIdx);
bool CanbeCandicateBackup(const DnArbCtx *ctx, int32 memIdx, const CandicateCond *cadiCond);
void ChooseCandicateIdxFromOtherBackup(DnArbCtx *ctx, const CandicateCond *cadiCond);
void GetCandicateIdxBackup(DnArbCtx *ctx, const CandicateCond *cadiCond);
void GetSyncListStr(const cm_instance_report_status *repGroup, DnInstInfo *instInfo);
void GetDnIntanceInfo(const DnArbCtx *ctx, DnInstInfo *instInfo);
void GetInstanceInfoStr(const StatusInstances *insInfo, char *logStr, size_t maxLen);
void PrintCurAndPeerDnInfo(const DnArbCtx *ctx, const char *str);
#endif