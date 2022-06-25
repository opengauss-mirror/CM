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
 * cms_process_messages.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_server/cms_process_messages.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_PROCESS_MESSAGES_H
#define CMS_PROCESS_MESSAGES_H

#ifndef CM_AZ_NAME
#define CM_AZ_NAME 65
#endif

// ETCD TIME THRESHOLD
#define ETCD_CLOCK_THRESHOLD 3
#define SWITCHOVER_SEND_CHECK_RATE 30

typedef struct CmdMsgProc_t {
    bool doSwitchover;
    bool isCnReport;
} CmdMsgProc;

typedef void (*CltCmdProc)(
    CM_Connection *con, CM_StringInfo inBuffer, int msgType, CmdMsgProc *msgProc);

extern CltCmdProc g_cmdProc[MSG_CM_TYPE_CEIL];

extern int cmserver_getenv(const char* env_var, char* output_env_value, uint32 env_value_len, int elevel);
extern int check_if_candidate_is_in_faulty_az(uint32 group_index, int candidate_member_index);
extern int findAzIndex(const char azArray[][CM_AZ_NAME], const char* azName);
extern int ReadCommand(Port* myport, CM_StringInfo inBuf);
extern int isNodeBalanced(uint32* switchedInstance);
int get_logicClusterId_by_dynamic_dataNodeId(uint32 dataNodeId);

extern bool process_auto_switchover_full_check();
extern bool existMaintenanceInstanceInGroup(uint32 group_index, int *init_primary_member_index);
extern bool isMaintenanceInstance(const char *file_path, uint32 notify_instance_id);

extern uint32 GetClusterUpgradeMode();

extern void cm_server_process_msg(CM_Connection* con, CM_StringInfo inBuffer);
extern void SwitchOverSetting(int time_out, int instanceType, uint32 ptrIndex, int memberIndex);

extern void getAZDyanmicStatus(int azCount,
    int* statusOnline, int* statusPrimary, int* statusFail, int* statusDnFail, const char azArray[][CM_AZ_NAME]);
extern void CheckClusterStatus();
int switchoverFullDone(void);
void set_cluster_status(void);

void ProcessCtlToCmBalanceResultMsg(CM_Connection* con);
void ProcessCtlToCmGetMsg(CM_Connection* con);
void ProcessCtlToCmBuildMsg(CM_Connection* con, ctl_to_cm_build* buildMsg);
void ProcessCtlToCmQueryMsg(CM_Connection *con, const ctl_to_cm_query *ctlToCmQry);
void ProcessCtlToCmQueryKerberosStatusMsg(CM_Connection* con);
void ProcessCtlToCmQueryCmserverMsg(CM_Connection* con);
void ProcessCtlToCmSwitchoverMsg(CM_Connection *con, const ctl_to_cm_switchover *switchoverMsg);
void ProcessCtlToCmSwitchoverAllMsg(CM_Connection *con, const ctl_to_cm_switchover *switchoverMsg);
void process_ctl_to_cm_switchover_full_msg(CM_Connection* con, const ctl_to_cm_switchover* ctl_to_cm_swithover_ptr);
void ProcessCtlToCmSwitchoverFullCheckMsg(CM_Connection* con);
void ProcessCtlToCmSwitchoverFullTimeoutMsg(CM_Connection* con);
void ProcessCtlToCmSwitchoverAzMsg(CM_Connection* con, ctl_to_cm_switchover* ctl_to_cm_swithover_ptr);
void ProcessCtlToCmSwitchoverAzCheckMsg(CM_Connection* con);
void process_ctl_to_cm_switchover_az_timeout_msg(CM_Connection* con);
void process_ctl_to_cm_setmode(CM_Connection* con);
void ProcessCtlToCmSetMsg(CM_Connection* con, const ctl_to_cm_set* ctl_to_cm_set_ptr);
void ProcessResInstanceStatusMsg(CM_Connection *con, const CmsToCtlGroupResStatus *queryStatusPtr);
void process_ctl_to_cm_balance_check_msg(CM_Connection* con);
void ProcessCtlToCmsSwitchMsg(CM_Connection *con, CtlToCmsSwitch *switchMsg);

void process_ctl_to_cm_get_datanode_relation_msg(
    CM_Connection* con, const ctl_to_cm_datanode_relation_info* ctl_to_cm_datanode_relation_info_ptr);

void process_gs_guc_feedback_msg(const agent_to_cm_gs_guc_feedback* feedback_ptr);
void process_notify_cn_feedback_msg(CM_Connection* con, const agent_to_cm_notify_cn_feedback* feedback_ptr);
uint32 AssignCnForAutoRepair(uint32 nodeId);
void process_agent_to_cm_heartbeat_msg(CM_Connection* con, const agent_to_cm_heartbeat* agent_to_cm_heartbeat_ptr);
void process_agent_to_cm_disk_usage_msg(CM_Connection* con, const AgentToCMS_DiskUsageStatusReport* agent2CmDiskUsage);
void process_agent_to_cm_current_time_msg(const agent_to_cm_current_time_report* etcd_time_ptr);
void process_agent_to_cm_kerberos_status_report_msg(
    CM_Connection* con, agent_to_cm_kerberos_status_report* agent_to_cm_kerberos_status_ptr);
void process_agent_to_cm_fenced_UDF_status_report_msg(
    const agent_to_cm_fenced_UDF_status_report* agent_to_cm_fenced_UDF_status_ptr);
void ProcessCtlToCmQueryGlobalBarrierMsg(CM_Connection* con);
void ProcessCtlToCmQueryBarrierMsg(CM_Connection *con);
void ProcessCtl2CmOneInstanceBarrierQueryMsg(CM_Connection* con, uint32 node, uint32 instanceId, int instanceType);
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
void ProcessGetDnSyncListMsg(CM_Connection *con, AgentToCmserverDnSyncList *agentToCmserverDnSyncList);
#endif
void ProcessAgent2CmResStatReportMsg(const ReportResStatus *resStatusPtr);

int GetCurAz();
uint32 GetPrimaryDnIndex(void);

inline bool CheckEnableFlag()
{
    if (IsBoolCmParamTrue(g_enableDcf)) {
        write_runlog(ERROR, "switchover is not support on dcf mode.\n");
        return true;
    }
    return false;
}

void ProcessCtlToCmReloadMsg(CM_Connection *con);
void ProcessCtlToCmExecDccCmdMsg(CM_Connection *con, ExecDdbCmdMsg *msg);
void ProcessRequestResStatusListMsg(CM_Connection *con);
void ProcessCltSendOper(CM_Connection *con, CltSendDdbOper *ddbOper);
void ProcessSslConnRequest(CM_Connection *con, const AgentToCmConnectRequest *requestMsg);
void ProcessSharedStorageMsg(CM_Connection *con);

void GetSyncListString(const DatanodeSyncList *syncList, char *syncListString, size_t maxLen);

void ProcessHotpatchMessage(CM_Connection *con, cm_hotpatch_msg *hotpatch_msg);
void process_to_query_instance_status_msg(CM_Connection *con, const cm_query_instance_status *query_status_ptr);
void SetAgentDataReportMsg(CM_Connection *con, CM_StringInfo inBuffer);
void ProcessStopArbitrationMessage(void);
void process_finish_redo_message(CM_Connection *con);
void process_finish_redo_check_message(CM_Connection *con);
void ProcessDnBarrierinfo(CM_Connection *con, CM_StringInfo inBuffer);
void ProcessCnBarrierinfo(CM_Connection *con, CM_StringInfo inBuffer);
void FlushCmToAgentMsg(CM_Connection *con, int msgType, const CmdMsgProc *msgProc);
void InitCltCmdProc(void);
void SetSwitchoverPendingCmd(
    uint32 groupIdx, int32 memIdx, int32 waitSecond, const char *str, bool isNeedDelay = false);
int CheckNotifyCnStatus();
int32 GetSwitchoverDone(const char *str);
void ProcessDnLocalPeerMsg(CM_Connection *con, AgentCmDnLocalPeer *dnLpInfo);

#ifdef ENABLE_MULTIPLE_NODES
void SetCmdStautus(int32 ret, const CmdMsgProc *msgProc);
#endif

#endif
