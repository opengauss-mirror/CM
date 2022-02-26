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
 * cma_process_messages.cpp
 *    cma process cms messages functions
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_process_messages.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "securec.h"
#include "cm/cm_elog.h"
#include "cm/cm_msg.h"
#include "cma_global_params.h"
#include "cma_client.h"
#include "cma_status_check.h"
#include "cma_connect.h"
#include "cma_common.h"
#include "cma_process_messages.h"
#include "cma_process_messages_hadr.h"
#include "cma_process_messages_client.h"
#include "cma_instance_management.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "cma_coordinator.h"
#include "cma_cn_process_message.h"
#include "cma_cn_gtm_instance_management.h"
#endif

#ifdef ENABLE_UT
#define static
#endif

static void InstancesStatusCheckAndReport(void)
{
    int ret;

    if (g_shutdownRequest) {
        return;
    }

#ifdef ENABLE_MULTIPLE_NODES
    gtm_status_check_and_report();
    coordinator_status_check_and_report();
#endif

    DatanodeStatusReport();
    fenced_UDF_status_check_and_report();
    etcd_status_check_and_report();
    kerberos_status_check_and_report();

    ret = cm_client_flush_msg(agent_cm_server_connect);
    if (ret < 0) {
        CloseConnToCmserver();
    }
}

static void AgentSendHeartbeat()
{
    int ret;
    agent_to_cm_heartbeat heartbeat_msg;
    errno_t rc;

    if (agent_cm_server_connect == NULL || agent_cm_server_connect->status != CONNECTION_OK) {
        return;
    }

    rc = memset_s(&heartbeat_msg, sizeof(heartbeat_msg), 0, sizeof(heartbeat_msg));
    securec_check_errno(rc, (void)rc);
    heartbeat_msg.msg_type = MSG_AGENT_CM_HEARTBEAT;
    heartbeat_msg.node = g_currentNode->node;
    heartbeat_msg.instanceType = CM_AGENT;

    /*
     * After pg_pool_validate execute successfully, we will request the cluster
     * status until it is normal.
     */
    if (g_poolerPingEndRequest)
        heartbeat_msg.cluster_status_request = 1;
    else
        heartbeat_msg.cluster_status_request = 0;

    ret = cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&heartbeat_msg, sizeof(heartbeat_msg));
    if (ret != 0) {
        write_runlog(ERROR, "send cm_agent heartbeat failed!\n");
        CloseConnToCmserver();
        return;
    }
    write_runlog(DEBUG5, "send cm_agent heartbeat.\n");
}

/**
 * @brief Check the current node data disk capacity usage and log disk capacity usage to cm_server
 * @note Check and report the disk usage per 1 minute (60s).
 *
 */
static void DiskUsageCheckAndReport(int logLevel)
{
    int32 ret;
    if (g_shutdownRequest) {
        return;
    }

    write_runlog(logLevel, "[%s][line:%d] Disk Usage status check thread start.\n", __FUNCTION__, __LINE__);

#ifdef ENABLE_MULTIPLE_NODES
    CheckDiskForCNDataPathAndReport(logLevel);
#endif
    CheckDiskForDNDataPathAndReport(logLevel);

    ret = cm_client_flush_msg(agent_cm_server_connect);
    if (ret < 0) {
        write_runlog(ERROR, "[%s][line:%d] send cm_agent disk usage failed!\n", __FUNCTION__, __LINE__);
        CloseConnToCmserver();
    }
}

bool FindIndexByLocalPath(const char* data_path, uint32* node_index)
{
    uint32 i = 0;
    uint32 data_node_num;
    if (data_path == NULL) {
        write_runlog(ERROR, "invalid data path.\n");
        return false;
    }
    if (g_currentNode == NULL) {
        write_runlog(ERROR, "invalid g_currentNode.\n");
        return false;
    }
    data_node_num = g_currentNode->datanodeCount;
    for (i = 0; i < data_node_num; i++) {
        if (strcmp(data_path, g_currentNode->datanode[i].datanodeLocalDataPath) == 0) {
            *node_index = i;
            return true;
        }
    }
    write_runlog(ERROR, "could not find datanode instance by path %s.\n", data_path);
    return false;
}

void ResetPhonyDeadCount(const char* data_path, InstanceTypes ins_type)
{
    uint32 node_index = 0;
    switch (ins_type) {
#ifdef ENABLE_MULTIPLE_NODES
        case INSTANCE_CN:
            g_cnPhonyDeadTimes = 0;
            break;
        case INSTANCE_GTM:
            g_gtmPhonyDeadTimes = 0;
            break;
#endif
        case INSTANCE_DN:
            if (FindIndexByLocalPath(data_path, &node_index)) {
                g_dnPhonyDeadTimes[node_index] = 0;
            }
            break;
        case INSTANCE_CM:
            break;
        case INSTANCE_FENCED:
            break;
        default:
            write_runlog(ERROR, "unknown instance type: %d.\n", ins_type);
            break;
    }
    return;
}

void kill_instance_force(const char* data_path, InstanceTypes ins_type)
{
    struct timeval timeOut = {0};
    char Lrealpath[PATH_MAX] = {0};
    char cmd[CM_PATH_LENGTH];
    char system_cmd[CM_PATH_LENGTH];
    int ret;
    char system_cmdexten[] = {"\" | grep -v grep | awk '{print $1}'  | xargs kill -9 "};
    char cmdexten[] = {"\")  print $(NF-2)}' | awk -F/ '{print $3 }' | xargs kill -9 "};
    struct stat stat_buf = {0};
    errno_t rc;
    int rcs;

    rcs = snprintf_s(system_cmd, CM_PATH_LENGTH, CM_PATH_LENGTH - 1,
        "ps  -eo pid,euid,cmd | grep `id -u` | grep -i %s | grep -i -w \"", type_int_to_str_binname(ins_type));
    securec_check_intval(rcs, (void)rcs);
    rcs = snprintf_s(cmd, CM_PATH_LENGTH, CM_PATH_LENGTH - 1,
        "ps -eo pid,euid,cmd | grep -i %s | grep -v grep | awk '{if($2 == curuid && $1!=\"-n\") print "
        "\"/proc/\"$1\"/cwd\"}' curuid=`id -u`| xargs ls -l | awk '{if ($NF==\"", type_int_to_str_binname(ins_type));
    securec_check_intval(rcs, (void)rcs);
    write_runlog(LOG, "killing %s by force ...\n", type_int_to_str_name(ins_type));

    if (strcmp(data_path, "fenced") && strcmp(data_path, "krb5kdc")) {
        (void)realpath(data_path, Lrealpath);
    } else if (!strcmp(data_path, "krb5kdc")) {
        rcs = snprintf_s(Lrealpath, PATH_MAX, PATH_MAX - 1, "%s", "krb5kdc");
        securec_check_intval(rcs, (void)rcs);
    } else {
        rcs = snprintf_s(Lrealpath, PATH_MAX, PATH_MAX - 1, "%s", "fenced");
        securec_check_intval(rcs, (void)rcs);
    }

    rc = strncat_s(cmd, CM_PATH_LENGTH, Lrealpath, strlen(Lrealpath));
    securec_check_errno(rc, (void)rc);

    if (ins_type != INSTANCE_CM) {
        rc = strncat_s(system_cmd, CM_PATH_LENGTH, Lrealpath, strlen(Lrealpath));
        securec_check_errno(rc, (void)rc);
    }

    rc = strncat_s(cmd, CM_PATH_LENGTH, cmdexten, strlen(cmdexten));
    securec_check_errno(rc, (void)rc);

    rc = strncat_s(system_cmd, CM_PATH_LENGTH, system_cmdexten, strlen(system_cmdexten));
    securec_check_errno(rc, (void)rc);

    if (stat(system_call_log, &stat_buf) == 0) {
        /* redirect to system_call.log */
        rc = strncat_s(cmd, CM_PATH_LENGTH, ">> ", strlen(">> "));
        securec_check_errno(rc, (void)rc);
        rc = strncat_s(cmd, CM_PATH_LENGTH, system_call_log, strlen(system_call_log));
        securec_check_errno(rc, (void)rc);
        rc = strncat_s(cmd, CM_PATH_LENGTH, " 2>&1", strlen(" 2>&1"));
        securec_check_errno(rc, (void)rc);

        rc = strncat_s(system_cmd, CM_PATH_LENGTH, ">> ", strlen(">> "));
        securec_check_errno(rc, (void)rc);
        rc = strncat_s(system_cmd, CM_PATH_LENGTH, system_call_log, strlen(system_call_log));
        securec_check_errno(rc, (void)rc);
        rc = strncat_s(system_cmd, CM_PATH_LENGTH, " 2>&1", strlen(" 2>&1"));
        securec_check_errno(rc, (void)rc);
    }

    timeOut.tv_sec = 10;
    timeOut.tv_usec = 0;

    ret = system(system_cmd);
    if (ret != 0) {
        write_runlog(ERROR, "kill_instance_force: run system command failed! %s, errno=%d.\n", system_cmd, errno);

        if (ExecuteCmd(cmd, timeOut)) {
            write_runlog(LOG, "kill_instance_force: execute command failed. %s \n", cmd);
            return;
        }
    } else {
        /* if kill cn/dn/gtm success by syscmd, clear some obsoleted paramter. */
        ResetPhonyDeadCount(data_path, ins_type);
    }

    write_runlog(LOG, "%s stopped.\n", type_int_to_str_name(ins_type));
    return;
}

void immediate_stop_one_instance(const char* instance_data_path, InstanceTypes instance_type)
{
    kill_instance_force(instance_data_path, instance_type);
    return;
}

void process_restart_command(const char *data_dir, int instance_type)
{
    write_runlog(LOG, "restart msg from cm_server, data_dir :%s  instance type is %d\n", data_dir, instance_type);

    switch (instance_type) {
#ifdef ENABLE_MULTIPLE_NODES
        case INSTANCE_TYPE_GTM:
            write_runlog(LOG, "gtm restart !\n");
            immediate_stop_one_instance(data_dir, INSTANCE_GTM);
            break;
        case INSTANCE_TYPE_COORDINATE:
            if (g_repairCn) {
                write_runlog(LOG, "cn is being repaired, do not restart!\n");
            } else if (g_restoreCn) {
                write_runlog(LOG, "cn is being restore, do not restart!\n");
            } else {
                write_runlog(LOG, "cn restart !\n");
                immediate_stop_one_instance(data_dir, INSTANCE_CN);
            }
            break;
#endif
        case INSTANCE_TYPE_DATANODE:
            write_runlog(LOG, "datanode restart !\n");
            immediate_stop_one_instance(data_dir, INSTANCE_DN);
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            return;
    }
    return;
}

char* get_logicClusterName_by_dnInstanceId(uint32 dnInstanceId)
{
    uint32 ii;
    uint32 jj;

    for (ii = 0; ii < g_nodeHeader.nodeCount; ii++) {
        for (jj = 0; jj < g_node[ii].datanodeCount; jj++) {
            if (g_node[ii].datanode[jj].datanodeId == dnInstanceId) {
                return g_node[ii].datanode[jj].LogicClusterName;
            }
        }
    }
    return NULL;
}

void RunCmd(const char* command)
{
    int ret = system(command);
    if (ret != 0) {
        write_runlog(LOG, "exec command failed !  command is %s, errno=%d.\n", command, errno);
    }
}

static void ExeSwitchoverZengineCmd(const char *dataDir)
{
    int ret;
    char cmd[CM_PATH_LENGTH] = {0};
    uint32 port = 0;

    for (uint32 i = 0; i < g_currentNode->datanodeCount; ++i) {
        if (strcmp(g_currentNode->datanode[i].datanodeLocalDataPath, dataDir) == 0) {
            port = g_currentNode->datanode[i].datanodePort;
            break;
        }
    }
    if (IsBoolCmParamTrue(g_agentEnableDcf)) {
        for (uint32 i = 0; i < g_currentNode->sshCount; ++i) {
            ret = snprintf_s(cmd,
                CM_PATH_LENGTH,
                CM_PATH_LENGTH - 1,
                SYSTEMQUOTE "sh %s/cm_script/dn_zenith_zpaxos/switchoverdb.sh %s %s %u >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                g_binPath,
                dataDir,
                g_currentNode->sshChannel[i],
                port,
                system_call_log);
            securec_check_intval(ret, (void)ret);
            ret = system(cmd);
            if (ret == 0) {
                write_runlog(LOG, "run success switchover cmd(%s).\n", cmd);
                break;
            }
            write_runlog(LOG, "exec command failed! command is %s, errno=%d.\n", cmd, errno);
        }
    } else {
        ret = snprintf_s(cmd,
            CM_PATH_LENGTH,
            CM_PATH_LENGTH - 1,
            SYSTEMQUOTE "sh %s/cm_script/dn_zenith_ha/switchoverdb.sh %s %u >> \"%s\" 2>&1 &" SYSTEMQUOTE,
            g_binPath,
            dataDir,
            port,
            system_call_log);
        securec_check_intval(ret, (void)ret);
        RunCmd(cmd);
    }

    return;
}

static void ProcessSwitchoverCommand(const char *dataDir, int instanceType, uint32 instanceId, uint32 term, bool doFast)
{
    char command[MAXPGPATH];
    errno_t rc;
    char instanceName[CM_NODE_NAME] = {0};
    char *lcName = NULL;
    Alarm alarm[1];
    AlarmAdditionalParam alarmParam;

    write_runlog(LOG, "switchover msg from cm_server, data_dir :%s  nodeType is %d\n", dataDir, instanceType);

    switch (instanceType) {
#ifdef ENABLE_MULTIPLE_NODES
        case INSTANCE_TYPE_GTM:
            rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                SYSTEMQUOTE "%s switchover -D %s >> \"%s\" 2>&1 &" SYSTEMQUOTE, GTM_CTL_NAME, dataDir, system_call_log);
            securec_check_intval(rc, (void)rc);

            /* Initialize the instance name */
            rc = snprintf_s(instanceName, sizeof(instanceName), sizeof(instanceName) - 1, "%s_%u", "gtm", instanceId);
            securec_check_intval(rc, (void)rc);
            /* Initialize the alarm item structure(typedef struct Alarm) */
            AlarmItemInitialize(&(alarm[0]), ALM_AI_GTMSwitchOver, ALM_AS_Normal, NULL);
            /* fill the alarm message */
            WriteAlarmAdditionalInfo(&alarmParam, instanceName, "", "", alarm, ALM_AT_Event, instanceName);
            /* report the alarm */
            ReportCMAEventAlarm(alarm, &alarmParam);
            break;
#endif
        case INSTANCE_TYPE_DATANODE:
            if (g_clusterType == V3SingleInstCluster) {
                ExeSwitchoverZengineCmd(dataDir);
                return;
            }
            lcName = get_logicClusterName_by_dnInstanceId(instanceId);
            if (doFast) {
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s switchover -D  %s  -T %u -f>> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME, dataDir, term, system_call_log);
            } else {
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s switchover -D  %s  -T %u >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME, dataDir, term, system_call_log);
            }
            securec_check_intval(rc, (void)rc);

            rc = snprintf_s(instanceName, sizeof(instanceName), sizeof(instanceName) - 1, "%s_%u", "dn", instanceId);
            securec_check_intval(rc, (void)rc);
            /* Initialize the alarm item structure(typedef struct Alarm) */
            AlarmItemInitialize(&(alarm[0]), ALM_AI_DatanodeSwitchOver, ALM_AS_Normal, NULL);
            /* fill the alarm message */
            WriteAlarmAdditionalInfoForLC(&alarmParam, instanceName, "", "", lcName, alarm, ALM_AT_Event, instanceName);
            /* report the alarm */
            ReportCMAEventAlarm(alarm, &alarmParam);
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            return;
    }
    RunCmd(command);

    return;
}

void GetDnFailoverCommand(char *command, uint32 cmdLen, const char *dataDir, uint32 term)
{
    errno_t rc;
    if (g_clusterType == V3SingleInstCluster) {
        rc = snprintf_s(command,
            cmdLen,
            cmdLen - 1,
            SYSTEMQUOTE "sh %s/cm_script/dn_zenith_ha/failoverdb.sh %s %u >> \"%s\" 2>&1 &" SYSTEMQUOTE,
            g_binPath,
            dataDir,
            term,
            system_call_log);
    } else {
        rc = snprintf_s(command,
            cmdLen,
            cmdLen - 1,
            SYSTEMQUOTE "%s failover -D  %s -T %u >> \"%s\" 2>&1 &" SYSTEMQUOTE,
            PG_CTL_NAME,
            dataDir,
            term,
            system_call_log);
    }
    securec_check_intval(rc, (void)rc);
}

static void process_failover_command(const char* dataDir, int instanceType, uint32 instance_id, uint32 term)
{
    char command[MAXPGPATH];
    errno_t rc;
    char instanceName[CM_NODE_NAME] = {0};
    char* logicClusterName = NULL;
    Alarm AlarmFailOver[1];
    AlarmAdditionalParam tempAdditionalParam;

    write_runlog(LOG, "failover msg from cm_server, data_dir :%s  nodetype is %d\n", dataDir, instanceType);

    switch (instanceType) {
#ifdef ENABLE_MULTIPLE_NODES
        case INSTANCE_TYPE_GTM:
            rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                SYSTEMQUOTE "%s failover -D  %s >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                GTM_CTL_NAME, dataDir, system_call_log);
            securec_check_intval(rc, (void)rc);

            /* Initialize the instance name */
            rc = snprintf_s(instanceName, sizeof(instanceName), sizeof(instanceName) - 1, "%s_%u", "gtm", instance_id);
            securec_check_intval(rc, (void)rc);
            /* Initialize the alarm item structure(typedef struct Alarm) */
            AlarmItemInitialize(&(AlarmFailOver[0]), ALM_AI_GTMFailOver, ALM_AS_Normal, NULL);
            /* fill the alarm message */
            WriteAlarmAdditionalInfo(
                &tempAdditionalParam, instanceName, "", "", AlarmFailOver, ALM_AT_Event, instanceName);
            /* report the alarm */
            ReportCMAEventAlarm(AlarmFailOver, &tempAdditionalParam);
            break;
#endif
        case INSTANCE_TYPE_DATANODE:
            GetDnFailoverCommand(command, MAXPGPATH, dataDir, term);
            logicClusterName = get_logicClusterName_by_dnInstanceId(instance_id);
            rc = snprintf_s(instanceName, sizeof(instanceName), sizeof(instanceName) - 1, "%s_%u", "dn", instance_id);
            securec_check_intval(rc, (void)rc);
            /* Initialize the alarm item structure(typedef struct Alarm) */
            AlarmItemInitialize(&(AlarmFailOver[0]), ALM_AI_DatanodeFailOver, ALM_AS_Normal, NULL);
            /* fill the alarm message */
            WriteAlarmAdditionalInfoForLC(&tempAdditionalParam,
                instanceName,
                "",
                "",
                logicClusterName,
                AlarmFailOver,
                ALM_AT_Event,
                instanceName);
            /* report the alarm */
            ReportCMAEventAlarm(AlarmFailOver, &tempAdditionalParam);
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            return;
    }
    RunCmd(command);

    return;
}

static void process_finish_redo_command(const char* dataDir, uint32 instd, bool isFinishRedoCmdSent)
{
    char command[MAXPGPATH];
    errno_t rc;

    write_runlog(LOG, "Finish redo msg from cm_server, data_dir :%s\n", dataDir);

    rc = snprintf_s(command,
        MAXPGPATH,
        MAXPGPATH - 1,
        SYSTEMQUOTE "%s finishredo -D  %s >> \"%s\" 2>&1 &" SYSTEMQUOTE,
        PG_CTL_NAME,
        dataDir,
        system_call_log);
    securec_check_intval(rc, (void)rc);

    if (!isFinishRedoCmdSent) {
        char instanceName[CM_NODE_NAME] = {0};
        char* logicClusterName = get_logicClusterName_by_dnInstanceId(instd);
        Alarm AlarmFinishRedo[1];
        AlarmAdditionalParam tempAdditionalParam;

        rc = snprintf_s(instanceName, sizeof(instanceName), sizeof(instanceName) - 1, "%s_%u", "dn", instd);
        securec_check_intval(rc, (void)rc);
        /* Initialize the alarm item structure(typedef struct Alarm) */
        AlarmItemInitialize(&(AlarmFinishRedo[0]), ALM_AI_ForceFinishRedo, ALM_AS_Reported, NULL);
        /* fill the alarm message */
        WriteAlarmAdditionalInfoForLC(&tempAdditionalParam, instanceName, "", "", logicClusterName,
            AlarmFinishRedo, ALM_AT_Event, instanceName);
        /* report the alarm */
        ReportCMAEventAlarm(AlarmFinishRedo, &tempAdditionalParam);
    }
    RunCmd(command);
    return;
}

static status_t FindDatanodeIndex(uint32 &index, const char *dataDir)
{
    for (index = 0; index < g_currentNode->datanodeCount; ++index) {
        if (strncmp(g_currentNode->datanode[index].datanodeLocalDataPath, dataDir, MAXPGPATH) == 0) {
            g_dnBuild[index] = true;
            g_isCmaBuildingDn[index] = true;
            write_runlog(LOG, "CMA is processing build command of %u, set is_cma_building_dn to true.\n",
                g_currentNode->datanode[index].datanodeId);
            return CM_SUCCESS;
        }
    }
    write_runlog(LOG, "can't find the DataNode instance id from the current node, dataDir:\"%s\"\n", dataDir);

    return CM_ERROR;
}

static status_t DeleteInstanceManualStartFile(uint32 datanodeId)
{
    int ret;
    struct stat instanceStatBuf = {0};
    char instanceManualStartFile[MAX_PATH_LEN] = {'\0'};

    ret = snprintf_s(instanceManualStartFile,
        MAX_PATH_LEN,
        MAX_PATH_LEN - 1,
        "%s_%u",
        g_cmInstanceManualStartPath,
        datanodeId);
    securec_check_intval(ret, (void)ret);

    if (stat(instanceManualStartFile, &instanceStatBuf) == 0) {
        char command[MAXPGPATH] = {0};
        ret = snprintf_s(command,
            MAXPGPATH,
            MAXPGPATH - 1,
            "rm -f %s_%u >> \"%s\" 2>&1 &",
            g_cmInstanceManualStartPath,
            datanodeId,
            system_call_log);
        securec_check_intval(ret, (void)ret);
        if (system(command) != 0) {
            write_runlog(ERROR, "failed to execute the command-line: %s, errno=%d.\n", command, errno);
            return CM_ERROR;
        }
    }

    return CM_SUCCESS;
}

static status_t ExecuteCommand(const char *cmd)
{
    int ret;

    ret = system(cmd);
    if (ret == -1) {
        write_runlog(ERROR, "Failed to call the system function: func_name=\"%s\", command=\"%s\","
                            " error=\"[%d]\".\n", "system", cmd, errno);
    } else if (WIFSIGNALED(ret)) {
        write_runlog(ERROR, "Failed to execute the shell command: the shell command was killed by"
                            " signal %d.\n", WTERMSIG(ret));
    } else if (ret != 0) {
        write_runlog(ERROR, "Failed to execute the shell command: the shell command ended abnormally:"
                            " shell_return=%d, command=\"%s\", errno=%d.\n", WEXITSTATUS(ret), cmd, errno);
    } else {
        return CM_SUCCESS;
    }

    return CM_ERROR;
}

static void ExecuteBuildDatanodeCommand(bool is_single_node, BuildMode build_mode, const char *data_dir,
    time_t wait_seconds, uint32 term)
{
    char command[MAXPGPATH] = {0};
    char build_mode_str[MAXPGPATH];
    char termStr[MAX_TIME_LEN] = {0};
    int rc = 0;

    if (IsBoolCmParamTrue(g_agentEnableDcf)) {
        build_mode = FULL_BUILD;
    }
    
    if (build_mode == FULL_BUILD) {
        rc = strncpy_s(build_mode_str, MAXPGPATH, "-b full", strlen("-b full"));
    } else if (build_mode == INC_BUILD) {
        rc = strncpy_s(build_mode_str, MAXPGPATH, "-b incremental", strlen("-b incremental"));
    } else {
        rc = strncpy_s(build_mode_str, MAXPGPATH, "", strlen(""));
    }
    securec_check_errno(rc, (void)rc);

    rc = snprintf_s(termStr, MAX_TIME_LEN, MAX_TIME_LEN - 1, "-T %u", term);
    securec_check_intval(rc, (void)rc);

#ifdef ENABLE_MULTIPLE_NODES
    rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
    SYSTEMQUOTE "%s build -Z %s %s %s -D %s %s -r %d >> \"%s\" 2>&1 &" SYSTEMQUOTE,
    PG_CTL_NAME, is_single_node ? "single_node" : "datanode",
    build_mode_str, security_mode ? "-o \"--securitymode\"" : "", data_dir,
    agent_backup_open == CLUSTER_OBS_STANDBY ? "" : termStr,
    wait_seconds, system_call_log);
#else
    rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
    SYSTEMQUOTE "%s build %s %s -D %s %s -r %d >> \"%s\" 2>&1 &" SYSTEMQUOTE,
    PG_CTL_NAME,
    build_mode_str, security_mode ? "-o \"--securitymode\"" : "", data_dir,
    agent_backup_open == CLUSTER_OBS_STANDBY ? "" : termStr,
    wait_seconds,
    system_call_log);
#endif
    securec_check_intval(rc, (void)rc);

    write_runlog(LOG, "start build operation: command=\"%s\".\n", command);

    (void)ExecuteCommand(command);

    return;
}

static int GetPrimaryIndex(uint32 nodeId)
{
    for (uint32 i = 0; i < g_cm_server_num; ++i) {
        if (g_node[g_nodeIndexForCmServer[i]].node == nodeId) {
            return static_cast<int>(g_nodeIndexForCmServer[i]);
        }
    }

    return -1;
}

static void ExecuteZengineBuildScriptPaxos(const char *dataDir, uint32 primaryNodeId)
{
    int ret;
    char buildCmd[MAXPGPATH] = { 0 };

    int primaryIndex = GetPrimaryIndex(primaryNodeId);
    if (primaryIndex == -1) {
        write_runlog(ERROR, "cms has no primary, can't do build.\n");
        return;
    }

    for (uint32 j = 0; j < g_node[primaryIndex].sshCount; ++j) {
        ret = snprintf_s(buildCmd,
            MAXPGPATH,
            MAXPGPATH - 1,
            SYSTEMQUOTE "sh %s/cm_script/dn_zenith_zpaxos/builddb.sh %s %s %u" SYSTEMQUOTE,
            g_binPath,
            dataDir,
            g_node[primaryIndex].sshChannel[j],
            g_node[primaryIndex].port);
        securec_check_intval(ret, (void)ret);

        write_runlog(LOG, "start to execute zpaxos buildCmd(%s)\n", buildCmd);

        if (ExecuteCommand(buildCmd) == CM_SUCCESS) {
            write_runlog(LOG, "Execute build zengine cmd %s success.\n", buildCmd);
            return;
        }
    }

    return;
}

static void ExecuteZengineBuildScript(uint32 index, bool isSingle, const char *dataDir, const BuildMode &mode,
    const cm_to_agent_build *buildMsg)
{
    int ret;
    errno_t rc;
    char buildCmd[MAXPGPATH] = { 0 };
    char buildModeStr[NAMEDATALEN] = { 0 };

    int primaryIndex = GetPrimaryIndex(buildMsg->primaryNodeId);
    if (primaryIndex == -1) {
        write_runlog(ERROR, "cms has no primary, can't do build.\n");
        return;
    }

    if (mode == FULL_BUILD) {
        rc = strncpy_s(buildModeStr, NAMEDATALEN, "full", strlen("full"));
    } else {
        rc = strncpy_s(buildModeStr, NAMEDATALEN, "auto", strlen("auto"));
    }
    securec_check_errno(rc, (void)rc);

    for (uint32 j = 0; j < g_node[primaryIndex].sshCount; ++j) {
        ret = snprintf_s(buildCmd,
            MAXPGPATH,
            MAXPGPATH - 1,
            SYSTEMQUOTE "sh %s/cm_script/dn_zenith_ha/builddb.sh %s %s %u %s %s %s %s %d %s" SYSTEMQUOTE,
            g_binPath,
            dataDir,
            "standby",
            g_currentNode->datanode[index].datanodePort,
            g_node[primaryIndex].sshChannel[j],
            isSingle ? "SINGLE_NODE" : "DN_ZENITH_HA",
            "ipv4",
            "300",
            buildMsg->parallel,
            buildModeStr);
        securec_check_intval(ret, (void)ret);

        write_runlog(LOG, "start to execute buildCmd(%s)\n", buildCmd);

        if (ExecuteCommand(buildCmd) == CM_SUCCESS) {
            write_runlog(LOG, "Execute build zengine cmd %s success.\n", buildCmd);
            return;
        }
    }

    return;
}

static void BuildDatanode(const char *dataDir, const cm_to_agent_build *buildMsg)
{
    BuildMode buildMode;

    if (g_only_dn_cluster) {
        if (buildMsg->full_build == 1) {
            buildMode = FULL_BUILD;
        } else if (incremental_build) {
            buildMode = AUTO_BUILD;
        } else {
            buildMode = FULL_BUILD;
        }
        ExecuteBuildDatanodeCommand(true, buildMode, dataDir, buildMsg->wait_seconds, buildMsg->term);
    } else if (g_multi_az_cluster) {
        if (buildMsg->full_build == 1) {
            buildMode = FULL_BUILD;
        } else if (incremental_build) {
            buildMode = AUTO_BUILD;
        } else {
            buildMode = FULL_BUILD;
        }
        ExecuteBuildDatanodeCommand(false, buildMode, dataDir, buildMsg->wait_seconds, buildMsg->term);
    }  else {
        if (buildMsg->full_build == 1) {
            buildMode = FULL_BUILD;
        } else if (incremental_build) {
            buildMode = INC_BUILD;
        } else {
            buildMode = AUTO_BUILD;
        }
        ExecuteBuildDatanodeCommand(false, buildMode, dataDir, buildMsg->wait_seconds, buildMsg->term);
    }

    return;
}

static void BuildZengine(uint32 dnIndex, const char *dataDir, const cm_to_agent_build *buildMsg)
{
    errno_t rc;
    BuildMode buildMode;
    GaussState state;
    int ret;
    char gaussdbStatePath[CM_PATH_LENGTH];

    if (buildMsg->full_build == 1) {
        buildMode = FULL_BUILD;
    } else {
        buildMode = AUTO_BUILD;
    }

    ret = snprintf_s(gaussdbStatePath,
        CM_PATH_LENGTH,
        CM_PATH_LENGTH - 1,
        "%s/gaussdb.state",
        dataDir);
    securec_check_intval(ret, (void)ret);
    canonicalize_path(gaussdbStatePath);

    rc = memset_s(&state, sizeof(GaussState), 0, sizeof(GaussState));
    securec_check_errno(rc, (void)rc);

    // build zengine need primary dn ip, so use conn_num replace primary index
    state.conn_num = GetPrimaryIndex(buildMsg->primaryNodeId);
    state.mode = STANDBY_MODE;
    state.state = BUILDING_STATE;
    state.sync_stat = false;
    UpdateDBStateFile(gaussdbStatePath, &state);

    if (IsBoolCmParamTrue(g_agentEnableDcf)) {
        ExecuteZengineBuildScriptPaxos(dataDir, buildMsg->primaryNodeId);
        g_isCmaBuildingDn[dnIndex] = false;
        return;
    }

    if (g_only_dn_cluster) {
        ExecuteZengineBuildScript(dnIndex, true, dataDir, buildMode, buildMsg);
    } else {
        ExecuteZengineBuildScript(dnIndex, false, dataDir, buildMode, buildMsg);
    }

    return;
}


/**
 * @brief  If cm_agent receive the build command from cm_server, the agent will perform the build operation.
 *         Currently, all shell commands in this function are executed in the background. Therefore,
 *         this function cannot obtain the actual return value of the shell command.
 *         An alarm is reported before the build operation is performed.
 * 
 * @param  dataDir          The instance data path.
 * @param  instanceType     The instance type.
 * @param  buildMsg         The build msg from cm server.
 */
static void ProcessBuildCommand(const char *dataDir, int instanceType, const cm_to_agent_build *buildMsg)
{
    int ret;
    uint32 dnIndex;

    write_runlog(LOG, "build msg from cm_server, dataDir :%s  instance type is %d waitSeconds is %d\n",
        dataDir, instanceType, buildMsg->wait_seconds);

    write_runlog(LOG, "%s\n", g_cmInstanceManualStartPath);
    if (agent_backup_open == CLUSTER_STREAMING_STANDBY) {
        ProcessCrossClusterBuildCommand(instanceType, dataDir, buildMsg);
        return;
    }
    if (FindDatanodeIndex(dnIndex, dataDir) != CM_SUCCESS) {
        write_runlog(LOG, "find the dn(%s) instanceId filed, can't do build.\n", dataDir);
        return;
    }

    if (DeleteInstanceManualStartFile(g_currentNode->datanode[dnIndex].datanodeId) != CM_SUCCESS) {
        write_runlog(LOG, "delete dn instance manual start file failed, can't do build.\n");
        return;
    }
    
    char instanceName[CM_NODE_NAME] = {0};
    Alarm AlarmBuild[1];
    AlarmAdditionalParam tempAdditionalParam;

    ret = snprintf_s(instanceName, sizeof(instanceName), (sizeof(instanceName) - 1), "%s_%u",
        "dn", g_currentNode->datanode[dnIndex].datanodeId);
    securec_check_intval(ret, (void)ret);
    /* Initialize the alarm item structure(typedef struct Alarm). */
    AlarmItemInitialize(&(AlarmBuild[0]), ALM_AI_Build, ALM_AS_Normal, NULL);
    /* fill the alarm message. */
    WriteAlarmAdditionalInfo(&tempAdditionalParam, instanceName, "", "", AlarmBuild, ALM_AT_Event, instanceName);
    /* report the alarm. */
    ReportCMAEventAlarm(AlarmBuild, &tempAdditionalParam);

    switch (instanceType) {
        case INSTANCE_TYPE_DATANODE:
            if (g_clusterType == V3SingleInstCluster) {
                BuildZengine(dnIndex, dataDir, buildMsg);
            } else {
                BuildDatanode(dataDir, buildMsg);
            }
            break;
        case INSTANCE_TYPE_GTM:
            write_runlog(LOG, "GTM no need to handle build command.\n");
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            break;
    }
    return;
}

/*
 * @Description: process notify to cmagent
 * @IN notify: cancle user session notify from cm server
 * @Return: void
 * @See also:
 */
static void process_cancle_session_command(const cm_to_agent_cancel_session* cancel_msg)
{
    write_runlog(LOG, "process_cancle_session_command()\n");

    if (g_currentNode->coordinate == 0)
        return;

    write_runlog(LOG, "cm_agent notify cn %u to cancel session.\n", cancel_msg->instanceId);

    (void)pthread_rwlock_wrlock(&g_coordinatorsCancelLock);
    g_coordinatorsCancel = true;
    (void)pthread_rwlock_unlock(&g_coordinatorsCancelLock);

    return;
}

static void process_rep_sync_command(const char* dataDir, int instType)
{
    char command[MAXPGPATH];
    errno_t rc;

    write_runlog(LOG, "rep sync msg from cm_server, data_dir :%s  nodetype is %d\n", dataDir, instType);
    switch (instType) {
        case INSTANCE_TYPE_GTM:
            rc = snprintf_s(command,
                MAXPGPATH,
                MAXPGPATH - 1,
                SYSTEMQUOTE "%s setsyncmode -Z gtm -A %s -D %s >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                GTM_CTL_NAME,
                "on",
                dataDir,
                system_call_log);
            securec_check_intval(rc, (void)rc);
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            return;
    }
    RunCmd(command);

    return;
}

static void process_rep_most_available_command(const char *dataDir, int instance_type)
{
    char command[MAXPGPATH];
    errno_t rc;

    write_runlog(LOG, "rep most available msg from cm_server, data_dir :%s  nodetype is %d\n", dataDir, instance_type);
    switch (instance_type) {
        case INSTANCE_TYPE_GTM:
            rc = snprintf_s(command,
                MAXPGPATH,
                MAXPGPATH - 1,
                SYSTEMQUOTE "%s setsyncmode -Z gtm -A %s -D %s >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                GTM_CTL_NAME,
                "auto",
                dataDir,
                system_call_log);
            securec_check_intval(rc, (void)rc);
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            return;
    }
    RunCmd(command);

    return;
}

static void ChangeRoleToCasCade(const char *dataDir, bool isCasCade)
{
    uint32 i = 0;
    for (i = 0; i < g_currentNode->datanodeCount; ++i) {
        if (strcmp(g_currentNode->datanode[i].datanodeLocalDataPath, dataDir) == 0) {
            break;
        }
    }
    if (g_dnCascade[i] == isCasCade) {
        return;
    }
    g_dnCascade[i] = isCasCade;
    if (isCasCade) {
        immediate_stop_one_instance(dataDir, INSTANCE_DN);
    }
}

static void process_notify_command(const char* data_dir, int instance_type, int role, uint32 term)
{
    char command[MAXPGPATH] = {0};
    int ret;
    errno_t rc;

    write_runlog(LOG, "notify msg from cm_server, data_dir :%s  nodetype is %d, role is %d.\n",
        data_dir, instance_type, role);

    switch (instance_type) {
        case INSTANCE_TYPE_DATANODE:
            if (role == INSTANCE_ROLE_PRIMARY) {
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s notify -M %s -D %s -T %u -w -t 1 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME, "primary", data_dir, term, system_call_log);
                securec_check_intval(rc, (void)rc);
                ChangeRoleToCasCade(data_dir, false);
            } else if (role == INSTANCE_ROLE_STANDBY) {
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s notify -M %s -D %s -w -t 1 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME, "standby", data_dir, system_call_log);
                securec_check_intval(rc, (void)rc);
                ChangeRoleToCasCade(data_dir, false);
            } else if (role == INSTANCE_ROLE_CASCADE_STANDBY) {
                ChangeRoleToCasCade(data_dir, true);
            } else {
                write_runlog(
                    LOG, "the instance datadir(%s) instance type(%d) role is unknown role\n", data_dir, instance_type);
                return;
            }
            break;
        case INSTANCE_TYPE_GTM:
            if (role == INSTANCE_ROLE_PRIMARY) {
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s notify -M %s -D %s -w -t 3 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    GTM_CTL_NAME, "primary", data_dir, system_call_log);
                securec_check_intval(rc, (void)rc);
            } else if (role == INSTANCE_ROLE_STANDBY) {
                if (!g_single_node_cluster) {
                    rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                        SYSTEMQUOTE "%s notify -M %s -D %s -w -t 3 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                        GTM_CTL_NAME, "standby", data_dir, system_call_log);
                    securec_check_intval(rc, (void)rc);
                }
            } else {
                write_runlog(
                    LOG, "the instance datadir(%s) instance type(%d) role is unknown role\n", data_dir, instance_type);
                return;
            }
            break;
        default:
            write_runlog(LOG, "node_type is unknown !\n");
            return;
    }
    ret = system(command);
    write_runlog(LOG, "exec notify command:%s\n", command);
    if (ret != 0) {
        write_runlog(LOG, "exec notify command failed ret=%d !  command is %s, errno=%d.\n", ret, command, errno);
    }

    return;
}

int datanode_status_check_before_restart(const char *dataDir, int *local_role)
{
    int ret = check_one_instance_status(GetDnProcessName(), dataDir, NULL);
    if (ret == PROCESS_RUNNING) {
        return CheckDatanodeStatus(dataDir, local_role);
    }

    return -1;
}


static void process_restart_by_mode_command(char* data_dir, int instance_type, int role_old, int role_new)
{
    char command[MAXPGPATH];
    int local_role = INSTANCE_ROLE_UNKNOWN;
    errno_t rc;

    int ret = datanode_status_check_before_restart(data_dir, &local_role);
    if (ret == 0 && (role_old == local_role)) {
        if (role_new == INSTANCE_ROLE_STANDBY) {
            if (g_single_node_cluster) {
                return;
            }
#ifdef ENABLE_MULTIPLE_NODES
            rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                SYSTEMQUOTE "%s restart -M %s -D %s -Z datanode -m i -w -t 2 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                PG_CTL_NAME,
                "standby",
                data_dir,
                system_call_log);
#else
            rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                SYSTEMQUOTE "%s restart -M %s -D %s -m i -w -t 2 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                PG_CTL_NAME,
                "standby",
                data_dir,
                system_call_log);
#endif
            securec_check_intval(rc, (void)rc);
        } else if (role_new == INSTANCE_ROLE_PRIMARY) {
            if (g_single_node_cluster) {
#ifdef ENABLE_MULTIPLE_NODES
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s restart  -D %s -Z datanode -m i -w -t 2 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME,
                    data_dir,
                    system_call_log);
#else
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s restart  -D %s -m i -w -t 2 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME,
                    data_dir,
                    system_call_log);
#endif
            } else {
#ifdef ENABLE_MULTIPLE_NODES
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s restart -M %s -D %s -Z datanode -m i -w -t 2 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME,
                    "primary",
                    data_dir,
                    system_call_log);
#else
                rc = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
                    SYSTEMQUOTE "%s restart -M %s -D %s -m i -w -t 2 >> \"%s\" 2>&1 &" SYSTEMQUOTE,
                    PG_CTL_NAME,
                    "primary",
                    data_dir,
                    system_call_log);
#endif
            }
            securec_check_intval(rc, (void)rc);
        } else {
            write_runlog(
                LOG, "the instance datadir(%s) instance type(%d) role is unknown role\n", data_dir, instance_type);
            return;
        }

        write_runlog(LOG, "restart datanode by mode , data_dir :%s  nodetype is %d\n", data_dir, instance_type);

        write_runlog(LOG, "exec restart datanode command:%s\n", command);

        RunCmd(command);
    }
    return;
}

/* Process the heartbeat from server to agent */
static void process_heartbeat_command(int cluster_status)
{
    /*
     * After the cluster is normal, agent will not continue to request, and
     * CN STATUS thread will close the pooler ping.
     */
    if (g_poolerPingEndRequest && (cluster_status == CM_STATUS_NORMAL || cluster_status == CM_STATUS_DEGRADE)) {
        g_poolerPingEndRequest = false;
        g_poolerPingEnd = true;
    }

    /* update cm_server heartbeat */
    (void)clock_gettime(CLOCK_MONOTONIC, &g_serverHeartbeatTime);
}

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
static void process_gs_guc_command(const cm_to_agent_gs_guc* gsGucPtr)
{
    char gsGucCommand[MAXPGPATH] = {0};
    int rc = 0;
    bool result = true;
    uint32 dnIndex;
    synchronous_standby_mode syncMode = gsGucPtr->type;
    bool azName_Valid = true;
    char* az1 = getAZNamebyPriority(g_az_master);
    char* az2 = getAZNamebyPriority(g_az_slave);

    for (dnIndex = 0; dnIndex < g_currentNode->datanodeCount; dnIndex++) {
        if (g_currentNode->datanode[dnIndex].datanodeId == gsGucPtr->instanceId) {
            break;
        }
    }

    switch (syncMode) {
        case AnyAz1:
            azName_Valid = (az1 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'ANY 1(%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az1);
            break;
        case FirstAz1:
            azName_Valid = (az1 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'FIRST 1(%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az1);
            break;
        case AnyAz2:
            azName_Valid = (az2 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'ANY 1(%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az2);
            break;
        case FirstAz2:
            azName_Valid = (az2 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'FIRST 1(%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az2);
            break;
        case Any2Az1Az2:
            azName_Valid = (az1 != NULL) && (az2 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s -c \"synchronous_standby_names = 'ANY 2(%s,%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az1,
                az2);
            break;
        case First2Az1Az2:
            azName_Valid = (az1 != NULL) && (az2 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'FIRST 2(%s,%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az1,
                az2);
            break;
        case Any3Az1Az2:
            azName_Valid = (az1 != NULL) && (az2 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'ANY 3(%s,%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az1,
                az2);
            break;
        case First3Az1Az2:
            azName_Valid = (az1 != NULL) && (az2 != NULL);
            rc = snprintf_s(gsGucCommand,
                MAXPGPATH,
                MAXPGPATH - 1,
                "gs_guc reload  -Z datanode -D %s  -c \"synchronous_standby_names = 'FIRST 3(%s,%s)'\"",
                g_currentNode->datanode[dnIndex].datanodeLocalDataPath,
                az1,
                az2);
            break;
        default:
            break;
    }

    securec_check_intval(rc, (void)rc);

    if (azName_Valid) {
        rc = system(gsGucCommand);
        if (rc != 0) {
            write_runlog(ERROR, "Execute %s failed: , errno=%d.\n", gsGucCommand, errno);
            result = false;
        } else {
            write_runlog(LOG, "Execute %s success: \n", gsGucCommand);
            result = true;
        }
    } else {
        result = false;
    }

    agent_to_cm_gs_guc_feedback agent_to_cm_gs_guc_feedback_content;
    agent_to_cm_gs_guc_feedback_content.msg_type = MSG_AGENT_CM_GS_GUC_ACK;
    agent_to_cm_gs_guc_feedback_content.node = g_currentNode->node;
    agent_to_cm_gs_guc_feedback_content.instanceId = gsGucPtr->instanceId;
    agent_to_cm_gs_guc_feedback_content.type = syncMode;
    agent_to_cm_gs_guc_feedback_content.status = result;
    if (cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&agent_to_cm_gs_guc_feedback_content,
        sizeof(agent_to_cm_gs_guc_feedback_content)) != 0) {
        write_runlog(ERROR, "cm_client_send_msg send gs guc fail  1!\n");
        CloseConnToCmserver();
        return;
    }
}
#endif

static int FindInstancePathAndType(uint32 node, uint32 instanceId, char *dataPath, int *instanceType)
{
    uint32 i;
    uint32 j;
    errno_t rc;

    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].gtm == 1) {
            if ((g_node[i].gtmId == instanceId) && (g_node[i].node == node)) {
                rc = memcpy_s(dataPath, MAXPGPATH, g_node[i].gtmLocalDataPath, CM_PATH_LENGTH - 1);
                securec_check_errno(rc, (void)rc);
                *instanceType = INSTANCE_TYPE_GTM;
                return 0;
            }
        }

        if (g_node[i].coordinate == 1) {
            if ((g_node[i].coordinateId == instanceId) && (g_node[i].node == node)) {
                rc = memcpy_s(dataPath, MAXPGPATH, g_node[i].DataPath, CM_PATH_LENGTH - 1);
                securec_check_errno(rc, (void)rc);
                *instanceType = INSTANCE_TYPE_COORDINATE;
                return 0;
            }
        }

        for (j = 0; j < g_node[i].datanodeCount; j++) {
            if ((g_node[i].datanode[j].datanodeId == instanceId) && (g_node[i].node == node)) {
                rc = memcpy_s(dataPath, MAXPGPATH, g_node[i].datanode[j].datanodeLocalDataPath, CM_PATH_LENGTH - 1);
                securec_check_errno(rc, (void)rc);
                *instanceType = INSTANCE_TYPE_DATANODE;
                return 0;
            }
        }
    }

    return -1;
}

static void ProcessDdbOperFromCms(const CmSendDdbOperRes *msgDdbOper)
{
    write_runlog(LOG, "receive ddbOper(%d) from cms.\n", msgDdbOper->dbOper);
    (void)pthread_rwlock_wrlock(&(g_gtmCmDdbOperRes.lock));
    if (g_gtmCmDdbOperRes.ddbOperRes == NULL) {
        (void)pthread_rwlock_unlock(&(g_gtmCmDdbOperRes.lock));
        return;
    }
    errno_t rc = memcpy_s(g_gtmCmDdbOperRes.ddbOperRes, sizeof(CmSendDdbOperRes),
        msgDdbOper, sizeof(CmSendDdbOperRes));
    securec_check_errno(rc, (void)rc);
    (void)pthread_rwlock_unlock(&(g_gtmCmDdbOperRes.lock));
}

static void ProcessSharedStorageModeFromCms(const CmsSharedStorageInfo *recvMsg)
{
    errno_t rc;

    if (strcmp(g_doradoIp, recvMsg->doradoIp) != 0) {
        write_runlog(LOG, "cma recv g_doradoIp has change from \"%s\" to \"%s\"\n", g_doradoIp, recvMsg->doradoIp);
    } else {
        write_runlog(DEBUG1, "cma recv g_doradoIp = %s\n", recvMsg->doradoIp);
    }

    rc = strcpy_s(g_doradoIp, CM_IP_LENGTH, recvMsg->doradoIp);
    securec_check_errno(rc, (void)rc);
}

static void MsgCmAgentRestart(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_restart* cmToAgentRestartPtr = NULL;
    int instanceType;
    int ret;

    cmToAgentRestartPtr = (const cm_to_agent_restart *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_restart));
    if (cmToAgentRestartPtr == NULL) {
        return;
    }
    if (cmToAgentRestartPtr->node == 0 && cmToAgentRestartPtr->instanceId == 0) {
        write_runlog(ERROR, "receive cmagent exit request.\n\n");
        exit(-1);
    }
    ret = FindInstancePathAndType(
        cmToAgentRestartPtr->node, cmToAgentRestartPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            cmToAgentRestartPtr->node,
            cmToAgentRestartPtr->instanceId);
        return;
    }
    process_restart_command(dataPath, instanceType);
}

static void MsgCmAgentSwitchoverOrFast(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_switchover* msgTypeSwithoverPtr = NULL;
    int instanceType;
    int ret;
    uint32 term = InvalidTerm;

    msgTypeSwithoverPtr = (const cm_to_agent_switchover *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_switchover));
    if (msgTypeSwithoverPtr == NULL) {
        return;
    }
    term = msgTypeSwithoverPtr->term;
    ret = FindInstancePathAndType(
        msgTypeSwithoverPtr->node, msgTypeSwithoverPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeSwithoverPtr->node,
            msgTypeSwithoverPtr->instanceId);
        return;
    }
    if (msgTypePtr->msg_type == MSG_CM_AGENT_SWITCHOVER) {
        ProcessSwitchoverCommand(dataPath, instanceType, msgTypeSwithoverPtr->instanceId, term, false);
    } else {
        ProcessSwitchoverCommand(dataPath, instanceType, msgTypeSwithoverPtr->instanceId, term, true);
    }
}

static void MsgCmAgentFailover(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_failover* msgTypeFailoverPtr = NULL;
    int instanceType;
    int ret;
    uint32 term = InvalidTerm;

    msgTypeFailoverPtr = (const cm_to_agent_failover *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_failover));
    if (msgTypeFailoverPtr == NULL) {
        return;
    }
    term = msgTypeFailoverPtr->term;
    ret = FindInstancePathAndType(
        msgTypeFailoverPtr->node, msgTypeFailoverPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeFailoverPtr->node,
            msgTypeFailoverPtr->instanceId);
        return;
    }
    process_failover_command(dataPath, instanceType, msgTypeFailoverPtr->instanceId, term);
}

static void MsgCmAgentBuild(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_build* msgTypeBuildPtr = NULL;
    int instanceType;
    int ret;

    msgTypeBuildPtr = (const cm_to_agent_build *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_build));
    if (msgTypeBuildPtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeBuildPtr->node, msgTypeBuildPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeBuildPtr->node,
            msgTypeBuildPtr->instanceId);
        return;
    }
    ProcessBuildCommand(dataPath, instanceType, msgTypeBuildPtr);
}

static void MsgCmAgentCancelSession(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_cancel_session* msgTypeCancelSeesionPtr = NULL;
    int instanceType;
    int ret;

    write_runlog(LOG, "message type is MSG_CM_AGENT_CANCLE_SESSION.\n");
    msgTypeCancelSeesionPtr = (const cm_to_agent_cancel_session *)CmGetmsgbytesPtr(msg,
        sizeof(cm_to_agent_cancel_session));
    if (msgTypeCancelSeesionPtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeCancelSeesionPtr->node, msgTypeCancelSeesionPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance node is %u, instance is %u\n",
            msgTypeCancelSeesionPtr->node,
            msgTypeCancelSeesionPtr->instanceId);
        return;
    }
    Assert(instanceType == INSTANCE_TYPE_COORDINATE);

    /*
     * Send a feedback to server if we handle the notify cn msg
     * successfully, let the server clean up the notify cn msg map.
     */
    process_cancle_session_command(msgTypeCancelSeesionPtr);
}

static void MsgCmAgentSync(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    write_runlog(DEBUG1, "receive sync msg.\n");
}

static void MsgCmAgentRepSync(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_rep_sync* msgTypeRepSyncPtr = NULL;
    int instanceType;
    int ret;
    msgTypeRepSyncPtr = (const cm_to_agent_rep_sync *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_rep_sync));
    if (msgTypeRepSyncPtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeRepSyncPtr->node, msgTypeRepSyncPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeRepSyncPtr->node,
            msgTypeRepSyncPtr->instanceId);
        return;
    }
    process_rep_sync_command(dataPath, instanceType);
}

static void MsgCmAgentRepMostAvailable(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_rep_most_available* msgTypeRepMostAvailablePtr = NULL;
    int instanceType;
    int ret;

    msgTypeRepMostAvailablePtr = (const cm_to_agent_rep_most_available *)CmGetmsgbytesPtr(msg,
        sizeof(cm_to_agent_rep_most_available));
    if (msgTypeRepMostAvailablePtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(msgTypeRepMostAvailablePtr->node,
        msgTypeRepMostAvailablePtr->instanceId,
        dataPath,
        &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeRepMostAvailablePtr->node,
            msgTypeRepMostAvailablePtr->instanceId);
        return;
    }
    process_rep_most_available_command(dataPath, instanceType);
}

static void MsgCmAgentNotify(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_notify* msgTypeNotifyPtr = NULL;
    int instanceType;
    int ret;
    uint32 term = InvalidTerm;
    msgTypeNotifyPtr = (const cm_to_agent_notify *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_notify));
    if (msgTypeNotifyPtr == NULL || g_clusterType == V3SingleInstCluster) {
        return;
    }
    term = msgTypeNotifyPtr->term;
    ret = FindInstancePathAndType(
        msgTypeNotifyPtr->node, msgTypeNotifyPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeNotifyPtr->node,
            msgTypeNotifyPtr->instanceId);
        return;
    }
    process_notify_command(dataPath, instanceType, msgTypeNotifyPtr->role, term);
}

static void MsgCmAgentRestartByMode(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_restart_by_mode* msgTypeRestartByModePtr = NULL;
    int instanceType;
    int ret;

    msgTypeRestartByModePtr = (const cm_to_agent_restart_by_mode *)CmGetmsgbytesPtr(msg,
        sizeof(cm_to_agent_restart_by_mode));
    if (msgTypeRestartByModePtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeRestartByModePtr->node, msgTypeRestartByModePtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance  node is %u, instance is %u\n",
            msgTypeRestartByModePtr->node,
            msgTypeRestartByModePtr->instanceId);
        return;
    }
    process_restart_by_mode_command(dataPath,
        instanceType,
        msgTypeRestartByModePtr->role_old,
        msgTypeRestartByModePtr->role_new);
}

static void MsgCmAgentHeartbeat(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_heartbeat* msgTypeHeartbeatPtr =
        (const cm_to_agent_heartbeat *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_heartbeat));
    if (msgTypeHeartbeatPtr == NULL) {
        return;
    }
    process_heartbeat_command(msgTypeHeartbeatPtr->cluster_status);
    if (msgTypeHeartbeatPtr->healthInstanceId != 0) {
        g_healthInstance = msgTypeHeartbeatPtr->healthInstanceId;
    }
}

static void MsgCmAgentGsGuc(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    const cm_to_agent_gs_guc* msgTypeGsGucPtr =
        (const cm_to_agent_gs_guc *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_gs_guc));
    if (msgTypeGsGucPtr == NULL) {
        return;
    }
    process_gs_guc_command(msgTypeGsGucPtr);
#endif
}

static void MsgCmCtlCmserver(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_ctl_cmserver_status* cmToCtlCmserverStatusPtr =
        (const cm_to_ctl_cmserver_status *)CmGetmsgbytesPtr(msg, sizeof(cm_to_ctl_cmserver_status));
    if (cmToCtlCmserverStatusPtr == NULL) {
        return;
    }
    g_cmServerInstanceStatus = cmToCtlCmserverStatusPtr->local_role;
    g_cmServerInstancePending = cmToCtlCmserverStatusPtr->is_pending;
}

static void MsgCmServerRepairCnAck(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    g_cleanDropCnFlag = false;
    write_runlog(LOG, "receive repair cn ack msg.\n");
}

static void MsgCmAgentLockNoPrimary(const CM_Result *msg, char *dataPath, const cm_msg_type *msgTypePtr)
{
    int ret;
    const cm_to_agent_lock1 *msgTypeLock1Ptr =
        (const cm_to_agent_lock1 *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_lock1));
    if (msgTypeLock1Ptr == NULL) {
        return;
    }
    ret = ProcessLockNoPrimaryCmd(msgTypeLock1Ptr->instanceId);
    if (ret != 0) {
        write_runlog(ERROR, "set lock1 to instance %u failed.\n", msgTypeLock1Ptr->instanceId);
    }
}

static void MsgCmAgentLockChosenPrimary(const CM_Result *msg, char *dataPath, const cm_msg_type *msgTypePtr)
{
    int ret;
    const cm_to_agent_lock2 *msgTypeLock2Ptr =
        (const cm_to_agent_lock2 *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_lock2));
    if (msgTypeLock2Ptr == NULL) {
        return;
    }
    ret = ProcessLockChosenPrimaryCmd(msgTypeLock2Ptr);
    if (ret != 0) {
        write_runlog(ERROR, "set lock2 to instance %u failed.\n", msgTypeLock2Ptr->instanceId);
    }
}

static void MsgCmAgentUnlock(const CM_Result *msg, char *dataPath, const cm_msg_type *msgTypePtr)
{
    int ret;
    const cm_to_agent_unlock *msgTypeUnlockPtr =
        (const cm_to_agent_unlock *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_unlock));
    if (msgTypeUnlockPtr == NULL) {
        return;
    }
    ret = ProcessUnlockCmd(msgTypeUnlockPtr);
    if (ret != 0) {
        write_runlog(ERROR, "set unlock to instance %u failed.\n", msgTypeUnlockPtr->instanceId);
    }
}

static void MsgCmAgentFinishRedo(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    int instanceType;
    const cm_to_agent_finish_redo* msgTypeFinishRedoPtr =
        (const cm_to_agent_finish_redo *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_finish_redo));
    if (msgTypeFinishRedoPtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeFinishRedoPtr->node, msgTypeFinishRedoPtr->instanceId, dataPath, &instanceType);
    process_finish_redo_command(
        dataPath, msgTypeFinishRedoPtr->instanceId, msgTypeFinishRedoPtr->is_finish_redo_cmd_sent);
    if (ret != 0) {
        write_runlog(ERROR, "set finish redo to instance %u failed.\n", msgTypeFinishRedoPtr->instanceId);
    }
}

static void MsgCmAgentObsDeleteXlog(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    if (agent_backup_open == CLUSTER_OBS_STANDBY) {
        const cm_to_agent_obs_delete_xlog* msgTypeObsDeleteXlog =
            (const cm_to_agent_obs_delete_xlog *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_obs_delete_xlog));
        if (msgTypeObsDeleteXlog == NULL) {
            return;
        }
        int ret = ProcessObsDeleteXlogCmd(
            msgTypeObsDeleteXlog->instanceId, msgTypeObsDeleteXlog->lsn);
        if (ret != 0) {
            write_runlog(
                ERROR, "set obs delete xlog to instance %u failed.\n", msgTypeObsDeleteXlog->instanceId);
        }
    }
}

static void MsgCmAgentDnSyncList(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    int ret;
    const CmToAgentGsGucSyncList *msgTypeDoGsGuc =
        (const CmToAgentGsGucSyncList *)CmGetmsgbytesPtr(msg, sizeof(CmToAgentGsGucSyncList));
    if (msgTypeDoGsGuc == NULL) {
        return;
    }
    write_runlog(LOG, "receive gs guc dn msg from cm_server.\n");
    ret = ProcessGsGucDnCommand(msgTypeDoGsGuc);
    if (ret != 0) {
        write_runlog(ERROR, "instance(%u) do gs guc dn Failed\n", msgTypeDoGsGuc->instanceId);
    }
#endif
}

static void MsgCmAgentResStatusChanged(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const CmsReportResStatList *msgResStatusList =
        (const CmsReportResStatList *)CmGetmsgbytesPtr(msg, sizeof(CmsReportResStatList));
    if (msgResStatusList == NULL) {
        return;
    }
    ProcessResStatusChanged(msgResStatusList);
}

static void MsgCmAgentResStatusList(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const CmsReportResStatList *msgResStatusList =
        (const CmsReportResStatList *)CmGetmsgbytesPtr(msg, sizeof(CmsReportResStatList));
    if (msgResStatusList == NULL) {
        return;
    }
    ProcessResStatusList(msgResStatusList);
}

static void MsgCmAgentReportSetStatus(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const CmsReportSetDataResult *msgSetState =
        (const CmsReportSetDataResult *)CmGetmsgbytesPtr(msg, sizeof(CmsReportSetDataResult));
    if (msgSetState == NULL) {
        return;
    }
    ProcessResDataSetResult(msgSetState);
}

static void MsgCmAgentReportResData(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const CmsReportResData *msgResData = (const CmsReportResData *)CmGetmsgbytesPtr(msg, sizeof(CmsReportResData));
    if (msgResData == NULL) {
        return;
    }
    ProcessResDataFromCms(msgResData);
}

static void MsgCmAgentClientDdbOperAck(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const CmSendDdbOperRes *msgDdbOper =
        (const CmSendDdbOperRes *)CmGetmsgbytesPtr(msg, sizeof(CmSendDdbOperRes));
    if (msgDdbOper == NULL) {
        return;
    }
    ProcessDdbOperFromCms(msgDdbOper);
}

static void MsgCmsCmdOtherDefault(const cm_msg_type* msgTypePtr)
{
    write_runlog(LOG, "received command type %d is unknown \n", msgTypePtr->msg_type);
}

static void MsgCmAgentDnInstanceBarrier(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    if (agent_backup_open == CLUSTER_STREAMING_STANDBY) {
        cm_to_agent_barrier_info *barrierRespMsg =
            (cm_to_agent_barrier_info *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_barrier_info));
        if (barrierRespMsg == NULL) {
            return;
        }
        ret = ProcessDnBarrierInfoResp(barrierRespMsg);
        if (ret != 0) {
            write_runlog(ERROR, "cn instance(%u) barrier info failed to refresh.\n", barrierRespMsg->instanceId);
        }
    } else {
        write_runlog(ERROR, "MSG_CM_AGENT_COORDINATE_INSTANCE_BARRIER do not support in this backup mode, "
            "agent_backup_open: %d.\n", agent_backup_open);
    }
}

static void MsgCmAgentGetSharedStorageModeAck(const CM_Result *msg, char *dataPath, const cm_msg_type *msgTypePtr)
{
    const CmsSharedStorageInfo *recvMsg =
        (const CmsSharedStorageInfo*)CmGetmsgbytesPtr(msg, sizeof(CmsSharedStorageInfo));
    if (recvMsg == NULL) {
        return;
    }
    ProcessSharedStorageModeFromCms(recvMsg);
}

#ifdef ENABLE_MULTIPLE_NODES
static void MsgCmAgentNotifyCn(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int instanceType;
    int ret;
    bool status = false;
    const cm_to_agent_notify_cn *msgTypeNotifyCnPtr =
        (const cm_to_agent_notify_cn *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_notify_cn));
    if (msgTypeNotifyCnPtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeNotifyCnPtr->node, msgTypeNotifyCnPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance node is %u, instance is %u\n",
            msgTypeNotifyCnPtr->node,
            msgTypeNotifyCnPtr->instanceId);
        return;
    }
    Assert(instanceType == INSTANCE_TYPE_COORDINATE);

    if (agent_backup_open == CLUSTER_PRIMARY) {
        /*
         * Send a feedback to server if we handle the notify cn msg
         * successfully, let the server clean up the notify cn msg map.
         */
        if (!g_pgxcNodeConsist) {
            write_runlog(ERROR, "notify cn find error, pgxc_node is not match.\n");
            return;
        }
        status = process_notify_cn_command(msgTypeNotifyCnPtr, msg->gr_msglen);
    } else {
        write_runlog(LOG,
            "agent_backup_open is true, we should ignore notify cn, instance is %u\n",
            msgTypeNotifyCnPtr->instanceId);
        status = true;
    }
    send_notify_cn_feedback_msg(
        msgTypeNotifyCnPtr->instanceId, msgTypeNotifyCnPtr->notifyCount, status);
}

static void MsgCmAgentNotifyCnCentralNode(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    if (agent_backup_open == CLUSTER_PRIMARY) {
        const cm_to_agent_notify_cn_central_node *msgTypeNotifyCnCentralPtr =
            (const cm_to_agent_notify_cn_central_node *)CmGetmsgbytesPtr(
                msg, sizeof(cm_to_agent_notify_cn_central_node));
        if (msgTypeNotifyCnCentralPtr == NULL) {
            return;
        }
        (void)process_notify_ccn_command(msgTypeNotifyCnCentralPtr);
    }
}

static void MsgCmAgentDropCn(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    int instanceType;
    const cm_to_agent_drop_cn *msgTypeDropCnPtr =
        (const cm_to_agent_drop_cn *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_drop_cn));
    if (msgTypeDropCnPtr == NULL) {
        return;
    }
    ret = FindInstancePathAndType(
        msgTypeDropCnPtr->node, msgTypeDropCnPtr->instanceId, dataPath, &instanceType);
    if (ret != 0) {
        write_runlog(ERROR,
            "can't find the instance node is %u, instance is %u\n",
            msgTypeDropCnPtr->node,
            msgTypeDropCnPtr->instanceId);
        return;
    }
    Assert(instanceType == INSTANCE_TYPE_COORDINATE);
    /*
     * Send a feedback to server if we handle the notify cn msg
     * successfully, let the server clean up the notify cn msg map.
     */
    process_drop_cn_command(msgTypeDropCnPtr, true);
}

static void MsgCmAgentDropCnObsXlog(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_obs_delete_xlog* msgTypeObsDeleteXlog =
        (const cm_to_agent_obs_delete_xlog *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_obs_delete_xlog));
    if (msgTypeObsDeleteXlog == NULL) {
        return;
    }
    (void)pthread_rwlock_wrlock(&g_cnDropLock);
    g_obsDropCnXlog = msgTypeObsDeleteXlog->lsn;
    (void)pthread_rwlock_unlock(&g_cnDropLock);
}

static void MsgCmAgentDroppedCn(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    const cm_to_agent_drop_cn *msgTypeDropCnPtr =
        (const cm_to_agent_drop_cn *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_drop_cn));
    if (msgTypeDropCnPtr == NULL) {
        return;
    }
    if (g_syncDroppedCoordinator == false) {
        write_runlog(LOG, "MSG_CM_AGENT_DROPPED_CN: sync_dropped_coordinator change to true.\n");
    }
    g_syncDroppedCoordinator = true;
    process_drop_cn_command(msgTypeDropCnPtr, false);
}

static void MsgCmAgentNotifyCnRecover(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    const Cm2AgentNotifyCnRecoverByObs *notifyCnRecover =
        (const Cm2AgentNotifyCnRecoverByObs *)CmGetmsgbytesPtr(msg, sizeof(Cm2AgentNotifyCnRecoverByObs));
    if (notifyCnRecover == NULL) {
        return;
    }
    ret = ProcessNotifyCnRecoverCommand(notifyCnRecover);
    if (ret != 0) {
        write_runlog(ERROR, "instance(%u) notify recover failed\n", notifyCnRecover->instanceId);
    }
}

static void MsgCmAgentFullBackupCnObs(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    if (agent_backup_open == CLUSTER_PRIMARY) {
        const Cm2AgentBackupCn2Obs *backupCnMsg =
            (const Cm2AgentBackupCn2Obs *)CmGetmsgbytesPtr(msg, sizeof(Cm2AgentBackupCn2Obs));
        if (backupCnMsg == NULL) {
            return;
        }
        ret = ProcessBackupCn2ObsCommand(backupCnMsg);
        if (ret != 0) {
            write_runlog(ERROR, "instance(%u) backup to obs failed\n", backupCnMsg->instanceId);
        }
    } else {
        write_runlog(ERROR, "MSG_CM_AGENT_FULL_BACKUP_CN_OBS only support in not backup cluster.\n");
    }
}

static void MsgCmAgentRefreshObsDelText(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    if (agent_backup_open == CLUSTER_PRIMARY) {
        const Cm2AgentRefreshObsDelText *refreshDelTextMsg =
            (const Cm2AgentRefreshObsDelText *)CmGetmsgbytesPtr(msg, sizeof(Cm2AgentRefreshObsDelText));
        if (refreshDelTextMsg == NULL) {
            return;
        }
        ret = ProcessRefreshDelText2ObsCommand(refreshDelTextMsg);
        if (ret != 0) {
            write_runlog(ERROR, "instance(%u) refresh del text to obs failed\n", refreshDelTextMsg->instanceId);
        }
    } else {
        write_runlog(ERROR, "MSG_CM_AGENT_REFRESH_OBS_DEL_TEXT only support in not backup cluster.\n");
    }
}

static void MsgCmAgentCnInstanceBarrier(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr)
{
    int ret;
    if (agent_backup_open == CLUSTER_STREAMING_STANDBY) {
        cm_to_agent_barrier_info *barrierRespMsg =
            (cm_to_agent_barrier_info *)CmGetmsgbytesPtr(msg, sizeof(cm_to_agent_barrier_info));
        if (barrierRespMsg == NULL) {
            return;
        }
        ret = ProcessCnBarrierInfoResp(barrierRespMsg);
        if (ret != 0) {
            write_runlog(ERROR, "dn instance(%u) barrier info failed to refresh.\n", barrierRespMsg->instanceId);
        }
    } else {
        write_runlog(ERROR, "MSG_CM_AGENT_DATANODE_INSTANCE_BARRIER do not support in this backup mode, "
            "agent_backup_open: %d.\n", agent_backup_open);
    }
}
#endif

typedef void (*cms_cmd_proc_t)(const CM_Result* msg, char *dataPath, const cm_msg_type* msgTypePtr);

static cms_cmd_proc_t g_cmsCmdProcessor[MSG_CM_TYPE_CEIL] = {0};

void CmServerCmdProcessorInit(void)
{
    g_cmsCmdProcessor[MSG_CM_AGENT_RESTART]                     = MsgCmAgentRestart;
    g_cmsCmdProcessor[MSG_CM_AGENT_SWITCHOVER]                  = MsgCmAgentSwitchoverOrFast;
    g_cmsCmdProcessor[MSG_CM_AGENT_SWITCHOVER_FAST]             = MsgCmAgentSwitchoverOrFast;
    g_cmsCmdProcessor[MSG_CM_AGENT_FAILOVER]                    = MsgCmAgentFailover;
    g_cmsCmdProcessor[MSG_CM_AGENT_BUILD]                       = MsgCmAgentBuild;
    g_cmsCmdProcessor[MSG_CM_AGENT_CANCEL_SESSION]              = MsgCmAgentCancelSession;
    g_cmsCmdProcessor[MSG_CM_AGENT_SYNC]                        = MsgCmAgentSync;
    g_cmsCmdProcessor[MSG_CM_AGENT_REP_SYNC]                    = MsgCmAgentRepSync;
    g_cmsCmdProcessor[MSG_CM_AGENT_REP_MOST_AVAILABLE]          = MsgCmAgentRepMostAvailable;
    g_cmsCmdProcessor[MSG_CM_AGENT_NOTIFY]                      = MsgCmAgentNotify;
    g_cmsCmdProcessor[MSG_CM_AGENT_RESTART_BY_MODE]             = MsgCmAgentRestartByMode;
    g_cmsCmdProcessor[MSG_CM_AGENT_HEARTBEAT]                   = MsgCmAgentHeartbeat;
    g_cmsCmdProcessor[MSG_CM_AGENT_GS_GUC]                      = MsgCmAgentGsGuc;
    g_cmsCmdProcessor[MSG_CM_CTL_CMSERVER]                      = MsgCmCtlCmserver;
    g_cmsCmdProcessor[MSG_CM_SERVER_REPAIR_CN_ACK]              = MsgCmServerRepairCnAck;
    g_cmsCmdProcessor[MSG_CM_AGENT_LOCK_NO_PRIMARY]             = MsgCmAgentLockNoPrimary;
    g_cmsCmdProcessor[MSG_CM_AGENT_LOCK_CHOSEN_PRIMARY]         = MsgCmAgentLockChosenPrimary;
    g_cmsCmdProcessor[MSG_CM_AGENT_UNLOCK]                      = MsgCmAgentUnlock;
    g_cmsCmdProcessor[MSG_CM_AGENT_FINISH_REDO]                 = MsgCmAgentFinishRedo;
    g_cmsCmdProcessor[MSG_CM_AGENT_OBS_DELETE_XLOG]             = MsgCmAgentObsDeleteXlog;
    g_cmsCmdProcessor[MSG_CM_AGENT_DN_SYNC_LIST]                = MsgCmAgentDnSyncList;
    g_cmsCmdProcessor[MSG_CM_AGENT_RES_STATUS_CHANGED]          = MsgCmAgentResStatusChanged;
    g_cmsCmdProcessor[MSG_CM_AGENT_RES_STATUS_LIST]             = MsgCmAgentResStatusList;
    g_cmsCmdProcessor[MSG_CM_AGENT_REPORT_SET_STATUS]           = MsgCmAgentReportSetStatus;
    g_cmsCmdProcessor[MSG_CM_AGENT_REPORT_RES_DATA]             = MsgCmAgentReportResData;
    g_cmsCmdProcessor[MSG_CM_CLIENT_DDB_OPER_ACK]               = MsgCmAgentClientDdbOperAck;
    g_cmsCmdProcessor[MSG_CM_AGENT_DATANODE_INSTANCE_BARRIER]   = MsgCmAgentDnInstanceBarrier;
    g_cmsCmdProcessor[MSG_GET_SHARED_STORAGE_INFO_ACK]          = MsgCmAgentGetSharedStorageModeAck;
#ifdef ENABLE_MULTIPLE_NODES
    g_cmsCmdProcessor[MSG_CM_AGENT_NOTIFY_CN]                   = MsgCmAgentNotifyCn;
    g_cmsCmdProcessor[MSG_CM_AGENT_NOTIFY_CN_CENTRAL_NODE]      = MsgCmAgentNotifyCnCentralNode;
    g_cmsCmdProcessor[MSG_CM_AGENT_DROP_CN]                     = MsgCmAgentDropCn;
    g_cmsCmdProcessor[MSG_CM_AGENT_DROP_CN_OBS_XLOG]            = MsgCmAgentDropCnObsXlog;
    g_cmsCmdProcessor[MSG_CM_AGENT_DROPPED_CN]                  = MsgCmAgentDroppedCn;
    g_cmsCmdProcessor[MSG_CM_AGENT_NOTIFY_CN_RECOVER]           = MsgCmAgentNotifyCnRecover;
    g_cmsCmdProcessor[MSG_CM_AGENT_FULL_BACKUP_CN_OBS]          = MsgCmAgentFullBackupCnObs;
    g_cmsCmdProcessor[MSG_CM_AGENT_REFRESH_OBS_DEL_TEXT]        = MsgCmAgentRefreshObsDelText;
    g_cmsCmdProcessor[MSG_CM_AGENT_COORDINATE_INSTANCE_BARRIER] = MsgCmAgentCnInstanceBarrier;
#endif
}

static void ProcessCmServerCmd(const CM_Result* msg)
{
    cm_msg_type* msgTypePtr = NULL;
    char dataPath[MAXPGPATH];
    errno_t rc = memset_s(dataPath, MAXPGPATH, 0, MAXPGPATH);
    securec_check_errno(rc, (void)rc);

    msgTypePtr = (cm_msg_type *)CmGetmsgbytesPtr(msg, sizeof(cm_msg_type));
    if (msgTypePtr == NULL) {
        return;
    }
    if (msgTypePtr->msg_type >= MSG_CM_TYPE_CEIL || msgTypePtr->msg_type < 0) {
        write_runlog(ERROR, "recv cms msg, msg_type=%d invalid.\n", msgTypePtr->msg_type);
        return;
    }
    write_runlog(DEBUG5, "receive cms msg: %s \n", cluster_msg_int_to_string(msgTypePtr->msg_type));

    cms_cmd_proc_t procFunc = g_cmsCmdProcessor[msgTypePtr->msg_type];
    if (procFunc) {
        procFunc(msg, dataPath, msgTypePtr);
        return;
    }

    MsgCmsCmdOtherDefault(msgTypePtr);
}

static void RecvCmServerCmd(void)
{
    CM_Result* res = NULL;
    if (agent_cm_server_connect == NULL) {
        write_runlog(DEBUG5, "cm_agent is not connect to the cm server \n");
        return;
    }
    int ret = cmpqReadData(agent_cm_server_connect);
    if (ret < 0) {
        write_runlog(LOG,
            "cm_agent is not connect to the cmserver ret=%d,errMsg:%s\n",
            ret,
            agent_cm_server_connect->errorMessage.data);
        CloseConnToCmserver();
        return;
    }
    if (ret > 0) {
        do {
            res = cmpqGetResult(agent_cm_server_connect);
            if (res != NULL) {
                ProcessCmServerCmd(res);
            }
        } while (res != NULL);
    }
}

static void EtcdCurrentTimeReport(void)
{
    agent_to_cm_current_time_report report_msg;
    int ret;

    if (agent_cm_server_connect == NULL || g_currentNode->etcd != 1) {
        return;
    }

    report_msg.msg_type = MSG_AGENT_CM_ETCD_CURRENT_TIME;
    report_msg.nodeid = g_nodeId;
    report_msg.etcd_time = (pg_time_t)time(NULL);

    ret = cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&report_msg, sizeof(agent_to_cm_current_time_report));
    if (ret != 0) {
        write_runlog(ERROR, "cm_client_send_msg the current time failed!\n");
        CloseConnToCmserver();
        return;
    }

    write_runlog(DEBUG5,
        "cm_client_send_msg send the current time is %ld,node is %u\n",
        report_msg.etcd_time,
        report_msg.nodeid);
}

static bool IsServerHeartbeatTimeout()
{
    struct timespec now = {0};
    (void)clock_gettime(CLOCK_MONOTONIC, &now);

    return (now.tv_sec - g_serverHeartbeatTime.tv_sec) >= (time_t)agent_heartbeat_timeout;
}

static void CheckLogLevel(int diskUsageCheckTimes, int* logLevel)
{
    if (diskUsageCheckTimes == 0) {
        *logLevel = LOG;
    } else {
        *logLevel = DEBUG1;
    }
}

static void SendCmDdbOper()
{
    if (agent_cm_server_connect == NULL) {
        return;
    }
    if (g_currentNode->coordinate == 0) {
        return;
    }
    CltSendDdbOper sendOper = {0};
    (void)pthread_rwlock_wrlock(&(g_gtmSendDdbOper.lock));
    if (g_gtmSendDdbOper.sendOper == NULL) {
        (void)pthread_rwlock_unlock(&(g_gtmSendDdbOper.lock));
        return;
    }
    if (g_gtmSendDdbOper.sendOper->dbOper == DDB_INIT_OPER) {
        (void)pthread_rwlock_unlock(&(g_gtmSendDdbOper.lock));
        return;
    }
    errno_t rc = memcpy_s(&sendOper, sizeof(CltSendDdbOper), g_gtmSendDdbOper.sendOper, sizeof(CltSendDdbOper));
    securec_check_errno(rc, (void)rc);
    int32 ret = cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&sendOper, sizeof(CltSendDdbOper));
    if (ret != 0) {
        write_runlog(ERROR, "send cm_agent ddb oper failed!\n");
        CloseConnToCmserver();
    }
    (void)pthread_rwlock_unlock(&(g_gtmSendDdbOper.lock));
    write_runlog(LOG, "send cm_agent ddb oper(%d), msgType is %d.\n", sendOper.dbOper, sendOper.msgType);
}

static void GetDoradoIpFromCms()
{
    if (!g_isNeedGetDoradoIp) {
        return;
    }
    GetSharedStorageInfo sendMsg = {0};
    sendMsg.msg_type = (int)MSG_GET_SHARED_STORAGE_INFO;
    if (cm_client_send_msg(agent_cm_server_connect, 'C', (char*)&sendMsg, sizeof(sendMsg)) != 0) {
        write_runlog(ERROR, "GetDoradoIpFromCms send msg to cms fail!\n");
    }
}

static void ReportInstanceStatus()
{
    if (agent_cm_server_connect == NULL || agent_cm_server_connect->status != CONNECTION_OK) {
        return;
    }

    InstancesStatusCheckAndReport();
    AgentSendHeartbeat();
    SendCmDdbOper();
    GetDoradoIpFromCms();
}

void* SendCmsMsgMain(void* arg)
{
    uint32 etcdTimeReportInterval = 0;
    uint32 diskCheckReportInterval = 0;
    struct timespec lastReportTime = {0, 0};
    struct timespec currentTime = {0, 0};
    long expiredTime = 0;
    (void)clock_gettime(CLOCK_MONOTONIC, &lastReportTime);
    int diskUsageCheckTimes = 0;
    int logLevel = DEBUG1;
    for (;;) {
        if (g_shutdownRequest) {
            CloseConnToCmserver();
            cm_sleep(5);
            continue;
        }
        (void)clock_gettime(CLOCK_MONOTONIC, &currentTime);
        expiredTime = (currentTime.tv_sec - lastReportTime.tv_sec);
        write_runlog(DEBUG5, "send cms msg expiredTime=%ld,currentTime=%ld,lastReportTime=%ld\n", expiredTime,
            currentTime.tv_sec, lastReportTime.tv_sec);
        if (expiredTime >= 1) {
            if ((agent_cm_server_connect != NULL) && IsServerHeartbeatTimeout()) {
                write_runlog(LOG, "connection to cm_server %u seconds timeout expired .\n",
                    agent_heartbeat_timeout);
                g_cmServerNeedReconnect = true;
            }
            if (g_cmServerNeedReconnect) {
                CloseConnToCmserver();
            }
            ReportInstanceStatus();
            /* Check and report the disk usage per 5s */
            if (diskCheckReportInterval >= g_agentDiskUsageStatusCheckTimes) {
                /* Report at level LOG per 1 minute */
                diskUsageCheckTimes %= 12;
                CheckLogLevel(diskUsageCheckTimes, &logLevel);
                DiskUsageCheckAndReport(logLevel);
                diskCheckReportInterval = 0;
                diskUsageCheckTimes++;
            }
            diskCheckReportInterval++;
            if (etcdTimeReportInterval >= AGENT_REPORT_ETCD_CYCLE || etcdTimeReportInterval == 0) {
                EtcdCurrentTimeReport();
                etcdTimeReportInterval = 0;
            }
            etcdTimeReportInterval++;
            (void)clock_gettime(CLOCK_MONOTONIC, &lastReportTime);
        }
        
        RecvCmServerCmd();
        cm_usleep(AGENT_RECV_CYCLE);
    }
    return NULL;
}
