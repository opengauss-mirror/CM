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
 * cms_process_messages_agent.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_server/cms_process_messages_agent.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cms_global_params.h"
#include "cms_process_messages.h"
#include "cms_ddb.h"
#include "cms_common.h"
#include "cs_ssl.h"
#include "cms_arbitrate_cluster.h"
#include "cm_util.h"

using namespace std;

void process_agent_to_cm_fenced_UDF_status_report_msg(
    const agent_to_cm_fenced_UDF_status_report *agent_to_cm_fenced_UDF_status_ptr)
{
    if (agent_to_cm_fenced_UDF_status_ptr->nodeid >= CM_NODE_MAXNUM) {
        write_runlog(ERROR, "udf nodeId(%u) is more than %d, cannot get udf report msg.\n",
            agent_to_cm_fenced_UDF_status_ptr->nodeid, CM_NODE_MAXNUM);
        return;
    }
    (void)pthread_rwlock_wrlock(&(g_fenced_UDF_report_status_ptr[agent_to_cm_fenced_UDF_status_ptr->nodeid].lk_lock));
    g_fenced_UDF_report_status_ptr[agent_to_cm_fenced_UDF_status_ptr->nodeid].heart_beat = 0;
    g_fenced_UDF_report_status_ptr[agent_to_cm_fenced_UDF_status_ptr->nodeid].status =
        agent_to_cm_fenced_UDF_status_ptr->status;
    (void)pthread_rwlock_unlock(&(g_fenced_UDF_report_status_ptr[agent_to_cm_fenced_UDF_status_ptr->nodeid].lk_lock));

    write_runlog(DEBUG5, "agent_to_cm_fenced_UDF_status_ptr process succeed.\n");
}
static void deal_keep_heart_beat_time_out(MsgRecvInfo *recvMsgInfo,
    const agent_to_cm_heartbeat *agent_to_cm_heartbeat_ptr, uint32 group_index, int member_index)
{
    /* keep heartbeat timeout doesn't work. */
    if (instance_keep_heartbeat_timeout == 0) {
        return;
    }

    /* record down instance was lost within last one second. */
    cm_instance_report_status *report = &g_instance_group_report_status_ptr[group_index].instance_status;
    write_runlog(LOG, "can't receive heart beat of instance %u for %d sec.\n", agent_to_cm_heartbeat_ptr->instanceId,
        report->command_member[member_index].keep_heartbeat_timeout);

    if (report->command_member[member_index].keep_heartbeat_timeout >= (int)instance_heartbeat_timeout &&
        agent_to_cm_heartbeat_ptr->instanceType == INSTANCE_TYPE_DATANODE &&
        report->data_node_member[member_index].local_status.local_role == INSTANCE_ROLE_PRIMARY) {
        report->data_node_member[member_index].local_status.local_role = INSTANCE_ROLE_UNKNOWN;
        write_runlog(WARNING, "can't receive report msg of primary dn %u for %d sec, set dn INSTANCE_ROLE_UNKNOWN.\n",
            agent_to_cm_heartbeat_ptr->instanceId, report->command_member[member_index].keep_heartbeat_timeout);
    }

    /* do nothing if no timeout is triggered. */
    if (report->command_member[member_index].keep_heartbeat_timeout <= (int)instance_keep_heartbeat_timeout) {
        return;
    }

    /* whether or not to restart instance while CN is always true. */
    bool sendRestart = (agent_to_cm_heartbeat_ptr->instanceType == INSTANCE_TYPE_COORDINATE) ? true : false;

    if (agent_to_cm_heartbeat_ptr->instanceType == INSTANCE_TYPE_DATANODE &&
        (report->data_node_member[member_index].local_status.db_state == INSTANCE_HA_STATE_UNKONWN ||
        report->data_node_member[member_index].local_status.db_state == INSTANCE_HA_STATE_NORMAL)) {
        sendRestart = true;
    }

    // gtm connect_status was last success(or reset by timeout) stat when hang, we can't rely on it.
    if (agent_to_cm_heartbeat_ptr->instanceType == INSTANCE_TYPE_GTM &&
        (report->gtm_member[member_index].local_status.connect_status == CON_OK ||
        report->gtm_member[member_index].local_status.connect_status == CON_UNKNOWN)) {
        /* restart normal GTM if it was OK. */
        sendRestart = true;

        if (report->gtm_member[member_index].local_status.local_role == INSTANCE_ROLE_PRIMARY) {
            for (int i = 0; i < g_instance_role_group_ptr[group_index].count && sendRestart; i++) {
                if (report->gtm_member[i].local_status.local_role == INSTANCE_ROLE_STANDBY &&
                    report->gtm_member[i].local_status.connect_status == CON_OK) {
                    write_runlog(LOG,
                        "instance %u role is standby, and db state is normal, "
                        "will not set keep timeout.\n",
                        agent_to_cm_heartbeat_ptr->instanceId);

                    /* To avoid mistake, don't restart primary GTM if some standby can connect to it. */
                    sendRestart = false;
                }
            }
        }
    }

    if (sendRestart) {
        cm_to_agent_restart restart_msg;

        /* build the restart message for timeout instance. */
        restart_msg.msg_type = MSG_CM_AGENT_RESTART;
        restart_msg.node = agent_to_cm_heartbeat_ptr->node;
        restart_msg.instanceId = agent_to_cm_heartbeat_ptr->instanceId;

        /* send message to CMA to restart CN instance. */
        write_runlog(LOG, "restart %u, there is not report msg for %d sec.\n", agent_to_cm_heartbeat_ptr->instanceId,
            report->command_member[member_index].keep_heartbeat_timeout);
        WriteKeyEventLog(KEY_EVENT_RESTART, agent_to_cm_heartbeat_ptr->instanceId,
            "send restart message, node=%u, instanceId=%u", agent_to_cm_heartbeat_ptr->node,
            agent_to_cm_heartbeat_ptr->instanceId);
        (void)RespondMsg(recvMsgInfo, 'S', (char *)(&restart_msg), sizeof(cm_to_agent_restart));

        /* after restart is sent, reset keep heartbeat timeout counter. */
        report->command_member[member_index].keep_heartbeat_timeout = 0;
    }
}

static uint32 AssignDnForCrossClusterBuild(uint32 nodeId)
{
    uint32 healthDnCount = 0;
    size_t healthDnArrLen = g_dynamic_header->relationCount * sizeof(uint32);
    uint32 *healthDnArr = (uint32 *)malloc(healthDnArrLen);
    if (healthDnArr == NULL) {
        write_runlog(FATAL, "malloc memory healthDnArr failed!\n");
        return 0;
    }
    errno_t rc = memset_s(healthDnArr, healthDnArrLen, 0, healthDnArrLen);
    securec_check_errno(rc, FREE_AND_RESET(healthDnArr));

    for (uint32 i = 0; i < g_dynamic_header->relationCount; i++) {
        if (g_instance_role_group_ptr[i].instanceMember[0].instanceType != INSTANCE_TYPE_DATANODE) {
            continue;
        }
        for (int j = 0; j < g_instance_role_group_ptr[i].count; j++) {
            cm_local_replconninfo dnStatus =
                g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].local_status;
            if ((dnStatus.local_role == INSTANCE_ROLE_PRIMARY || dnStatus.local_role == INSTANCE_ROLE_STANDBY) &&
                dnStatus.db_state == INSTANCE_HA_STATE_NORMAL) {
                healthDnArr[healthDnCount] = g_instance_role_group_ptr[i].instanceMember[j].instanceId;
                healthDnCount++;
                break;
            }
        }
    }

    if (healthDnCount == 0) {
        FREE_AND_RESET(healthDnArr);
        return 0;
    }

    uint32 dnForCrossClusterBuild = healthDnArr[nodeId % healthDnCount];
    FREE_AND_RESET(healthDnArr);
    return dnForCrossClusterBuild;
}

static uint32 ProvideHealthyInstanceForAgent(uint32 nodeId)
{
    if (backup_open == CLUSTER_STREAMING_STANDBY) {
        return AssignDnForCrossClusterBuild(nodeId);
    }
#ifdef ENABLE_MULTIPLE_NODES
    return AssignCnForAutoRepair(nodeId);
#else
    return 0;
#endif
}

void process_agent_to_cm_heartbeat_msg(
    MsgRecvInfo* recvMsgInfo, const agent_to_cm_heartbeat *agent_to_cm_heartbeat_ptr)
{
    uint32 group_index = 0;
    int member_index = 0;
    int ret;

    if (agent_to_cm_heartbeat_ptr->instanceType == CM_AGENT) {
        write_runlog(DEBUG5, "agent_to_cm_heartbeat_ptr->instanceType=CM_AGENT\n");
        /* respond heartbeat to cm_agent */
        cm_to_agent_heartbeat msgServerHeartbeat = {0};
        msgServerHeartbeat.msg_type = MSG_CM_AGENT_HEARTBEAT;
        msgServerHeartbeat.node = agent_to_cm_heartbeat_ptr->node;
        msgServerHeartbeat.type = CM_SERVER;

        /* clean kill time, because cma can send heart beat msg. */
        for (uint32 i = 0; i < g_dynamic_header->relationCount; i++) {
            if (g_instance_group_report_status_ptr[i].instance_status.cma_kill_instance_timeout == 0) {
                continue;
            }
            for (int j = 0; j < g_instance_role_group_ptr[i].count; j++) {
                if ((msgServerHeartbeat.node == g_instance_role_group_ptr[i].instanceMember[j].node) &&
                    (g_instance_role_group_ptr[i].instanceMember[j].instanceType == INSTANCE_TYPE_DATANODE) &&
                    (g_instance_role_group_ptr[i].instanceMember[j].role == INSTANCE_ROLE_PRIMARY)) {
                    write_runlog(
                        LOG, "get cma(%u) heart beat, will reset kill static primary time.\n", msgServerHeartbeat.node);
                    g_instance_group_report_status_ptr[i].instance_status.cma_kill_instance_timeout = 0;
                    break;
                }
            }
        }

        for (uint32 i = 0; i < g_dynamic_header->relationCount; i++) {
            if (g_instance_role_group_ptr[i].instanceMember[0].instanceType == INSTANCE_TYPE_COORDINATE &&
                msgServerHeartbeat.node == g_instance_role_group_ptr[i].instanceMember[0].node) {
                g_instance_group_report_status_ptr[i].instance_status.coordinatemember.cma_fault_timeout_to_killcn = 0;
                break;
            }
        }

        /* If agent request the cluster status, first we should check it. */
        if (agent_to_cm_heartbeat_ptr->cluster_status_request) {
            set_cluster_status();
            msgServerHeartbeat.cluster_status = g_HA_status->status;
        } else {
            msgServerHeartbeat.cluster_status = CM_STATUS_UNKNOWN;
        }

        msgServerHeartbeat.healthInstanceId = ProvideHealthyInstanceForAgent(msgServerHeartbeat.node);

        (void)RespondMsg(recvMsgInfo, 'S', (char *)(&msgServerHeartbeat), sizeof(msgServerHeartbeat), DEBUG5);
        NotifyResRegOrUnreg();
    } else {
        write_runlog(DEBUG5, "agent_to_cm_heartbeat_ptr->instanceType=CM_CTL\n");
        ret = find_node_in_dynamic_configure(agent_to_cm_heartbeat_ptr->node,
            agent_to_cm_heartbeat_ptr->instanceId,
            &group_index,
            &member_index);
        if (ret != 0) {
            write_runlog(LOG,
                "can't find the instance(node =%u instanceid =%u)\n",
                agent_to_cm_heartbeat_ptr->node,
                agent_to_cm_heartbeat_ptr->instanceId);
            return;
        }
        (void)pthread_rwlock_wrlock(&(g_instance_group_report_status_ptr[group_index].lk_lock));
        g_instance_group_report_status_ptr[group_index].instance_status.command_member[member_index].heat_beat = 0;
        if ((member_index != (int)(g_dn_replication_num - 1) && !g_multi_az_cluster && g_dn_replication_num == 3) ||
            g_multi_az_cluster) {
            deal_keep_heart_beat_time_out(recvMsgInfo, agent_to_cm_heartbeat_ptr, group_index, member_index);
        }
        (void)pthread_rwlock_unlock(&(g_instance_group_report_status_ptr[group_index].lk_lock));
        if (member_index == (int)(g_dn_replication_num - 1) && !g_multi_az_cluster &&
            !g_single_node_cluster && g_dn_replication_num == 3) {
            g_instance_group_report_status_ptr[group_index]
                .instance_status.data_node_member[member_index]
                .local_status.local_role = INSTANCE_ROLE_DUMMY_STANDBY;
            g_instance_group_report_status_ptr[group_index]
                .instance_status.data_node_member[member_index]
                .local_status.db_state = INSTANCE_HA_STATE_NORMAL;
        }
    }
}

void process_agent_to_cm_disk_usage_msg(const AgentToCmDiskUsageStatusReport *diskUsage)
{
    const int maxUsage = 100;
    if (diskUsage->dataPathUsage > maxUsage || diskUsage->logPathUsage > maxUsage) {
        write_runlog(ERROR,
            "the percentage of disk usage is illegal, it must be [0-100], dataDiskUsage=%u, logDiskUsage=%u.\n",
            diskUsage->dataPathUsage, diskUsage->logPathUsage);
        return;
    }

    /* find and set instance's log&data usage */
    for (uint32 i = 0; i < g_node_num; i++) {
        DynamicNodeReadOnlyInfo *curNodeInfo = &g_dynamicNodeReadOnlyInfo[i];
        /* CN */
        if (diskUsage->instanceType == INSTANCE_TYPE_COORDINATE) {
            if (diskUsage->instanceId == curNodeInfo->coordinateNode.instanceId) {
                curNodeInfo->coordinateNode.dataDiskUsage = diskUsage->dataPathUsage;
                curNodeInfo->coordinateNode.readOnly = diskUsage->readOnly;
                curNodeInfo->coordinateNode.instanceType = INSTANCE_TYPE_COORDINATE;
                curNodeInfo->logDiskUsage = diskUsage->logPathUsage;
                return;
            }
        }
        /* DN */
        for (uint32 j = 0; j < curNodeInfo->dataNodeCount; j++) {
            DataNodeReadOnlyInfo *curDn = &curNodeInfo->dataNode[j];
            if (diskUsage->instanceId == curDn->instanceId) {
                curDn->dataDiskUsage = diskUsage->dataPathUsage;
                curDn->readOnly = diskUsage->readOnly;
                curDn->instanceType = INSTANCE_TYPE_DATANODE;
                curNodeInfo->logDiskUsage = diskUsage->logPathUsage;
                return;
            }
        }
    }
}

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
bool IsInstanceIdInGroup(uint32 groupIndex, int newInstanceId)
{
    if (newInstanceId <= 0) {
        return false;
    }
    for (int i = 0; i < g_instance_role_group_ptr[groupIndex].count; ++i) {
        if (newInstanceId == (int)g_instance_role_group_ptr[groupIndex].instanceMember[i].instanceId) {
            return true;
        }
    }
    return false;
}

void SetInstanceSyncList(DatanodeSyncList *list, uint32 groupIndex, uint32 instanceId)
{
    errno_t rc = memset_s(list, sizeof(DatanodeSyncList), 0, sizeof(DatanodeSyncList));
    securec_check_errno(rc, (void)rc);
    int index = 0;
    for (int k = 0; k < g_instance_role_group_ptr[groupIndex].count; ++k) {
        uint32 newInstanceId = g_instance_role_group_ptr[groupIndex].instanceMember[k].instanceId;
        write_runlog(DEBUG1, "instanceId(%u): find '*': syncList[%d]=%u.\n", instanceId, index, newInstanceId);
        list->dnSyncList[index++] = newInstanceId;
    }
    list->count = index;
}

DatanodeSyncList GetSyncList(uint32 groupIndex, uint32 instanceId, char *syncList, size_t len)
{
    DatanodeSyncList list;
    errno_t rc = memset_s(&list, sizeof(DatanodeSyncList), 0, sizeof(DatanodeSyncList));
    securec_check_errno(rc, (void)rc);
    list.dnSyncList[0] = instanceId;
    if (len == 0) {
        write_runlog(ERROR, "instanceId(%u) the synclist(%s) len is 0.\n", instanceId, syncList);
        list.count = -1;
        return list;
    }
    int index = 1;
    char *syncListStr = syncList;
    while (*syncListStr != '\0') {
        if (index >= CM_PRIMARY_STANDBY_NUM) {
            if (strstr(syncListStr, "dn_") != NULL) {
                write_runlog(
                    ERROR, "instanceId(%u) the synclist is more than %d.\n", instanceId, CM_PRIMARY_STANDBY_NUM);
                list.count = -1;
                return list;
            }
            break;
        }
        // * is all instanceId.
        if (*syncListStr == '*') {
            SetInstanceSyncList(&list, groupIndex, instanceId);
            return list;
        }
        // dn instaneId begin from 'dn_'
        if (strlen(syncListStr) >= strlen("dn_") && strncmp(syncListStr, "dn_", strlen("dn_")) == 0) {
            // syncListStr is dn_6001, instance need to skip 'dn_'
            syncListStr += strlen("dn_");
            int newInstanceId = (int)strtol(syncListStr, &syncListStr, 10);
            if (!IsInstanceIdInGroup(groupIndex, newInstanceId)) {
                write_runlog(ERROR, "InstanceId(%u) synchronous_standby_names is invalid(%d).\n",
                    instanceId, newInstanceId);
                list.count = -1;
                return list;
            }
            write_runlog(DEBUG1, "instanceId(%u) syncList[%d]=%d.\n", instanceId, index, newInstanceId);
            list.dnSyncList[index++] = (uint32)newInstanceId;
            continue;
        }
        syncListStr++;
    }
    list.count = index;
    return list;
}

void ProcessGetDnSyncListMsg(AgentToCmserverDnSyncList *agentDnSyncList)
{
    if (agentDnSyncList->instanceType != INSTANCE_TYPE_DATANODE) {
        write_runlog(ERROR, "cms get instance(%u) is not dn, this type is %d.\n",
            agentDnSyncList->instanceId, agentDnSyncList->instanceType);
        return;
    }
    agentDnSyncList->dnSynLists[DN_SYNC_LEN - 1] = '\0';
    uint32 groupIdx = 0;
    int memIdx = 0;
    uint32 node = agentDnSyncList->node;
    uint32 instanceId = agentDnSyncList->instanceId;
    // get groupIndex, memberIndex
    int ret = find_node_in_dynamic_configure(node, instanceId, &groupIdx, &memIdx);
    if (ret != 0) {
        write_runlog(LOG, "can't find the instance(node =%u  instanceid =%u)\n", node, instanceId);
        return;
    }
    char *syncList = agentDnSyncList->dnSynLists;
    if (strcmp(syncList, "") == 0 || strlen(syncList) == 0) {
        return;
    }
    DatanodeSyncList list;
    errno_t rc = memset_s(&list, sizeof(DatanodeSyncList), 0, sizeof(DatanodeSyncList));
    securec_check_errno(rc, (void)rc);
    cm_instance_datanode_report_status *roleMember =
        g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member;
    char syncListStr[MAX_PATH_LEN] = {0};
    char afterSortsyncListStr[MAX_PATH_LEN] = {0};
    list = GetSyncList(groupIdx, instanceId, syncList, strlen(syncList));
    if (list.count == -1) {
        roleMember[memIdx].dnSyncList.count = -1;
        return;
    }
    if (log_min_messages <= DEBUG1) {
        GetSyncListString(&list, syncListStr, sizeof(syncListStr));
    }
#undef qsort
    qsort(list.dnSyncList, list.count, sizeof(uint32), node_index_Comparator);
    if (log_min_messages <= DEBUG1) {
        GetSyncListString(&list, afterSortsyncListStr, sizeof(afterSortsyncListStr));
        write_runlog(DEBUG1, "instanceId(%u) syncListStr is [%s], afterSortsyncListStr is [%s].\n",
            instanceId, syncListStr, afterSortsyncListStr);
    }
    rc = memset_s(&(roleMember[memIdx].dnSyncList), sizeof(DatanodeSyncList), 0, sizeof(DatanodeSyncList));
    securec_check_errno(rc, (void)rc);
    rc = memcpy_s(&(roleMember[memIdx].dnSyncList), sizeof(DatanodeSyncList), &list, sizeof(DatanodeSyncList));
    securec_check_errno(rc, (void)rc);
    roleMember[memIdx].syncDone = agentDnSyncList->syncDone;
}
#endif

static void CmsClearKerberosInfo()
{
    char kerberosKey[MAX_PATH_LEN] = {0};
    char kerberosValue[MAX_PATH_LEN] = {0};
    errno_t rc;
    /* Clear kerberos global variables info */
    rc = memset_s(&g_kerberos_group_report_status,
        sizeof(kerberos_group_report_status), 0, sizeof(kerberos_group_report_status));
    securec_check_errno(rc, (void)rc);

    status_t st = CM_SUCCESS;
    /* Clear kerberos ddb info */
    for (int i = 0; i < KERBEROS_NUM; i++) {
        rc = snprintf_s(kerberosKey, MAX_PATH_LEN, MAX_PATH_LEN - 1, "/%s/kerberosKey%d", pw->pw_name, i);
        securec_check_intval(rc, (void)rc);
        rc = snprintf_s(kerberosValue, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%d", 0);
        securec_check_intval(rc, (void)rc);
        st = SetKV2Ddb(kerberosKey, MAX_PATH_LEN, kerberosValue, MAX_PATH_LEN, NULL);
        if (st != CM_SUCCESS) {
            write_runlog(ERROR, "ddb set(SetOnlineStatusToDdb) failed. key=%s, value=%s,\n",
                kerberosKey, kerberosValue);
            continue;
        }
        write_runlog(LOG, "clear ddb /%s/kerberosKey%d successfully.\n", pw->pw_name, i);
    }
    return;
}

/* cm server process the msg from cm_agent kerberos info and save these */
void process_agent_to_cm_kerberos_status_report_msg(
    agent_to_cm_kerberos_status_report *agent_to_cm_kerberos_status_ptr)
{
    agent_to_cm_kerberos_status_ptr->kerberos_ip[CM_IP_LENGTH - 1] = '\0';
    agent_to_cm_kerberos_status_ptr->nodeName[CM_NODE_NAME - 1] = '\0';
    agent_to_cm_kerberos_status_ptr->role[MAXLEN - 1] = '\0';
    errno_t rc = 0;
    char kerberosDdbKey[MAX_PATH_LEN] = {0};
    char kerberosDdbValue[MAX_PATH_LEN] = {0};
    char *kerberosIpPtr = g_kerberos_group_report_status.kerberos_status.kerberos_ip[0];
    char *kerberosIpPtr1 = g_kerberos_group_report_status.kerberos_status.kerberos_ip[1];

    status_t st = CM_SUCCESS;
    if (agent_to_cm_kerberos_status_ptr->port != 0) {
        if (*kerberosIpPtr != '\0' && *kerberosIpPtr1 != '\0' &&
            strcmp(agent_to_cm_kerberos_status_ptr->kerberos_ip, kerberosIpPtr) &&
            strcmp(agent_to_cm_kerberos_status_ptr->kerberos_ip, kerberosIpPtr1)) {
            CmsClearKerberosInfo();
        }

        (void)pthread_rwlock_wrlock(&g_kerberos_group_report_status.lk_lock);
        if (*kerberosIpPtr == '\0' || strcmp(agent_to_cm_kerberos_status_ptr->kerberos_ip, kerberosIpPtr) == 0) {
            g_kerberos_group_report_status.kerberos_status.node[0] = agent_to_cm_kerberos_status_ptr->node;
            g_kerberos_group_report_status.kerberos_status.port[0] = agent_to_cm_kerberos_status_ptr->port;
            g_kerberos_group_report_status.kerberos_status.status[0] = agent_to_cm_kerberos_status_ptr->status;
            g_kerberos_group_report_status.kerberos_status.heartbeat[0] = 0;

            /* Write the port, kerberos_ip, node and node name to ddb when cm_server switched */
            rc = snprintf_s(kerberosDdbKey, MAX_PATH_LEN, MAX_PATH_LEN - 1, "/%s/kerberosKey0", pw->pw_name);
            securec_check_intval(rc, (void)rc);
            rc = snprintf_s(kerberosDdbValue,
                MAX_PATH_LEN,
                MAX_PATH_LEN - 1,
                "%u,%s,%s,%u",
                agent_to_cm_kerberos_status_ptr->node,
                agent_to_cm_kerberos_status_ptr->nodeName,
                agent_to_cm_kerberos_status_ptr->kerberos_ip,
                agent_to_cm_kerberos_status_ptr->port);
            securec_check_intval(rc, (void)rc);
            st = SetKV2Ddb(kerberosDdbKey, MAX_PATH_LEN, kerberosDdbValue, MAX_PATH_LEN, NULL);
            if (st != CM_SUCCESS) {
                write_runlog(ERROR, "ddb set(SetOnlineStatusToDdb) failed. key=%s, value=%s,.\n",
                    kerberosDdbKey, kerberosDdbValue);
                return;
            }

            rc = strncpy_s(g_kerberos_group_report_status.kerberos_status.kerberos_ip[0],
                CM_IP_LENGTH,
                agent_to_cm_kerberos_status_ptr->kerberos_ip,
                strlen(agent_to_cm_kerberos_status_ptr->kerberos_ip));
            securec_check_errno(rc, (void)rc);

            rc = strncpy_s(g_kerberos_group_report_status.kerberos_status.role[0],
                MAXLEN,
                agent_to_cm_kerberos_status_ptr->role,
                strlen(agent_to_cm_kerberos_status_ptr->role));
            securec_check_errno(rc, (void)rc);

            rc = strncpy_s(g_kerberos_group_report_status.kerberos_status.nodeName[0],
                CM_NODE_NAME,
                agent_to_cm_kerberos_status_ptr->nodeName,
                strlen(agent_to_cm_kerberos_status_ptr->nodeName));
            securec_check_errno(rc, (void)rc);
        } else if (*kerberosIpPtr1 == '\0' ||
                   strcmp(agent_to_cm_kerberos_status_ptr->kerberos_ip, kerberosIpPtr1) == 0) {
            g_kerberos_group_report_status.kerberos_status.node[1] = agent_to_cm_kerberos_status_ptr->node;
            g_kerberos_group_report_status.kerberos_status.port[1] = agent_to_cm_kerberos_status_ptr->port;
            g_kerberos_group_report_status.kerberos_status.status[1] = agent_to_cm_kerberos_status_ptr->status;
            g_kerberos_group_report_status.kerberos_status.heartbeat[1] = 0;

            /* Write the port, kerberos_ip, node and node name to ddb when cm_server switched */
            rc = snprintf_s(kerberosDdbKey, MAX_PATH_LEN, MAX_PATH_LEN - 1, "/%s/kerberosKey1", pw->pw_name);
            securec_check_intval(rc, (void)rc);
            rc = snprintf_s(kerberosDdbValue,
                MAX_PATH_LEN,
                MAX_PATH_LEN - 1,
                "%u,%s,%s,%u",
                agent_to_cm_kerberos_status_ptr->node,
                agent_to_cm_kerberos_status_ptr->nodeName,
                agent_to_cm_kerberos_status_ptr->kerberos_ip,
                agent_to_cm_kerberos_status_ptr->port);
            securec_check_intval(rc, (void)rc);
            st = SetKV2Ddb(kerberosDdbKey, MAX_PATH_LEN, kerberosDdbValue, MAX_PATH_LEN, NULL);
            if (st != CM_SUCCESS) {
                write_runlog(ERROR, "ddb set(SetOnlineStatusToDdb) failed. key=%s, value=%s.\n",
                    kerberosDdbKey, kerberosDdbValue);
                return;
            }

            rc = strncpy_s(g_kerberos_group_report_status.kerberos_status.kerberos_ip[1],
                CM_IP_LENGTH,
                agent_to_cm_kerberos_status_ptr->kerberos_ip,
                strlen(agent_to_cm_kerberos_status_ptr->kerberos_ip));
            securec_check_errno(rc, (void)rc);

            rc = strncpy_s(g_kerberos_group_report_status.kerberos_status.role[1],
                MAXLEN,
                agent_to_cm_kerberos_status_ptr->role,
                strlen(agent_to_cm_kerberos_status_ptr->role));
            securec_check_errno(rc, (void)rc);

            rc = strncpy_s(g_kerberos_group_report_status.kerberos_status.nodeName[1],
                CM_NODE_NAME,
                agent_to_cm_kerberos_status_ptr->nodeName,
                strlen(agent_to_cm_kerberos_status_ptr->nodeName));
            securec_check_errno(rc, (void)rc);
        }

        (void)pthread_rwlock_unlock(&g_kerberos_group_report_status.lk_lock);
    }
}

void process_agent_to_cm_current_time_msg(const agent_to_cm_current_time_report *etcd_time_ptr)
{
    if (etcd_time_ptr == NULL) {
        return;
    }
    /* etcd node time difference */
    static long int etcd_time_difference = -1;
    pg_time_t timedifference;
    pg_time_t local_time = (pg_time_t)time(NULL);
    timedifference = etcd_time_ptr->etcd_time - local_time;
    if (g_currentNode->etcd == 1 && llabs(timedifference) > ETCD_CLOCK_THRESHOLD) {
        write_runlog(
            WARNING, "The node %u local time is out of the threshold that ETCD required.\n", etcd_time_ptr->nodeid);
    }

    if (g_currentNode->etcd != 1 && etcd_time_difference == -1) {
        etcd_time_difference = timedifference;
    } else if (g_currentNode->etcd != 1 && (llabs(etcd_time_difference - timedifference)) > ETCD_CLOCK_THRESHOLD) {
        write_runlog(WARNING, "The node %u time is out of the threshold that ETCD required.\n", etcd_time_ptr->nodeid);
    }
}

void process_gs_guc_feedback_msg(const agent_to_cm_gs_guc_feedback *feedback_ptr)
{
    char status_key[MAX_PATH_LEN] = {0};
    char value[MAX_PATH_LEN] = {0};
    char cluster_status_key[MAX_PATH_LEN] = {0};
    char sync_standby_mode_value[MAX_PATH_LEN] = {0};
    int rc = 0;
    bool hasDoGsGucFlag = false;

    (void)pthread_rwlock_wrlock(&(gsguc_feedback_rwlock));
    for (uint32 i = 0; i < g_dynamic_header->relationCount; i++) {
        for (int j = 0; j < g_instance_role_group_ptr[i].count; j++) {
            if (feedback_ptr->node == g_instance_role_group_ptr[i].instanceMember[j].node &&
                feedback_ptr->instanceId == g_instance_role_group_ptr[i].instanceMember[j].instanceId &&
                g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].sync_standby_mode !=
                AnyFirstNo) {
                g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].send_gs_guc_time = 0;
                if (feedback_ptr->status &&
                    feedback_ptr->type ==
                    g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].sync_standby_mode) {
                    write_runlog(LOG,
                        "do gs_guc reload success, type:%d, node:%u, instanceId:%u.\n",
                        g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].sync_standby_mode,
                        g_instance_role_group_ptr[i].instanceMember[j].node,
                        feedback_ptr->instanceId);
                    g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].sync_standby_mode =
                        AnyFirstNo;
                } else {
                    write_runlog(ERROR,
                        "do gs_guc reload failed, feedback type:%d, local type:%d, node:%u, instanceId:%u.\n",
                        feedback_ptr->type,
                        g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].sync_standby_mode,
                        g_instance_role_group_ptr[i].instanceMember[j].node,
                        feedback_ptr->instanceId);
                }
            }
            if (g_instance_group_report_status_ptr[i].instance_status.data_node_member[j].sync_standby_mode !=
                AnyFirstNo) {
                hasDoGsGucFlag = true;
            }
        }
    }
    (void)pthread_rwlock_unlock(&(gsguc_feedback_rwlock));
    if (!hasDoGsGucFlag) {
        /* We set cluster AZ status before we mark the AZ auto switchover is done */
        rc = snprintf_s(cluster_status_key,
            MAX_PATH_LEN,
            MAX_PATH_LEN - 1,
            "/%s/CMServer/status_key/sync_standby_mode",
            pw->pw_name);
        securec_check_intval(rc, (void)rc);
        rc = snprintf_s(sync_standby_mode_value, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%d", feedback_ptr->type);
        securec_check_intval(rc, (void)rc);

        status_t st = SetKV2Ddb(cluster_status_key, MAX_PATH_LEN, sync_standby_mode_value, MAX_PATH_LEN, NULL);
        if (st != CM_SUCCESS) {
            write_runlog(ERROR, "ddb set failed. key=%s, value=%s.\n", cluster_status_key, sync_standby_mode_value);
        } else {
            write_runlog(LOG,
                "ddb set status gs guc success, key=%s, value=%s.\n",
                cluster_status_key,
                sync_standby_mode_value);
            current_cluster_az_status = feedback_ptr->type;
            write_runlog(LOG, "setting current_cluster_az_status to %d.\n", current_cluster_az_status);
        }

        rc = snprintf_s(status_key,
            MAX_PATH_LEN,
            MAX_PATH_LEN - 1,
            "/%s/CMServer/status_key/gsguc/%d",
            pw->pw_name,
            GS_GUC_SYNCHRONOUS_STANDBY_MODE);
        securec_check_intval(rc, (void)rc);
        rc = snprintf_s(value, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%d", AnyFirstNo);
        securec_check_intval(rc, (void)rc);
        st = SetKV2Ddb(status_key, MAX_PATH_LEN, value, MAX_PATH_LEN, NULL);
        if (st != CM_SUCCESS) {
            write_runlog(ERROR, "ddb set failed. key=%s, value=%s.\n", status_key, value);
        } else {
            write_runlog(LOG, "ddb set status gs guc success, key=%s, value=%s.\n", status_key, value);
        }
    }
}

void RemoveCmagentSslConn(MsgRecvInfo* recvMsgInfo)
{
    if (g_sslOption.enable_ssl == CM_TRUE) {
        AsyncProcMsg(recvMsgInfo, PM_REMOVE_CONN, NULL, 0);
    }
}

void ProcessSslConnRequest(MsgRecvInfo* recvMsgInfo, const AgentToCmConnectRequest *requestMsg)
{
    if (requestMsg == NULL || requestMsg->msg_type != MSG_CM_SSL_CONN_REQUEST) {
        write_runlog(ERROR, "ssl connect error.\n");
        RemoveCmagentSslConn(recvMsgInfo);
        return;
    }

    write_runlog(DEBUG5, "g_sslOption.enable_ssl=%s\n", g_sslOption.enable_ssl ? "TRUE" : "FALSE");

    CmToAgentConnectAck ackMsg;
    ackMsg.msg_type = MSG_CM_SSL_CONN_ACK;
    if (g_sslOption.enable_ssl == CM_TRUE) {
        ackMsg.status = SSL_ENABLE;
        CmsSSLConnMsg msg;
        msg.startConnTime = GetMonotonicTimeMs();
        AsyncProcMsg(recvMsgInfo, PM_REMOVE_EPOLL, (char *)&msg, sizeof(CmsSSLConnMsg));
    } else {
        ackMsg.status = SSL_DISABLE;
    }

    int ret = RespondMsg(recvMsgInfo, 'S', (char *)(&ackMsg), sizeof(CmToAgentConnectAck));
    if (ret != 0) {
        write_runlog(ERROR, "ProcessSslConnRequest send msg failed.\n");
        return;
    }

    if (g_sslOption.enable_ssl == CM_FALSE) {
        return;
    }

    write_runlog(LOG, "ProcessSslConnRequest, node id: %u.\n", requestMsg->nodeid);
    if (g_ssl_acceptor_fd == NULL) {
        write_runlog(ERROR, "[ProcessSslConnRequest]srv ssl_acceptor_fd null.\n");
        RemoveCmagentSslConn(recvMsgInfo);
        return;
    }

    CmsSSLConnMsg msg;
    msg.startConnTime = GetMonotonicTimeMs();

    AsyncProcMsg(recvMsgInfo, PM_SSL_ACCEPT, (char *)&msg, sizeof(CmsSSLConnMsg));

    return;
}

void GetInstanceIdByIp(uint32 localInstd, uint32 *peerInstId, uint32 groupIdx, DnLocalPeer *dnLpInfo)
{
    dnLpInfo->peerIp[CM_IP_LENGTH - 1] = '\0';
    dnLpInfo->localIp[CM_IP_LENGTH - 1] = '\0';
    dnLpInfo->reserver[DN_SYNC_LEN - 1] = '\0';
    if ((dnLpInfo->peerIp[0] == '\0') || (dnLpInfo->peerPort == 0)) {
        return;
    }
    for (int32 i = 0; i < g_instance_role_group_ptr[groupIdx].count; ++i) {
        DatanodelocalPeer *dnLp =
            &(g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member[i].dnLp);
        for (uint32 j = 0; (j < dnLp->ipCount && j < CM_IP_NUM); ++j) {
            write_runlog(DEBUG1, "[GetInstanceIdByIp] instId(%u) ip[%s:%u, %s:%u].\n", localInstd,
                dnLp->localIp[j], dnLp->localPort, dnLpInfo->peerIp, dnLpInfo->peerPort);
            if ((strcmp(dnLp->localIp[j], dnLpInfo->peerIp) == 0) && (dnLp->localPort == dnLpInfo->peerPort)) {
                (*peerInstId) = g_instance_role_group_ptr[groupIdx].instanceMember[i].instanceId;
                write_runlog(DEBUG1, "[GetInstanceIdByIp] instId(%u) successfully find the peerInstId(%u).\n",
                    localInstd, (*peerInstId));
                return;
            }
        }
    }
    write_runlog(ERROR, "[GetInstanceIdByIp] instId(%u) cannot find the peerInst.\n", localInstd);
}

void ProcessDnLocalPeerMsg(MsgRecvInfo* recvMsgInfo, AgentCmDnLocalPeer *dnLpInfo)
{
    if (dnLpInfo->instanceType != INSTANCE_TYPE_DATANODE) {
        write_runlog(ERROR, "cms get instance(%u) is not dn, this type is %d.\n",
            dnLpInfo->instanceId, dnLpInfo->instanceType);
        return;
    }
    uint32 groupIdx = 0;
    int32 memIdx = 0;
    uint32 node = dnLpInfo->node;
    uint32 instanceId = dnLpInfo->instanceId;
    // get groupIndex, memberIndex
    int32 ret = find_node_in_dynamic_configure(node, instanceId, &groupIdx, &memIdx);
    if (ret != 0) {
        write_runlog(LOG, "can't find the instance(node=%u  instanceid =%u)\n", node, instanceId);
        return;
    }
    GetInstanceIdByIp(instanceId,
        &(g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member[memIdx].dnLp.peerInst),
        groupIdx, &(dnLpInfo->dnLpInfo));
}

static status_t FindAvaliableFloatIpPrimary(uint32 groupIdx, int32 *memIdx)
{
    cm_instance_datanode_report_status *dnReport =
        g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member;
    cm_local_replconninfo *dnLocal;
    uint32 primaryDnCnt = 0;
    for (int32 i = 0; i < g_instance_role_group_ptr[groupIdx].count; ++i) {
        dnLocal = &(dnReport[i].local_status);
        if (dnLocal->local_role == INSTANCE_ROLE_PRIMARY && dnLocal->db_state == INSTANCE_HA_STATE_NORMAL) {
            *memIdx = i;
            ++primaryDnCnt;
        }
    }
    if (primaryDnCnt != 1) {
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static void ArbitrateFloatIpOper(
    MsgRecvInfo *recvMsgInfo, const CmaDnFloatIpInfo *floatIp, NetworkOper oper, NetworkState state)
{
    CmsDnFloatIpAck ack = {{0}};
    errno_t rc = memcpy_s(&(ack.baseInfo), sizeof(BaseInstInfo), &(floatIp->baseInfo), sizeof(BaseInstInfo));
    securec_check_errno(rc, (void)rc);
    ack.baseInfo.msgType = (int32)MSG_CM_AGENT_FLOAT_IP_ACK;
    ack.oper = (int32)oper;
    const DnFloatIpInfo *dnFloatIp = &(floatIp->info);
    for (uint32 i = 0; i < dnFloatIp->count; ++i) {
        if (dnFloatIp->dnNetState[i] != (int32)state || dnFloatIp->nicNetState[i] != (int32)state) {
            (void)RespondMsg(recvMsgInfo, 'S', (const char *)(&ack), sizeof(CmsDnFloatIpAck));
            return;
        }
    }
}

static void ArbitateFloatIp(MsgRecvInfo *recvMsgInfo, const CmaDnFloatIpInfo *floatIp, uint32 groupIdx, int32 memIdx)
{
    cm_instance_datanode_report_status *dnReport =
        &(g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member[memIdx]);
    (void)pthread_rwlock_wrlock(&(g_instance_group_report_status_ptr[groupIdx].lk_lock));
    errno_t rc = memcpy_s(&(dnReport->floatIp), sizeof(DnFloatIpInfo), &(floatIp->info), sizeof(DnFloatIpInfo));
    (void)pthread_rwlock_unlock(&(g_instance_group_report_status_ptr[groupIdx].lk_lock));
    securec_check_errno(rc, (void)rc);
    int32 avaliMemIdx = -1;
    status_t st = FindAvaliableFloatIpPrimary(groupIdx, &avaliMemIdx);
    if (st != CM_SUCCESS) {
        return;
    }
    if (avaliMemIdx == memIdx) {
        ArbitrateFloatIpOper(recvMsgInfo, floatIp, NETWORK_OPER_UP, NETWORK_STATE_UP);
    } else {
        ArbitrateFloatIpOper(recvMsgInfo, floatIp, NETWORK_OPER_DOWN, NETWORK_STATE_DOWN);
    }
}

void ProcessDnFloatIpMsg(MsgRecvInfo *recvMsgInfo, CmaDnFloatIpInfo *floatIp)
{
    const char *str = "[ProcessDnLocalPeerMsg]";
    const BaseInstInfo *baseInst = &(floatIp->baseInfo);
    if (baseInst->instType != INSTANCE_TYPE_DATANODE) {
        write_runlog(ERROR, "%s cms get instance(%u) is not dn, this type is %d.\n",
            str, baseInst->instId, baseInst->instType);
        return;
    }
    uint32 groupIdx = 0;
    int32 memIdx = 0;
    uint32 node = baseInst->node;
    uint32 instId = baseInst->instId;
    // get groupIndex, memberIndex
    int32 ret = find_node_in_dynamic_configure(node, instId, &groupIdx, &memIdx);
    if (ret != 0) {
        write_runlog(LOG, "[%s] can't find the instance(node=%u  instanceid =%u)\n", __FUNCTION__, node, instId);
        return;
    }
    write_runlog(DEBUG1, "cms receive dnFloatIpMsg, and group[%u: %d], node[%u], instId[%u].\n",
        groupIdx, memIdx, node, instId);
    ArbitateFloatIp(recvMsgInfo, floatIp, groupIdx, memIdx);
}

static void InitFloatIpAck(CmFloatIpStatAck *ack)
{
    ack->msgType = (int32)MSG_CTL_CM_FLOAT_IP_ACK;
    ack->count = 0;
    ack->canShow = CM_TRUE;
}

static bool8 IsCurInstanceExistingFloatIp(uint32 groupIdx, int32 memIdx)
{
    DnFloatIpInfo *dnFloatIp =
        &(g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member[memIdx].floatIp);
    for (uint32 i = 0; i < dnFloatIp->count; ++i) {
        if (dnFloatIp->nicNetState[i] == (int32)NETWORK_STATE_UP) {
            return CM_TRUE;
        }
    }
    return CM_FALSE;
}

static void GetFloatIpInfo(CmFloatIpStatAck *ack, size_t *curMsgLen, uint32 groupIdx, int32 memIdx)
{
    uint32 point = ack->count;
    CmFloatIpStatInfo *info = &(ack->info[point]);
    info->nodeId = g_instance_role_group_ptr[groupIdx].instanceMember[memIdx].node;
    info->instId = g_instance_role_group_ptr[groupIdx].instanceMember[memIdx].instanceId;
    if (!IsCurInstanceExistingFloatIp(groupIdx, memIdx)) {
        return;
    }
    DnFloatIpInfo *dnFloatIp =
        &(g_instance_group_report_status_ptr[groupIdx].instance_status.data_node_member[memIdx].floatIp);
    (void)pthread_rwlock_rdlock(&(g_instance_group_report_status_ptr[groupIdx].lk_lock));
    uint32 i = 0;
    for (; i < dnFloatIp->count && i < MAX_FLOAT_IP_COUNT; ++i) {
        info->nicNetState[i] = dnFloatIp->nicNetState[i];
    }
    info->count = i;
    (void)pthread_rwlock_unlock(&(g_instance_group_report_status_ptr[groupIdx].lk_lock));
    ++ack->count;
    *curMsgLen += sizeof(CmFloatIpStatInfo);
}

void GetFloatIpSet(CmFloatIpStatAck *ack, size_t maxMsgLen, size_t *curMsgLen)
{
    InitFloatIpAck(ack);
    if (!IsNeedCheckFloatIp() || (backup_open != CLUSTER_PRIMARY)) {
        ack->canShow = CM_FALSE;
        return;
    }
    for (uint32 i = 0; i < g_dynamic_header->relationCount; ++i) {
        if (g_instance_role_group_ptr[i].instanceMember[0].instanceType != INSTANCE_TYPE_DATANODE) {
            continue;
        }
        for (int32 j = 0; j < g_instance_role_group_ptr[i].count; ++j) {
            if (*curMsgLen + sizeof(CmFloatIpStatInfo) > maxMsgLen) {
                write_runlog(LOG, "tmpMsgLen is %zu, and maxMsgLen is %zu.\n", *curMsgLen, maxMsgLen);
                return;
            }
            GetFloatIpInfo(ack, curMsgLen, i, j);
        }
    }
}
