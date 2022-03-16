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
 * ctl_query_base.cpp
 *    cm_ctl query [-z ALL] [-n NODEID [-D DATADIR -R]] [-l FILENAME][-v [-C [-s] [-S] [-d] [-i] [-F]
 *                      [-L ALL] [-x] [-p]] | [-r]] [-t SECS] [--minorityAz=AZ_NAME]
 *
 * IDENTIFICATION
 *    src/cm_ctl/ctl_query_base.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <signal.h>
#include "common/config/cm_config.h"
#include "cm/libpq-fe.h"
#include "cm/cm_misc.h"
#include "ctl_common.h"
#include "cm/cm_msg.h"
#include <sys/time.h>
#include "ctl_query_base.h"

extern bool g_detailQuery;
extern bool g_coupleQuery;
extern bool g_balanceQuery;
extern bool g_startStatusQuery;
extern bool g_abnormalQuery;
extern bool g_portQuery;
extern bool g_paralleRedoState;
extern bool g_dataPathQuery;
extern bool g_availabilityZoneCommand;
extern bool g_ipQuery;
extern int g_fencedUdfQuery;
extern bool g_nodeIdSet;
extern int g_waitSeconds;
extern uint32 g_nodeIndexForCmServer[CM_PRIMARY_STANDBY_NUM];
extern bool g_commandRelationship;
extern char g_cmData[CM_PATH_LENGTH];
extern const char* g_cmServerState[CM_PRIMARY_STANDBY_NUM + 1];
extern uint32 g_commandOperationNodeId;
extern char* g_logFile;
extern bool g_gtmBalance;
extern bool g_datanodesBalance;
extern cm_to_ctl_central_node_status g_centralNode;
extern FILE* g_logFilePtr;


static void PrintClusterStatus(int clusterStatus = CM_STATUS_UNKNOWN, bool redistributing = false,
                int switchedCount = -1, int nodeID = -1);

void PrintLogicResult(uint32 nameLen, int stateLen, const cm_to_ctl_logic_cluster_status *clusterStatusPtr)
{
    for (uint32 ii = 0; ii < g_logic_cluster_count; ii++) {
        fprintf(g_logFilePtr, "%-*s ", nameLen - SPACE_LEN,
            g_logicClusterStaticConfig[ii].LogicClusterName);
        fprintf(g_logFilePtr, "%-*s ", stateLen - SPACE_LEN,
            cluster_state_int_to_string(clusterStatusPtr->logic_cluster_status[ii]));
        fprintf(g_logFilePtr, "%-*s ", stateLen - SPACE_LEN,
            clusterStatusPtr->logic_is_all_group_mode_pending[ii] ? "Yes" : "No");
        fprintf(g_logFilePtr, "%-*s\n", stateLen - SPACE_LEN,
            (clusterStatusPtr->logic_switchedCount[ii] == 0) ? "Yes" : "No");
    }
    /* if elastic exist node get its status,else set default status */
    if (clusterStatusPtr->logic_switchedCount[LOGIC_CLUSTER_NUMBER - 1] >= 0) {
        fprintf(g_logFilePtr, "%-*s ", nameLen - SPACE_LEN, ELASTICGROUP);
        fprintf(g_logFilePtr, "%-*s ", stateLen - SPACE_LEN,
            cluster_state_int_to_string(clusterStatusPtr->logic_cluster_status[LOGIC_CLUSTER_NUMBER - 1]));
        fprintf(g_logFilePtr, "%-*s ", stateLen - SPACE_LEN,
            clusterStatusPtr->logic_is_all_group_mode_pending[LOGIC_CLUSTER_NUMBER - 1]
                ? "Yes" : "No");
        fprintf(g_logFilePtr, "%-*s\n", stateLen - SPACE_LEN,
            (clusterStatusPtr->logic_switchedCount[LOGIC_CLUSTER_NUMBER - 1] == 0) ? "Yes" : "No");
    } else {
        fprintf(g_logFilePtr, "%-*s ", nameLen - SPACE_LEN, ELASTICGROUP);
        fprintf(g_logFilePtr, "%-*s ", stateLen - SPACE_LEN, "Normal");
        fprintf(g_logFilePtr, "%-*s ", stateLen - SPACE_LEN, "No");
        fprintf(g_logFilePtr, "%-*s\n", stateLen - SPACE_LEN, "Yes");
    }
}

static void PrintClusterStatus(int clusterStatus, bool redistributing, int switchedCount, int nodeID)
{
    fprintf(g_logFilePtr, "[   Cluster State   ]\n\n");
    fprintf(g_logFilePtr,
            "cluster_state   : %s\n",
            cluster_state_int_to_string(clusterStatus));
    fprintf(g_logFilePtr,
            "redistributing  : %s\n",
            redistributing ? "Yes" : "No");
    if (!g_startStatusQuery || (logic_cluster_query && g_logic_cluster_count)) {
        fprintf(g_logFilePtr,
                "balanced        : %s\n",
                (switchedCount == 0) ? "Yes" : "No");
        int nid = nodeID;
        if (nid == -1) {
            fprintf(g_logFilePtr, "current_az      : %s\n", "AZ_ALL");
        } else if (nid >= 0 && nid < (int)g_node_num) {
            fprintf(g_logFilePtr, "current_az      : %s\n", g_node[nid].azName);
        } else {
            fprintf(g_logFilePtr, "current_az      : %s\n", "AZ_DOWN");
        }
    }
}

int PrintLogicClusterStatus(const char *receiveMsg, int nodeId)
{
    cm_to_ctl_logic_cluster_status *clusterStatusPtr = NULL;

    clusterStatusPtr = (cm_to_ctl_logic_cluster_status*)receiveMsg;
    uint32 nameLen = max_logic_cluster_name_len + SPACE_NUM * SPACE_LEN;
    uint32 stateLen = max_logic_cluster_state_len;

    if (clusterStatusPtr->inReloading) {
        PrintClusterStatus();
        return CYCLE_BREAK;
    }
    PrintClusterStatus(clusterStatusPtr->cluster_status,
        clusterStatusPtr->is_all_group_mode_pending,
        clusterStatusPtr->switchedCount,
        nodeId);

    fprintf(g_logFilePtr, "[   logicCluster State   ]\n\n");
    fprintf(g_logFilePtr,
        "%-*s%-*s%-*s%s\n", nameLen,
        "logiccluster_name", stateLen,
        "logiccluster_state", stateLen,
        "redistributing", "balanced");

    for (uint32 i = 0;
        i < (nameLen - SPACE_LEN + STATE_NUM * (stateLen - SPACE_LEN));
        i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");

    PrintLogicResult(nameLen, stateLen, clusterStatusPtr);
    return 0;
}

void SetCmQueryContentDetail(ctl_to_cm_query *cmQueryContent)
{
    if (g_coupleQuery) {
        cmQueryContent->detail = CLUSTER_COUPLE_STATUS_QUERY;
        if (g_detailQuery) {
            cmQueryContent->detail = CLUSTER_COUPLE_DETAIL_STATUS_QUERY;
            if (g_balanceQuery && !g_abnormalQuery) {
                cmQueryContent->detail = CLUSTER_BALANCE_COUPLE_DETAIL_STATUS_QUERY;
            }
            if (logic_cluster_query) {
                cmQueryContent->detail = CLUSTER_LOGIC_COUPLE_DETAIL_STATUS_QUERY;
            }
            if (g_abnormalQuery == true && !g_balanceQuery) {
                cmQueryContent->detail = CLUSTER_ABNORMAL_COUPLE_DETAIL_STATUS_QUERY;
            }
            if (g_abnormalQuery == true && g_balanceQuery == true) {
                cmQueryContent->detail = CLUSTER_ABNORMAL_BALANCE_COUPLE_DETAIL_STATUS_QUERY;
            }
            if (g_startStatusQuery) {
                cmQueryContent->detail = CLUSTER_START_STATUS_QUERY;
            }
        }
    } else if (g_paralleRedoState) {
        cmQueryContent->detail = CLUSTER_PARALLEL_REDO_REPLAY_STATUS_QUERY;
        if (g_detailQuery) {
            cmQueryContent->detail = CLUSTER_PARALLEL_REDO_REPLAY_DETAIL_STATUS_QUERY;
        }
    } else if (g_detailQuery) {
        cmQueryContent->detail = CLUSTER_DETAIL_STATUS_QUERY;
    } else {
        cmQueryContent->detail = CLUSTER_STATUS_QUERY;
    }
}

status_t SetCmQueryContent(ctl_to_cm_query *cmQueryContent)
{
    cmQueryContent->msg_type = MSG_CTL_CM_QUERY;
    if (g_nodeIdSet) {
        cmQueryContent->node = g_commandOperationNodeId;
    } else {
        cmQueryContent->node = INVALID_NODE_NUM;
    }
    cmQueryContent->relation = 0;
    if (g_nodeIdSet && g_cmData[0] != '\0' && g_only_dn_cluster && g_commandRelationship) {
        int ret = -1;
        int instanceType;
        uint32 instanceId;
        ret = FindInstanceIdAndType(g_commandOperationNodeId, g_cmData, &instanceId, &instanceType);
        if (ret != 0) {
            write_runlog(FATAL, "can't find the node_id:%u, data_path:%s.\n", g_commandOperationNodeId, g_cmData);
            return CM_ERROR;
        }
        if (instanceType != INSTANCE_TYPE_DATANODE) {
            write_runlog(FATAL, "data path %s is not dn.\n", g_cmData);
            return CM_ERROR;
        }
        cmQueryContent->instanceId = instanceId;
        cmQueryContent->relation = 1;
    } else {
        cmQueryContent->instanceId = INVALID_INSTACNE_NUM;
    }
    cmQueryContent->wait_seconds = g_waitSeconds;
    SetCmQueryContentDetail(cmQueryContent);
    return CM_SUCCESS;
}

void PrintCnHeaderLine(uint32 nodeLen, uint32 instanceLen)
{
    fprintf(g_logFilePtr, "\n[ Coordinator State ]\n\n");
    uint32 tmpInstanceLen = instanceLen;
    if (g_portQuery) {
        tmpInstanceLen = tmpInstanceLen + INSTANCE_LEN;
    }
    if (g_ipQuery) {
        fprintf(g_logFilePtr, "%-*s%-*s%-*s%s\n", nodeLen, "node", MAX_IP_LEN + 1, "node_ip",
            tmpInstanceLen, "instance", "state");
    } else {
        fprintf(
            g_logFilePtr, "%-*s%-*s%s\n", nodeLen, "node", tmpInstanceLen, "instance", "state");
    }
    uint32 maxLen = nodeLen + instanceLen + INSTANCE_DYNAMIC_ROLE_LEN + (g_ipQuery ? (MAX_IP_LEN + 1) : 0);
    for (uint32 i = 0; i < maxLen; i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");
}

int ProcessCoupleDetailQuery(const char *receiveMsg, int clusterState)
{
    int ret = 0;
    const cm_to_ctl_cluster_status* clusterStatusPtr = (const cm_to_ctl_cluster_status*)receiveMsg;
    uint32 nodeLen = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    const uint32 instanceLen =
        INSTANCE_ID_LEN + SPACE_LEN + (g_dataPathQuery ? (max_cnpath_len + 1) : DEFAULT_PATH_LEN);
    if (g_availabilityZoneCommand) {
        nodeLen += max_az_name_len + SPACE_LEN;
    }
    if (logic_cluster_query && g_logic_cluster_count) {
        ret = PrintLogicClusterStatus(receiveMsg, clusterStatusPtr->node_id);
        if (ret != 0) {
            return ret;
        }
    } else {
        if (clusterStatusPtr->inReloading && !g_startStatusQuery) {
            PrintClusterStatus();
            return CYCLE_BREAK;
        }
        if (HAS_RES_DEFINED_ONLY) {
            PrintClusterStatus(clusterState);
            return CYCLE_RETURN;
        }
        PrintClusterStatus(clusterStatusPtr->cluster_status,
            clusterStatusPtr->is_all_group_mode_pending,
            clusterStatusPtr->switchedCount,
            clusterStatusPtr->node_id);
    }

    if (g_only_dn_cluster) {
        return 0;
    }
    if (HAS_RES_DEFINED_ONLY) {
        return CYCLE_RETURN;
    }
    PrintCnHeaderLine(nodeLen, instanceLen);
    return 0;
}

int ProcessDataBeginMsg(const char *receiveMsg, int clusterState, bool *recDataEnd)
{
    cm_to_ctl_cluster_status* clusterStatusPtr = NULL;
    int ret;
    if (g_coupleQuery && g_detailQuery) {
        ret = ProcessCoupleDetailQuery(receiveMsg, clusterState);
        if (ret != 0) {
            return ret;
        }
    } else {
        clusterStatusPtr = (cm_to_ctl_cluster_status*)receiveMsg;
        fprintf(g_logFilePtr,
            "-----------------------------------------------------------------------\n\n");
        fprintf(g_logFilePtr,
            "cluster_state             : %s\n",
            cluster_state_int_to_string(clusterStatusPtr->cluster_status));
        fprintf(g_logFilePtr,
            "redistributing            : %s\n",
            clusterStatusPtr->is_all_group_mode_pending ? "Yes" : "No");
        fprintf(g_logFilePtr,
            "balanced                  : %s\n\n",
            (clusterStatusPtr->switchedCount == 0) ? "Yes" : "No");
        fprintf(g_logFilePtr,
            "-----------------------------------------------------------------------\n\n");
    }
    if (g_detailQuery != true) {
        *recDataEnd = true;
    }

    return CM_SUCCESS;
}

void CalcGtmHeaderSize(uint32 *nodeLen, uint32 *instanceLen, uint32 *stateLen)
{
    *nodeLen = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    *instanceLen = INSTANCE_ID_LEN + SPACE_LEN + (g_dataPathQuery ? (max_gtmpath_len + 1) : DEFAULT_PATH_LEN);
    if (g_availabilityZoneCommand) {
        *nodeLen += max_az_name_len + SPACE_LEN;
    }
    if (g_single_node_cluster) {
        *stateLen =
            INSTANCE_STATIC_ROLE_LEN + SPACE_LEN + INSTANCE_DYNAMIC_ROLE_LEN + SPACE_LEN;
    } else {
        *stateLen = INSTANCE_STATIC_ROLE_LEN + SPACE_LEN + INSTANCE_DYNAMIC_ROLE_LEN +
                    SPACE_LEN + MAX_GTM_CONNECTION_STATE_LEN + SPACE_LEN;
    }
}
/*
 * @Description: print central node detail info
 * @IN file: file pointer
 * @Return: void
 * @See also:
 */
static void PrintCentralNodeDetail(FILE* file)
{
    if (g_centralNode.instanceId == 0)
        return;

    /* query cm_server */
    uint32 nodeLen = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    const uint32 instanceLen = INSTANCE_ID_LEN + SPACE_LEN +
        (g_dataPathQuery ? (max_cnpath_len + 1) : DEFAULT_PATH_LEN);

    if (g_availabilityZoneCommand) {
        nodeLen += max_az_name_len + SPACE_LEN;
    }

    /* information head */
    fprintf(file, "[ Central Coordinator State ]\n\n");

    /* show ip */
    if (g_ipQuery) {
        fprintf(
            file, "%-*s%-*s%-*s%s\n", nodeLen, "node", MAX_IP_LEN + 1, "node_ip", instanceLen, "instance", "state");
    } else {
        fprintf(file, "%-*s%-*s%s\n", nodeLen, "node", instanceLen, "instance", "state");
    }

    for (uint32 i = 0; i < nodeLen + instanceLen + INSTANCE_DYNAMIC_ROLE_LEN + (g_ipQuery ? (MAX_IP_LEN + 1) : 0);
         ++i) {
        fprintf(file, "-");
    }

    fprintf(file, "\n");

    int nodeIndex = g_centralNode.node_index;

    /* it's couple query */
    if (g_coupleQuery) {
        if (g_abnormalQuery == true && (strcmp(datanode_role_int_to_string(g_centralNode.status), "Normal") == 0)) {
            return;
        }
        if (g_availabilityZoneCommand) {
            fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[nodeIndex].azName);
        }
        fprintf(file, "%-2u ", g_node[nodeIndex].node);
        fprintf(file, "%-*s ", max_node_name_len, g_node[nodeIndex].nodeName);

        if (g_ipQuery)
            fprintf(file, "%-15s ", g_node[nodeIndex].coordinateListenIP[0]);

        fprintf(file, "%u ", g_centralNode.instanceId);

        if (g_dataPathQuery)
            fprintf(file, "%-*s ", max_cnpath_len, g_node[nodeIndex].DataPath);
        else
            fprintf(file, "    ");

        fprintf(file, "%s\n", datanode_role_int_to_string(g_centralNode.status));
    } else {
        fprintf(file, "node                      : %u\n", g_node[nodeIndex].node);
        fprintf(file, "instance_id               : %u\n", g_centralNode.instanceId);
        fprintf(file, "node_ip                   : %s\n", g_node[nodeIndex].coordinateListenIP[0]);
        fprintf(file, "data_path                 : %s\n", g_node[nodeIndex].DataPath);
        fprintf(file, "type                      : %s\n", type_int_to_string(INSTANCE_TYPE_COORDINATE));
        fprintf(file, "state                     : %s\n\n", datanode_role_int_to_string(g_centralNode.status));
    }
}

void PrintGtmHeaderLine()
{
    uint32 nodeLen;
    uint32 instanceLen;
    uint32 stateLen;

    CalcGtmHeaderSize(&nodeLen, &instanceLen, &stateLen);

    if (g_only_dn_cluster)
        return;

    if (!g_balanceQuery || g_abnormalQuery) {
        fprintf(g_logFilePtr, "\n");
        PrintCentralNodeDetail(g_logFilePtr);
    }

    if (!g_balanceQuery) {
        fprintf(g_logFilePtr, "\n");
    }
    fprintf(g_logFilePtr, "[     GTM State     ]\n\n");

    if (g_ipQuery) {
        if (g_single_node_cluster) {
            fprintf(g_logFilePtr, "%-*s%-*s%-*s%-*s\n", nodeLen, "node", MAX_IP_LEN + 1, "node_ip",
                instanceLen, "instance", stateLen, "state");
        } else {
            fprintf(g_logFilePtr, "%-*s%-*s%-*s%-*s%s\n", nodeLen, "node", MAX_IP_LEN + 1, "node_ip",
                instanceLen, "instance", stateLen, "state", "sync_state");
        }
    } else {
        if (g_single_node_cluster) {
            fprintf(g_logFilePtr, "%-*s%-*s%-*s\n", nodeLen, "node", instanceLen, "instance", stateLen, "state");
        } else {
            fprintf(g_logFilePtr, "%-*s%-*s%-*s%s\n",
                nodeLen, "node", instanceLen, "instance", stateLen, "state", "sync_state");
        }
    }
    for (uint32 i = 0; i < nodeLen + instanceLen + stateLen +
                        (g_single_node_cluster ? 0 : MAX_GTM_SYNC_STATE_LEN) +
                        (g_ipQuery ? (MAX_IP_LEN + 1) : 0);
        i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");
}

void CalcDnHeaderSize(uint32 *nodeLen, uint32 *instanceLen, uint32 *stateLen)
{
    uint32 nameLen;
    uint32 nodeLength;

    nodeLength = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    *instanceLen =
        INSTANCE_ID_LEN + SPACE_LEN + (g_dataPathQuery ? (max_datapath_len + 1) : DEFAULT_PATH_LEN);
    *stateLen = INSTANCE_STATIC_ROLE_LEN + SPACE_LEN + INSTANCE_DYNAMIC_ROLE_LEN +
                             SPACE_LEN + INSTANCE_DB_STATE_LEN + SPACE_LEN;
    nameLen = max_logic_cluster_name_len + SPACE_NUM * SPACE_LEN;
    if (g_availabilityZoneCommand) {
        nodeLength += max_az_name_len + SPACE_LEN;
    }
    if (g_balanceQuery && g_gtmBalance && !g_only_dn_cluster) {
        fprintf(g_logFilePtr, "(no need to switchover gtm)\n");
    }
    if (logic_cluster_query) {
        fprintf(g_logFilePtr, "%-*s| ", nameLen, "logiccluster_name");
    }
    *nodeLen = nodeLength;
}

void PrintDnHeaderLine(uint32 nodeLen, uint32 instanceLen, uint32 tmpInstanceLen, uint32 stateLen)
{
    if (g_ipQuery) {
        if (g_multi_az_cluster) {
            for (uint32 jj = 0; jj < g_dn_replication_num - 1; jj++) {
                fprintf(g_logFilePtr, "%-*s%-*s%-*s%-*s| ",
                    nodeLen, "node", MAX_IP_LEN + 1, "node_ip",
                    tmpInstanceLen, "instance", stateLen, "state");
            }
        } else if (!g_single_node_cluster) {
            fprintf(g_logFilePtr, "%-*s%-*s%-*s%-*s| ", nodeLen, "node", MAX_IP_LEN + 1,
                "node_ip", tmpInstanceLen, "instance", stateLen, "state");
            fprintf(g_logFilePtr, "%-*s%-*s%-*s%-*s| ", nodeLen, "node", MAX_IP_LEN + 1,
                "node_ip", tmpInstanceLen, "instance", stateLen, "state");
        }
        fprintf(g_logFilePtr, "%-*s%-*s%-*s%s\n", nodeLen, "node", MAX_IP_LEN + 1,
            "node_ip", tmpInstanceLen, "instance", "state");
    } else {
        if (g_multi_az_cluster) {
            for (uint32 jj = 0; jj < g_dn_replication_num - 1; jj++) {
                fprintf(g_logFilePtr, "%-*s%-*s%-*s| ", nodeLen, "node",
                    tmpInstanceLen, "instance", stateLen, "state");
            }
        } else if (!g_single_node_cluster) {
            fprintf(g_logFilePtr, "%-*s%-*s%-*s| ", nodeLen, "node",
                tmpInstanceLen, "instance", stateLen, "state");
            fprintf(g_logFilePtr, "%-*s%-*s%-*s| ",
                nodeLen, "node", tmpInstanceLen, "instance", stateLen, "state");
        }
        fprintf(g_logFilePtr, "%-*s%-*s%s\n", nodeLen, "node",
            g_single_node_cluster ? tmpInstanceLen : instanceLen,
            "instance", "state");
    }
}

void PrintDnStatusLine()
{
    uint32 nodeLen;
    uint32 instanceLen;
    uint32 stateLen;

    CalcDnHeaderSize(&nodeLen, &instanceLen, &stateLen);
    uint32 tmpInstanceLen = instanceLen;
    if (g_portQuery) {
        tmpInstanceLen = tmpInstanceLen + INSTANCE_LEN;
    }

    fprintf(g_logFilePtr, "\n[  Datanode State   ]\n\n");

    PrintDnHeaderLine(nodeLen, instanceLen, tmpInstanceLen, stateLen);

    uint32 maxLen = 0;
    uint32 secondryStateLen = INSTANCE_STATIC_ROLE_LEN + SPACE_LEN +
                                      SECONDARY_DYNAMIC_ROLE_LEN + SPACE_LEN +
                                      INSTANCE_DB_STATE_LEN;
    if (g_multi_az_cluster || g_single_node_cluster) {
        maxLen = g_dn_replication_num *
                      (nodeLen + tmpInstanceLen + (g_ipQuery ? (MAX_IP_LEN + 1) : 0)) +
                  g_dn_replication_num * (stateLen + SEPERATOR_LEN + SPACE_LEN);
    } else {
        maxLen = NODE_NUM * (nodeLen + tmpInstanceLen + (g_ipQuery ? (MAX_IP_LEN + 1) : 0)) +
                  SPACE_NUM * (stateLen + SEPERATOR_LEN + SPACE_LEN) + secondryStateLen;
    }
    for (uint32 i = 0; i < maxLen; i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");
}

void PrintFenceHeaderLine()
{
    const uint32 nodeLen = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    if (g_balanceQuery && g_datanodesBalance) {
        fprintf(g_logFilePtr, "(no need to switchover datanodes)\n");
    }
    if (g_fencedUdfQuery && !g_balanceQuery) {
        fprintf(g_logFilePtr, "\n[  Fenced UDF State   ]\n\n");
        if (g_ipQuery) {
            fprintf(g_logFilePtr, "%-*s%-*s%s\n", nodeLen, "node", MAX_IP_LEN + 1,
                "node_ip", "state");
        } else {
            fprintf(g_logFilePtr, "%-*s%s\n", nodeLen, "node", "state");
        }
        for (uint32 i = 0; i < nodeLen + INSTANCE_DYNAMIC_ROLE_LEN + (g_ipQuery ? (MAX_IP_LEN + 1) : 0);
            i++) {
            fprintf(g_logFilePtr, "-");
        }
        fprintf(g_logFilePtr, "\n");
    }
}

void DoProcessNodeEndMsg(const char *receiveMsg)
{
    cm_to_ctl_instance_status* instanceStatusPtr = NULL;

    instanceStatusPtr = (cm_to_ctl_instance_status*)receiveMsg;
    if (g_coupleQuery && !g_startStatusQuery) {
        if (instanceStatusPtr->instance_type == INSTANCE_TYPE_COORDINATE) {
            PrintGtmHeaderLine();
        }
        if (instanceStatusPtr->instance_type == INSTANCE_TYPE_GTM) {
            PrintDnStatusLine();
        }
        if (instanceStatusPtr->instance_type == INSTANCE_TYPE_DATANODE) {
            PrintFenceHeaderLine();
        }
    } else {
        fprintf(g_logFilePtr,
            "-----------------------------------------------------------------------\n\n");
    }
}
