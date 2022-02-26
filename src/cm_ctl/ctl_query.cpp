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
 * ctl_query.cpp
 *    cm_ctl query [-z ALL] [-n NODEID [-D DATADIR -R]] [-l FILENAME][-v [-C [-s] [-S] [-d] [-i] [-F]
 *                      [-L ALL] [-x] [-p]] | [-r]] [-t SECS] [--minorityAz=AZ_NAME]
 *
 * IDENTIFICATION
 *    src/cm_ctl/ctl_query.cpp
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

static void query_cmserver_and_etcd_status(void);
const char* query_etcd(uint32 node_id);
static void query_kerberos(void);
static void query_kerberos_status();
static void do_query_cmserver(uint32 node_id, const char* state);
static const char* query_cm_server(uint32 node_id);
static const char* query_cm_server_directory(uint32 node_id);
static status_t PrintResult(uint32 *pre_node, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr);
static void PrintParallelRedoResult(uint32 *pre_node, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr);
static void PrintSimpleResult(uint32 *pre_node, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr);
static void print_simple_DN_result(uint32 node_index, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr);
static void QueryResourceStatus(int *clusterState);
static void GetRersourceStatusInfo(int *clusterState);

static bool hasFindCmSP = false;
static bool hasFindEtcdL = false;
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
extern const char* g_cmServerState[CM_PRIMARY_STANDBY_NUM];
extern uint32 g_commandOperationNodeId;
extern char* g_logFile;
extern CM_Conn* CmServer_conn;
extern CM_Conn* CmServer_conn1;
extern CM_Conn* CmServer_conn2;
extern bool g_logFileSet;
extern char* g_commandMinortityAzName;


extern bool g_gtmBalance;
extern bool g_datanodesBalance;
extern cm_to_ctl_central_node_status g_centralNode;
extern FILE* g_logFilePtr;


static void PrintGlobalBarrierInfoNew(const cm2CtlGlobalBarrierNew *globalBarrierInfo)
{
    fprintf(g_logFilePtr, 
        "global_recovery_barrierId: %s\n",
        globalBarrierInfo->globalRecoveryBarrierId);
    fprintf(g_logFilePtr, 
        "GlobalBarrierSlotCount: %d\n",
        globalBarrierInfo->globalStatus.slotCount);
    for (int32 i = 0; i < globalBarrierInfo->globalStatus.slotCount; i++) {
        const GlobalBarrierItem *curBarrier = &globalBarrierInfo->globalStatus.globalBarriers[i];
        fprintf(g_logFilePtr,
            "GlobalSlotName: %s\n",
            curBarrier->slotname);
        fprintf(g_logFilePtr,
            "  GlobalBarrierId: %s\n",
            curBarrier->globalBarrierId);
        fprintf(g_logFilePtr, 
            "  GlobalAchiveBarrierId: %s\n",
            curBarrier->globalAchiveBarrierId);
    }
}

int do_global_barrier_query(void)
{
    ctl_to_cm_global_barrier_query cm_ctl_cm_query_content;
    GetUpgradeVersionFromCmaConfig();
    cm_ctl_cm_query_content.msg_type = (undocumentedVersion == 0 || undocumentedVersion >= g_barrierSlotVersion)
                                        ? MSG_CTL_CM_GLOBAL_BARRIER_QUERY_NEW
                                        : MSG_CTL_CM_GLOBAL_BARRIER_QUERY;
    cm_to_ctl_cluster_global_barrier_info *global_barrier_info;
    char *receiveMsg = NULL;
    int ret;
    bool success = false;
    if (g_logFileSet) {
        g_logFilePtr = fopen(g_logFile, "w");
        if (g_logFilePtr == NULL) {
            if (errno == ENOENT) {
                write_runlog(ERROR, "log file not found.\n");
                return -1;
            } else {
                char errBuffer[ERROR_LIMIT_LEN];
                write_runlog(ERROR,
                    "could not open log file \"%s\": %s\n",
                    g_logFile,
                    strerror_r(errno, errBuffer, ERROR_LIMIT_LEN));
                return -1;
            }
        }
    } else {
        g_logFilePtr = stdout;
    }

    do_conn_cmserver(false, 0);
    if (CmServer_conn == NULL) {
        write_runlog(ERROR,
            "can't connect to cm_server.\n"
            "Maybe cm_server is not running, or timeout expired. Please try again.\n");
        return -1;
    }
    ret = cm_client_send_msg(CmServer_conn, 'C', (char*)&cm_ctl_cm_query_content, sizeof(ctl_to_cm_global_barrier_query));
    if (ret != 0) {
        CMPQfinish(CmServer_conn);
        CmServer_conn = NULL;
        return -1;
    }
    CmSleep(1);
    int wait_time = GLOBAL_BARRIER_WAIT_SECONDS*1000;
    while(wait_time > 0) {
        ret = cm_client_flush_msg(CmServer_conn);
        if (ret == TCP_SOCKET_ERROR_EPIPE) {
            CMPQfinish(CmServer_conn);
            CmServer_conn = NULL;
            return -1;
        }

        receiveMsg = recv_cm_server_cmd(CmServer_conn);
        if (receiveMsg != NULL) {
            global_barrier_info = (cm_to_ctl_cluster_global_barrier_info*) receiveMsg;
            switch (global_barrier_info->msg_type) {
                case MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN:
                    fprintf(g_logFilePtr,
                        "global_barrierId: %s\n",
                        global_barrier_info->global_barrierId);
                    fprintf(g_logFilePtr, 
                        "global_achive_barrierId: %s\n",
                        global_barrier_info->global_achive_barrierId);
                    fprintf(g_logFilePtr, 
                        "global_recovery_barrierId: %s\n",
                        global_barrier_info->globalRecoveryBarrierId);
                    break;
                case MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW: {
                    cm2CtlGlobalBarrierNew *globalBarrierInfo = (cm2CtlGlobalBarrierNew*) receiveMsg;
                    PrintGlobalBarrierInfoNew(globalBarrierInfo);
                    break;
                }
                case MSG_CM_CTL_GLOBAL_BARRIER_DATA:
                    cm_to_ctl_instance_barrier_info *instance_barrier_info;
                    instance_barrier_info = (cm_to_ctl_instance_barrier_info *)receiveMsg;
                    fprintf(g_logFilePtr,
                        "instanceId: %u\n",
                        instance_barrier_info->instanceId);
                    fprintf(g_logFilePtr,
                        "instance_type: %s\n",
                        type_int_to_string(instance_barrier_info->instance_type));
                    fprintf(g_logFilePtr,
                        "ckpt_redo_point: %X/%X\n",
                        uint32((instance_barrier_info->ckpt_redo_point) >> 32), uint32(instance_barrier_info->ckpt_redo_point));
                    fprintf(g_logFilePtr,
                        "barrierID: %s\n",
                        instance_barrier_info->barrierID);
                    fprintf(g_logFilePtr,
                        "barrierLSN: %X/%X\n",
                        uint32((instance_barrier_info->barrierLSN) >> 32), uint32(instance_barrier_info->barrierLSN));
                    fprintf(g_logFilePtr,
                        "archive_LSN: %X/%X\n",
                        uint32((instance_barrier_info->archive_LSN) >> 32), uint32(instance_barrier_info->archive_LSN));
                    fprintf(g_logFilePtr,
                        "flush_LSN: %X/%X\n",
                        uint32((instance_barrier_info->flush_LSN) >> 32), uint32(instance_barrier_info->flush_LSN));
                    break;
                case MSG_CM_CTL_BARRIER_DATA_END:
                    fprintf(g_logFilePtr, "end\n");
                    success = true;
                    break;
                default:
                    write_runlog(ERROR, "unknown the msg type is %d.\n", global_barrier_info->msg_type);
                    break;
            }
            if (success == true) {
                return 0;
            }
        }
        CmSleep(1);
        wait_time --;
    }
    return 0;
}

status_t SetQueryLogHander(void)
{
    if (g_logFileSet) {
        g_logFilePtr = fopen(g_logFile, "w");
        if (g_logFilePtr == NULL) {
            if (errno == ENOENT) {
                write_runlog(ERROR, "log file not found.\n");
                return CM_ERROR;
            } else {
                char errBuffer[ERROR_LIMIT_LEN];
                write_runlog(ERROR, "could not open log file \"%s\": %s\n",
                    g_logFile, strerror_r(errno, errBuffer, ERROR_LIMIT_LEN));
                return CM_ERROR;
            }
        }
    } else {
        g_logFilePtr = stdout;
    }
    return CM_SUCCESS;
}

/*
 * @Description: get the state of etcd specified by node id. if failed connect or query etcd, return NULL.
 *
 * @in node_id:node id of etcd
 *
 * @out: state of etcd
 */
status_t QueryEtcdAndCms(void)
{
    if (g_cm_server_num > CM_PRIMARY_STANDBY_NUM) {
        write_runlog(ERROR, "the number of cm_server is bigger than %d.\n", CM_PRIMARY_STANDBY_NUM);
        return CM_ERROR;
    }
    
    if (g_coupleQuery && g_detailQuery && (!g_balanceQuery || g_abnormalQuery) && !g_startStatusQuery) {
        (void)query_cmserver_and_etcd_status();
        (void)query_kerberos();
    }
    if (!g_coupleQuery && g_detailQuery && !g_balanceQuery && !g_startStatusQuery) {
        for (uint32 kk = 0; kk < g_cm_server_num; kk++) {
            g_cmServerState[kk] = query_cm_server(g_nodeIndexForCmServer[kk]);
        }
    }
    return CM_SUCCESS;
}

int DoProcessQueryMsg(char *receiveMsg, bool *recDataEnd, int clusterState)
{
    cm_msg_type *cm_msg_type_ptr = NULL;
    cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr = NULL;
    uint32 pre_node = INVALID_NODE_NUM;
    int ret = 0;

    cm_msg_type_ptr = (cm_msg_type *)receiveMsg;
    switch (cm_msg_type_ptr->msg_type) {
        case MSG_CM_CTL_DATA_BEGIN:
            ret = ProcessDataBeginMsg((const char *)receiveMsg, clusterState, recDataEnd);
            break;
        case MSG_CM_CTL_DATA:
            cm_to_ctl_instance_status_ptr = (cm_to_ctl_instance_status *)receiveMsg;
            if (g_coupleQuery) {
                PrintSimpleResult(&pre_node, cm_to_ctl_instance_status_ptr);
            } else if (g_paralleRedoState) {
                PrintParallelRedoResult(&pre_node, cm_to_ctl_instance_status_ptr);
            } else {
                ret = PrintResult(&pre_node, cm_to_ctl_instance_status_ptr);
            }
            break;
        case MSG_CM_CTL_NODE_END:
            DoProcessNodeEndMsg((const char *)receiveMsg);
            break;
        case MSG_CM_CTL_DATA_END:
            *recDataEnd = true;
            break;
        default:
            write_runlog(ERROR, "unknown the msg type is %d.\n", cm_msg_type_ptr->msg_type);
            break;
    }
    return ret;
}

status_t ProcessMsgAndPrintStatus(char *receiveMsg, int clusterState)
{
    int wait_time;
    int ret;

    wait_time = g_waitSeconds * 1000;
    bool recDataEnd = false;
    for (; wait_time > 0;) {
        ret = cm_client_flush_msg(CmServer_conn);
        if (ret == TCP_SOCKET_ERROR_EPIPE) {
            CMPQfinish(CmServer_conn);
            CmServer_conn = NULL;
            return CM_ERROR;
        }

        receiveMsg = recv_cm_server_cmd(CmServer_conn);
        while (receiveMsg != NULL) {
            ret = DoProcessQueryMsg(receiveMsg, &recDataEnd, clusterState);
            if (ret != 0) {
                if (ret == CYCLE_BREAK) {
                    break;
                } else if (ret == CYCLE_RETURN) {
                    return CM_SUCCESS;
                }
                return CM_ERROR;
            }
            receiveMsg = recv_cm_server_cmd(CmServer_conn);
        }
        if (recDataEnd) {
            break;
        }

        CmSleep(1);
        wait_time--;
        if (wait_time <= 0) {
            break;
        }
    }

    if (wait_time <= 0) {
        CMPQfinish(CmServer_conn);
        CmServer_conn = NULL;
        write_runlog(ERROR, "send query msg to cm_server failed.\n");
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

int do_query(void)
{
    char* receiveMsg = NULL;
    int clusterState;
    ctl_to_cm_query queryContent;
    int ret;

    CM_RETURN_IFERR(SetQueryLogHander());
    CM_RETURN_IFERR(QueryEtcdAndCms());
    if (HAS_RES_DEFINED) {
        QueryResourceStatus(&clusterState);
    }

    CM_RETURN_IFERR(SetCmQueryContent(&queryContent));

    /* return conn to cm_server */
    do_conn_cmserver(false, 0);
    if (CmServer_conn == NULL) {
        write_runlog(ERROR,
            "can't connect to cm_server.\n"
            "Maybe cm_server is not running, or timeout expired. Please try again.\n");
         return CM_ERROR;
    }

    ret = cm_client_send_msg(CmServer_conn, 'C', (char*)&queryContent, sizeof(queryContent));
    if (ret != 0) {
        CMPQfinish(CmServer_conn);
        CmServer_conn = NULL;
        return CM_ERROR;
    }
    CmSleep(1);

    CM_RETURN_IFERR(ProcessMsgAndPrintStatus(receiveMsg, clusterState));

    CMPQfinish(CmServer_conn);
    CmServer_conn = NULL;
    return CM_SUCCESS;
}

static void query_cmserver_and_etcd_status(void)
{
    uint32 i = 0;
    uint32 etcd_index = 0;
    /* query cm_server */
    uint32 node_len = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    uint32 instance_len = INSTANCE_ID_LEN + SPACE_LEN + (g_dataPathQuery ? (max_cmpath_len + 11) : 4);
    bool query_cmserver = false;

    if (g_availabilityZoneCommand) {
        node_len += max_az_name_len + SPACE_LEN;
    }

    fprintf(g_logFilePtr, "[  CMServer State   ]\n\n");
    if (g_ipQuery) {
        fprintf(g_logFilePtr,
            "%-*s%-*s%-*s%s\n",
            node_len,
            "node",
            MAX_IP_LEN + 1,
            "node_ip",
            instance_len,
            "instance",
            "state");
    } else {
        fprintf(g_logFilePtr, "%-*s%-*s%s\n", node_len, "node", instance_len, "instance", "state");
    }
    for (i = 0; i < node_len + instance_len + INSTANCE_DYNAMIC_ROLE_LEN + (g_ipQuery ? (MAX_IP_LEN + 1) : 0); i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");

    if (g_cm_server_num > CM_PRIMARY_STANDBY_NUM) {
        write_runlog(ERROR, "the number of cm_server is bigger than %d.\n", CM_PRIMARY_STANDBY_NUM);
        exit(1);
    }

    if (0 == g_commandOperationNodeId) {
        query_cmserver = true;
    } else {
        for (uint32 kk = 0; kk < g_cm_server_num; kk++) {
            uint32 cm_server_node_index = g_nodeIndexForCmServer[kk];

            if (g_node[cm_server_node_index].node == g_commandOperationNodeId) {
                query_cmserver = true;
                break;
            }
        }
    }
    if (query_cmserver) {
        bool findAbnormal = false;
        for (uint32 kk = 0; kk < g_cm_server_num; kk++) {
            uint32 cm_server_node_index = g_nodeIndexForCmServer[kk];
            g_cmServerState[kk] = query_cm_server(cm_server_node_index);
            if (g_cmServerState[kk] == NULL) {
                write_runlog(ERROR, "unexpected cmserver state.\n");
                exit(1);
            }
            if (g_abnormalQuery && strcmp("Primary", g_cmServerState[kk]) != 0 && 
                strcmp("Standby", g_cmServerState[kk]) != 0) {
                findAbnormal = true;
            }
        }

        if (findAbnormal == true || g_abnormalQuery == false) {
            for (uint32 kk = 0; kk < g_cm_server_num; kk++) {
                uint32 cm_server_node_index = g_nodeIndexForCmServer[kk];
                do_query_cmserver(cm_server_node_index, g_cmServerState[kk]);
            }
        }
    }

    fprintf(g_logFilePtr, "\n");

    /* query etcd */
    if (g_etcd_num > 0) {
        instance_len = INSTANCE_ID_LEN + SPACE_LEN + (g_dataPathQuery ? (max_etcdpath_len + 1) : 4);

        fprintf(g_logFilePtr, "[    ETCD State     ]\n\n");
        if (g_ipQuery) {
            fprintf(g_logFilePtr,
                "%-*s%-*s%-*s%s\n",
                node_len,
                "node",
                MAX_IP_LEN + 1,
                "node_ip",
                instance_len,
                "instance",
                "state");
        } else {
            fprintf(g_logFilePtr, "%-*s%-*s%s\n", node_len, "node", instance_len, "instance", "state");
        }
        for (i = 0; i < node_len + instance_len + ETCD_DYNAMIC_ROLE_LEN + (g_ipQuery ? (MAX_IP_LEN + 1) : 0); i++) {
            fprintf(g_logFilePtr, "-");
        }
        fprintf(g_logFilePtr, "\n");
    }

    for (etcd_index = 0; etcd_index < g_node_num; etcd_index++) {
        if (g_node[etcd_index].etcd && g_node[etcd_index].node == g_commandOperationNodeId) {
            break;
        }
    }

    if (0 == g_commandOperationNodeId || etcd_index != g_node_num) {
        const char* state[CM_PRIMARY_STANDBY_NUM] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
        bool findAbnormal = false;
        int state_index = 0;
        if (g_etcd_num > CM_PRIMARY_STANDBY_NUM) {
            write_runlog(ERROR, "the number of etcd is bigger than %d.\n", CM_PRIMARY_STANDBY_NUM);
            exit(1);
        }
        for (etcd_index = 0; etcd_index < g_node_num; etcd_index++) {
            if (g_node[etcd_index].etcd) {
                state[state_index] = query_etcd(etcd_index);
                if (g_abnormalQuery &&
                    ((state[state_index] != NULL && strcmp("StateFollower", state[state_index]) != 0 &&
                         strcmp("StateLeader", state[state_index]) != 0) ||
                        (state[state_index] == NULL))) {
                    findAbnormal = true;
                }
                state_index++;
            }
        }

        if (findAbnormal == true || g_abnormalQuery == false) {
            state_index = 0;
            for (etcd_index = 0; etcd_index < g_node_num; etcd_index++) {
                if (g_node[etcd_index].etcd) {
                    if (g_availabilityZoneCommand) {
                        fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[etcd_index].azName);
                    }
                    fprintf(g_logFilePtr, "%-2u ", g_node[etcd_index].node);
                    fprintf(g_logFilePtr, "%-*s ", max_node_name_len, g_node[etcd_index].nodeName);
                    if (g_ipQuery)
                        fprintf(g_logFilePtr, "%-15s ", g_node[etcd_index].etcdClientListenIPs[0]);
                    fprintf(g_logFilePtr, "%u ", g_node[etcd_index].etcdId);
                    if (g_dataPathQuery)
                        fprintf(g_logFilePtr, "%-*s ", max_etcdpath_len, g_node[etcd_index].etcdDataPath);
                    else
                        fprintf(g_logFilePtr, "    ");

                    if (state[state_index] != NULL) {
                        fprintf(g_logFilePtr, "%s\n", state[state_index]);
                    } else {
                        fprintf(g_logFilePtr, "Down\n");
                    }
                    if (state[state_index] != NULL && strcmp("StateFollower", state[state_index]) != 0 &&
                        strcmp("StateLeader", state[state_index]) != 0) {
                        write_runlog(DEBUG1,
                            "ETCD State: node=%u nodeName=%s ip=%s instanceId=%u DataPath=%s state=%s\n",
                            g_node[etcd_index].node, g_node[etcd_index].nodeName,
                            g_node[etcd_index].etcdClientListenIPs[0], g_node[etcd_index].etcdId,
                            g_node[etcd_index].etcdDataPath, state[state_index]);
                    }
                    state_index++;
                }
            }
        }
    }

    if (g_etcd_num > 0) {
        fprintf(g_logFilePtr, "\n");
    }
}

static void GetDdbCfgApi(DrvApiInfo *drvApiInfo, ServerSocket *server, uint32 serverLen)
{
    drvApiInfo->nodeNum = serverLen - 1;
    drvApiInfo->serverList = server;
    drvApiInfo->serverLen = serverLen;
    drvApiInfo->modId = MOD_CMCTL;
    drvApiInfo->nodeId = g_currentNode->node;
    drvApiInfo->timeOut = DDB_DEFAULT_TIMEOUT;

    drvApiInfo->client_t.tlsPath = &g_tlsPath;
}

const char* query_etcd(uint32 node_id)
{
    if (g_etcd_num == 0) {
        return etcd_role_to_string(CM_ETCD_DOWN);
    }
    const uint32 serverLen = 2;
    ServerSocket server[serverLen] = {{0}};
    SetServerSocketWithEtcdInfo(&server[0], &(g_node[node_id]));
    server[1].host = NULL;
    status_t st = CM_SUCCESS;
    DdbInitConfig config = {DB_ETCD};
    GetDdbCfgApi(&config.drvApiInfo, server, serverLen);
    DdbNodeState nodeState = {DDB_STATE_UNKOWN, DDB_ROLE_UNKNOWN};
    DdbConn dbCon = {0};
    if (g_commandMinortityAzName != NULL && g_commandMinortityAzName[0] != '0') {
        return (strcmp(g_node[node_id].azName, g_commandMinortityAzName) != 0) ? "Skip" : "Down";
    }

    st = InitDdbConn(&dbCon, &config);
    if (st != CM_SUCCESS) {
        write_runlog(ERROR, "etcd open failed: %s.\n", DdbGetLastError(&dbCon));
        exit(1);
    }

    st = DdbInstanceState(&dbCon, g_node[node_id].etcdName, &nodeState);

    if (DdbFreeConn(&dbCon) != CM_SUCCESS) {
        write_runlog(WARNING, "etcd_close failed,%s", DdbGetLastError(&dbCon));
    }

    if (st == CM_SUCCESS) {
        if (nodeState.role == DDB_ROLE_FOLLOWER) {
            return etcd_role_to_string(CM_ETCD_FOLLOWER);
        } else {
            if (hasFindEtcdL) {
                return etcd_role_to_string(CM_ETCD_FOLLOWER);
            }
            hasFindEtcdL = true;
            return etcd_role_to_string(CM_ETCD_LEADER);
        }
    }

    cm_msg_type* cm_msg_type_ptr = NULL;
    cm_query_instance_status cm_query_instance_status_content;
    char* receive_msg = NULL;
    cm_query_instance_status* cm_query_instance_status_ptr = NULL;
    int ret;
    int doSendTryTime = 3;

    cm_query_instance_status_content.msg_type = MSG_CM_QUERY_INSTANCE_STATUS;
    cm_query_instance_status_content.nodeId = g_node[node_id].node;
    cm_query_instance_status_content.instanceType = PROCESS_ETCD;
    cm_query_instance_status_content.msg_step = QUERY_STATUS_CMSERVER_STEP;

    if (CmServer_conn2 == NULL) {
        do_conn_cmserver(false, node_id, true);
        if (CmServer_conn2 == NULL) {
            write_runlog(DEBUG1, "failed to build cmserver connection in query_etcd.\n");
            return etcd_role_to_string(CM_ETCD_DOWN);
        }
    }

    do {
        int doGetAckTryTime = 10;
        if (CmServer_conn2 == NULL) {
            do_conn_cmserver(false, node_id, true);
            if (CmServer_conn2 == NULL) {
                doSendTryTime--;
                continue;
            }
        }

        ret = cm_client_send_msg(
            CmServer_conn2, 'C', (char*)&cm_query_instance_status_content, sizeof(cm_query_instance_status_content));
        if (ret != 0) {
            doSendTryTime--;
            continue;
        }

        do {
            ret = cm_client_flush_msg(CmServer_conn2);
            if (ret == TCP_SOCKET_ERROR_EPIPE) {
                doGetAckTryTime--;
                continue;
            }

            CmSleep(1);
            receive_msg = recv_cm_server_cmd(CmServer_conn2);
            if (receive_msg != NULL) {
                cm_msg_type_ptr = (cm_msg_type*)receive_msg;
                if (cm_msg_type_ptr->msg_type == MSG_CM_QUERY_INSTANCE_STATUS) {
                    int local_role = 0;
                    cm_query_instance_status_ptr = (cm_query_instance_status*)receive_msg;
                    if (cm_query_instance_status_ptr->msg_step != QUERY_STATUS_CMSERVER_STEP) {
                        CmSleep(1);
                        doGetAckTryTime--;
                        continue;
                    }
                    local_role = (int)cm_query_instance_status_ptr->status;
                    if (CmServer_conn2 != NULL) {
                        CMPQfinish(CmServer_conn2);
                        CmServer_conn2 = NULL;
                    }
                    if (local_role == CM_ETCD_LEADER && hasFindEtcdL) {
                        local_role = CM_ETCD_FOLLOWER;
                    }
                    if (local_role == CM_ETCD_LEADER) {
                        hasFindEtcdL = true;
                    }

                    return etcd_role_to_string(local_role);
                }
            }
            doGetAckTryTime--;
            CmSleep(1);
        } while (doGetAckTryTime > 0);
        doSendTryTime--;
    } while (doSendTryTime > 0);
    if (CmServer_conn2 != NULL) {
        CMPQfinish(CmServer_conn2);
        CmServer_conn2 = NULL;
    }
    write_runlog(DEBUG1, "failed to query etcd's status from cmserver.\n");
    return etcd_role_to_string(CM_ETCD_DOWN);
}

static void QueryResourceStatus(int *clusterState)
{
    uint32 node_len = MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN;
    int instance_len = INSTANCE_ID_LEN + SPACE_LEN + 4;
    int res_len = 9;

    fprintf(g_logFilePtr, "\n[ Defined Resource State ]\n\n");
    if (g_ipQuery) {
        fprintf(g_logFilePtr,
            "%-*s%-*s%-*s%-*s%-s\n",
            node_len,
            "node",
            MAX_IP_LEN + 1,
            "node_ip",
            res_len,
            "res_name",
            instance_len,
            "instance",
            "state");
    } else {
        fprintf(
            g_logFilePtr, "%-*s%-*s%-*s%-s\n", node_len, "node", res_len, "res_name", instance_len, "instance", "state");
    }
    for (uint32 i = 0; i < node_len + instance_len + INSTANCE_DYNAMIC_ROLE_LEN + res_len +
                        (g_ipQuery ? (MAX_IP_LEN + 1) : 0);
        i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");

    GetRersourceStatusInfo(clusterState);
}

static int GetStatusFromMsg(uint32 instanceId, const OneNodeResourceStatus *nodeStatus)
{
    for (uint32 i = 0; i < CM_MAX_RES_NODE_COUNT; i++) {
        if (nodeStatus[i].count == 0) {
            continue;
        }
        if (nodeStatus[i].node == 0) {
            continue;
        }
        for (uint32 j = 0; j < nodeStatus[i].count; j++)
        {
            if (instanceId == nodeStatus[i].status[j].instanceId) {
                return nodeStatus[i].status[j].status;
            }
        }
    }
    return 0;
}

static void PrintResStatusLine(const OneNodeResourceStatus *nodeStatus, int *clusterState)
{
    char *status = NULL;
    int32 ret = 0;

    RES_PTR resInfo;
    for(resInfo = g_res_list.begin(); resInfo != g_res_list.end(); resInfo++) { 
        ret = GetStatusFromMsg(resInfo->cmInstanceId, nodeStatus);
        if (ret == 1) {
            status = "OnLine";
        } else if (ret == 2) {
            status = "OffLine";
            *clusterState = CM_STATUS_NEED_REPAIR;
        } else {
            status = "Unknown";
            *clusterState = CM_STATUS_NEED_REPAIR;
        }
        fprintf(g_logFilePtr, "%u %s  %s    %s    %u    %s\n", resInfo->nodeId,
            g_node[resInfo->nodeId - 1].nodeName, g_node[resInfo->nodeId - 1].sshChannel[0], resInfo->resName,
            resInfo->cmInstanceId, status);
    }

    fprintf(g_logFilePtr, "\n");
    return;
}

static int ProcessRersourceStatusInfo()
{
    cm_to_ctl_group_resource_status *cm_query_instance_status_ptr = NULL;
    cm_msg_type *cm_msg_type_ptr = NULL;
    char* receiveMsg = NULL;
    OneNodeResourceStatus *group_status;
    int doSendTryTime = 3;
    int clusterState = CM_STATUS_NORMAL;
    int ret = -1;

    do {
        int doGetAckTryTime = 10;
        if (CmServer_conn == NULL) {
            do_conn_cmserver(false, 0);
            if (CmServer_conn == NULL) {
                doSendTryTime--;
                continue;
            }
        }

        do {
            ret = cm_client_flush_msg(CmServer_conn);
            if (ret == TCP_SOCKET_ERROR_EPIPE) {
                doGetAckTryTime--;
                continue;
            }

            CmSleep(1);
            receiveMsg = recv_cm_server_cmd(CmServer_conn);
            if (receiveMsg != NULL) {
                cm_msg_type_ptr = (cm_msg_type*)receiveMsg;
                if (cm_msg_type_ptr->msg_type == MSG_CM_QUERY_INSTANCE_STATUS) {
                    cm_query_instance_status_ptr = (cm_to_ctl_group_resource_status*)receiveMsg;
                    if (cm_query_instance_status_ptr->msg_step != QUERY_STATUS_CMSERVER_STEP) {
                        CmSleep(1);
                        doGetAckTryTime--;
                        continue;
                    }
                    group_status = cm_query_instance_status_ptr->group_status;
                    PrintResStatusLine(group_status, &clusterState);
                    break;
                }
            }
            doGetAckTryTime--;
            CmSleep(1);
        } while (doGetAckTryTime > 0);
        doSendTryTime--;
    } while (doSendTryTime > 0);

    return clusterState;
}

static void GetRersourceStatusInfo(int *clusterState)
{
    cm_to_ctl_group_resource_status  cm_query_instance_status_content;
    int ret;

    size_t count = g_res_list.size();
    if (count == 0) {
        return;
    }

    cm_query_instance_status_content.msg_type = MSG_CTL_CM_RESOURCE_STATUS;
    cm_query_instance_status_content.msg_step = QUERY_STATUS_CMSERVER_STEP;
    cm_query_instance_status_content.instance_type = PROCESS_RESOURCE;

    if (CmServer_conn == NULL) {
        do_conn_cmserver(false, 0);
        if (CmServer_conn == NULL) {
            write_runlog(DEBUG1, "failed to build cmserver connection in query_etcd.\n");
            exit(0);
        }
    }

    ret = cm_client_send_msg(CmServer_conn, 'C', (char*)&cm_query_instance_status_content, sizeof(cm_to_ctl_group_resource_status));
    if (ret != 0) {
        CMPQfinish(CmServer_conn);
        CmServer_conn = NULL;
        exit(0);
    }
    CmSleep(1);

    *clusterState = ProcessRersourceStatusInfo(); 
}


static void query_kerberos(void)
{
    char kerberos_config_path[MAX_PATH_LEN] = {0}; 
    int isKerberos = cmctl_getenv("MPPDB_KRB5_FILE_PATH", kerberos_config_path, sizeof(kerberos_config_path)); 
    if (isKerberos != EOK) {
        write_runlog(DEBUG1, "query_kerberos: MPPDB_KRB5_FILE_PATH get fail.\n");
        return;
    }
    struct stat stat_buf;
    if (stat(kerberos_config_path, &stat_buf) != 0) {
        write_runlog(DEBUG1, "query_kerberos: kerberos config file not exist.\n");
        return;
    }
    int node_len = (int)(MAX_NODE_ID_LEN + SPACE_LEN + max_node_name_len + SPACE_LEN);
    int kerberos_len = 2 * (INSTANCE_ID_LEN + SPACE_LEN);
    if (g_availabilityZoneCommand) {
        node_len += (int)(max_az_name_len + SPACE_LEN);
    }
    fprintf(g_logFilePtr, "[  Kerberos State  ]\n\n");
    fprintf(g_logFilePtr, "%-*s%-*s%-*s%-*s\n", node_len, "node", MAX_IP_LEN + 1,
        "kerberos_ip", kerberos_len, "port", kerberos_len, "state");
    for (int i = 0; i < MAX_IP_LEN + kerberos_len + kerberos_len + node_len; i++) {
        fprintf(g_logFilePtr, "-");
    }
    fprintf(g_logFilePtr, "\n");
    query_kerberos_status();
    return;
}

/* query kerberos status */
static void query_kerberos_status()
{
    cm_to_ctl_kerberos_status_query* kerberos_status_ptr = NULL;
    cm_msg_type* cm_msg_type_ptr = NULL;
    cm_msg_type cm_ctl_query_kerberos = {0};
    int wait_time;
    char* receive_msg = NULL;
    int ret;
    char state[MAXLEN] = {0};
    errno_t rc;
    bool sortFlag = false;
    uint32 node_index = 0;
    do_conn_cmserver(false, 0);
    if (CmServer_conn == NULL) {
        write_runlog(DEBUG1,
            "send kerberos query msg to cm_server, connect fail! \n");
        return;
    }
    cm_ctl_query_kerberos.msg_type = MSG_CTL_CM_QUERY_KERBEROS;
    ret = cm_client_send_msg(
        CmServer_conn, 'C', (char*)&cm_ctl_query_kerberos, sizeof(cm_ctl_query_kerberos));
    if (ret != 0) {
        CMPQfinish(CmServer_conn);
        CmServer_conn = NULL;
        write_runlog(DEBUG1, "send kerberos query msg fail!\n");
        return;
    }
    ret = cm_client_flush_msg(CmServer_conn);
    if (ret == TCP_SOCKET_ERROR_EPIPE) {
        CMPQfinish(CmServer_conn);
        CmServer_conn = NULL;
        write_runlog(DEBUG1, "flush kerberos query msg fail!\n");
        return;
    }
    for (wait_time = g_waitSeconds * 1000; wait_time > 0; wait_time--) {
        receive_msg = recv_cm_server_cmd(CmServer_conn);
        if (receive_msg != NULL) {
            cm_msg_type_ptr = (cm_msg_type*)receive_msg;
            if (cm_msg_type_ptr->msg_type == MSG_CTL_CM_QUERY_KERBEROS_ACK) {
                kerberos_status_ptr = (cm_to_ctl_kerberos_status_query*)receive_msg;
                if (kerberos_status_ptr->node[0] > kerberos_status_ptr->node[1]) {
                    sortFlag = true;
                }
                for (uint32 kk = 0; kk < KERBEROS_NUM; kk++) {
                    node_index = kk;
                    if (sortFlag) {
                        node_index = 1 - kk;
                    }
                    rc = strncpy_s(state,
                        MAXLEN,
                        kerberos_status_to_string(kerberos_status_ptr->status[node_index]),
                        strlen(kerberos_status_to_string(kerberos_status_ptr->status[node_index])));
                    securec_check_errno(rc, (void)rc);
                    if (g_availabilityZoneCommand) {
                        fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[node_index].azName);
                    }
                    fprintf(g_logFilePtr, "%-2u ", kerberos_status_ptr->node[node_index]);
                    fprintf(g_logFilePtr, "%-*s ", max_node_name_len, kerberos_status_ptr->nodeName[node_index]);
                    fprintf(g_logFilePtr, "%-15s ", kerberos_status_ptr->kerberos_ip[node_index]);
                    fprintf(g_logFilePtr, "%-9u ", kerberos_status_ptr->port[node_index]);
                    fprintf(g_logFilePtr, "%-7s\n", state);
                }
                fprintf(g_logFilePtr, "\n");
                CMPQfinish(CmServer_conn);
                CmServer_conn = NULL;
                return;
            }
        }
    }
}

/*
 * @Description: get the state of cm_server specified by node id. if failed connect or query cm_server, return "Down".
 *
 * @in node_id:node id of cm_server
 *
 * @out: state of cm_server
 * 
 * @Description: print the state info of cm_server specified by node id.
 */
static void do_query_cmserver(uint32 node_id, const char *state)
{
    int ret;
    char data_path[MAXPGPATH] = {0};

    if (g_availabilityZoneCommand) {
        fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[node_id].azName);
    }
    fprintf(g_logFilePtr, "%-2u ", g_node[node_id].node);
    fprintf(g_logFilePtr, "%-*s ", max_node_name_len, g_node[node_id].nodeName);
    if (g_ipQuery)
        fprintf(g_logFilePtr, "%-15s ", g_node[node_id].cmServer[0]);
    fprintf(g_logFilePtr, "%-4u ", g_node[node_id].cmServerId);
    if (g_dataPathQuery) {
        ret = snprintf_s(data_path, MAXPGPATH, MAXPGPATH - 1, "%s/cm_server", g_node[node_id].cmDataPath);
        securec_check_intval(ret, (void)ret);
        fprintf(g_logFilePtr, "%-*s ", max_cmpath_len + 10, data_path);
    } else {
        fprintf(g_logFilePtr, "    ");
    }

    fprintf(g_logFilePtr, "%s\n", state);
    if (strcmp("Primary", state) != 0 && strcmp("Standby", state) != 0) {
        write_runlog(DEBUG1, "CMServer State: node=%u nodeName=%s ip=%s instanceId=%u DataPath=%s state=%s\n",
            g_node[node_id].node, g_node[node_id].nodeName, g_node[node_id].cmServer[0], g_node[node_id].cmServerId,
            data_path, state);
    }
    return;
}

static const char* query_cm_server(uint32 node_id)
{
    if (g_commandMinortityAzName != NULL && g_commandMinortityAzName[0] != '0' &&
        strcmp(g_node[node_id].azName, g_commandMinortityAzName) != 0) {
        return "Skip";
    }
    return (char*)query_cm_server_directory(node_id);
}

static const char* query_cm_server_directory(uint32 node_id)
{
    cm_msg_type* cm_msg_type_ptr = NULL;
    cm_msg_type cm_ctl_cm_query_cmserver_content = {0};
    int wait_time;
    char* receive_msg = NULL;
    cm_to_ctl_cmserver_status* cm_to_ctl_cmserver_status_ptr = NULL;
    int ret;

    do_conn_cmserver(true, node_id);
    if (CmServer_conn1 == NULL) {
        write_runlog(DEBUG1, "can't connect to cm_server node %u node_name %s\n", 
            g_node[node_id].node, g_node[node_id].nodeName);
        return "Down";
    }

    cm_ctl_cm_query_cmserver_content.msg_type = MSG_CTL_CM_QUERY_CMSERVER;
    ret = cm_client_send_msg(
        CmServer_conn1, 'C', (char*)&cm_ctl_cm_query_cmserver_content, sizeof(cm_ctl_cm_query_cmserver_content));
    if (ret != 0) {
        FINISH_CONNECTION1();
    }

    ret = cm_client_flush_msg(CmServer_conn1);
    if (ret == TCP_SOCKET_ERROR_EPIPE) {
        FINISH_CONNECTION1();
    }

    CmSleep(1);

    for (wait_time = g_waitSeconds * 1000; wait_time > 0; wait_time--) {
        receive_msg = recv_cm_server_cmd(CmServer_conn1);
        if (receive_msg != NULL) {
            cm_msg_type_ptr = (cm_msg_type*)receive_msg;
            if (cm_msg_type_ptr->msg_type == MSG_CM_CTL_CMSERVER) {
                int local_role = 0;
                bool is_pending = false;
                cm_to_ctl_cmserver_status_ptr = (cm_to_ctl_cmserver_status*)receive_msg;
                local_role = cm_to_ctl_cmserver_status_ptr->local_role;
                is_pending = cm_to_ctl_cmserver_status_ptr->is_pending;
                CMPQfinish(CmServer_conn1);
                CmServer_conn1 = NULL;
                if (hasFindCmSP && local_role == CM_SERVER_PRIMARY) {
                    local_role = CM_SERVER_STANDBY;
                }
                return server_role_to_string(local_role, is_pending);
            }
        }

        CmSleep(1);
    }

    FINISH_CONNECTION1();
}

static const uint32 timeMaxLen = 10;

static void GetEstTimeStr(int32 estTime, char *estTimeStr, uint32 maxLen)
{
    const int32 sec2hour = 60 * 60;
    const int32 sec2min = 60;

    error_t rc = 0;
    if (estTime == -1) {
        rc = strncpy_s(estTimeStr, maxLen, "--:--:--", maxLen - 1);
        securec_check_errno(rc, (void)rc);
    } else {
        rc = snprintf_s(estTimeStr, maxLen, maxLen - 1, "%.2d:%.2d:%.2d",
            estTime / sec2hour, (estTime % sec2hour) / sec2min, (estTime % sec2hour) % sec2min);
        securec_check_intval(rc, (void)rc);
    }
}

static void GetBytesStr(uint64 size, char *bytesStr, uint32 maxLen)
{
    const int32 k2m = 1024;
    const int32 k2g = k2m * 1024;
    const int32 k2t = k2g * 1024;

    float showSize = 0;
    const char *unit = NULL;
    if (size >= k2t) {
        showSize = (float)size / k2t;
        unit = "TB";
    } else if (size >= k2g) {
        showSize = (float)size / k2g;
        unit = "GB";
    } else if (size >= k2m) {
        showSize = (float)size / k2m;
        unit = "MB";
    } else {
        showSize = (float)size;
        unit = "KB";
    }

    error_t rc = snprintf_s(bytesStr, maxLen, maxLen - 1, "%.2f%s", showSize, unit);
    securec_check_intval(rc, (void)rc);
}

static void GetCnStatus(char *cnStatus, size_t len, const cm_to_ctl_instance_status *cmToCtlInstanceStatusPtr)
{
    errno_t rc;
    int status = cmToCtlInstanceStatusPtr->coordinatemember.status;
    if (undocumentedVersion == 0 || undocumentedVersion >= 92214) {
        int dbState = cmToCtlInstanceStatusPtr->data_node_member.local_status.db_state;
        int buildReason = cmToCtlInstanceStatusPtr->data_node_member.local_status.buildReason;
        if (status == INSTANCE_ROLE_NORMAL && dbState != INSTANCE_HA_STATE_NORMAL) {
            if (dbState == INSTANCE_HA_STATE_WAITING) {
                rc = strcpy_s(cnStatus, len, "Waiting");
                securec_check_errno(rc, (void)rc);
            } else {
                rc = strcpy_s(cnStatus, len, datanode_dbstate_int_to_string(dbState));
                securec_check_errno(rc, (void)rc);
            }
            if (dbState == INSTANCE_HA_STATE_NEED_REPAIR) {
                char buildReasonStr[MAXPGPATH] = {0};
                rc = snprintf_s(buildReasonStr, MAXPGPATH, MAXPGPATH - 1, "(%s)",
                    datanode_rebuild_reason_int_to_string(buildReason));
                securec_check_intval(rc, (void)rc);
                rc = strcat_s(cnStatus, len, buildReasonStr);
                securec_check_errno(rc, (void)rc);
            }
        } else {
            rc = strcpy_s(cnStatus, len, datanode_role_int_to_string(status));
            securec_check_errno(rc, (void)rc);
        }
    } else {
        rc = strcpy_s(cnStatus, len, datanode_role_int_to_string(status));
        securec_check_errno(rc, (void)rc);
    }
    return;
}

static status_t PrintResult(uint32 *pre_node, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr)
{
    uint32 i;
    uint32 j;
    uint32 node_index = 0;
    uint32 instance_index = 0;
    uint32 cm_server_index = 0;

    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].node == cm_to_ctl_instance_status_ptr->node) {
            node_index = i;
            break;
        }
    }

    if (i >= g_node_num) {
        write_runlog(ERROR, "can't find the node(%u).", cm_to_ctl_instance_status_ptr->node);
        return CM_ERROR;
    }
    
    if (g_cm_server_num > CM_PRIMARY_STANDBY_NUM) {
        write_runlog(ERROR, "the number of cm_server is bigger than %d.\n", CM_PRIMARY_STANDBY_NUM);
        return CM_ERROR;
    }

    /* Set the central node information, including: node index, instance id, and node status */
    if (cm_to_ctl_instance_status_ptr->is_central) {
        g_centralNode.node_index = node_index;
        g_centralNode.instanceId = cm_to_ctl_instance_status_ptr->instanceId;
        g_centralNode.status = cm_to_ctl_instance_status_ptr->coordinatemember.status;
    }

    if ((g_detailQuery && (cm_to_ctl_instance_status_ptr->node != *pre_node)) || g_coupleQuery) {
        fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
        fprintf(g_logFilePtr, "node_name                 : %s\n\n", g_node[node_index].nodeName);
        *pre_node = cm_to_ctl_instance_status_ptr->node;

        if (g_node[node_index].cmServerLevel) {
            fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
            fprintf(g_logFilePtr, "instance_id               : %u\n", g_node[node_index].cmServerId);
            fprintf(g_logFilePtr, "node_ip                   : %s\n", g_node[node_index].cmServer[0]);
            fprintf(g_logFilePtr, "data_path                 : %s/cm_server\n", g_node[node_index].cmDataPath);
            fprintf(g_logFilePtr, "type                      : CMServer\n");
            for (uint32 k = 0; k < g_cm_server_num; k++) {
                if (g_nodeIndexForCmServer[k] == node_index) {
                    cm_server_index = k;
                    break;
                }
            }
            fprintf(g_logFilePtr, "instance_state            : %s\n\n", g_cmServerState[cm_server_index]);
        }

        if (g_node[node_index].etcd) {
            const char* etcd_state = NULL;

            fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
            fprintf(g_logFilePtr, "instance_id               : %u\n", g_node[node_index].etcdId);
            fprintf(g_logFilePtr, "node_ip                   : %s\n", g_node[node_index].etcdClientListenIPs[0]);
            fprintf(g_logFilePtr, "data_path                 : %s\n", g_node[node_index].etcdDataPath);
            fprintf(g_logFilePtr, "type                      : ETCD\n");
            etcd_state = query_etcd(node_index);
            if (etcd_state != NULL) {
                fprintf(g_logFilePtr, "state                     : %s\n\n", etcd_state);
            } else {
                fprintf(g_logFilePtr, "state                     : Down\n\n");
            }
        }
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_COORDINATE) {
        fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
        fprintf(g_logFilePtr, "instance_id               : %u\n", cm_to_ctl_instance_status_ptr->instanceId);
        fprintf(g_logFilePtr, "node_ip                   : %s\n", g_node[node_index].coordinateListenIP[0]);
        fprintf(g_logFilePtr, "data_path                 : %s\n", g_node[node_index].DataPath);
        fprintf(g_logFilePtr,
            "type                      : %s\n",
            type_int_to_string(cm_to_ctl_instance_status_ptr->instance_type));
        char cnStatus[MAXPGPATH] = {0};
        GetCnStatus(cnStatus, sizeof(cnStatus), cm_to_ctl_instance_status_ptr);
        fprintf(g_logFilePtr,
            "state                     : %s\n\n", cnStatus);
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_GTM) {
        fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
        fprintf(g_logFilePtr, "instance_id               : %u\n", cm_to_ctl_instance_status_ptr->instanceId);
        fprintf(g_logFilePtr, "node_ip                   : %s\n", g_node[node_index].gtmLocalListenIP[0]);
        fprintf(g_logFilePtr, "data_path                 : %s\n", g_node[node_index].gtmLocalDataPath);
        fprintf(g_logFilePtr,
            "type                      : %s\n",
            type_int_to_string(cm_to_ctl_instance_status_ptr->instance_type));
        fprintf(g_logFilePtr,
            "instance_state            : %s\n",
            datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->gtm_member.local_status.local_role));
        if (!g_single_node_cluster) {
            fprintf(g_logFilePtr,
                "con_state                 : %s\n",
                gtm_con_int_to_string(cm_to_ctl_instance_status_ptr->gtm_member.local_status.connect_status));
        }
        fprintf(g_logFilePtr,
            "transaction_id            : %lu\n",
            cm_to_ctl_instance_status_ptr->gtm_member.local_status.xid);
        fprintf(g_logFilePtr,
            "send_count                : %llu\n",
            (long long unsigned int)(cm_to_ctl_instance_status_ptr->gtm_member.local_status.send_msg_count));
        fprintf(g_logFilePtr,
            "receive_count             : %llu\n",
            (long long unsigned int)(cm_to_ctl_instance_status_ptr->gtm_member.local_status.receive_msg_count));
        if (!g_single_node_cluster) {
            fprintf(g_logFilePtr,
                "sync_state                : %s\n\n",
                datanode_wal_sync_state_int_to_string(
                    cm_to_ctl_instance_status_ptr->gtm_member.local_status.sync_mode));
        }
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_DATANODE) {
        for (j = 0; j < g_node[node_index].datanodeCount; j++) {
            if (g_node[node_index].datanode[j].datanodeId == cm_to_ctl_instance_status_ptr->instanceId) {
                instance_index = j;
                break;
            }
        }

        if (j >= g_node[node_index].datanodeCount) {
            write_runlog(ERROR, "can't find the instance(%d).", cm_to_ctl_instance_status_ptr->instanceId);
            return CM_ERROR;
        }

        fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
        fprintf(g_logFilePtr, "instance_id               : %u\n", cm_to_ctl_instance_status_ptr->instanceId);
        fprintf(g_logFilePtr,
            "node_ip                   : %s\n",
            g_node[node_index].datanode[instance_index].datanodeListenIP[0]);
        fprintf(g_logFilePtr,
            "data_path                 : %s\n",
            g_node[node_index].datanode[instance_index].datanodeLocalDataPath);
        fprintf(g_logFilePtr,
            "type                      : %s\n",
            type_int_to_string(cm_to_ctl_instance_status_ptr->instance_type));
        fprintf(g_logFilePtr,
            "instance_state            : %s\n",
            datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role));
        if (cm_to_ctl_instance_status_ptr->data_node_member.receive_status.local_role != 0) {
            fprintf(g_logFilePtr,
                "dcf_role                  : %s\n",
                DcfRoleToString(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.local_role));
        }
        if (g_node[node_index].datanode[instance_index].datanodeRole == DUMMY_STANDBY_DN) {
            fprintf(g_logFilePtr, "\n");
            return CM_SUCCESS;
        }
        fprintf(g_logFilePtr,
            "static_connections        : %d\n",
            cm_to_ctl_instance_status_ptr->data_node_member.local_status.static_connections);
        fprintf(g_logFilePtr,
            "HA_state                  : %s\n",
            datanode_dbstate_int_to_string(cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state));

        if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == INSTANCE_HA_STATE_BUILDING) {
            fprintf(g_logFilePtr, "sync_state                : %s\n",
                    datanode_wal_sync_state_int_to_string(INSTANCE_DATA_REPLICATION_ASYNC));
            if (cm_to_ctl_instance_status_ptr->data_node_member.build_info.build_mode == FULL_BUILD) {
                fprintf(g_logFilePtr, "build_mode                : Full\n");
            } else if (cm_to_ctl_instance_status_ptr->data_node_member.build_info.build_mode == INC_BUILD) {
                fprintf(g_logFilePtr, "build_mode                : Incremental\n");
            }

            char bytesStr[MAXPGPATH] = {0};
            GetBytesStr(cm_to_ctl_instance_status_ptr->data_node_member.build_info.total_done, bytesStr, MAXPGPATH);
            fprintf(g_logFilePtr, "data_synchronized         : %s\n", bytesStr);
            GetBytesStr(cm_to_ctl_instance_status_ptr->data_node_member.build_info.total_size, bytesStr, MAXPGPATH);
            fprintf(g_logFilePtr, "estimated_total_data      : %s\n", bytesStr);
            fprintf(g_logFilePtr,
                "process_schedule          : %d%%\n",
                cm_to_ctl_instance_status_ptr->data_node_member.build_info.process_schedule);
            char estTimeStr[timeMaxLen] = {0};
            GetEstTimeStr(
                cm_to_ctl_instance_status_ptr->data_node_member.build_info.estimated_time, estTimeStr, timeMaxLen);
            fprintf(g_logFilePtr, "estimated_remaining_time  : %s\n\n", estTimeStr);
            return CM_SUCCESS;
        }
        fprintf(g_logFilePtr,
            "reason                    : %s\n",
            datanode_rebuild_reason_int_to_string(
                cm_to_ctl_instance_status_ptr->data_node_member.local_status.buildReason));

        if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role == INSTANCE_ROLE_PRIMARY) {
            if (STANDBY_DN == g_node[node_index].datanode[instance_index].datanodePeerRole ||
                PRIMARY_DN == g_node[node_index].datanode[instance_index].datanodePeerRole) {
                fprintf(g_logFilePtr,
                    "standby_node              : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeerHAIP[0]);
                fprintf(g_logFilePtr,
                    "standby_data_path         : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeerDataPath);
            }
            if (STANDBY_DN == g_node[node_index].datanode[instance_index].datanodePeer2Role ||
                PRIMARY_DN == g_node[node_index].datanode[instance_index].datanodePeer2Role) {
                fprintf(g_logFilePtr,
                    "standby_node              : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeer2HAIP[0]);
                fprintf(g_logFilePtr,
                    "standby_data_path         : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeer2DataPath);
            }

            fprintf(g_logFilePtr,
                "standby_state             : %s\n",
                datanode_role_int_to_string(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].peer_role));
            fprintf(g_logFilePtr,
                "sender_sent_location      : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_sent_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_sent_location);
            fprintf(g_logFilePtr,
                "sender_write_location     : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_write_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_write_location);
            fprintf(g_logFilePtr,
                "sender_flush_location     : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_flush_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_flush_location);
            fprintf(g_logFilePtr,
                "sender_replay_location    : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_replay_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sender_replay_location);
            fprintf(g_logFilePtr,
                "receiver_received_location: %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_received_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_received_location);
            fprintf(g_logFilePtr,
                "receiver_write_location   : %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_write_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_write_location);
            fprintf(g_logFilePtr,
                "receiver_flush_location   : %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_flush_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_flush_location);
            fprintf(g_logFilePtr,
                "receiver_replay_location  : %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_replay_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].receiver_replay_location);
            fprintf(g_logFilePtr,
                "sync_state                : %s\n",
                datanode_wal_sync_state_int_to_string(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sync_state));
            if (DUMMY_STANDBY_DN == g_node[node_index].datanode[instance_index].datanodePeerRole) {
                fprintf(g_logFilePtr,
                    "secondary_node            : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeerHAIP[0]);
                fprintf(g_logFilePtr,
                    "secondary_data_path       : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeerDataPath);
            }
            if (DUMMY_STANDBY_DN == g_node[node_index].datanode[instance_index].datanodePeer2Role) {
                fprintf(g_logFilePtr,
                    "secondary_node            : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeer2HAIP[0]);
                fprintf(g_logFilePtr,
                    "secondary_data_path       : %s\n",
                    g_node[node_index].datanode[instance_index].datanodePeer2DataPath);
            }
            fprintf(g_logFilePtr,
                "secondary_state           : %s\n",
                datanode_role_int_to_string(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].peer_role));
            fprintf(g_logFilePtr,
                "sender_sent_location      : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_sent_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_sent_location);
            fprintf(g_logFilePtr,
                "sender_write_location     : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_write_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_write_location);
            fprintf(g_logFilePtr,
                "sender_flush_location     : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_flush_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_flush_location);
            fprintf(g_logFilePtr,
                "sender_replay_location    : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_replay_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sender_replay_location);
            fprintf(g_logFilePtr,
                "receiver_received_location: %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_received_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_received_location);
            fprintf(g_logFilePtr,
                "receiver_write_location   : %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_write_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_write_location);
            fprintf(g_logFilePtr,
                "receiver_flush_location   : %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_flush_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_flush_location);
            fprintf(g_logFilePtr,
                "receiver_replay_location  : %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_replay_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].receiver_replay_location);
            fprintf(g_logFilePtr,
                "sync_state                : %s\n\n",
                datanode_wal_sync_state_int_to_string(
                    cm_to_ctl_instance_status_ptr->data_node_member.sender_status[1].sync_state));
        } else {
            fprintf(g_logFilePtr,
                "sender_sent_location      : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_sent_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_sent_location);
            fprintf(g_logFilePtr,
                "sender_write_location     : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_write_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_write_location);
            fprintf(g_logFilePtr,
                "sender_flush_location     : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_flush_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_flush_location);
            fprintf(g_logFilePtr,
                "sender_replay_location    : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_replay_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_replay_location);
            fprintf(g_logFilePtr,
                "receiver_received_location: %X/%X\n",
                (uint32)(
                    cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_received_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_received_location);
            fprintf(g_logFilePtr,
                "receiver_write_location   : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_write_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_write_location);
            fprintf(g_logFilePtr,
                "receiver_flush_location   : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_flush_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_flush_location);
            fprintf(g_logFilePtr,
                "receiver_replay_location  : %X/%X\n",
                (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_replay_location >> 32),
                (uint32)cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_replay_location);
            if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role == INSTANCE_ROLE_STANDBY) {
                fprintf(g_logFilePtr,
                    "sync_state                : %s\n\n",
                    datanode_wal_sync_state_int_to_string(INSTANCE_DATA_REPLICATION_ASYNC));
            } else {
                fprintf(g_logFilePtr,
                    "sync_state                : %s\n\n",
                    datanode_wal_sync_state_int_to_string(INSTANCE_DATA_REPLICATION_UNKONWN));
            }
        }
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_FENCED_UDF) {
        fprintf(g_logFilePtr, "node                      : %u\n", g_node[node_index].node);
        fprintf(g_logFilePtr, "node_ip                   : %s\n", g_node[node_index].sshChannel[0]);
        fprintf(g_logFilePtr,
            "type                      : %s\n",
            type_int_to_string(cm_to_ctl_instance_status_ptr->instance_type));
        fprintf(g_logFilePtr,
            "state                     : %s\n\n",
            datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->fenced_UDF_status));
    }
    return CM_SUCCESS;
}

static void PrintParallelRedoResult(uint32 *pre_node, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr)
{
    uint32 i;
    uint32 j;
    uint32 node_index = 0;
    uint32 instance_index = 0;
    uint64 speed = 0;
    int64 delay = 0;
    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].node == cm_to_ctl_instance_status_ptr->node) {
            node_index = i;
            break;
        }
    }

    if (i >= g_node_num) {
        write_runlog(DEBUG1, "can't find the node(%u).", cm_to_ctl_instance_status_ptr->node);
        return;
    }

    /* Set the central node information, including: node index, instance id, and node status */
    if (cm_to_ctl_instance_status_ptr->is_central) {
        g_centralNode.node_index = (int)node_index;
        g_centralNode.instanceId = cm_to_ctl_instance_status_ptr->instanceId;
        g_centralNode.status = cm_to_ctl_instance_status_ptr->coordinatemember.status;
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_DATANODE &&
        cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role != INSTANCE_ROLE_PRIMARY) {
        for (j = 0; j < g_node[node_index].datanodeCount; j++) {
            if (g_node[node_index].datanode[j].datanodeId == cm_to_ctl_instance_status_ptr->instanceId) {
                instance_index = j;
                break;
            }
        }

        if (j >= g_node[node_index].datanodeCount) {
            write_runlog(DEBUG1, "can't find the instance(%d).", cm_to_ctl_instance_status_ptr->instanceId);
            return;
        }

        fprintf(g_logFilePtr, "node                           : %u\n", g_node[node_index].node);
        fprintf(g_logFilePtr, "instance_id                    : %u\n", cm_to_ctl_instance_status_ptr->instanceId);
        fprintf(g_logFilePtr,
            "node_ip                        : %s\n",
            g_node[node_index].datanode[instance_index].datanodeListenIP[0]);
        fprintf(g_logFilePtr,
            "data_path                      : %s\n",
            g_node[node_index].datanode[instance_index].datanodeLocalDataPath);
        fprintf(g_logFilePtr,
            "type                           : %s\n",
            type_int_to_string(cm_to_ctl_instance_status_ptr->instance_type));
        fprintf(g_logFilePtr,
            "instance_state                 : %s\n",
            datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role));
        if (cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.curr_time == 0) {
            fprintf(g_logFilePtr, "\n");
            return;
        }
        fprintf(g_logFilePtr,
            "is_by_query                    : %d\n",
            cm_to_ctl_instance_status_ptr->data_node_member.local_redo_stats.is_by_query);

        XLogRecPtr primary_flush_location =
            cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sender_flush_location;
        XLogRecPtr standby_replay_location =
            cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_replay_location;
        XLogRecPtr standby_received_location =
            cm_to_ctl_instance_status_ptr->data_node_member.receive_status.receiver_received_location;
        XLogRecPtr last = cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.last_replayed_read_ptr;
        XLogRecPtr min_point = cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.min_recovery_point;
        XLogRecPtr local_max_lsn = cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.local_max_lsn;
        speed = cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.speed_according_seg;

        if (cm_to_ctl_instance_status_ptr->data_node_member.local_redo_stats.is_by_query &&
            cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role == INSTANCE_ROLE_STANDBY) {
            fprintf(g_logFilePtr,
                "primary_flush_location         : %08X/%08X\n",
                (uint32)(primary_flush_location >> 32),
                (uint32)primary_flush_location);
            fprintf(g_logFilePtr,
                "standby_received_location      : %08X/%08X\n",
                (uint32)(standby_received_location >> 32),
                (uint32)standby_received_location);
            fprintf(g_logFilePtr,
                "standby_replay_location        : %08X/%08X\n",
                (uint32)(standby_replay_location >> 32),
                (uint32)standby_replay_location);
            if (speed > 0) {
                if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == 
                    INSTANCE_HA_STATE_NORMAL) {
                    delay = (primary_flush_location - standby_replay_location) / speed;
                } else {
                    if (local_max_lsn != 0)
                        delay = (local_max_lsn - last) / speed;
                    else
                        delay = -1;
                }
            } else {
                if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == 
                    INSTANCE_HA_STATE_NORMAL) {
                    (primary_flush_location < (standby_replay_location + DELAY_THRESHOLD)) ? 
                        (delay = 0) : (delay = -1);
                } else {
                    delay = -1;
                }
            }
        } else {
            if (speed > 0) {
                if (cm_to_ctl_instance_status_ptr->data_node_member.local_redo_stats.is_by_query == 0) {
                    delay = (int64)((min_point - last) / speed);
                } else if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state != 
                    INSTANCE_HA_STATE_NORMAL) {
                    if (local_max_lsn != 0)
                        delay = (int64)((local_max_lsn - last) / speed);
                    else
                        delay = -1;
                }
            } else {
                if (local_max_lsn == last) {
                    delay = 0;
                } else {
                    delay = -1;
                }
            }
        }

        fprintf(g_logFilePtr,
            "static_connections             : %d\n",
            cm_to_ctl_instance_status_ptr->data_node_member.local_status.static_connections);
        fprintf(g_logFilePtr,
            "HA_state                       : %s\n",
            datanode_dbstate_int_to_string(cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state));

        if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == INSTANCE_HA_STATE_BUILDING) {
            fprintf(g_logFilePtr,
                "sync_state                     : %s\n",
                datanode_wal_sync_state_int_to_string(INSTANCE_DATA_REPLICATION_ASYNC));
            if (cm_to_ctl_instance_status_ptr->data_node_member.build_info.build_mode == FULL_BUILD) {
                fprintf(g_logFilePtr, "build_mode                     : Full\n");
            } else if (cm_to_ctl_instance_status_ptr->data_node_member.build_info.build_mode == INC_BUILD) {
                fprintf(g_logFilePtr, "build_mode                     : Incremental\n");
            }

            char bytesStr[MAXPGPATH] = {0};
            GetBytesStr(cm_to_ctl_instance_status_ptr->data_node_member.build_info.total_done, bytesStr, MAXPGPATH);
            (void)fprintf(g_logFilePtr, "data_synchronized              : %s\n", bytesStr);
            GetBytesStr(cm_to_ctl_instance_status_ptr->data_node_member.build_info.total_size, bytesStr, MAXPGPATH);
            (void)fprintf(g_logFilePtr, "estimated_total_data           : %s\n", bytesStr);
            (void)fprintf(g_logFilePtr,
                "process_schedule               : %d%%\n",
                cm_to_ctl_instance_status_ptr->data_node_member.build_info.process_schedule);

            char estTimeStr[timeMaxLen] = {0};
            GetEstTimeStr(
                cm_to_ctl_instance_status_ptr->data_node_member.build_info.estimated_time, estTimeStr, timeMaxLen);
            (void)fprintf(g_logFilePtr, "estimated_remaining_time       : %s\n\n", estTimeStr);
            return;
        }
        int reason = cm_to_ctl_instance_status_ptr->data_node_member.local_status.buildReason;
        fprintf(g_logFilePtr, "reason                         : %s\n", datanode_rebuild_reason_int_to_string(reason));

        fprintf(g_logFilePtr, "redo_start_location            : %08X/%08X\n",
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.redo_start_ptr >> 32),
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.redo_start_ptr));

        fprintf(g_logFilePtr, "min_recovery_location          : %08X/%08X\n", (uint32)(min_point >> 32),
            (uint32)(min_point));

        fprintf(g_logFilePtr, "read_location                  : %08X/%08X\n",
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.read_ptr >> 32),
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.read_ptr));

        fprintf(g_logFilePtr, "last_replayed_end_location     : %08X/%08X\n", (uint32)(last >> 32), (uint32)(last));

        fprintf(g_logFilePtr, "recovery_done_location         : %08X/%08X\n",
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.recovery_done_ptr >> 32),
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.recovery_done_ptr));

        fprintf(g_logFilePtr, "local_max_lsn                  : %08X/%08X\n",
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.local_max_lsn >> 32),
            (uint32)(cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.local_max_lsn));

        fprintf(g_logFilePtr, "read_xlog_io_counter           : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[0].counter);

        fprintf(g_logFilePtr, "read_xlog_io_total_dur         : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[0].total_duration);

        fprintf(g_logFilePtr, "read_data_io_counter           : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[1].counter);

        fprintf(g_logFilePtr, "read_data_io_total_dur         : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[1].total_duration);

        fprintf(g_logFilePtr, "write_data_io_counter          : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[2].counter);

        fprintf(g_logFilePtr, "write_data_io_total_dur        : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[2].total_duration);

        fprintf(g_logFilePtr, "process_pending_counter        : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[3].counter);

        fprintf(g_logFilePtr, "process_pending_total_dur      : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[3].total_duration);

        fprintf(g_logFilePtr, "apply_counter                  : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[4].counter);

        fprintf(g_logFilePtr, "apply_total_dur                : %ld\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.wait_info[4].total_duration);

        fprintf(g_logFilePtr, "speed(est.)                    : %u KB/s\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.speed_according_seg / 1024);

        if (cm_to_ctl_instance_status_ptr->data_node_member.local_redo_stats.is_by_query == 1) {
            if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == INSTANCE_HA_STATE_NORMAL) {
                if (delay >= 0) {
                    fprintf(g_logFilePtr, "delay(est.)                    : %ld s\n", delay);
                } else {
                    fprintf(g_logFilePtr, "delay(est.)                    : unknown\n");
                }
            } else {
                if (delay >= 0)
                    fprintf(g_logFilePtr, "local_replay_remain_time(est.) : %ld s\n", delay);
                else
                    fprintf(g_logFilePtr, "local_replay_remain_time(est.) : unknown\n");
            }
        } else if (cm_to_ctl_instance_status_ptr->data_node_member.local_redo_stats.is_by_query == 0) {
            if (delay >= 0)
                fprintf(g_logFilePtr, "min_recovery_point_delay(est.) : %ld s\n", delay);
            else
                fprintf(g_logFilePtr, "min_recovery_point_delay(est.) : unknown\n");
        }

        // redo percent, it's usefull when need change qurom
        fprintf(g_logFilePtr, "senderPercent                  : %d%%\n",
            cm_to_ctl_instance_status_ptr->data_node_member.sender_status[0].sync_percent);
        fprintf(g_logFilePtr, "receiverPercent                : %d%%\n",
            cm_to_ctl_instance_status_ptr->data_node_member.receive_status.sync_percent);

        fprintf(g_logFilePtr, "worker_info                    : \n%s\n\n",
            cm_to_ctl_instance_status_ptr->data_node_member.parallel_redo_status.worker_info);
    }
}

static void PrintSimpleCnResult(uint32 nodeIndex, const cm_to_ctl_instance_status* cmToCtlInstanceStatusPtr)
{
    int status = cmToCtlInstanceStatusPtr->coordinatemember.status;
    if (g_availabilityZoneCommand) {
        fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[nodeIndex].azName);
    }
    fprintf(g_logFilePtr, "%-2u ", g_node[nodeIndex].node);
    fprintf(g_logFilePtr, "%-*s ", max_node_name_len, g_node[nodeIndex].nodeName);
    if (g_ipQuery) {
        fprintf(g_logFilePtr, "%-15s ", g_node[nodeIndex].coordinateListenIP[0]);
    }
    fprintf(g_logFilePtr, "%u ", cmToCtlInstanceStatusPtr->instanceId);
    if (g_portQuery) {
        fprintf(g_logFilePtr, "%-*u ", 6, g_node[nodeIndex].coordinatePort);
    }
    if (g_dataPathQuery) {
        fprintf(g_logFilePtr, "%-*s ", max_cnpath_len, g_node[nodeIndex].DataPath);
    } else {
        fprintf(g_logFilePtr, "    ");
    }
    char cnStatus[MAXPGPATH] = {0};
    GetCnStatus(cnStatus, sizeof(cnStatus), cmToCtlInstanceStatusPtr);
    fprintf(g_logFilePtr, "%s", cnStatus);
    fprintf(g_logFilePtr, "\n");
    if (status != INSTANCE_ROLE_NORMAL) {
        InstanceInformationRecord(nodeIndex, cmToCtlInstanceStatusPtr);
    }
}

static void PrintSimpleResult(uint32 *pre_node, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr)
{
    uint32 i;
    uint32 node_index = 0;

    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].node == cm_to_ctl_instance_status_ptr->node) {
            node_index = i;
            break;
        }
    }

    if (i >= g_node_num) {
        write_runlog(ERROR, "can't find the node(%u).", cm_to_ctl_instance_status_ptr->node);
        return;
    }

    /* Set the central node information, including: node index, instance id, and node status */
    if (cm_to_ctl_instance_status_ptr->is_central) {
        g_centralNode.node_index = (int32)node_index;
        g_centralNode.instanceId = cm_to_ctl_instance_status_ptr->instanceId;
        g_centralNode.status = cm_to_ctl_instance_status_ptr->coordinatemember.status;
    }

    if ((g_detailQuery && (cm_to_ctl_instance_status_ptr->node != *pre_node)) || g_coupleQuery) {
        *pre_node = cm_to_ctl_instance_status_ptr->node;
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_COORDINATE &&
        (!g_balanceQuery || g_abnormalQuery)) {
        if (g_abnormalQuery == true &&
            (0 == strcmp(
                      datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->coordinatemember.status), "Normal"))) {
            return;
        }
        PrintSimpleCnResult(node_index, cm_to_ctl_instance_status_ptr);
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_GTM && !g_startStatusQuery) {
        if (g_single_node_cluster && (PRIMARY_GTM != g_node[node_index].gtmRole)) {
            return;
        }
        // end.
        g_gtmBalance = false;

        if (g_availabilityZoneCommand) {
            fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[node_index].azName);
        }

        fprintf(g_logFilePtr, "%-2u ", g_node[node_index].node);
        fprintf(g_logFilePtr, "%-*s ", max_node_name_len, g_node[node_index].nodeName);
        if (g_ipQuery)
            fprintf(g_logFilePtr, "%-15s ", g_node[node_index].gtmLocalListenIP[0]);
        fprintf(g_logFilePtr, "%u ", cm_to_ctl_instance_status_ptr->instanceId);
        if (g_dataPathQuery)
            fprintf(g_logFilePtr, "%-*s ", max_gtmpath_len, g_node[node_index].gtmLocalDataPath);
        else
            fprintf(g_logFilePtr, "    ");
        fprintf(g_logFilePtr, "%s ", datanode_static_role_int_to_string(g_node[node_index].gtmRole));
        fprintf(g_logFilePtr, "%-7s ",
            datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->gtm_member.local_status.local_role));
        if ((cm_to_ctl_instance_status_ptr->gtm_member.local_status.local_role != INSTANCE_ROLE_PRIMARY) &&
            (cm_to_ctl_instance_status_ptr->gtm_member.local_status.local_role != INSTANCE_ROLE_STANDBY)) {
            InstanceInformationRecord(node_index, cm_to_ctl_instance_status_ptr);
        }
        if (!g_single_node_cluster) {
            fprintf(g_logFilePtr, "%-14s ",
                gtm_con_int_to_string(cm_to_ctl_instance_status_ptr->gtm_member.local_status.connect_status));
            fprintf(g_logFilePtr, "%s\n",
                datanode_wal_sync_state_int_to_string(
                    cm_to_ctl_instance_status_ptr->gtm_member.local_status.sync_mode));
        }
    }

    if (cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_DATANODE && !g_startStatusQuery) {
        (void)print_simple_DN_result(node_index, cm_to_ctl_instance_status_ptr);
    }

    if (g_fencedUdfQuery && cm_to_ctl_instance_status_ptr->instance_type == INSTANCE_TYPE_FENCED_UDF &&
        !g_balanceQuery) {
        fprintf(g_logFilePtr, "%-2u ", g_node[node_index].node);
        fprintf(g_logFilePtr, "%-*s ", max_node_name_len, g_node[node_index].nodeName);
        if (g_ipQuery)
            fprintf(g_logFilePtr, "%-15s ", g_node[node_index].sshChannel[0]);
        fprintf(g_logFilePtr, "%s\n", datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->fenced_UDF_status));
    }
}

static void print_simple_DN_result(uint32 node_index, cm_to_ctl_instance_status *cm_to_ctl_instance_status_ptr)
{
    uint32 j = 0;
    uint32 instance_index = 0;

    for (j = 0; j < g_node[node_index].datanodeCount; j++) {
        if (g_node[node_index].datanode[j].datanodeId == cm_to_ctl_instance_status_ptr->instanceId) {
            instance_index = j;
            break;
        }
    }

    if (j >= g_node[node_index].datanodeCount) {
        write_runlog(DEBUG1, "can't find the instance(%d).", cm_to_ctl_instance_status_ptr->instanceId);
        return;
    }

    if (g_single_node_cluster && (PRIMARY_DN != g_node[node_index].datanode[instance_index].datanodeRole)) {
        return;
    }

    if (g_node[node_index].datanode[instance_index].datanodeRole == PRIMARY_DN &&
        cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role != INSTANCE_ROLE_PRIMARY) {
        g_datanodesBalance = false;
    }

    if (logic_cluster_query) {
        if (!g_single_node_cluster && (cm_to_ctl_instance_status_ptr->member_index == 0)) {
            fprintf(g_logFilePtr, "%-*s ",
                max_logic_cluster_name_len,
                g_node[node_index].datanode[instance_index].LogicClusterName);
            fprintf(g_logFilePtr, " | ");
        }
    }

    if (g_availabilityZoneCommand) {
        fprintf(g_logFilePtr, "%-*s ", max_az_name_len, g_node[node_index].azName);
    }

    fprintf(g_logFilePtr, "%-2u ", g_node[node_index].node);
    fprintf(g_logFilePtr, "%-*s ", max_node_name_len, g_node[node_index].nodeName);
    if (g_ipQuery)
        fprintf(g_logFilePtr, "%-15s ", g_node[node_index].datanode[instance_index].datanodeListenIP[0]);
    fprintf(g_logFilePtr, "%u ", cm_to_ctl_instance_status_ptr->instanceId);
    if (g_portQuery && g_node[node_index].datanode[instance_index].datanodeRole != DUMMY_STANDBY_DN) {
        fprintf(g_logFilePtr, "%-*u ", 6, g_node[node_index].datanode[instance_index].datanodePort);
    }
    if (g_dataPathQuery)
        fprintf(g_logFilePtr, "%-*s ", max_datapath_len, 
            g_node[node_index].datanode[instance_index].datanodeLocalDataPath);
    else
        fprintf(g_logFilePtr, "    ");
    fprintf(g_logFilePtr, "%s ",
        datanode_static_role_int_to_string(g_node[node_index].datanode[instance_index].datanodeRole));
    fprintf(g_logFilePtr, "%-7s",
        datanode_role_int_to_string(cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role));
    fprintf(g_logFilePtr, " %s",
        datanode_dbstate_int_to_string(cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state));
    if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state != INSTANCE_HA_STATE_NORMAL) {
        InstanceInformationRecord(node_index, cm_to_ctl_instance_status_ptr);
    }
    if (cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == INSTANCE_HA_STATE_BUILDING) {
        fprintf(g_logFilePtr, "(%d%%)", cm_to_ctl_instance_status_ptr->data_node_member.build_info.process_schedule);
    }

    if ((cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role == INSTANCE_ROLE_STANDBY ||
        cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role == INSTANCE_ROLE_MAIN_STANDBY ||
        cm_to_ctl_instance_status_ptr->data_node_member.local_status.local_role == INSTANCE_ROLE_CASCADE_STANDBY) &&
        cm_to_ctl_instance_status_ptr->data_node_member.local_status.db_state == INSTANCE_HA_STATE_NEED_REPAIR) {
        fprintf(g_logFilePtr, "(%s)",
            datanode_rebuild_reason_int_to_string(
                cm_to_ctl_instance_status_ptr->data_node_member.local_status.buildReason));
    }

    if (g_multi_az_cluster && ((uint32)cm_to_ctl_instance_status_ptr->member_index < (g_dn_replication_num - 1))) {
        fprintf(g_logFilePtr, " | ");
    } else if (!g_single_node_cluster && !g_multi_az_cluster &&
        (cm_to_ctl_instance_status_ptr->member_index == 0 || cm_to_ctl_instance_status_ptr->member_index == 1)) {
        fprintf(g_logFilePtr, " | ");
    } else {
        fprintf(g_logFilePtr, "\n");
    }
}
