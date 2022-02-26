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
 * cma_datanode_utils.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/client_adpts/libpq/cma_datanode_utils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cma_global_params.h"
#include "cma_datanode_utils.h"
#include "cma_common.h"

cltPqConn_t* g_dnConn[CM_MAX_DATANODE_PER_NODE] = {NULL};
THR_LOCAL cltPqConn_t* g_Conn = NULL;
extern const char* g_progname;
#ifdef ENABLE_MULTIPLE_NODES
static cltPqConn_t* GetDnConnect(int index, const char *dbname);
static int GetDnDatabaseResult(cltPqConn_t* dnConn, const char* runCommand, char* databaseName);
int GetDBTableFromSQL(int index, uint32 databaseId, uint32 tableId, uint32 tableIdSize,
                      DNDatabaseInfo *dnDatabaseInfo, int dnDatabaseCount, char* databaseName, char* tableName);
#endif

#ifdef ENABLE_UT
#define static
#endif

static const char *g_roleLeader = "LEADER";
static const char *g_roleFollower = "FOLLOWER";
static const char *g_rolePassive = "PASSIVE";
static const char *g_roleLogger = "LOGGER";
static const char *g_rolePrecandicate = "PRE_CANDIDATE";
static const char *g_roleCandicate = "CANDIDATE";

static int g_errCountPgStatBadBlock[CM_MAX_DATANODE_PER_NODE] = {0};

static void fill_sql6_report_msg1(agent_to_cm_datanode_status_report* report_msg, cltPqResult_t* node_result)
{
    int rc;
    rc = sscanf_s(Getvalue(node_result, 0, 0), "%lu", &(report_msg->parallel_redo_status.redo_start_ptr));
    check_sscanf_s_result(rc, 1);

    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(Getvalue(node_result, 0, 1), "%ld", &(report_msg->parallel_redo_status.redo_start_time));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);

    rc = sscanf_s(Getvalue(node_result, 0, 2), "%ld", &(report_msg->parallel_redo_status.redo_done_time));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);

    rc = sscanf_s(Getvalue(node_result, 0, 3), "%ld", &(report_msg->parallel_redo_status.curr_time));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);

    rc = sscanf_s(Getvalue(node_result, 0, 4), "%lu", &(report_msg->parallel_redo_status.min_recovery_point));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);

    rc = sscanf_s(Getvalue(node_result, 0, 5), "%lu", &(report_msg->parallel_redo_status.read_ptr));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);

    rc = sscanf_s(Getvalue(node_result, 0, 6), "%lu", &(report_msg->parallel_redo_status.last_replayed_read_ptr));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);

    rc = sscanf_s(Getvalue(node_result, 0, 7), "%lu", &(report_msg->parallel_redo_status.recovery_done_ptr));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
}

static void fill_sql6_report_msg2(agent_to_cm_datanode_status_report* report_msg, cltPqResult_t* node_result)
{
    int rc;
    rc = sscanf_s(Getvalue(node_result, 0, 8), "%ld", &(report_msg->parallel_redo_status.wait_info[0].counter));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc =
        sscanf_s(Getvalue(node_result, 0, 9), "%ld", &(report_msg->parallel_redo_status.wait_info[0].total_duration));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(Getvalue(node_result, 0, 10), "%ld", &(report_msg->parallel_redo_status.wait_info[1].counter));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(
        Getvalue(node_result, 0, 11), "%ld", &(report_msg->parallel_redo_status.wait_info[1].total_duration));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(Getvalue(node_result, 0, 12), "%ld", &(report_msg->parallel_redo_status.wait_info[2].counter));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(
        Getvalue(node_result, 0, 13), "%ld", &(report_msg->parallel_redo_status.wait_info[2].total_duration));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(Getvalue(node_result, 0, 14), "%ld", &(report_msg->parallel_redo_status.wait_info[3].counter));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(
        Getvalue(node_result, 0, 15), "%ld", &(report_msg->parallel_redo_status.wait_info[3].total_duration));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(Getvalue(node_result, 0, 16), "%ld", &(report_msg->parallel_redo_status.wait_info[4].counter));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
    rc = sscanf_s(
        Getvalue(node_result, 0, 17), "%ld", &(report_msg->parallel_redo_status.wait_info[4].total_duration));
    check_sscanf_s_result(rc, 1);
    securec_check_intval(rc, (void)rc);
}

int ReadRedoStateFile(RedoStatsData* redo_state, const char* redo_state_path)
{
    FILE* statef = NULL;

    if (redo_state == NULL) {
        write_runlog(LOG, "Could not get information from redo.state\n");
        return -1;
    }
    statef = fopen(redo_state_path, "re");
    if (statef == NULL) {
        if (errno == ENOENT) {
            char errBuffer[ERROR_LIMIT_LEN];
            write_runlog(LOG,
                "redo state file \"%s\" is not exist, could not get the build infomation: %s\n",
                redo_state_path,
                strerror_r(errno, errBuffer, ERROR_LIMIT_LEN));
        } else {
            char errBuffer[ERROR_LIMIT_LEN];
            write_runlog(LOG,
                "open redo state file \"%s\" failed, could not get the build infomation: %s\n",
                redo_state_path,
                strerror_r(errno, errBuffer, ERROR_LIMIT_LEN));
        }
        return -1;
    }
    if ((fread(redo_state, 1, sizeof(RedoStatsData), statef)) == 0) {
        write_runlog(LOG, "get redo state infomation from the file \"%s\" failed\n", redo_state_path);
        fclose(statef);
        return -1;
    }
    fclose(statef);
    return 0;
}

void check_parallel_redo_status_by_file(
    agent_to_cm_datanode_status_report *report_msg, uint32 ii, const char *redo_state_path)
{
    RedoStatsData parallel_redo_state;

    int rcs = memset_s(&parallel_redo_state, sizeof(parallel_redo_state), 0, sizeof(parallel_redo_state));
    securec_check_errno(rcs, (void)rcs);

    rcs = ReadRedoStateFile(&parallel_redo_state, redo_state_path);
    if (rcs == 0) {
        report_msg->local_redo_stats.is_by_query = 0;
        report_msg->parallel_redo_status.redo_start_ptr = parallel_redo_state.redo_start_ptr;

        report_msg->parallel_redo_status.redo_start_time = parallel_redo_state.redo_start_time;

        report_msg->parallel_redo_status.redo_done_time = parallel_redo_state.redo_done_time;

        report_msg->parallel_redo_status.curr_time = parallel_redo_state.curr_time;

        report_msg->parallel_redo_status.min_recovery_point = parallel_redo_state.min_recovery_point;

        report_msg->parallel_redo_status.read_ptr = parallel_redo_state.read_ptr;

        report_msg->parallel_redo_status.last_replayed_read_ptr = parallel_redo_state.last_replayed_read_ptr;

        report_msg->parallel_redo_status.local_max_lsn = parallel_redo_state.local_max_lsn;

        report_msg->parallel_redo_status.recovery_done_ptr = parallel_redo_state.recovery_done_ptr;

        report_msg->parallel_redo_status.worker_info_len = parallel_redo_state.worker_info_len;

        report_msg->parallel_redo_status.speed_according_seg = parallel_redo_state.speed_according_seg;

        rcs = memcpy_s(report_msg->parallel_redo_status.worker_info,
            REDO_WORKER_INFO_BUFFER_SIZE,
            parallel_redo_state.worker_info,
            parallel_redo_state.worker_info_len);
        securec_check_errno(rcs, (void)rcs);
        rcs = memcpy_s(report_msg->parallel_redo_status.wait_info,
            WAIT_REDO_NUM * sizeof(RedoWaitInfo),
            parallel_redo_state.wait_info,
            WAIT_REDO_NUM * sizeof(RedoWaitInfo));
        securec_check_errno(rcs, (void)rcs);
    }
}

int check_datanode_status_by_SQL0(agent_to_cm_datanode_status_report* report_msg, uint32 ii)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;

    /* in case we return 0 without set the db_state. */
    const char* sqlCommands =
        "select local_role,static_connections,db_state,detail_information from pg_stat_get_stream_replications();";
    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[0] fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[ii]);
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(LOG, "sqlCommands[0] fail  is 0\n");
            CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
        } else {
            int rc;

            maxColums = Nfields(node_result);
            if (maxColums != 4) {
                write_runlog(ERROR, "sqlCommands[0] fail  FAIL! col is %d\n", maxColums);
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            }

            report_msg->local_status.local_role = datanode_role_string_to_int(Getvalue(node_result, 0, 0));
            if (report_msg->local_status.local_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[0] get local_status.local_role is: INSTANCE_ROLE_UNKNOWN\n");
            rc = sscanf_s(Getvalue(node_result, 0, 1), "%d", &(report_msg->local_status.static_connections));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            report_msg->local_status.db_state = datanode_dbstate_string_to_int(Getvalue(node_result, 0, 2));
            report_msg->local_status.buildReason = datanode_rebuild_reason_string_to_int(Getvalue(node_result, 0, 3));
            if (report_msg->local_status.buildReason == INSTANCE_HA_DATANODE_BUILD_REASON_UNKNOWN) {
                write_runlog(LOG,
                    "build reason is %s, buildReason = %d\n",
                    Getvalue(node_result, 0, 3),
                    report_msg->local_status.buildReason);
            }
        }
    } else {
        write_runlog(ERROR, "sqlCommands[0] fail  FAIL! Status=%d\n", ResultStatus(node_result));
        CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
    }
    Clear(node_result);
    return 0;
}

/* DN instance status check SQL 1 */
int check_datanode_status_by_SQL1(agent_to_cm_datanode_status_report* report_msg, uint32 ii)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;
    uint32 hi = 0;
    uint32 lo = 0;

    const char* sqlCommands = "select term, lsn from pg_last_xlog_replay_location();";

    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[1] fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[ii]);
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(LOG, "sqlCommands[1] is 0\n");
            CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
        } else {
            int rc;
            char* xlog_location = NULL;
            char* term = NULL;

            maxColums = Nfields(node_result);
            if (maxColums != 2) {
                write_runlog(ERROR, "sqlCommands[1] fail ! col is %d\n", maxColums);
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            }

            term = Getvalue(node_result, 0, 0);
            if (term == NULL || 0 == strcmp(term, "")) {
                write_runlog(ERROR, "term is invalid.\n");
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            } else {
                report_msg->local_status.term = strtoul(term, NULL, 0);
            }

            xlog_location = Getvalue(node_result, 0, 1);
            if (xlog_location == NULL || strcmp(xlog_location, "") == 0) {
                write_runlog(ERROR, "pg_last_xlog_replay_location is empty.\n");
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            } else {
                /* Shielding %x format read Warning. */
                rc = sscanf_s(xlog_location, "%X/%X", &hi, &lo);
                check_sscanf_s_result(rc, 2);
                securec_check_intval(rc, (void)rc);
                report_msg->local_status.last_flush_lsn = (((uint64)hi) << 32) | lo;
            }
        }
    } else {
        write_runlog(ERROR, "sqlCommands[1] fail ResultStatus=%d!\n", ResultStatus(node_result));
        CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
    }
    Clear(node_result);
    return 0;
}

int check_datanode_status_by_SQL2(agent_to_cm_datanode_status_report* report_msg, uint32 ii)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;
    uint32 hi = 0;
    uint32 lo = 0;
    int dn_sync_state = 0;
    char* most_available = NULL;

    /* DN instance status check SQL 2 */
    const char* sqlCommands =
        "select sender_pid,local_role,peer_role,peer_state,state,sender_sent_location,sender_write_location,"
        "sender_flush_location,sender_replay_location,receiver_received_location,receiver_write_location,"
        "receiver_flush_location,receiver_replay_location,sync_percent,sync_state,sync_priority,"
        "sync_most_available,channel from pg_stat_get_wal_senders() where peer_role='Standby';";
    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[2] fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[ii]);
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(DEBUG5, "walsender information is empty.\n");
        } else {
            int rc;

            maxColums = Nfields(node_result);
            if (maxColums != 18) {
                write_runlog(ERROR, "sqlCommands[2] fail! col is %d\n", maxColums);
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            }

            rc = sscanf_s(Getvalue(node_result, 0, 0), "%d", &(report_msg->sender_status[0].sender_pid));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].local_role = datanode_role_string_to_int(Getvalue(node_result, 0, 1));
            if (report_msg->sender_status[0].local_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[2] get sender_status.local_role is: INSTANCE_ROLE_UNKNOWN\n");
            report_msg->sender_status[0].peer_role = datanode_role_string_to_int(Getvalue(node_result, 0, 2));
            if (report_msg->sender_status[0].peer_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[2] get sender_status.peer_role is: INSTANCE_ROLE_UNKNOWN\n");
            report_msg->sender_status[0].peer_state = datanode_dbstate_string_to_int(Getvalue(node_result, 0, 3));
            report_msg->sender_status[0].state = datanode_wal_send_state_string_to_int(Getvalue(node_result, 0, 4));
            /* Shielding %x format read Warning. */
            rc = sscanf_s(Getvalue(node_result, 0, 5), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].sender_sent_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 6), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].sender_write_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 7), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].sender_flush_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 8), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].sender_replay_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 9), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].receiver_received_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 10), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].receiver_write_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 11), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].receiver_flush_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 12), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[0].receiver_replay_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 13), "%d", &(report_msg->sender_status[0].sync_percent));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            dn_sync_state = datanode_wal_sync_state_string_to_int(Getvalue(node_result, 0, 14));
            if (!g_multi_az_cluster) {
                most_available = Getvalue(node_result, 0, 16);
                if (dn_sync_state == INSTANCE_DATA_REPLICATION_ASYNC) {
                    report_msg->sender_status[0].sync_state = INSTANCE_DATA_REPLICATION_ASYNC;
                } else if (dn_sync_state == INSTANCE_DATA_REPLICATION_SYNC && (strcmp(most_available, "Off") == 0)) {
                    report_msg->sender_status[0].sync_state = INSTANCE_DATA_REPLICATION_SYNC;
                } else if (dn_sync_state == INSTANCE_DATA_REPLICATION_SYNC && (strcmp(most_available, "On") == 0)) {
                    report_msg->sender_status[0].sync_state = INSTANCE_DATA_REPLICATION_MOST_AVAILABLE;
                } else {
                    report_msg->sender_status[0].sync_state = INSTANCE_DATA_REPLICATION_UNKONWN;
                    write_runlog(ERROR,
                        "datanode status report get wrong sync mode:%d, most available:%s\n",
                        dn_sync_state,
                        most_available);
                }
            } else {
                report_msg->sender_status[0].sync_state = dn_sync_state;
            }
            rc = sscanf_s(Getvalue(node_result, 0, 15), "%d", &(report_msg->sender_status[0].sync_priority));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
        }
    } else {
        write_runlog(ERROR, "sqlCommands[2] fail ResultStatus=%d!\n", ResultStatus(node_result));
        CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
    }
    Clear(node_result);
    return 0;
}

int check_datanode_status_by_SQL3(agent_to_cm_datanode_status_report* report_msg, uint32 ii)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;
    uint32 hi = 0;
    uint32 lo = 0;
    int dn_sync_state = 0;
    char* most_available = NULL;

    /* DN instance status check SQL 3 */
    const char* sqlCommands =
        "select sender_pid,local_role,peer_role,peer_state,state,sender_sent_location,sender_write_location,"
        "sender_flush_location,sender_replay_location,receiver_received_location,receiver_write_location,"
        "receiver_flush_location,receiver_replay_location,sync_percent,sync_state,sync_priority,"
        "sync_most_available,channel from pg_stat_get_wal_senders() where peer_role='Secondary';";
    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[3] fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[ii]);
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(DEBUG5, "walsender information is empty.\n");
        } else {
            int rc;

            maxColums = Nfields(node_result);
            if (maxColums != 18) {
                write_runlog(ERROR, "sqlCommands[3] fail! col is %d\n", maxColums);
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            }

            rc = sscanf_s(Getvalue(node_result, 0, 0), "%d", &(report_msg->sender_status[1].sender_pid));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].local_role = datanode_role_string_to_int(Getvalue(node_result, 0, 1));
            if (report_msg->sender_status[1].local_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[3] get sender_status.local_role is: INSTANCE_ROLE_UNKNOWN\n");
            report_msg->sender_status[1].peer_role = datanode_role_string_to_int(Getvalue(node_result, 0, 2));
            if (report_msg->sender_status[1].peer_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[3] get sender_status.peer_role is: INSTANCE_ROLE_UNKNOWN\n");
            report_msg->sender_status[1].peer_state = datanode_dbstate_string_to_int(Getvalue(node_result, 0, 3));
            report_msg->sender_status[1].state = datanode_wal_send_state_string_to_int(Getvalue(node_result, 0, 4));
            /* Shielding %x format read Warning. */
            rc = sscanf_s(Getvalue(node_result, 0, 5), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].sender_sent_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 6), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].sender_write_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 7), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].sender_flush_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 8), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].sender_replay_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 9), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].receiver_received_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 10), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].receiver_write_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 11), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].receiver_flush_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 12), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->sender_status[1].receiver_replay_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 13), "%d", &(report_msg->sender_status[1].sync_percent));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            dn_sync_state = datanode_wal_sync_state_string_to_int(Getvalue(node_result, 0, 14));
            if (!g_multi_az_cluster) {
                most_available = Getvalue(node_result, 0, 16);
                if (dn_sync_state == INSTANCE_DATA_REPLICATION_ASYNC) {
                    report_msg->sender_status[1].sync_state = INSTANCE_DATA_REPLICATION_ASYNC;
                } else if (dn_sync_state == INSTANCE_DATA_REPLICATION_SYNC && (strcmp(most_available, "Off") == 0)) {
                    report_msg->sender_status[1].sync_state = INSTANCE_DATA_REPLICATION_SYNC;
                } else if (dn_sync_state == INSTANCE_DATA_REPLICATION_SYNC && (strcmp(most_available, "On") == 0)) {
                    report_msg->sender_status[1].sync_state = INSTANCE_DATA_REPLICATION_MOST_AVAILABLE;
                } else {
                    report_msg->sender_status[1].sync_state = INSTANCE_DATA_REPLICATION_UNKONWN;
                    write_runlog(ERROR,
                        "datanode status report get wrong sync mode:%d, most available:%s\n",
                        dn_sync_state,
                        most_available);
                }
            } else {
                report_msg->sender_status[1].sync_state = dn_sync_state;
            }
            rc = sscanf_s(Getvalue(node_result, 0, 15), "%d", &(report_msg->sender_status[1].sync_priority));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
        }
    } else {
        write_runlog(ERROR, "sqlCommands[3] fail ResultStatus=%d!\n", ResultStatus(node_result));
        CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
    }
    Clear(node_result);
    return 0;
}

static void GetLpInfoByStr(char *channel, DnLocalPeer *lpInfo, uint32 instId)
{
    char *outerPtr = NULL;
    char *token = strtok_r(channel, ":", &outerPtr);
    errno_t rc = strcpy_s(lpInfo->localIp, CM_IP_LENGTH, token);
    securec_check_errno(rc, (void)rc);
    if (outerPtr == NULL) {
        write_runlog(ERROR, "[GetLpInfoByStr] line: %d, instId is %u, channel is %s.\n", __LINE__, instId, channel);
        return;
    }
    token = strtok_r(NULL, "<", &outerPtr);
    lpInfo->localPort = (uint32)CmAtoi(token, 0);
    if (outerPtr == NULL) {
        write_runlog(ERROR, "[GetLpInfoByStr] line: %d, instId is %u, channel is %s.\n", __LINE__, instId, channel);
        return;
    }
    outerPtr = outerPtr + strlen("--");
    if (outerPtr == NULL) {
        write_runlog(ERROR, "[GetLpInfoByStr] line: %d, instId is %u, channel is %s.\n", __LINE__, instId, channel);
        return;
    }
    token = strtok_r(NULL, ":", &outerPtr);
    rc = strcpy_s(lpInfo->peerIp, CM_IP_LENGTH, token);
    securec_check_errno(rc, (void)rc);
    lpInfo->peerPort = (uint32)CmAtoi(outerPtr, 0);
    write_runlog(DEBUG1, "%u, channel is %s:%u<--%s:%u.\n", instId,
        lpInfo->localIp, lpInfo->localPort, lpInfo->peerIp, lpInfo->peerPort);
}

int check_datanode_status_by_SQL4(agent_to_cm_datanode_status_report *report_msg, DnLocalPeer *lpInfo, uint32 ii)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;
    uint32 hi = 0;
    uint32 lo = 0;

    /* DN instance status check SQL 4 */
    const char* sqlCommands =
        "select receiver_pid,local_role,peer_role,peer_state,state,sender_sent_location,sender_write_location,"
        "sender_flush_location,sender_replay_location,receiver_received_location,receiver_write_location,"
        "receiver_flush_location,receiver_replay_location,sync_percent,channel from pg_stat_get_wal_receiver();";
    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[4] fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[ii]);
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(DEBUG5, "walreceviver information is empty.\n");
        } else {
            int rc;

            maxColums = Nfields(node_result);
            if (maxColums != 15) {
                write_runlog(ERROR, "sqlCommands[4] fail  FAIL! col is %d\n", maxColums);
                CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
            }

            rc = sscanf_s(Getvalue(node_result, 0, 0), "%d", &(report_msg->receive_status.receiver_pid));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.local_role = datanode_role_string_to_int(Getvalue(node_result, 0, 1));
            if (report_msg->receive_status.local_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[4] get receive_status.local_role is: INSTANCE_ROLE_UNKNOWN\n");
            report_msg->receive_status.peer_role = datanode_role_string_to_int(Getvalue(node_result, 0, 2));
            if (report_msg->receive_status.peer_role == INSTANCE_ROLE_UNKNOWN)
                write_runlog(LOG, "sqlCommands[4] get receive_status.peer_role is: INSTANCE_ROLE_UNKNOWN\n");
            report_msg->receive_status.peer_state = datanode_dbstate_string_to_int(Getvalue(node_result, 0, 3));
            report_msg->receive_status.state = datanode_wal_send_state_string_to_int(Getvalue(node_result, 0, 4));
            /* Shielding %x format read Warning. */
            rc = sscanf_s(Getvalue(node_result, 0, 5), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.sender_sent_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 6), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.sender_write_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 7), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.sender_flush_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 8), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.sender_replay_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 9), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.receiver_received_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 10), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.receiver_write_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 11), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.receiver_flush_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 12), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            report_msg->receive_status.receiver_replay_location = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(node_result, 0, 13), "%d", &(report_msg->receive_status.sync_percent));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            if (report_msg->receive_status.local_role == INSTANCE_ROLE_CASCADE_STANDBY) {
                GetLpInfoByStr(Getvalue(node_result, 0, 14), lpInfo, g_currentNode->datanode[ii].datanodeId);
            }
        }
    } else {
        write_runlog(ERROR, "sqlCommands[4] fail ResultStatus=%d!\n", ResultStatus(node_result));
        CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
    }
    Clear(node_result);
    return 0;
}

void check_datanode_status_by_SQL5(agent_to_cm_datanode_status_report *report_msg, uint32 ii, const char *data_path)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;
    bool needClearResult = true;
    /* we neednot check the bad block during upgrading. */
    if (undocumentedVersion != 0) {
        return;
    }
    /* DN instance status check SQL 5 */
    const char* sqlCommands = "select sum(error_count) from pg_stat_bad_block;";
    node_result = Exec(g_dnConn[ii], sqlCommands);

    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[5] fail return NULL!\n");
        needClearResult = false;
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(LOG, "sqlCommands[5] is 0\n");
        } else {
            int rc;
            maxColums = Nfields(node_result);
            if (maxColums != 1) {
                write_runlog(ERROR, "sqlCommands[5] fail  FAIL! col is %d\n", maxColums);
            }

            int tmpErrCount = 0;
            char* tmpErrCountValue = Getvalue(node_result, 0, 0);
            if (tmpErrCountValue != NULL)
                tmpErrCount = CmAtoi(tmpErrCountValue, 0);
            tmpErrCount = (tmpErrCount < 0) ? 0 : tmpErrCount;

            char instanceName[CM_NODE_NAME] = {0};
            rc = snprintf_s(
                instanceName, sizeof(instanceName), sizeof(instanceName) - 1, "%s_%u", "dn", report_msg->instanceId);
            securec_check_intval(rc, (void)rc);

            /*
             * 1. tmpErrCount > g_errCountPgStatBadBlock[ii], have new bad block, make a alarm.
             * 2. tmpErrCount < g_errCountPgStatBadBlock[ii], the gaussdb may killed, restart or execute.
             * when this happen, check tmpErrCount !=0 (it means have new bad block after reset ), make a alarm.
             */
            if (((tmpErrCount - g_errCountPgStatBadBlock[ii]) >= 1) ||
                (((tmpErrCount - g_errCountPgStatBadBlock[ii]) < 0) && (tmpErrCount != 0))) {
                /* report the alarm. */
                report_dn_disk_alarm(ALM_AT_Fault, instanceName, ii, data_path);
                write_runlog(WARNING, "pg_stat_bad_block error count is %d\n", tmpErrCount);
            } else {
                if (tmpErrCount == 0) {
                    report_dn_disk_alarm(ALM_AT_Resume, instanceName, ii, data_path);
                }
            }

            g_errCountPgStatBadBlock[ii] = tmpErrCount;
        }
    } else {
        write_runlog(ERROR, "sqlCommands[5] fail  FAIL! Status=%d\n", ResultStatus(node_result));
    }
    if (needClearResult) {
        Clear(node_result);
    }
}

int check_datanode_status_by_SQL6(agent_to_cm_datanode_status_report* report_msg, uint32 ii, const char* data_path)
{
    cltPqResult_t* node_result = NULL;
    int maxRows = 0;
    int maxColums = 0;
    /* DN instance status check SQL 6 */
    const char* sqlCommands =
        "SELECT redo_start_ptr, redo_start_time, redo_done_time, curr_time,"
        "min_recovery_point, read_ptr, last_replayed_read_ptr, recovery_done_ptr,"
        "read_xlog_io_counter, read_xlog_io_total_dur, read_data_io_counter, read_data_io_total_dur,"
        "write_data_io_counter, write_data_io_total_dur, process_pending_counter, process_pending_total_dur,"
        "apply_counter, apply_total_dur,speed, local_max_ptr, worker_info FROM local_redo_stat();";
    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[6] fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[ii]);
    }
    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(node_result);
        if (maxRows == 0) {
            write_runlog(DEBUG5, "parallel redo status information is empty.\n");
        } else {
            int rc;

            maxColums = Nfields(node_result);
            report_msg->local_redo_stats.is_by_query = 1;
            fill_sql6_report_msg1(report_msg, node_result);
            fill_sql6_report_msg2(report_msg, node_result);
            report_msg->parallel_redo_status.speed_according_seg = -1;

            rc = sscanf_s(Getvalue(node_result, 0, 19), "%lu", &(report_msg->parallel_redo_status.local_max_lsn));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);

            char* info = Getvalue(node_result, 0, 20);
            report_msg->parallel_redo_status.worker_info_len = strlen(info);
            rc = memcpy_s(
                report_msg->parallel_redo_status.worker_info, REDO_WORKER_INFO_BUFFER_SIZE, info, strlen(info));
            securec_check_errno(rc, (void)rc);
        }
    } else {
        char redo_state_path[MAXPGPATH] = {0};
        int rcs = snprintf_s(redo_state_path, MAXPGPATH, MAXPGPATH - 1, "%s/redo.state", data_path);
        securec_check_intval(rcs, (void)rcs);
        check_input_for_security(redo_state_path);
        canonicalize_path(redo_state_path);
        check_parallel_redo_status_by_file(report_msg, ii, redo_state_path);
        write_runlog(ERROR, "sqlCommands[6] fail  FAIL! Status=%d\n", ResultStatus(node_result));
        write_runlog(LOG, "read parallel redo status from redo.state file\n");
    }
    Clear(node_result);
    /* single node cluster does not need to continue executing. */
    if (g_single_node_cluster) {
        return 0;
    }
    /* DN instance status check SQL 6 */
    sqlCommands = "select disconn_mode, disconn_host, disconn_port, local_host, local_port, redo_finished from "
                  "read_disable_conn_file();";
    char* is_redo_finished = NULL;
    bool needClearResult = true;
    node_result = Exec(g_dnConn[ii], sqlCommands);
    if (node_result == NULL) {
        write_runlog(ERROR, "sqlCommands[6] fail return NULL!\n");
        needClearResult = false;
    }

    if ((ResultStatus(node_result) == CLTPQRES_CMD_OK) || (ResultStatus(node_result) == CLTPQRES_TUPLES_OK)) {
        maxRows = Nfields(node_result);
        if (maxRows == 0) {
            write_runlog(LOG, "sqlCommands[6] is 0\n");
            CLEAR_AND_CLOSE_CONNECTION(node_result, g_dnConn[ii]);
        } else {
            maxColums = Nfields(node_result);
            if (maxColums != 6) {
                write_runlog(ERROR, "sqlCommands[6] fail FAIL! col is %d\n", maxColums);
            }

            report_msg->local_status.disconn_mode = datanode_lockmode_string_to_int(Getvalue(node_result, 0, 0));
            errno_t rc = memset_s(report_msg->local_status.disconn_host, HOST_LENGTH, 0, HOST_LENGTH);
            securec_check_errno(rc, (void)rc);
            char* tmp_result = NULL;
            tmp_result = Getvalue(node_result, 0, 1);
            if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
                rc = snprintf_s(report_msg->local_status.disconn_host, HOST_LENGTH, HOST_LENGTH - 1, "%s", tmp_result);
                securec_check_intval(rc, (void)rc);
            }
            rc = sscanf_s(Getvalue(node_result, 0, 2), "%u", &(report_msg->local_status.disconn_port));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            rc = memset_s(report_msg->local_status.local_host, HOST_LENGTH, 0, HOST_LENGTH);
            securec_check_errno(rc, (void)rc);
            tmp_result = Getvalue(node_result, 0, 3);
            if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
                rc = snprintf_s(report_msg->local_status.local_host, HOST_LENGTH, HOST_LENGTH - 1, "%s", tmp_result);
                securec_check_intval(rc, (void)rc);
            }
            rc = sscanf_s(Getvalue(node_result, 0, 4), "%u", &(report_msg->local_status.local_port));
            check_sscanf_s_result(rc, 1);
            securec_check_intval(rc, (void)rc);
            is_redo_finished = Getvalue(node_result, 0, 5);
            if (strcmp(is_redo_finished, "true") == 0) {
                report_msg->local_status.redo_finished = true;
            } else {
                report_msg->local_status.redo_finished = false;
            }
        }
    } else {
        write_runlog(ERROR, "sqlCommands[6] fail  FAIL! Status=%d\n", ResultStatus(node_result));
    }
    if (needClearResult) {
        Clear(node_result);
    }
    return 0;
}

int GetCkptRedoPoint(cltPqConn_t *conn, uint64 *ckptRedoPointInfo)
{
    uint32 hi = 0;
    uint32 lo = 0;
    const char *sqlCommands = "select ckpt_redo_point from local_ckpt_stat();";
    cltPqResult_t *resultSet = Exec(conn, sqlCommands);
    if (resultSet == NULL) {
        write_runlog(ERROR, "GetCkptRedoPoint fail return NULL!\n");
        return -1;
    }
    if ((ResultStatus(resultSet) == CLTPQRES_CMD_OK) || (ResultStatus(resultSet) == CLTPQRES_TUPLES_OK)) {
        int maxRows = Ntuples(resultSet);
        if (maxRows == 0) {
            write_runlog(DEBUG1, "GetCkptRedoPoint is 0\n");
            Clear(resultSet);
            return 0;
        } else {
            char *ckpt_redo_point = Getvalue(resultSet, 0, 0);
            if (ckpt_redo_point == NULL || strcmp(ckpt_redo_point, "") == 0) {
                write_runlog(ERROR, "local_ckpt_stat is empty.\n");
                Clear(resultSet);
                return 0;
            } else {
                int rc = sscanf_s(ckpt_redo_point, "%X/%X", &hi, &lo);
                check_sscanf_s_result(rc, 2);
                securec_check_intval(rc, (void)rc);
                *ckptRedoPointInfo = (((uint64)hi) << 32) | lo;
            }
        }
    } else {
        write_runlog(ERROR, "GetCkptRedoPoint fail ResultStatus=%d!\n", ResultStatus(resultSet));
        Clear(resultSet);
        return -1;
    }
    Clear(resultSet);
    return 0;
}

int GetLocalBarrierStatus(cltPqConn_t *conn, LocalBarrierStatus *localBarrierStatus)
{
    uint32 hi = 0;
    uint32 lo = 0;
    const char *sqlCommand = "select * from gs_get_local_barrier_status ();";
    cltPqResult_t *resultSet = Exec(conn, sqlCommand);
    if (resultSet == NULL) {
        write_runlog(ERROR, "GetLocalBarrierStatus: sql:%s, return NULL!\n", sqlCommand);
        return -1;
    }
    if ((ResultStatus(resultSet) == CLTPQRES_CMD_OK) || (ResultStatus(resultSet) == CLTPQRES_TUPLES_OK)) {
        int maxRows = Ntuples(resultSet);
        if (maxRows == 0) {
            write_runlog(DEBUG1, "GetLocalBarrierStatus: sql:%s, return 0!\n", sqlCommand);
            Clear(resultSet);
            return 0;
        } else {
            int rc;
            char *tmp_result = Getvalue(resultSet, 0, 0);
            if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
                rc = snprintf_s(localBarrierStatus->barrierID, BARRIERLEN, BARRIERLEN - 1, "%s", tmp_result);
                securec_check_intval(rc, (void)rc);
            }
            rc = sscanf_s(Getvalue(resultSet, 0, 1), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            localBarrierStatus->barrierLSN = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(resultSet, 0, 2), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            localBarrierStatus->archiveLSN = (((uint64)hi) << 32) | lo;
            rc = sscanf_s(Getvalue(resultSet, 0, 3), "%X/%X", &hi, &lo);
            check_sscanf_s_result(rc, 2);
            securec_check_intval(rc, (void)rc);
            localBarrierStatus->flushLSN = (((uint64)hi) << 32) | lo;
        }
    } else {
        write_runlog(
            ERROR, "GetLocalBarrierStatus: sql:%s, ResultStatus=%d!\n", sqlCommand, ResultStatus(resultSet));
        Clear(resultSet);
        return -1;
    }
    Clear(resultSet);
    return 0;
}

static void RecordGlobalBarrier(const cltPqResult_t *resultSet, GlobalBarrierStatus *globalStatus)
{
    const int archFieldNum = 2;
    int rc;
    if (globalStatus->slotCount > MAX_BARRIER_SLOT_COUNT) {
        write_runlog(WARNING,
            "RecordGlobalBarrier:return rows(%d) that great than max(%d) will ignore!\n",
            globalStatus->slotCount, MAX_BARRIER_SLOT_COUNT);
        globalStatus->slotCount = MAX_BARRIER_SLOT_COUNT;
    }

    for (int32 numRows = 0; numRows < globalStatus->slotCount; numRows++) {
        char *tmp_result = Getvalue(resultSet, numRows, 0);
        if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
            rc = snprintf_s(globalStatus->globalBarriers[numRows].slotname,
                MAX_SLOT_NAME_LEN, MAX_SLOT_NAME_LEN - 1, "%s", tmp_result);
            securec_check_intval(rc, (void)rc);
        }
        tmp_result = Getvalue(resultSet, numRows, 1);
        if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
            rc = snprintf_s(globalStatus->globalBarriers[numRows].globalBarrierId,
                BARRIERLEN, BARRIERLEN - 1, "%s", tmp_result);
            securec_check_intval(rc, (void)rc);
        }
        tmp_result = Getvalue(resultSet, numRows, archFieldNum);
        if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
            rc = snprintf_s(globalStatus->globalBarriers[numRows].globalAchiveBarrierId,
                BARRIERLEN, BARRIERLEN - 1, "%s", tmp_result);
            securec_check_intval(rc, (void)rc);
        }
    }
}

int GetGlobalBarrierInfoNew(cltPqConn_t *conn, GlobalBarrierStatus *globalStatus)
{
    const char *sqlCommand =
        "select slot_name, global_barrier_id, global_achive_barrier_id from gs_get_global_barriers_status();";
    cltPqResult_t *resultSet = Exec(conn, sqlCommand);
    if (resultSet == NULL) {
        write_runlog(ERROR, "GetGlobalBarrierInfoNew: sql:%s, return NULL!\n", sqlCommand);
        return -1;
    }

    if ((ResultStatus(resultSet) == CLTPQRES_CMD_OK) || (ResultStatus(resultSet) == CLTPQRES_TUPLES_OK)) {
        globalStatus->slotCount = Ntuples(resultSet);
        if (globalStatus->slotCount == 0) {
            write_runlog(DEBUG1, "GetGlobalBarrierInfoNew: sql:%s, return rows is 0!\n", sqlCommand);
            Clear(resultSet);
            return 0;
        } else {
            RecordGlobalBarrier(resultSet, globalStatus);
        }
    } else {
        write_runlog(ERROR,
            "GetGlobalBarrierInfoNew: sqlCommands:%s ResultStatus=%d!\n",
            sqlCommand, ResultStatus(resultSet));
        Clear(resultSet);
        return -1;
    }
    Clear(resultSet);
    return 0;
}

int CheckDatanodeSyncList(uint32 instd, AgentToCmserverDnSyncList *syncListMsg, cltPqConn_t **curDnConn)
{
    cltPqResult_t *nodeResult = NULL;
    int maxRows = 0;
    int maxColums = 0;
    const char *sqlCommands = "show synchronous_standby_names;";
    nodeResult = Exec((*curDnConn), sqlCommands);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "instd is %u, CheckDatanodeSyncList fail return NULL!\n", instd);
        CLOSE_CONNECTION((*curDnConn));
    }
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "instd is %u, synchronous_standby_names information is empty.\n", instd);
        } else {
            int rc;
            char *result = NULL;
            maxColums = Nfields(nodeResult);
            if (maxColums != 1) {
                write_runlog(ERROR, "instd is %u, CheckDatanodeSyncList fail! col is %d.\n", instd, maxColums);
                CLEAR_AND_CLOSE_CONNECTION(nodeResult, (*curDnConn));
            }
            result = Getvalue(nodeResult, 0, 0);
            if (result == NULL || strcmp(result, "") == 0) {
                write_runlog(ERROR, "instd is %u, synchronous_standby_names is empty.\n", instd);
                CLEAR_AND_CLOSE_CONNECTION(nodeResult, (*curDnConn));
            }
            rc = strcpy_s(syncListMsg->dnSynLists, DN_SYNC_LEN, result);
            securec_check_errno(rc, (void)rc);
            write_runlog(DEBUG1, "instd is %u, result=%s, len is %ld, report_msg->dnSynLists=%s.\n", instd, result,
                strlen(result), syncListMsg->dnSynLists);
        }
    } else {
        write_runlog(ERROR, "instd is %u, CheckDatanodeSyncList fail Status=%d!\n", instd, ResultStatus(nodeResult));
    }
    Clear(nodeResult);
    return 0;
}

int GetGlobalBarrierInfo(cltPqConn_t *conn, agent_to_cm_coordinate_barrier_status_report *barrierMsg)
{
    char *tmp_result = NULL;
    const char *sqlCommand = "select * from gs_get_global_barrier_status();";
    cltPqResult_t *resultSet = Exec(conn, sqlCommand);
    if (resultSet == NULL) {
        write_runlog(ERROR, "sqlCommands[9]: sqlCommands:%s, return NULL!\n", sqlCommand);
        return -1;
    }
    if ((ResultStatus(resultSet) == CLTPQRES_CMD_OK) || (ResultStatus(resultSet) == CLTPQRES_TUPLES_OK)) {
        int maxRows = Ntuples(resultSet);
        if (maxRows == 0) {
            write_runlog(DEBUG1, "dn_report_wrapper_1: sqlCommands:%s, return 0!\n", sqlCommand);
            Clear(resultSet);
            return 0;
        } else {
            int rc;
            tmp_result = Getvalue(resultSet, 0, 0);
            if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
                rc = snprintf_s(barrierMsg->global_barrierId, BARRIERLEN, BARRIERLEN - 1, "%s", tmp_result);
                securec_check_intval(rc, (void)rc);
            }
            tmp_result = Getvalue(resultSet, 0, 1);
            if (tmp_result != NULL && (strlen(tmp_result) > 0)) {
                rc = snprintf_s(barrierMsg->global_achive_barrierId, BARRIERLEN, BARRIERLEN - 1, "%s", tmp_result);
                securec_check_intval(rc, (void)rc);
            }
        }
    } else {
        write_runlog(
            ERROR, "cn_report_wrapper_1: sqlCommands:%s ResultStatus=%d!\n", sqlCommand, ResultStatus(resultSet));
        Clear(resultSet);
        return -1;
    }
    Clear(resultSet);
    return 0;
}

/* check whether query barrier id exists or not */
int StandbyClusterCheckQueryBarrierID(cltPqConn_t *conn, agent_to_cm_coordinate_barrier_status_report *barrierInfo)
{
    int rc;
    char *tmpResult = NULL;
    char queryBarrier[BARRIERLEN] = {0};
    char sqlCommand[MAX_PATH_LEN] = {0};
    // need locked
    rc = memcpy_s(queryBarrier, BARRIERLEN - 1, g_agentQueryBarrier, BARRIERLEN - 1);
    securec_check_errno(rc, (void)rc);
    if (queryBarrier[0] == '\0') {
        write_runlog(LOG, "query barrier is NULL when checking it's existance.\n");
        return 0;
    }
    rc = snprintf_s(sqlCommand, MAX_PATH_LEN, MAX_PATH_LEN - 1,
        "select gs_query_standby_cluster_barrier_id_exist('%s');", queryBarrier);
    securec_check_intval(rc, (void)rc);
    cltPqResult_t *nodeResult = Exec(conn, sqlCommand);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "sqlCommands query barrier: sqlCommands:%s, return NULL!\n", sqlCommand);
        CLOSE_CONNECTION(conn);
    }
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        int maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "sqlCommands[8]: sqlCommands:%s, return 0!\n", sqlCommand);
            CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
        } else {
            tmpResult = Getvalue(nodeResult, 0, 0);
            if (strcmp(tmpResult, "t") == 0) {
                barrierInfo->is_barrier_exist = true;
            } else {
                write_runlog(LOG, "value %s is not exists\n", queryBarrier);
            }
            // query success, so we need update the query_barrierId
            rc = snprintf_s(barrierInfo->query_barrierId, BARRIERLEN, BARRIERLEN - 1, "%s", queryBarrier);
            securec_check_intval(rc, (void)rc);
        }
    } else {
        write_runlog(ERROR, "sqlCommands: sqlCommands:%s ResultStatus=%d!\n",
            sqlCommand, ResultStatus(nodeResult));
        CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
    }
    Clear(nodeResult);
    write_runlog(LOG, "check_query_barrierID, etcd val is %s, query barrier ID is %s, result is %s\n",
        queryBarrier, barrierInfo->query_barrierId, tmpResult);
    return 0;
}

int StandbyClusterSetTargetBarrierID(cltPqConn_t *conn)
{
    int maxRows = 0;
    char *tmpResult = NULL;
    char targetBarrier[BARRIERLEN] = {0};
    char sqlCommand[MAX_PATH_LEN] = {0};
    int rc;
    // need locked
    rc = memcpy_s(targetBarrier, BARRIERLEN - 1, g_agentTargetBarrier, BARRIERLEN - 1);
    securec_check_errno(rc, (void)rc);
    if (targetBarrier[0] == '\0') {
        write_runlog(LOG, "target barrier is NULL when setting it.\n");
        return 0;
 
    }
    write_runlog(LOG, "get target value from etcd is %s.\n", targetBarrier);
    rc = snprintf_s(sqlCommand, MAX_PATH_LEN, MAX_PATH_LEN - 1,
        "select gs_set_standby_cluster_target_barrier_id('%s');", targetBarrier);
    securec_check_intval(rc, (void)rc);
    cltPqResult_t *nodeResult = Exec(conn, sqlCommand);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "sqlCommands set barrier: sqlCommands:%s, return NULL!\n", sqlCommand);
        CLOSE_CONNECTION(conn);
    }
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "sqlCommands set barrier: sqlCommands:%s, return 0!\n", sqlCommand);
            CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
        } else {
            tmpResult = Getvalue(nodeResult, 0, 0);
            if (strncmp(tmpResult, targetBarrier, BARRIERLEN) != 0) {
                write_runlog(WARNING, "the return target barrier value %s is not euqal to etcd value %s\n",
                    tmpResult, targetBarrier);
            }
        }
    } else {
        write_runlog(ERROR, "sqlCommands set barrier: sqlCommands:%s ResultStatus=%d!\n",
            sqlCommand, ResultStatus(nodeResult));
        CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
    }
    Clear(nodeResult);
    write_runlog(LOG, "set_tatget_barrierID, etcd val is %s, set result is %s\n", targetBarrier, tmpResult);
    return 0;
}

int StandbyClusterGetBarrierInfo(cltPqConn_t *conn, agent_to_cm_coordinate_barrier_status_report *barrierInfo)
{
    int maxRows = 0;
    char* tmpResult = NULL;
    const char* sqlCommand = "select barrier_id from gs_get_standby_cluster_barrier_status();";
    cltPqResult_t *nodeResult = Exec(conn, sqlCommand);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "StandbyClusterGetBarrierInfo sqlCommands: sqlCommands:%s, return NULL!\n", sqlCommand);
        CLOSE_CONNECTION(conn);
    }
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "StandbyClusterGetBarrierInfo sqlCommands: sqlCommands:%s, return 0!\n", sqlCommand);
            CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
        } else {
            tmpResult = Getvalue(nodeResult, 0, 0);
            if (tmpResult != NULL && (strlen(tmpResult) > 0)) {
                int rc = snprintf_s(barrierInfo->barrierID, BARRIERLEN, BARRIERLEN - 1, "%s", tmpResult);
                securec_check_intval(rc, (void)rc);
            }
        }
    } else {
        write_runlog(ERROR, "StandbyClusterGetBarrierInfo sqlCommands: sqlCommands:%s ResultStatus=%d!\n",
            sqlCommand, ResultStatus(nodeResult));
        CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
    }
    Clear(nodeResult);
    write_runlog(LOG, "StandbyClusterGetBarrierInfo, get barrier ID is %s\n", barrierInfo->barrierID);
    return 0;
}

int StandbyClusterCheckCnWaiting(cltPqConn_t *conn, agent_to_cm_coordinate_status_report *reportMsg)
{
    int maxRows = 0;
    char* tmpResult = NULL;
    char localBarrier[BARRIERLEN] = {0};
    const char* sqlCommand = "select barrier_id from gs_get_local_barrier_status();";
    cltPqResult_t *nodeResult = Exec(conn, sqlCommand);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "StandbyClusterCheckCnWaiting sqlCommands: sqlCommands:%s, return NULL!\n", sqlCommand);
        CLOSE_CONNECTION(conn);
    }
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "StandbyClusterCheckCnWaiting sqlCommands: sqlCommands:%s, return 0!\n", sqlCommand);
            CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
        } else {
            tmpResult = Getvalue(nodeResult, 0, 0);
            if (tmpResult != NULL && (strlen(tmpResult) > 0)) {
                int rc = snprintf_s(localBarrier, BARRIERLEN, BARRIERLEN - 1, "%s", tmpResult);
                securec_check_intval(rc, (void)rc);
            }
        }
    } else {
        write_runlog(ERROR, "StandbyClusterCheckCnWaiting sqlCommands: sqlCommands:%s ResultStatus=%d!\n",
            sqlCommand, ResultStatus(nodeResult));
        CLEAR_AND_CLOSE_CONNECTION(nodeResult, conn);
    }
    Clear(nodeResult);
    if (strlen(g_agentTargetBarrier) != 0 && strncmp(localBarrier, g_agentTargetBarrier, BARRIERLEN - 1) > 0) {
        write_runlog(LOG, "localBarrier %s is bigger than targetbarrier %s\n", localBarrier, g_agentTargetBarrier);
        reportMsg->status.db_state = INSTANCE_HA_STATE_WAITING;
    }
    write_runlog(LOG, "StandbyClusterCheckCnWaiting, get localbarrier is %s\n", localBarrier);
    return 0;
}

int SetDnRoleOnDcfMode(const cltPqResult_t *nodeResult)
{
    char* tmpResult = NULL;
    int role = 0;

    tmpResult = Getvalue(nodeResult, 0, 0);
    if (tmpResult != NULL && (strlen(tmpResult) > 0)) {
        if (strstr(tmpResult, g_roleLeader) != NULL) {
            role = DCF_ROLE_LEADER;
        } else if (strstr(tmpResult, g_roleFollower) != NULL) {
            role = DCF_ROLE_FOLLOWER;
        } else if (strstr(tmpResult, g_rolePassive) != NULL) {
            role = DCF_ROLE_PASSIVE;
        } else if (strstr(tmpResult, g_roleLogger) != NULL) {
            role = DCF_ROLE_LOGGER;
        } else if (strstr(tmpResult, g_rolePrecandicate) != NULL) {
            role = DCF_ROLE_PRE_CANDIDATE;
        } else if (strstr(tmpResult, g_roleCandicate) != NULL) {
            role = DCF_ROLE_CANDIDATE;
        } else {
            role = DCF_ROLE_UNKNOWN;
        }
    }

    return role;
}

int CheckDatanodeStatusBySqL10(agent_to_cm_datanode_status_report *reportMsg,
    uint32 ii)
{
    cltPqResult_t* nodeResult = NULL;

    const char* sqlCommand = "SELECT substring(dcf_replication_info, "
        "position('role' in dcf_replication_info)+6, 10) from get_paxos_replication_info();";
    nodeResult = Exec(g_dnConn[ii], sqlCommand);    
    if (nodeResult == NULL) {
        write_runlog(ERROR, "sqlCommands[10]: sqlCommands:%s, return NULL!\n", sqlCommand);
        CLOSE_CONNECTION(g_dnConn[ii]);
    }

    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        int maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "dn_report_wrapper_1: sqlCommands:%s, return 0!\n", sqlCommand);
            CLEAR_AND_CLOSE_CONNECTION(nodeResult, g_dnConn[ii]);
        } else {
            reportMsg->receive_status.local_role = SetDnRoleOnDcfMode(nodeResult);
        }
    } else {
        write_runlog(ERROR, "cn_report_wrapper_1: sqlCommands:%s ResultStatus=%d!\n",
            sqlCommand, ResultStatus(nodeResult));
        CLEAR_AND_CLOSE_CONNECTION(nodeResult, g_dnConn[ii]);
    }

    Clear(nodeResult);
    return 0;
}

int cmagent_execute_query(cltPqConn_t* db_connection, const char* run_command)
{
    cltPqResult_t* node_result = NULL;
    if (db_connection == NULL) {
        write_runlog(ERROR, "error, the connection to coordinator is NULL!\n");
        return -1;
    }

    node_result = Exec(db_connection, run_command);
    if (node_result == NULL) {
        write_runlog(ERROR, "execute command(%s) return NULL!\n", run_command);
        return -1;
    }
    if ((ResultStatus(node_result) != CLTPQRES_CMD_OK) && (ResultStatus(node_result) != CLTPQRES_TUPLES_OK)) {
        if (ResHasError(node_result)) {
            write_runlog(ERROR, "execute command(%s) failed, errMsg is: %s!\n", run_command, GetResErrMsg(node_result));
        } else {
            write_runlog(ERROR, "execute command(%s) failed!\n", run_command);
        }

        Clear(node_result);
        return -1;
    }

    Clear(node_result);
    return 0;
}

int cmagent_execute_query_and_check_result(cltPqConn_t* db_connection, const char* run_command)
{
    cltPqResult_t* node_result = NULL;
    char* res_s = NULL;
    if (db_connection == NULL) {
        write_runlog(ERROR, "error, the connection to coordinator is NULL!\n");
        return -1;
    }

    node_result = Exec(db_connection, run_command);
    if (node_result == NULL) {
        write_runlog(ERROR, "execute command(%s) return NULL!\n", run_command);
        return -1;
    }
    if ((ResultStatus(node_result) != CLTPQRES_CMD_OK) && (ResultStatus(node_result) != CLTPQRES_TUPLES_OK)) {
        if (ResHasError(node_result)) {
            write_runlog(ERROR, "execute command(%s) failed, errMsg is: %s!\n", run_command, GetResErrMsg(node_result));
        } else {
            write_runlog(ERROR, "execute command(%s) failed!\n", run_command);
        }

        Clear(node_result);
        return -1;
    }
    res_s = Getvalue(node_result, 0, 0);
    write_runlog(LOG, "execute command(%s) result %s!\n", run_command, res_s);
    if (strcmp(res_s, "t") == 0) {
        Clear(node_result);
        return 0;
    } else if (strcmp(res_s, "f") == 0) {
        Clear(node_result);
        return -1;
    }
    Clear(node_result);
    return 0;
}

/*
 * get connection to coordinator and set statement timeout.
 */
int cmagent_to_coordinator_connect(const char* pid_path)
{
    cltPqResult_t* res = NULL;

    if (pid_path == NULL)
        return -1;

    g_Conn = get_connection(pid_path, true, g_agentToDb);
    if (g_Conn == NULL) {
        write_runlog(ERROR, "get coordinate connect failed!\n");
        return -1;
    }

    if (!IsConnOk(g_Conn)) {
        write_runlog(ERROR, "connect is not ok, errmsg is %s!\n", ErrorMessage(g_Conn));
        CLOSE_CONNECTION(g_Conn);
    }

    res = Exec(g_Conn, "SET statement_timeout = 10000000;");
    if (res == NULL) {
        write_runlog(ERROR, "cmagent_to_coordinator_connect: set command time out fail return NULL!\n");
        CLOSE_CONNECTION(g_Conn);
    }
    if ((ResultStatus(res) != CLTPQRES_CMD_OK) && (ResultStatus(res) != CLTPQRES_TUPLES_OK)) {
        write_runlog(ERROR, "cmagent_to_coordinator_connect: set command time out fail return FAIL!\n");
        CLEAR_AND_CLOSE_CONNECTION(res, g_Conn);
    }
    Clear(res);

    return 0;
}

uint32 find_cn_active_info_index(agent_to_cm_coordinate_status_report_old* report_msg, uint32 coordinatorId)
{
    uint32 index = 0;
    for (index = 0; index < max_cn_node_num_for_old_version; index++) {
        if (coordinatorId == report_msg->cn_active_info[index].cn_Id)
            return index;
    }
    write_runlog(ERROR, "find_cn_active_info_index: can not find cn %u\n", coordinatorId);
    return index;
}

/* before drop cn_xxx, we test wheather cn_xxx can be connected, if cn_xxx can be connected, do not drop it.
in the scene: cm_agent is down but cn_xxx is normal, cm_server can not receive status of cn_xxx from cm_agent,
so cm_server think cn_xxx is fault and drop it, but cn_xxx is running and status is normal, we should not drop it.
 */
int is_cn_connect_ok(uint32 coordinatorId)
{
    int test_result = 0;
    errno_t rc = 0;
    char connStr[MAXCONNINFO] = {0};
    cltPqConn_t* test_cn_conn = NULL;

    for (uint32 i = 0; i < g_node_num; i++) {
        if (g_node[i].coordinateId == coordinatorId) {
            /* use HA port(coordinatePort+1) to connect CN */
            rc = snprintf_s(connStr,
                sizeof(connStr),
                sizeof(connStr) - 1,
                "dbname=postgres port=%u host='%s' connect_timeout=2 rw_timeout=3 application_name=%s "
                "options='-c xc_maintenance_mode=on'",
                g_node[i].coordinatePort + 1,
                g_node[i].coordinateListenIP[0],
                g_progname);
            securec_check_intval(rc, (void)rc);
            break;
        }
    }

    test_cn_conn = Connect(connStr);
    if (test_cn_conn == NULL) {
        write_runlog(LOG, "[autodeletecn] connect to cn_%u failed, connStr: %s.\n", coordinatorId, connStr);
        test_result = -1;
    }
    if (!IsConnOk(test_cn_conn)) {
        write_runlog(LOG,
            "[autodeletecn] connect to cn_%u failed, PQstatus is not ok, connStr: %s, errmsg is %s.\n",
            coordinatorId,
            connStr,
            ErrorMessage(test_cn_conn));
        test_result = -1;
    }

    close_and_reset_connection(test_cn_conn);
    return test_result;
}

/* Covert the enum of Ha rebuild reason to int */
int datanode_rebuild_reason_enum_to_int(HaRebuildReason reason)
{
    switch (reason) {
        case NONE_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_NORMAL;
        case WALSEGMENT_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_WALSEGMENT_REMOVED;
        case CONNECT_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_DISCONNECT;
        case VERSION_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_VERSION_NOT_MATCHED;
        case MODE_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_MODE_NOT_MATCHED;
        case SYSTEMID_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_SYSTEMID_NOT_MATCHED;
        case TIMELINE_REBUILD:
            return INSTANCE_HA_DATANODE_BUILD_REASON_TIMELINE_NOT_MATCHED;
        default:
            break;
    }
    return INSTANCE_HA_DATANODE_BUILD_REASON_UNKNOWN;
}

cltPqConn_t* get_connection(const char* pid_path, bool isCoordinater, int connectTimeOut, const int32 rwTimeout)
{
    char** optlines;
    long pmpid;
    cltPqConn_t* dbConn = NULL;

    /* Try to read the postmaster.pid file */
    if ((optlines = CmReadfile(pid_path)) == NULL) {
        write_runlog(ERROR, "[%s: %d]: fail to read pid file (%s).\n", __FUNCTION__, __LINE__, pid_path);
        return NULL;
    }

    if (optlines[0] == NULL || /* optlines[0] means pid of datapath */
        optlines[1] == NULL || /* optlines[1] means datapath */
        optlines[2] == NULL || /* optlines[2] means start time */
        optlines[3] == NULL || /* optlines[3] means port */
        optlines[4] == NULL || /* optlines[4] means socket dir */
        optlines[5] == NULL) { /* optlines[5] means listen addr */
        /* File is exactly three lines, must be pre-9.1 */
        write_runlog(ERROR, " -w option is not supported when starting a pre-9.1 server\n");

        freefile(optlines);
        optlines = NULL;
        return NULL;
    }

    /* File is complete enough for us, parse it */
    pmpid = CmAtol(optlines[LOCK_FILE_LINE_PID - 1], 0);
    if (pmpid > 0) {
        /*
         * OK, seems to be a valid pidfile from our child.
         */
        int portnum;
        char* sockdir = NULL;
        char* hostaddr = NULL;
        char host_str[MAXPGPATH] = {0};
        char local_conninfo[MAXCONNINFO] = {0};
        char* cptr = NULL;
        int rc;

        /*
         * Extract port number and host string to use.
         * We used to prefer unix domain socket.
         * With thread pool, we prefer tcp port and connect to cn/dn ha port
         * so that we do not need to be queued by thread pool controller.
         */
        portnum = CmAtoi(optlines[LOCK_FILE_LINE_PORT - 1], 0);
        sockdir = optlines[LOCK_FILE_LINE_SOCKET_DIR - 1];
        hostaddr = optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1];

        if (hostaddr != NULL && hostaddr[0] != '\0' && hostaddr[0] != '\n') {
            rc = strncpy_s(host_str, sizeof(host_str), hostaddr, sizeof(host_str) - 1);
            securec_check_errno(rc, (void)rc);

        } else if (sockdir[0] == '/') {
            rc = strncpy_s(host_str, sizeof(host_str), sockdir, sizeof(host_str) - 1);
            securec_check_errno(rc, (void)rc);
        }

        /* remove trailing newline */
        cptr = strchr(host_str, '\n');
        if (cptr != NULL)
            *cptr = '\0';

        /* Fail if couldn't get either sockdir or host addr */
        if (host_str[0] == '\0') {
            write_runlog(ERROR, "option cannot use a relative socket directory specification\n");
            freefile(optlines);
            optlines = NULL;
            return NULL;
        }

        /* If postmaster is listening on "*", use localhost */
        if (strcmp(host_str, "*") == 0) {
            rc = strncpy_s(host_str, sizeof(host_str), "localhost", sizeof("localhost"));
            securec_check_errno(rc, (void)rc);
        }
        /* ha port equals normal port plus 1, required by om */
        if (isCoordinater) {
            rc = snprintf_s(local_conninfo,
                sizeof(local_conninfo),
                sizeof(local_conninfo) - 1,
                "dbname=postgres port=%d host='127.0.0.1' connect_timeout=%d rw_timeout=5 application_name=%s "
                "options='%s %s'",
                portnum + 1,
                connectTimeOut,
                g_progname,
                enable_xc_maintenance_mode ? "-c xc_maintenance_mode=on" : "",
                "-c remotetype=internaltool");
                securec_check_intval(rc, freefile(optlines));
        } else {
            rc = snprintf_s(local_conninfo,
                sizeof(local_conninfo),
                sizeof(local_conninfo) - 1,
                "dbname=postgres port=%d host='%s' connect_timeout=%d rw_timeout=%d application_name=%s "
                "options='%s %s'",
                portnum + 1,
                host_str,
                connectTimeOut,
                rwTimeout,
                g_progname,
                enable_xc_maintenance_mode ? "-c xc_maintenance_mode=on" : "",
                "-c remotetype=internaltool");
            securec_check_intval(rc, freefile(optlines));
        }

        write_runlog(DEBUG1, "cm agent connect cn/dn instance local_conninfo: %s\n", local_conninfo);

        dbConn = Connect(local_conninfo);
    }

    freefile(optlines);
    optlines = NULL;
    return dbConn;
}

#ifdef ENABLE_MULTIPLE_NODES
static cltPqConn_t* GetDnConnect(int index, const char *dbname)
{
    char** optlines;
    long pmpid;
    cltPqConn_t* dbConn = NULL;
    char pidPath[MAXPGPATH] = {0};
    int rcs = snprintf_s(pidPath, MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.pid", 
        g_currentNode->datanode[index].datanodeLocalDataPath);
    securec_check_intval(rcs, (void)rcs);

    /* Try to read the postmaster.pid file */
    if ((optlines = CmReadfile(pidPath)) == NULL) {
        write_runlog(ERROR, "[%s: %d]: fail to read pid file (%s).\n", __FUNCTION__, __LINE__, pidPath);
        return NULL;
    }

    if (optlines[0] == NULL || /* optlines[0] means pid of datapath */
        optlines[1] == NULL || /* optlines[1] means datapath */
        optlines[2] == NULL || /* optlines[2] means start time */
        optlines[3] == NULL || /* optlines[3] means port */
        optlines[4] == NULL || /* optlines[4] means socket dir */
        optlines[5] == NULL) { /* optlines[5] means listen addr */
        /* File is exactly three lines, must be pre-9.1 */
        write_runlog(ERROR, " -w option is not supported when starting a pre-9.1 server\n");

        freefile(optlines);
        optlines = NULL;
        return NULL;
    }

    /* File is complete enough for us, parse it */
    pmpid = atol(optlines[LOCK_FILE_LINE_PID - 1]);
    if (pmpid > 0) {
        /*
         * OK, seems to be a valid pidfile from our child.
         */
        int portnum;
        char* sockdir = NULL;
        char* hostaddr = NULL;
        char host_str[MAXPGPATH] = {0};
        char local_conninfo[MAXCONNINFO] = {0};
        char* cptr = NULL;
        int rc = 0;

        /*
         * Extract port number and host string to use.
         * We used to prefer unix domain socket.
         * With thread pool, we prefer tcp port and connect to cn/dn ha port
         * so that we do not need to be queued by thread pool controller.
         */
        portnum = CmAtoi(optlines[LOCK_FILE_LINE_PORT - 1], 0);
        sockdir = optlines[LOCK_FILE_LINE_SOCKET_DIR - 1];
        hostaddr = optlines[LOCK_FILE_LINE_LISTEN_ADDR - 1];

        if (hostaddr != NULL && hostaddr[0] != '\0' && hostaddr[0] != '\n') {
            rc = strncpy_s(host_str, sizeof(host_str), hostaddr, sizeof(host_str) - 1);
            securec_check_errno(rc, (void)rc);
        } else if (sockdir[0] == '/') {
            rc = strncpy_s(host_str, sizeof(host_str), sockdir, sizeof(host_str) - 1);
            securec_check_errno(rc, (void)rc);
        }

        /* remove trailing newline */
        cptr = strchr(host_str, '\n');
        if (cptr != NULL)
            *cptr = '\0';

        /* Fail if couldn't get either sockdir or host addr */
        if (host_str[0] == '\0') {
            write_runlog(ERROR, "[%s()][line:%d] option cannot use a relative socket directory specification\n",
                __FUNCTION__, __LINE__);
            freefile(optlines);
            optlines = NULL;
            return NULL;
        }

        /* If postmaster is listening on "*", use localhost */
        if (strcmp(host_str, "*") == 0) {
            rc = strncpy_s(host_str, sizeof(host_str), "localhost", sizeof("localhost"));
            securec_check_errno(rc, (void)rc);
        }
        rc = snprintf_s(local_conninfo,
            sizeof(local_conninfo),
            sizeof(local_conninfo) - 1,
            "dbname=%s port=%d host='%s' connect_timeout=5 rw_timeout=10 application_name=%s "
            "options='%s %s'",
            dbname,
            portnum + 1,
            host_str,
            g_progname,
            enable_xc_maintenance_mode ? "-c xc_maintenance_mode=on" : "",
            "-c remotetype=internaltool");
        securec_check_intval(rc, freefile(optlines));

        write_runlog(DEBUG1, "[%s()][line:%d] cm agent connect cn/dn instance local_conninfo: %s\n",
            __FUNCTION__, __LINE__, local_conninfo);

        dbConn = Connect(local_conninfo);
    }

    freefile(optlines);
    optlines = NULL;
    return dbConn;
}

static int GetDnDatabaseResult(cltPqConn_t* dnConn, const char* runCommand, char* databaseName)
{
    errno_t rcs = 0;
    cltPqResult_t* node_result = NULL;

    write_runlog(DEBUG1, "[%s()][line:%d] runCommand = %s\n", __FUNCTION__, __LINE__, runCommand);

    node_result = Exec(dnConn, runCommand);
    if (node_result == NULL) {
        write_runlog(ERROR, "[%s()][line:%d]  datanode check set command time out fail return NULL!\n",
            __FUNCTION__, __LINE__);
        return -1;
    }

    if ((ResultStatus(node_result) != CLTPQRES_CMD_OK) && (ResultStatus(node_result) != CLTPQRES_TUPLES_OK)) {
        if (ResHasError(node_result)) {
            write_runlog(ERROR, "[%s()][line:%d]  execute command(%s) is failed, errMsg is: %s!\n",
                __FUNCTION__, __LINE__, runCommand, GetResErrMsg(node_result));
        }
        Clear(node_result);
        return -1;
    }

    const int tuplesNum = Ntuples(static_cast<const cltPqResult_t*>(node_result));
    if (tuplesNum == 1) {
        rcs = strncpy_s(databaseName, NAMEDATALEN,
            Getvalue(static_cast<const cltPqResult_t*>(node_result), 0, 0), NAMEDATALEN - 1);
        securec_check_errno(rcs, (void)rcs);
        write_runlog(LOG, "[%s()][line:%d] databaseName:[%s]\n", __FUNCTION__, __LINE__, databaseName);
    } else {
        write_runlog(LOG, "[%s()][line:%d] check_datanode_status: sqlCommands result is %d\n",
            __FUNCTION__, __LINE__, tuplesNum);
    }
    Clear(node_result);
    return 0;
}

int GetDBTableFromSQL(int index, uint32 databaseId, uint32 tableId, uint32 tableIdSize,
                      DNDatabaseInfo *dnDatabaseInfo, int dnDatabaseCount, char* databaseName, char* tableName)
{
    char runCommand[CM_MAX_COMMAND_LONG_LEN] = {0};
    errno_t rc;
    int i = 0;
    int rcs = 0;

    if (dnDatabaseInfo == NULL) {
        write_runlog(ERROR, "[%s()][line:%d] dnDatabaseInfo is NULL!\n", __FUNCTION__, __LINE__);
        return -1;
    }

    if (dnDatabaseCount == 0) {
        write_runlog(ERROR, "[%s()][line:%d] dnDatabaseCount is 0!\n", __FUNCTION__, __LINE__);
        return -1;
    }
    write_runlog(DEBUG1, "[%s()][line:%d] database databaseId:%u\n", __FUNCTION__, __LINE__, databaseId);
    rc = memset_s(databaseName, NAMEDATALEN, 0, NAMEDATALEN);
    securec_check_errno(rc, (void)rc);
    for (i = 0; i < dnDatabaseCount; i++) {
        write_runlog(DEBUG1, "[%s()][line:%d] oid:[%u] dbname:[%s]\n",
            __FUNCTION__, __LINE__, dnDatabaseInfo[i].oid, dnDatabaseInfo[i].dbname);
        if (databaseId == dnDatabaseInfo[i].oid) {
            rcs = strncpy_s(databaseName, NAMEDATALEN, dnDatabaseInfo[i].dbname, NAMEDATALEN - 1);
            securec_check_errno(rcs, (void)rcs);
            write_runlog(LOG, "[%s()][line:%d] databaseName:[%s]\n", __FUNCTION__, __LINE__, databaseName);
            break;
        }
    }
    write_runlog(LOG, "[%s()][line:%d] databaseName:%s tableId:%u tableIdSize:%u\n",
        __FUNCTION__, __LINE__, databaseName, tableId, tableIdSize);
    /* Get tablename from relfilenode */
    if (databaseName != NULL) {
        rc = memset_s(runCommand, CM_MAX_COMMAND_LONG_LEN, 0, CM_MAX_COMMAND_LONG_LEN);
        securec_check_errno(rc, (void)rc);
        rcs = snprintf_s(
            runCommand,
            CM_MAX_COMMAND_LONG_LEN,
            CM_MAX_COMMAND_LONG_LEN - 1,
            "select pg_catalog.get_large_table_name('%u', %u);",
            tableId,
            tableIdSize);
        securec_check_intval(rcs, (void)rcs);
        write_runlog(DEBUG1, "[%s()][line:%d] tablename runCommand:%s\n", __FUNCTION__, __LINE__, runCommand);
        
        cltPqConn_t* dnConn = GetDnConnect(index, databaseName);
        
        if (dnConn == NULL) {
            write_runlog(ERROR, "[%s()][line:%d]get coordinate connect failed!\n", __FUNCTION__, __LINE__);
            return -1;
        }

        if (!IsConnOk(dnConn)) {
            write_runlog(ERROR, "[%s()][line:%d]connect is not ok, errmsg is %s!\n",
                __FUNCTION__, __LINE__, ErrorMessage(dnConn));
            close_and_reset_connection(dnConn);
            return -1;
        }
        rcs = GetDnDatabaseResult(dnConn, runCommand, tableName);
        if (rcs < 0) {
            write_runlog(ERROR, "[%s()][line:%d] get dn tableName failed \n", __FUNCTION__, __LINE__);
        }
        close_and_reset_connection(dnConn);
    }
    return 0;
}
#endif

int GetAllDatabaseInfo(int index, DNDatabaseInfo **dnDatabaseInfo, int *dnDatabaseCount)
{
    char *dbname = NULL;
    int database_count;
    errno_t rc = 0;
    cltPqConn_t *dnConn = NULL;
    cltPqResult_t *node_result = NULL;
    DNDatabaseInfo *localDnDBInfo = NULL;
    char postmaster_pid_path[MAXPGPATH] = {0};
    const char *STMT_GET_DATABASE_LIST = "SELECT DATNAME,OID FROM PG_DATABASE;";
    errno_t rcs = snprintf_s(postmaster_pid_path,
        MAXPGPATH, MAXPGPATH - 1, "%s/postmaster.pid", g_currentNode->datanode[index].datanodeLocalDataPath);
    securec_check_intval(rcs, (void)rcs);

    dnConn = get_connection(postmaster_pid_path);
    if (dnConn == NULL) {
        write_runlog(ERROR, "[%s()][line:%d] get connect failed!\n", __FUNCTION__, __LINE__);
        return -1;
    }

    if (!IsConnOk(dnConn)) {
        write_runlog(ERROR, "[%s()][line:%d] get connect failed! PQstatus IS NOT OK, errmsg is %s\n",
            __FUNCTION__, __LINE__, ErrorMessage(dnConn));
        close_and_reset_connection(dnConn);
        return -1;
    }

    node_result = Exec(dnConn, STMT_GET_DATABASE_LIST);
    if (node_result == NULL) {
        write_runlog(ERROR, "[%s()][line:%d] sqlCommands[0] fail return NULL!\n", __FUNCTION__, __LINE__);
        close_and_reset_connection(dnConn);
        return -1;
    }

    if ((ResultStatus(node_result) != CLTPQRES_CMD_OK) && (ResultStatus(node_result) != CLTPQRES_TUPLES_OK)) {
        if (ResHasError(node_result)) {
            write_runlog(ERROR, "[%s()][line:%d] execute command(%s) failed, errMsg is: %s!\n",
                __FUNCTION__, __LINE__, STMT_GET_DATABASE_LIST, GetResErrMsg(node_result));
        } else {
            write_runlog(ERROR, "[%s()][line:%d] execute command(%s) failed!\n",
                __FUNCTION__, __LINE__, STMT_GET_DATABASE_LIST);
        }
        Clear(node_result);
        close_and_reset_connection(dnConn);
        return -1;
    }

    database_count = Ntuples(node_result);
    if (!(database_count > 0)) {
        write_runlog(ERROR, "[%s()][line:%d] sqlCommands[1] is 0\n", __FUNCTION__, __LINE__);
        Clear(node_result);
        close_and_reset_connection(dnConn);
        return -1;
    }

    if (dnDatabaseCount == NULL) {
        write_runlog(ERROR, "[%s()][line:%d] dnDatabaseCount is NULL!\n", __FUNCTION__, __LINE__);
        Clear(node_result);
        close_and_reset_connection(dnConn);
        return -1;
    }
    *dnDatabaseCount = database_count;

    localDnDBInfo = (DNDatabaseInfo *)malloc(sizeof(DNDatabaseInfo) * database_count);
    if (localDnDBInfo == NULL) {
        write_runlog(ERROR, "[%s()][line:%d] g_dnDatabaseList malloc failed!\n", __FUNCTION__, __LINE__);
        Clear(node_result);
        close_and_reset_connection(dnConn);
        return -1;
    }
    rcs = memset_s(localDnDBInfo, sizeof(DNDatabaseInfo) * database_count, 0,
                   sizeof(DNDatabaseInfo) * database_count);
    securec_check_errno(rcs, FREE_AND_RESET(localDnDBInfo));

    for (int i = 0; i < database_count; i++) {
        dbname = Getvalue(node_result, i, 0);
        rc = strncpy_s(localDnDBInfo[i].dbname, NAMEDATALEN, dbname, NAMEDATALEN - 1);
        securec_check_errno(rc, (void)rc);
        rc = sscanf_s(Getvalue(node_result, i, 1), "%u", &(localDnDBInfo[i].oid));
        check_sscanf_s_result(rc, 1);
        securec_check_intval(rc, (void)rc);
    }

    *dnDatabaseInfo = localDnDBInfo;
    Clear(node_result);
    close_and_reset_connection(dnConn);
    return 0;
}

void ChangeNewBarrierStatues2Old(
    const LocalBarrierStatus *newBarrierStatus, agent_to_cm_coordinate_barrier_status_report *old)
{
    int rc = snprintf_s(old->barrierID, BARRIERLEN, BARRIERLEN - 1, "%s", newBarrierStatus->barrierID);
    securec_check_intval(rc, (void)rc);
    old->barrierLSN = newBarrierStatus->barrierLSN;
    old->archive_LSN = newBarrierStatus->archiveLSN;
    old->flush_LSN = newBarrierStatus->flushLSN;
}

int CheckMostAvailableSync(uint32 index)
{
    cltPqResult_t *nodeResult = NULL;
    int maxRows = 0;
    int maxColums = 0;
    const char *sqlCommands = "show most_available_sync;";
    nodeResult = Exec(g_dnConn[index], sqlCommands);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "CheckMostAvailableSync fail return NULL!\n");
        CLOSE_CONNECTION(g_dnConn[index]);
    }
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "most_available_sync information is empty.\n");
        } else {
            char *result = NULL;
            maxColums = Nfields(nodeResult);
            if (maxColums != 1) {
                write_runlog(ERROR, "CheckMostAvailableSync fail! col is %d.\n", maxColums);
                CLEAR_AND_CLOSE_CONNECTION(nodeResult, g_dnConn[index]);
            }
            result = Getvalue(nodeResult, 0, 0);
            write_runlog(DEBUG1, "CheckMostAvailableSync most_available_sync is %s.\n", result);
            if (strcmp(result, "on") == 0) {
                g_mostAvailableSync[index] = true;
            } else {
                g_mostAvailableSync[index] = false;
            }
        }
    } else {
        write_runlog(ERROR, "CheckMostAvailableSync fail Status=%d!\n", ResultStatus(nodeResult));
    }
    Clear(nodeResult);
    return 0;
}

int32 CheckDnSyncDone(uint32 instd, AgentToCmserverDnSyncList *syncListMsg, cltPqConn_t **curDnConn)
{
    const char *sqlCommands = "select * from gs_write_term_log();";
    cltPqResult_t *nodeResult = Exec((*curDnConn), sqlCommands);
    if (nodeResult == NULL) {
        write_runlog(ERROR, "instd is %u, CheckDnSyncDone fail return NULL!\n", instd);
        CLOSE_CONNECTION((*curDnConn));
    }
    int32 st = 0;
    if ((ResultStatus(nodeResult) == CLTPQRES_CMD_OK) || (ResultStatus(nodeResult) == CLTPQRES_TUPLES_OK)) {
        int32 maxRows = Ntuples(nodeResult);
        if (maxRows == 0) {
            write_runlog(ERROR, "instd is %u, CheckDnSyncDone information is empty.\n", instd);
            st = -1;
        } else {
            int32 maxColums = Nfields(nodeResult);
            if (maxColums != 1) {
                write_runlog(ERROR, "instd is %u, CheckDnSyncDone fail! col is %d.\n", instd, maxColums);
                CLEAR_AND_CLOSE_CONNECTION(nodeResult, (*curDnConn));
            }
            char *result = Getvalue(nodeResult, 0, 0);
            write_runlog(DEBUG1, "instd is %u, CheckDnSyncDone result is %s.\n", instd, result);
            if (strcmp(result, "t") == 0) {
                syncListMsg->syncDone = SUCCESS_SYNC_DATA;
            } else {
                syncListMsg->syncDone = FAILED_SYNC_DATA;
                st = -1;
            }
        }
    } else {
        write_runlog(ERROR, "instd is %u, CheckDnSyncDone fail Status=%d!\n", instd, ResultStatus(nodeResult));
        syncListMsg->syncDone = FAILED_SYNC_DATA;
        st = -1;
    }
    Clear(nodeResult);
    return st;
}
