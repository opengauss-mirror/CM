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
 * cma_threads.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_threads.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm/cm_elog.h"
#include "cma_global_params.h"
#include "cma_client.h"
#include "cma_create_conn_cms.h"
#include "cma_log_management.h"
#include "cma_phony_dead_check.h"
#include "cma_instance_management.h"
#include "cma_process_messages.h"
#include "cma_status_check.h"
#include "cma_threads.h"
#include "cma_common.h"
#include "cma_connect_client.h"
#include "cma_datanode_scaling.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "cma_gtm.h"
#include "cma_coordinator.h"
#include "cma_cn_gtm_work_threads_mgr.h"
#endif

void CreateETCDStatusCheckThread()
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, ETCDStatusCheckMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
}

void CreateETCDConnectionStatusCheckThread()
{
    int err;
    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, ETCDConnectionStatusCheckMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
    }
}

void CreatePhonyDeadCheckThread()
{
    int err;
    pthread_t thr_id;
#ifdef ENABLE_MULTIPLE_NODES
    if (g_currentNode->gtm == 1) {
        if ((err = pthread_create(&thr_id, NULL, GTMPhonyDeadStatusCheckMain, NULL)) != 0) {
            write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
            exit(-1);
        }
    }
    if (g_currentNode->coordinate == 1) {
        if ((err = pthread_create(&thr_id, NULL, CNPhonyDeadStatusCheckMain, NULL)) != 0) {
            write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
            exit(-1);
        }
    }
#endif
    if (g_currentNode->datanodeCount > 0) {
        for (uint32 i = 0; i < g_currentNode->datanodeCount; i++) {
            if ((err = pthread_create(
                     &thr_id, NULL, DNPhonyDeadStatusCheckMain, &(g_currentNode->datanode[i].datanodeId))) != 0) {
                write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
                exit(-1);
            }
        }
    }
}

void CreateStartAndStopThread()
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, agentStartAndStopMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
}

void CreateDNBackupStatusCheckThread(int* i)
{
    int err;
    pthread_t thr_id;

    if (agent_backup_open != CLUSTER_STREAMING_STANDBY) {
        return;
    }
    if ((err = pthread_create(&thr_id, NULL, DNBackupStatusCheckMain, i)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
}

void CreateDNStatusCheckThread(int* i)
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, DNStatusCheckMain, i)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
    save_thread_id(thr_id);
}

void CreateDNCheckSyncListThread(int *idx)
{
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    if (g_currentNode->datanode[(*idx)].datanodeRole == DUMMY_STANDBY_DN) {
        write_runlog(LOG, "inst(%d) is dummy standby, not need to create synclist thread.\n", (*idx));
        return;
    }
    int err;
    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, DNSyncCheckMain, idx)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
#endif
}

void CreateDNConnectionStatusCheckThread(int* i)
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, DNConnectionStatusCheckMain, i)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
}


/* create kerberos thread check */ 
void CreateKerberosStatusCheckThread()
{
    int err;
    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, KerberosStatusCheckMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(err);
    }
}

/* create kerberos thread check */ 
void CreateDefResStatusCheckThread(void)
{
    int err;
    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, ResourceStatusCheckMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a ResourceStatusCheckMain thread: error %d\n", err);
        exit(err);
    }
}

void CreateFaultDetectThread()
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, FaultDetectMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        exit(-1);
    }
}

void CreateConnCmsPThread()
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, ConnCmsPMain, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread: error %d\n", err);
        exit(err);
    }
}

void CreateSendCmsMsgThread()
{
    int err;

    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, SendCmsMsgMain, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread: error %d\n", err);
        exit(err);
    }
}

/*
 * Create compress and remove thread for trace.
 * Use Thread for this task avoid taking too much starting time of cm server.
 */
void CreateLogFileCompressAndRemoveThread()
{
    int err;
    pthread_t thr_id;

    if ('\0' != g_logBasePath[0] && IsBoolCmParamTrue(g_enableLogCompress)) {
        write_runlog(LOG, "Get GAUSSLOG from environment %s.\n", g_logBasePath);
        g_logPattern = (LogPattern*)malloc(sizeof(LogPattern) * MAX_PATH_LEN);
        if (g_logPattern == NULL) {
            write_runlog(FATAL, "out of memory!\n");
            exit(-1);
        }
        get_log_pattern();
        if ((err = pthread_create(&thr_id, NULL, CompressAndRemoveLogFile, NULL)) != 0) {
#ifndef ENABLE_LLT
            write_runlog(ERROR, "Failed to create log file thread: error %d\n", err);
            exit(-1);
#endif
        }
    } else {
        write_runlog(ERROR,
            "Get GAUSSLOG from environment failed or enable_log_compress is off "
            "GAUSSLOG=%s,enable_log_compress=%s.\n",
            g_logBasePath,
            g_enableLogCompress);
    }
}

void CreateCheckUpgradeModeThread()
{
    int err;
    pthread_t thrId;

    if ((err = pthread_create(&thrId, NULL, CheckUpgradeMode, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread: error %d\n", err);
        exit(err);
    }
}

void CreateRecvClientMessageThread()
{
    int err;
    pthread_t thrId;

    if ((err = pthread_create(&thrId, NULL, RecvClientEventsMain, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread: error %d\n", err);
        exit(err);
    }
}

void CreateSendMessageToClientThread()
{
    int err;
    pthread_t thrId;

    if ((err = pthread_create(&thrId, NULL, SendMessageToClientMain, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread: error %d\n", err);
        exit(err);
    }
}

void CreateProcessMessageThread()
{
    int err;
    pthread_t thrId;

    if ((err = pthread_create(&thrId, NULL, ProcessMessageMain, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread: error %d\n", err);
        exit(err);
    }
}
