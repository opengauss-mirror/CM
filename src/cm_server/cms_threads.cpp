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
 * cms_threads.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_server/cms_threads.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/epoll.h>
#include "cms_global_params.h"
#include "cms_threads.h"
#include "cms_arbitrate_cms.h"
#include "cms_az.h"
#include "cms_cluster_switchover.h"
#include "cms_conn.h"
#include "cms_disk_check.h"
#include "cms_monitor_main.h"
#include "cms_phony_dead_check.h"
#include "cms_sync_dynamic_info.h"
#include "cms_common.h"
#include "cms_write_dynamic_config.h"
#include "cms_barrier_check.h"

static const int GET_DORADO_IP_TIMES = 3;

/**
 * @brief cm_server arbitrate self
 * 
 * @return int 
 */
int CM_CreateHA(void)
{
    CM_HAThread* pHAthread = NULL;
    int err;
    errno_t rc = 0;

    for (int i = 0; i < CM_HA_THREAD_NUM; i++) {
        pHAthread = &(gHAThreads.threads[i]);

        rc = memset_s(pHAthread, sizeof(CM_HAThread), 0, sizeof(CM_HAThread));
        securec_check_errno(rc, (void)rc);
        if ((err = pthread_create(&(pHAthread->thread.tid), NULL, CM_ThreadHAMain, pHAthread)) != 0) {
            write_runlog(ERROR, "Create HA thread failed %d: %m\n", err);
            return -1;
        }
        gHAThreads.count++;
    }
    return 0;
}
/**
 * @brief Create a monitor Thread object
 * 
 * @return int 
 */
int CM_CreateMonitor(void)
{
    CM_MonitorThread* monitor = &gMonitorThread;
    errno_t rc = memset_s(monitor, sizeof(CM_MonitorThread), 0, sizeof(CM_MonitorThread));
    securec_check_errno(rc, (void)rc);
    if (pthread_create(&(gMonitorThread.thread.tid), NULL, CM_ThreadMonitorMain, monitor) != 0) {
        return -1;
    }
    return 0;
}

#ifdef ENABLE_MULTIPLE_NODES
status_t CmCreateCheckGtmModThread()
{
    int32 err = 0;
    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, CheckGtmModMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
        return CM_ERROR;
    }
    if ((err = pthread_detach(thrId)) != 0) {
        write_runlog(ERROR, "Failed to detach a new gtm mod thread: error %d.\n", err);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}
#endif

/**
 * @brief Create a Storage Threshold Check Thread object
 * 
 */
static void CreateStorageThresholdCheckThread()
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, StorageDetectMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread: error %d\n", err);
    }
}

/**
 * @brief Create a deal phony alarm thread object
 * 
 */
static void CreateDealPhonyAlarmThread()
{
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, deal_phony_dead_alarm, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for deal phony alarm: error %d\n", err);
    }
}

/**
 * @brief Create a deal global barrier thread object
 */
void CreateDealGlobalBarrierThread()
{
    int err;
    pthread_t thr_id;
    if (backup_open == CLUSTER_PRIMARY || g_clusterInstallType != INSTALL_TYPE_STREAMING) {
        return;
    }
    if ((err = pthread_create(&thr_id, NULL, DealGlobalBarrier, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for deal global barrier: error %d\n", err);
    }
}

void *CheckDoradoIp(void *arg)
{
    errno_t rc;

    for (;;) {
        char tmpIp[CM_IP_LENGTH] = {0};
        rc = strcpy_s(tmpIp, CM_IP_LENGTH, g_doradoIp);
        securec_check_errno(rc, (void)rc);
        GetDoradoOfflineIp(g_doradoIp, CM_IP_LENGTH);
        if (strcmp(tmpIp, g_doradoIp) != 0) {
            write_runlog(LOG, "cms get g_doradoIp has change from \"%s\" to \"%s\"\n", tmpIp, g_doradoIp);
        } else {
            write_runlog(DEBUG1, "cms get g_doradoIp = %s\n", g_doradoIp);
        }
        cm_sleep(GET_DORADO_IP_TIMES);
    }

    return NULL;
}

void CreateDoradoCheckThread()
{
    int err;
    pthread_t thrId;
    if (!GetIsSharedStorageMode()) {
        return;
    }
    if ((err = pthread_create(&thrId, NULL, CheckDoradoIp, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for CreateDoradoCheckThread: error %d\n", err);
    }
    return;
}

/**
 * @brief 
 * 
 * @return int 
 */
int CM_CreateMonitorStopNode(void)
{
    CM_MonitorNodeStopThread* monitor = &gMonitorNodeStopThread;
    errno_t rc = memset_s(monitor, sizeof(CM_MonitorNodeStopThread), 0, sizeof(CM_MonitorNodeStopThread));
    securec_check_errno(rc, (void)rc);

    if (pthread_create(&(gMonitorNodeStopThread.thread.tid), NULL, CM_ThreadMonitorNodeStopMain, monitor) != 0) {
        return -1;
    }
    return 0;
}

int CM_CreateThreadPool(int thrCount)
{
    CM_Thread* thrinfo = NULL;
    int err;
    errno_t rc = 0;
    int trueThrCount = thrCount;

    /* include cm_ctl, so g_node_num need add. */
    int ctlThreadNum = GetCtlThreadNum();
    if ((uint32)thrCount > (g_node_num + (uint32)ctlThreadNum)) {
        trueThrCount = ((int)g_node_num + ctlThreadNum);
    }
    if (trueThrCount <= ctlThreadNum) {
        trueThrCount += ctlThreadNum;
    }

    for (int i = 0; i < trueThrCount; i++) {
        thrinfo = &(gThreads.threads[i]);

        rc = memset_s(&gThreads.threads[i], sizeof(CM_Thread), 0, sizeof(CM_Thread));
        securec_check_errno(rc, (void)rc);
        /* create epoll fd, MAX_EVENTS just a HINT */
        int epollFd = epoll_create(MAX_EVENTS);
        if (epollFd < 0) {
            write_runlog(ERROR, "create epoll failed %d.\n", epollFd);
            return -1;
        }

        thrinfo->epHandle = epollFd;
        thrinfo->type = (i >= trueThrCount - ctlThreadNum) ? CM_CTL : CM_AGENT;
        thrinfo->isBusy = false;

        if ((err = pthread_create(&thrinfo->tid, NULL, CM_ThreadMain, thrinfo)) != 0) {
            write_runlog(ERROR, "Failed to create a new thread %d: %m\n", err);
            return -1;
        }

        gThreads.count++;
    }
    return 0;
}


/**
 * @brief create status check for az 
 * 
 */
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
static void CreateAzStatusCheckForAzThread()
{
    if (!g_multi_az_cluster) {
        return;
    }
    int err;
    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, AZStatusCheckAndArbitrate, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for az: error %d\n", err);
    }
}
#endif

void SetVoteAzInstanceId()
{
    bool result = false;
    for (uint32 groupIndex = 0; groupIndex < g_dynamic_header->relationCount; ++groupIndex) {
        if (g_instance_role_group_ptr[groupIndex].instanceMember[0].instanceType != INSTANCE_TYPE_DATANODE) {
            continue;
        }
        for (int memberIndex = 0; memberIndex < g_instance_role_group_ptr[groupIndex].count; ++memberIndex) {
            result = IsCurInstanceInVoteAz(groupIndex, memberIndex);
            if (result) {
                DatanodeDynamicStatus *voteAzInstance =
                    &g_instance_group_report_status_ptr[groupIndex].instance_status.voteAzInstance;
                voteAzInstance->dnStatus[voteAzInstance->count++] =
                    g_instance_role_group_ptr[groupIndex].instanceMember[memberIndex].instanceId;
            }
        }
    }
}

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
bool CreateDnGroupStatusCheckAndArbitrateThread()
{
    if (!g_multi_az_cluster) {
        g_isEnableUpdateSyncList = CANNOT_START_SYNCLIST_THREADS;
        return false;
    }
    bool isVoteAz = (GetVoteAzIndex() != AZ_ALL_INDEX);
    if (GetAzDeploymentType(isVoteAz) != TWO_AZ_DEPLOYMENT) {
        g_isEnableUpdateSyncList = CANNOT_START_SYNCLIST_THREADS;
        write_runlog(LOG, "DnGroupStatusCheckAndArbitrateMain exit, because this is not two az deployment.\n");
        return false;
    }
    if (g_dynamic_header->relationCount >= BIG_CLUSTER_FRAGENT_COUNT) {
        g_isEnableUpdateSyncList = CANNOT_START_SYNCLIST_THREADS;
        write_runlog(
            LOG, "DnGroupStatusCheckAndArbitrateMain exit, because this cannot support big cluster deployment .\n");
        return false;
    }
    const int onePrimaryTwoStandby = 3;
    if (g_dn_replication_num <= onePrimaryTwoStandby) {
        g_isEnableUpdateSyncList = CANNOT_START_SYNCLIST_THREADS;
        write_runlog(LOG, "DnGroupStatusCheckAndArbitrateMain exit, because this is one Primary Two Standby.\n");
        return false;
    }
    if (backup_open == CLUSTER_STREAMING_STANDBY) {
        g_isEnableUpdateSyncList = CANNOT_START_SYNCLIST_THREADS;
        write_runlog(LOG, "DnGroupStatusCheckAndArbitrateMain exit, because this is streaming standby cluster.\n");
        return false;
    }
    SetVoteAzInstanceId();
    int err;
    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, DnGroupStatusCheckAndArbitrateMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for az: error %d\n", err);
        return false;
    }
    return true;
}
#endif

/**
 * @brief create check az1 and az2 connect state
 * 
 */
static void CreateBothConnectStateCheckThread()
{
    if (!g_multi_az_cluster) {
        return;
    }
    int err;
    pthread_t thr_id;
    if ((err = pthread_create(&thr_id, NULL, BothAzConnectStateCheckMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for both connect state check: error %d\n", err);
    }
}

/*
 * @brief create check multiAz connect state
 *
 */
static void CreateMultiAzConnectStateCheckThread()
{
    if (!g_multi_az_cluster) {
        return;
    }
    int err;
    pthread_t thr_id;

    if ((err = pthread_create(&thr_id, NULL, MultiAzConnectStateCheckMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create a new thread for multiAz connect stste chheck: error %d\n", err);
    }
}

static void Init_cluster_to_switchover()
{
    char execPath[MAX_PATH_LEN] = {0};
    errno_t rcs;
    if (GetHomePath(execPath, sizeof(execPath)) != 0) {
        FreeNotifyMsg();
        exit(-1);
    }
    rcs = memset_s(switchover_flag_file_path, MAX_PATH_LEN, 0, MAX_PATH_LEN);
    securec_check_errno(rcs, (void)rcs);
    rcs = snprintf_s(
        switchover_flag_file_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/%s", execPath, SWITCHOVER_FLAG_FILE);
    securec_check_intval(rcs, (void)rcs);

    if (access(switchover_flag_file_path, 0) != 0) {
        write_runlog(LOG, "don't have switchover flag file.\n");
    } else {
        pthread_t thr_id;
        if ((rcs = pthread_create(&thr_id, NULL, Deal_switchover_for_init_cluster, NULL)) != 0) {
            write_runlog(ERROR, "Failed to create a new thread for switchover: error %d\n", rcs);
        }
    }
}

/**
 * @brief create thread for deal the arbitrate DN for deal with ddb and dynamic config
 * 
 */
static void CreateSyncDynamicInfoThread()
{
    int err;
    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, SyncDynamicInfoFromDdb, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create new thread for SyncDynamicInfo: error %d\n", err);
    }
}

/**
 * @brief create thread for check upgrade blacklist
 * 
 */
static void CreateCheckBlackListThread()
{
    int err;
    pthread_t thrId;
    if ((err = pthread_create(&thrId, NULL, CheckBlackList, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create CheckBlackList: error %d\n", err);
    }
}

static void CreateDynamicCfgSyncThread()
{
    int err;
    pthread_t thr_id;
    if (!NeedCreateWriteDynamicThread()) {
        return;
    }
    
    if ((err = pthread_create(&thr_id, NULL, WriteDynamicCfgMain, NULL) != 0)) {
        write_runlog(ERROR, "Failed to create CreateDynamicCfgSyncThread: error %d\n", err);
    }
}

CM_Thread* GetNextThread(int ctlThreadNum)
{
    static uint32 next = 0;
    CM_Thread* thread = &gThreads.threads[next];
    next = (next + 1) % (gThreads.count - (uint32)ctlThreadNum);
    return thread;
}

CM_Thread* GetNextCtlThread(int threadNum)
{
    if (threadNum == 0) {
        return NULL;
    }
    int retryTimes = 3;
    const int interval = 100;
    while ((retryTimes--) > 0) {
        for (int32 i = (int32)(gThreads.count - 1); i >= static_cast<int32>(gThreads.count) - threadNum; i--) {
            if (!gThreads.threads[i].isBusy) {
                return &gThreads.threads[i];
            }
        }
        cm_usleep(interval);
    }

    static int next = 0;
    int threadIndex = gThreads.count - threadNum + next;
    CM_Thread* thread = &gThreads.threads[threadIndex];
    next = (next + 1) % threadNum;
    return thread;
}

status_t CmsCreateThreads()
{
#ifdef ENABLE_MULTIPLE_NODES
    status_t st = CmCreateCheckGtmModThread();
    if (st != CM_SUCCESS) {
        return CM_ERROR;
    }
#endif

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
    if (!(g_clusterInstallType == INSTALL_TYPE_SHARE_STORAGE && backup_open == CLUSTER_PRIMARY)) {
        bool isResult = CreateDnGroupStatusCheckAndArbitrateThread();
        if (!isResult) {
            CreateAzStatusCheckForAzThread();
        }
    }
#endif

    CreateBothConnectStateCheckThread();
    CreateSyncDynamicInfoThread();
    CreateCheckBlackListThread();
    CreateDynamicCfgSyncThread();
    Init_cluster_to_switchover();
    CreateMultiAzConnectStateCheckThread();

    /* Data disk storage threshold check */
    CreateStorageThresholdCheckThread();

    CreateDealPhonyAlarmThread();
    CreateDealGlobalBarrierThread();
    CreateDoradoCheckThread();

    return CM_SUCCESS;
}
