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
 * cma_instance_management_ext.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_instance_management_ext.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/wait.h>
#include "cma_global_params.h"
#include "cm/libpq-fe.h"
#include "cm/libpq-int.h"
#include "common/config/cm_config.h"
#include "cma_alarm.h"
#include "cma_common.h"
#include "cma_instance_management.h"

#define RES_EXEC_ERROR    (-1)

extern vector<ResourceListInfo> g_res_list;

int SystemExecute(const char *scriptPath, const char *oper)
{
    int status;
    int rcs;
    char command[MAXPGPATH] = {0};
    int ret = -1;

    rcs = snprintf_s(command, MAXPGPATH, MAXPGPATH - 1,
        SYSTEMQUOTE "%s %s >> \"%s\"" SYSTEMQUOTE, scriptPath, oper, system_call_log);
    securec_check_intval(rcs, (void)rcs);
    status = system(command);
    if (status == -1) {
        write_runlog(ERROR, "run system command failed %s.\n", command);
        return ret;
    }
    if (WIFEXITED(status)) {
        ret = WEXITSTATUS(status);
        write_runlog(DEBUG1, "run script command %s, ret=%d.\n", command, ret);
        return ret;
    } else {
        write_runlog(ERROR, "run system command failed %s, errno=%d.\n", command, WEXITSTATUS(status));
    }
    return ret;
}

void StartOneResourceInstance(const char *scriptPath, uint32 resInstanceId)
{
    int ret;
    int rcs;
    char oper[MAX_OPTION_LEN];

    rcs = snprintf_s(oper, MAX_OPTION_LEN, MAX_OPTION_LEN - 1, "-start %u", resInstanceId);
    securec_check_intval(rcs, (void)rcs);

    ret = SystemExecute(scriptPath, oper);
    if (ret == 0) {
        write_runlog(DEBUG1, "run system script %s successfully\n", scriptPath);
    } else {
        write_runlog(ERROR, "run system script %s failed.\n", scriptPath);
    }
}

void StopOneResourceInstance(const char *scriptPath, uint32 resInstanceId)
{
    int ret;
    int rcs;
    char oper[MAX_OPTION_LEN];

    rcs = snprintf_s(oper, MAX_OPTION_LEN, MAX_OPTION_LEN - 1, "-stop %u", resInstanceId);
    securec_check_intval(rcs, (void)rcs);

    ret = SystemExecute(scriptPath, oper);
    if (ret == 0) {
        write_runlog(LOG, "stopResourceInstances: run stop command (%s) successfully\n", scriptPath);
    } else {
        write_runlog(ERROR, "stopResourceInstances: run stop command (%s) failed\n", scriptPath);
    }
}

void StopResourceInstances(void)
{
    int ret;
    RES_PTR resInfo;
    for(resInfo = g_res_list.begin(); resInfo != g_res_list.end(); resInfo++) { 
        if (resInfo->nodeId != g_currentNode->node) {
            continue;
        }
        ret =  CheckOneResourceState(resInfo->scriptPath, resInfo->resInstanceId);
        if (ret == PROCESS_NOT_EXIST) {
            continue;
        }
        StopOneResourceInstance(resInfo->scriptPath, resInfo->resInstanceId);
    }
}

int CheckOneResourceState(const char *scriptPath, uint32 resInstanceId)
{
    int ret;
    int rcs;
    int value;
    char command[MAXPGPATH] = {0};
    char oper[MAX_OPTION_LEN];

    rcs = snprintf_s(oper, MAX_OPTION_LEN, MAX_OPTION_LEN - 1, "-check %u", resInstanceId);
    securec_check_intval(rcs, (void)rcs);

    value = SystemExecute(scriptPath, oper);
    if (value == -1) {
        write_runlog(ERROR, "run system command failed.\n");
        return value;
    }

    if (value == 0) {
        return PROCESS_RUNNING;
    } else if (value == 1) {
        ret = PROCESS_NOT_EXIST;
    } else {
        ret = PROCESS_CORPSE;
    }
    write_runlog(DEBUG1, "CheckOneResourceState %s, result: %d\n", command, ret);
    return ret;
}

void StartResourceCheck(void)
{
    int ret;
    int rcs;
    struct stat instanceStatBuf = {0};
    struct stat cluster_stat_buf = {0};
    char instance_manual_start_path[MAX_PATH_LEN] = {0};

    char exec_path[MAX_PATH_LEN] = {0};
    if (GetHomePath(exec_path, sizeof(exec_path)) != 0) {
        return;
    }

    RES_PTR resInfo;
    for (resInfo = g_res_list.begin(); resInfo != g_res_list.end(); resInfo++) {
        if (resInfo->nodeId != g_currentNode->node) {
            continue;
        }

        ret =  CheckOneResourceState(resInfo->scriptPath, resInfo->resInstanceId);
        switch (ret) {
        case PROCESS_RUNNING:
            break;
        case PROCESS_NOT_EXIST:
            rcs = memset_s(instance_manual_start_path, MAX_PATH_LEN, 0, MAX_PATH_LEN);
            securec_check_errno(rcs, (void)rcs);
            rcs = snprintf_s(instance_manual_start_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/instance_manual_start_%u", 
                exec_path, resInfo->cmInstanceId);
            securec_check_intval(rcs, (void)rcs);
            if (stat(instance_manual_start_path, &instanceStatBuf) == 0 ||
            stat(g_cmManualStartPath, &cluster_stat_buf) == 0) {
                continue;
            }
            StartOneResourceInstance(resInfo->scriptPath, resInfo->resInstanceId);
            break;
         case PROCESS_CORPSE:
            StopOneResourceInstance(resInfo->scriptPath, resInfo->resInstanceId);
            break;
         default :
            write_runlog(ERROR, "Unknown status %d\n", ret);
        }
    }
}

void StopResourceCheck(void)
{
    int rcs;
    int ret;
    struct stat instanceStatBuf = {0};
    struct stat cluster_stat_buf = {0};
    char instance_manual_start_path[MAX_PATH_LEN] = {0};
    char instance_replace[MAX_PATH_LEN] = {0};
    char exec_path[MAX_PATH_LEN] = {0};

    if (GetHomePath(exec_path, sizeof(exec_path)) != 0) {
        return;
    }

    RES_PTR resInfo;
    for (resInfo = g_res_list.begin(); resInfo != g_res_list.end(); resInfo++) {
        if (resInfo->nodeId != g_currentNode->node) {
            continue;
        }
        instance_manual_start_path[0] = '\0';
        instance_replace[0] = '\0';
        rcs = snprintf_s(instance_manual_start_path, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/instance_manual_start_%u", 
            exec_path, resInfo->cmInstanceId);
        securec_check_intval(rcs, (void)rcs);
        rcs = snprintf_s(instance_replace, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/bin/%s_%u", exec_path, 
            CM_INSTANCE_REPLACE, resInfo->cmInstanceId);
        securec_check_intval(rcs, (void)rcs);
        if (stat(instance_replace, &instanceStatBuf) == 0) {
            write_runlog(LOG, "Resource instance is being replaced and can't be stopped.\n");
            continue;
        }

        if (stat(instance_manual_start_path, &instanceStatBuf) == 0 ||
            stat(g_cmManualStartPath, &cluster_stat_buf) == 0) {
            ret =  CheckOneResourceState(resInfo->scriptPath, resInfo->resInstanceId);
            if (ret == PROCESS_RUNNING) {
                write_runlog(LOG, "Resource fast shutdown, res name: %s.\n", resInfo->resName);
                StopOneResourceInstance(resInfo->scriptPath, resInfo->resInstanceId);
            }
        }
    }
}

int ResourceStoppedCheck(void)
{
    int ret;

    RES_PTR resInfo;
    for (resInfo = g_res_list.begin(); resInfo != g_res_list.end(); resInfo++) {
        if (resInfo->nodeId != g_currentNode->node) {
            continue;
        }
        ret = CheckOneResourceState(resInfo->scriptPath, resInfo->resInstanceId);
        if (ret == PROCESS_RUNNING) {
            write_runlog(LOG, "resource is running path is %s\n", resInfo->scriptPath);
            return PROCESS_RUNNING;
        }
    }
    return PROCESS_NOT_EXIST;
}
