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
#include "cma_alarm.h"
#include "cma_common.h"
#include "cma_instance_management.h"

int SystemExecute(const char *scriptPath, const char *oper, uint32 timeout)
{
    char command[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(command,
                         MAX_PATH_LEN,
                         MAX_PATH_LEN - 1,
                         SYSTEMQUOTE "timeout -s SIGKILL %us %s %s >> \"%s\"" SYSTEMQUOTE,
                         timeout,
                         scriptPath,
                         oper,
                         system_call_log);
    securec_check_intval(ret, (void)ret);
    int status = system(command);
    if (status == -1) {
        write_runlog(ERROR, "run system command failed %s.\n", command);
        return -1;
    }
    if (WIFEXITED(status)) {
        ret = WEXITSTATUS(status);
        write_runlog(DEBUG1, "run script command %s, ret=%d.\n", command, ret);
        return ret;
    } else {
        write_runlog(ERROR, "run system command failed %s, errno=%d.\n", command, WEXITSTATUS(status));
    }
    return -1;
}

void StartOneResInst(const char *scriptPath, uint32 resInstanceId, uint32 timeout)
{
    char oper[MAX_OPTION_LEN];
    int ret = snprintf_s(oper, MAX_OPTION_LEN, MAX_OPTION_LEN - 1, "-start %u", resInstanceId);
    securec_check_intval(ret, (void)ret);

    ret = SystemExecute(scriptPath, oper, timeout);
    if (ret == 0) {
        write_runlog(DEBUG1, "StartOneResInst: run system script %s successfully\n", scriptPath);
    } else {
        write_runlog(ERROR, "StartOneResInst: run system script %s failed, ret=%d.\n", scriptPath, ret);
    }
}

void StopOneResInst(const char *scriptPath, uint32 resInstanceId, uint32 timeout)
{
    char oper[MAX_OPTION_LEN];
    int ret = snprintf_s(oper, MAX_OPTION_LEN, MAX_OPTION_LEN - 1, "-stop %u", resInstanceId);
    securec_check_intval(ret, (void)ret);

    ret = SystemExecute(scriptPath, oper, timeout);
    if (ret == 0) {
        write_runlog(LOG, "StopOneResInst: run stop command (%s) successfully\n", scriptPath);
    } else {
        write_runlog(ERROR, "StopOneResInst: run stop command (%s) failed, ret=%d\n", scriptPath, ret);
    }
}

void StopResourceInstances(void)
{
    for (uint32 i = 0; i < (uint32)g_resConf.size(); ++i) {
        if (CheckOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut) ==
            CM_RES_STAT_OFFLINE) {
            continue;
        }
        StopOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut);
    }
}

ResStatus CheckOneResInst(const char *scriptPath, uint32 resInstanceId, uint32 timeout)
{
    char oper[MAX_OPTION_LEN] = {0};
    int ret = snprintf_s(oper, MAX_OPTION_LEN, MAX_OPTION_LEN - 1, "-check %u", resInstanceId);
    securec_check_intval(ret, (void)ret);

    ret = SystemExecute(scriptPath, oper, timeout);
    if (ret == -1) {
        write_runlog(ERROR, "run system command failed.\n");
        return CM_RES_STAT_UNKNOWN;
    }
    ResStatus stat = CM_RES_STAT_UNKNOWN;
    if (ret == 0) {
        return CM_RES_STAT_ONLINE;
    } else if (ret == 1) {
        stat = CM_RES_STAT_OFFLINE;
    }
    write_runlog(DEBUG1, "CheckOneResInst, result: %d\n", (int)stat);
    return stat;
}

static void StopLocalResInstance(CmResConfList *conf)
{
    char instanceStartFile[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(instanceStartFile, MAX_PATH_LEN, MAX_PATH_LEN - 1,
                         "%s_%u", g_cmInstanceManualStartPath, conf->cmInstanceId);
    securec_check_intval(ret, (void)ret);

    char command[MAX_PATH_LEN] = {0};
    ret = snprintf_s(command, MAX_PATH_LEN, MAX_PATH_LEN - 1,
                     SYSTEMQUOTE "touch %s;chmod 600 %s < \"%s\" 2>&1" SYSTEMQUOTE,
                     instanceStartFile, instanceStartFile, DEVNULL);
    securec_check_intval(ret, (void)ret);

    ret = system(command);
    if (ret != 0) {
        write_runlog(ERROR, "stop resource(%s) inst(%u) failed, ret=%d.\n", conf->resName, conf->resInstanceId, ret);
    } else {
        write_runlog(LOG, "stop resource(%s) inst(%u) success.\n", conf->resName, conf->resInstanceId);
        conf->checkInfo.startCount = 0;
        conf->checkInfo.startTime = 0;
        conf->isWorkMember = 0;
    }
}

static void ProcessOfflineInstance(CmResConfList *conf)
{
    if (conf->checkInfo.startCount == 0) {
        StartOneResInst(conf->script, conf->resInstanceId, conf->checkInfo.timeOut);
        conf->checkInfo.startCount++;
        conf->checkInfo.startTime = time(NULL);
        write_runlog(DEBUG1, "[CLIENT] startCount = %u, startTime = %ld.\n",
                     conf->checkInfo.startCount, conf->checkInfo.startTime);
        return;
    }
    if (conf->checkInfo.restartTimes != 0 && conf->checkInfo.startCount > conf->checkInfo.restartTimes) {
        write_runlog(LOG, "[CLIENT] res(%s) inst(%u) get out from cluster.\n", conf->resName, conf->resInstanceId);
        StopLocalResInstance(conf);
        return;
    }
    if ((time(NULL) - conf->checkInfo.startTime) < conf->checkInfo.restartPeriod) {
        write_runlog(DEBUG5, "[CLIENT] res(%s) inst(%u) startTime = %ld, restartPeriod = %u.\n",
                     conf->resName, conf->resInstanceId, conf->checkInfo.startTime, conf->checkInfo.restartPeriod);
        return;
    }
    if ((time(NULL) - conf->checkInfo.startTime) < conf->checkInfo.restartDelay) {
        write_runlog(DEBUG5, "[CLIENT] res(%s) inst(%u) startTime = %ld, restartPeriod = %u.\n",
                     conf->resName, conf->resInstanceId, conf->checkInfo.startTime, conf->checkInfo.restartDelay);
        return;
    }
    write_runlog(DEBUG1, "[CLIENT] startCount = %u, startTime = %ld.\n",
                 conf->checkInfo.startCount, conf->checkInfo.startTime);
    StartOneResInst(conf->script, conf->resInstanceId, conf->checkInfo.timeOut);
    conf->checkInfo.startCount++;
    conf->checkInfo.startTime = time(NULL);
}

void StartResourceCheck(void)
{
    for (uint32 i = 0; i < (uint32)g_resConf.size(); ++i) {
        ResStatus status =
                CheckOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut);
        switch (status) {
            case CM_RES_STAT_ONLINE:
                g_resConf[i].checkInfo.startCount = 0;
                g_resConf[i].checkInfo.startTime = 0;
                g_resConf[i].isWorkMember = 1;
                break;
            case CM_RES_STAT_OFFLINE: {
                char instManualStartPath[MAX_PATH_LEN] = {0};
                int ret = snprintf_s(instManualStartPath,
                                     MAX_PATH_LEN,
                                     MAX_PATH_LEN - 1,
                                     "%s/instance_manual_start_%u",
                                     g_binPath,
                                     g_resConf[i].cmInstanceId);
                securec_check_intval(ret, (void)ret);
                struct stat instStatBuf = {0};
                struct stat clusterStatBuf = {0};
                if (stat(instManualStartPath, &instStatBuf) == 0 || stat(g_cmManualStartPath, &clusterStatBuf) == 0) {
                    continue;
                }
                ProcessOfflineInstance(&g_resConf[i]);
                break;
            }
            case CM_RES_STAT_UNKNOWN:
                StopOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut);
                break;
            case CM_RES_STAT_COUNT:
            default :
                write_runlog(ERROR, "Unknown status.\n");
                break ;
        }
    }
}

void StopResourceCheck(void)
{
    for (uint32 i = 0; i < (uint32)g_resConf.size(); ++i) {
        char instReplace[MAX_PATH_LEN] = {0};
        char instManualStartPath[MAX_PATH_LEN] = {0};
        int ret = snprintf_s(instManualStartPath, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/instance_manual_start_%u",
                             g_binPath, g_resConf[i].cmInstanceId);
        securec_check_intval(ret, (void)ret);
        ret = snprintf_s(instReplace, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s/%s_%u",
                         g_binPath, CM_INSTANCE_REPLACE, g_resConf[i].cmInstanceId);
        securec_check_intval(ret, (void)ret);

        struct stat instStatBuf = {0};
        struct stat clusterStatBuf = {0};
        if (stat(instReplace, &instStatBuf) == 0) {
            write_runlog(LOG, "Resource instance is being replaced and can't be stopped.\n");
            continue;
        }
        if (stat(instManualStartPath, &instStatBuf) != 0 && stat(g_cmManualStartPath, &clusterStatBuf) != 0) {
            continue;
        }
        if (CheckOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut) ==
            CM_RES_STAT_ONLINE) {
            write_runlog(LOG, "Resource fast shutdown, res name: %s.\n", g_resConf[i].resName);
            StopOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut);
        }
    }
}

int ResourceStoppedCheck(void)
{
    for (uint32 i = 0; i < (uint32)g_resConf.size(); ++i) {
        if (CheckOneResInst(g_resConf[i].script, g_resConf[i].resInstanceId, g_resConf[i].checkInfo.timeOut) ==
            CM_RES_STAT_ONLINE) {
            write_runlog(LOG, "resource is running, script is %s\n", g_resConf[i].script);
            return PROCESS_RUNNING;
        }
    }
    return PROCESS_NOT_EXIST;
}
