/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
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
 * cma_ub_fence.cpp
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_ub_fence.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cma_ub_fence.h"

#ifdef ENABLE_XALARMD

#include "securec.h"
#include "cm_elog.h"
#include "cm_util.h"
#include "cma_global_params.h"
#include "cma_common.h"
#include "cma_instance_management.h"

static const uint32 XALARM_UB_FENCE_RELOAD_TIMEOUT_MS = 3000;
static const uint32 XALARM_UB_FENCE_CMD_DEFAULT_TIMEOUT_MS = 10000;
static const uint32 MILLISECONDS_PER_SECOND = 1000;
static const uint32 MICROSECONDS_PER_MILLISECOND = 1000;

static struct timeval UbFenceCmdTimeout(uint32 timeoutMs)
{
    struct timeval timeout = {0, 0};
    uint32 effectiveMs = (timeoutMs > 0) ? timeoutMs : XALARM_UB_FENCE_CMD_DEFAULT_TIMEOUT_MS;

    timeout.tv_sec = effectiveMs / MILLISECONDS_PER_SECOND;
    timeout.tv_usec = (effectiveMs % MILLISECONDS_PER_SECOND) * MICROSECONDS_PER_MILLISECOND;
    return timeout;
}

static bool ReloadGucOnLocalDn(uint32 timeoutMs)
{
    if (g_currentNode == NULL || g_currentNode->datanodeCount == 0) {
        write_runlog(ERROR, "ub fence: no local datanode to reload guc.\n");
        return false;
    }

    struct timeval timeout = UbFenceCmdTimeout(timeoutMs);
    bool allOk = true;

    for (uint32 i = 0; i < g_currentNode->datanodeCount; ++i) {
        const char *dataPath = g_currentNode->datanode[i].datanodeLocalDataPath;
        int running = check_one_instance_status(GetDnProcessName(), dataPath, NULL);
        if (running != PROCESS_RUNNING) {
            write_runlog(LOG, "ub fence: datanode not running, skip gs_guc reload, dataPath=%s.\n", dataPath);
            allOk = false;
            continue;
        }

        char cmd[MAXPGPATH] = {0};
        int rc = snprintf_s(cmd, MAXPGPATH, MAXPGPATH - 1,
            "gs_guc reload -Z datanode -D %s -c \"enable_ub_ha=off\"", dataPath);
        securec_check_intval(rc, (void)rc);

        write_runlog(LOG, "ub fence: execute gs_guc reload, dataPath=%s, cmd=%s.\n", dataPath, cmd);
        int ret = ExecuteCmd(cmd, timeout);
        if (ret != 0) {
            write_runlog(ERROR, "ub fence: gs_guc reload failed, ret=%d, dataPath=%s.\n", ret, dataPath);
            allOk = false;
        } else {
            write_runlog(LOG, "ub fence: gs_guc reload success, dataPath=%s.\n", dataPath);
        }
    }

    return allOk;
}

void TriggerLocalUbFenceOnXalarm(uint32 faultNodeId)
{
    if (g_currentNode == NULL) {
        return;
    }

    write_runlog(LOG,
        "xalarm triggers local gs_guc reload, localNodeId=%u, faultNodeId=%u, timeoutMs=%u.\n",
        g_currentNode->node, faultNodeId, XALARM_UB_FENCE_RELOAD_TIMEOUT_MS);

    bool reloadOk = ReloadGucOnLocalDn(XALARM_UB_FENCE_RELOAD_TIMEOUT_MS);
    if (!reloadOk) {
        write_runlog(ERROR,
            "xalarm gs_guc reload failed or timeout(%u ms), localNodeId=%u, faultNodeId=%u, continue fault flow.\n",
            XALARM_UB_FENCE_RELOAD_TIMEOUT_MS, g_currentNode->node, faultNodeId);
    }
}

#endif /* ENABLE_XALARMD */
