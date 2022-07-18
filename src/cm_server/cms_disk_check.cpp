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
 * cms_disk_check.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_server/cms_disk_check.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "cm_server.h"
#include "cms_alarm.h"
#include "cms_ddb.h"
#include "cms_global_params.h"
#include "cms_disk_check.h"
#include "cms_common.h"

/* Set database to read only mode */
static char* g_readOnlySetCmd = NULL;
static char* g_readOnlyCorSetCmd = NULL;
/* Set database to read write mode */
static char* g_readWriteSetCmd = NULL;
static char* g_readWriteCorSetCmd = NULL;

const int PRE_ALARM_OFF = 0;
const int PRE_ALARM_ON = 1;


/**
 * @brief 
 * 
 * @param  instanceId       My Param doc
 * @return true 
 * @return false 
 */
bool checkReadOnlyStatus(uint32 instanceId)
{
    uint32 i = 0;
    uint32 j = 0;

    write_runlog(DEBUG1, "[%s][line:%d] instanceId: %u\n", __FUNCTION__, __LINE__, instanceId);

    for (i = 0; i < g_node_num; i++) {
        if (instanceId == DATANODE_ALL) {
            if (g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastReadOnly == READONLY_ON) {
                return true;
            }
            for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
                if (g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastReadOnly == READONLY_ON) {
                    return true;
                }
            }
        } else if (IS_CN_INSTANCEID(instanceId)) {
            if (instanceId == g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId &&
                g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastReadOnly == READONLY_ON) {
                return true;
            }
        } else if (IS_DN_INSTANCEID(instanceId)) {
            for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
                if (instanceId == g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId &&
                    g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastReadOnly == READONLY_ON) {
                    return true;
                }
            }
        } else {
            write_runlog(LOG, "[%s][line:%d] instance is not cn or dn , instanceId: %u\n",
                __FUNCTION__, __LINE__, instanceId);
        }
    }
    return false;
}

/**
 * @brief Initalize dynamic datanode storge status
 * 
 */
void SaveNodeReadOnlyConfig(int logLevel)
{
    uint32 i;
    uint32 j;
    errno_t rc = 0;
    int rci = 0;
    char instanceName[CM_NODE_NAME] = {0};
    char bitsString[MAX_PATH_LEN] = {0};
    uint32 alarmIndex = 0;
    uint32 bitIndex = 0;

    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].coordinate == 1) {
            uint32 cnLastReadOnly = g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastReadOnly;
            uint32 cnCurrReadOnly = g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly;
            uint32 cnInstanceId = g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId;
            bitsString[bitIndex++] = ((cnLastReadOnly == READONLY_OFF) ? '0' : '1');
            write_runlog(DEBUG1,
                "[%s][line:%d] bitIndex = %u cnLastReadOnly:%u cnCurrReadOnly:%u, bitsString[bitIndex] = [%c].\n",
                __FUNCTION__, __LINE__, bitIndex, cnLastReadOnly, cnCurrReadOnly, bitsString[bitIndex - 1]);
            if (cnLastReadOnly != cnCurrReadOnly) {
                char command[CM_MAX_COMMAND_LEN] = {0};
                if (cnCurrReadOnly == READONLY_OFF) {
                    rci = snprintf_s(command,
                        CM_MAX_COMMAND_LEN,
                        CM_MAX_COMMAND_LEN - 1,
                        g_readWriteCorSetCmd,
                        g_dynamicNodeReadOnlyInfo[i].nodeName,
                        g_dynamicNodeReadOnlyInfo[i].coordinateNode.dataNodePath);
                } else if (cnCurrReadOnly == READONLY_ON) {
                    rci = snprintf_s(command,
                        CM_MAX_COMMAND_LEN,
                        CM_MAX_COMMAND_LEN - 1,
                        g_readOnlyCorSetCmd,
                        g_dynamicNodeReadOnlyInfo[i].nodeName,
                        g_dynamicNodeReadOnlyInfo[i].coordinateNode.dataNodePath);
                }
                securec_check_intval(rci, (void)rci);
                write_runlog(DEBUG1, "[%s][line:%d] CN command:[%s].\n", __FUNCTION__, __LINE__, command);

                rc = system(command);
                if (rc == 0) {
                    rc = snprintf_s(instanceName,
                        sizeof(instanceName),
                        sizeof(instanceName) - 1,
                        "%s_%u",
                        "cn",
                        cnInstanceId);
                    securec_check_intval(rc, (void)rc);
                    write_runlog(DEBUG1, "[%s][line:%d] CN instanceName:[%s].\n", __FUNCTION__, __LINE__, instanceName);

                    if (cnCurrReadOnly == READONLY_ON && cnLastReadOnly == READONLY_OFF) {
                        write_runlog(DEBUG1,
                            "[%s][line:%d] CN ALM_AT_Fault instanceName = %s, alarmIndex: %u.\n",
                            __FUNCTION__, __LINE__, instanceName, alarmIndex);
                        ReportReadOnlyAlarm(ALM_AT_Fault, instanceName, alarmIndex++);
                    } else if (cnCurrReadOnly == READONLY_OFF && cnLastReadOnly == READONLY_ON) {
                        write_runlog(DEBUG1,
                            "[%s][line:%d] CN ALM_AT_Resume instanceName = %s, alarmIndex: %u.\n",
                            __FUNCTION__, __LINE__, instanceName, alarmIndex);
                        ReportReadOnlyAlarm(ALM_AT_Resume, instanceName, alarmIndex++);
                    }
                    g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastReadOnly = cnCurrReadOnly;
                    g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly = READONLY_OFF;
                } else {
                    write_runlog(WARNING,
                        "[%s][line:%d] CN command:[%s] failed, rc=%d, errno=%d.\n",
                        __FUNCTION__, __LINE__, command, rc, errno);
                }
            } else {
                alarmIndex++;
            }
        }
        for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
            uint32 dnLastReadOnly = g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastReadOnly;
            uint32 dnCurrReadOnly = g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly;
            uint32 dnInstanceId = g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId;
            bitsString[bitIndex++] = dnLastReadOnly == READONLY_OFF ? '0' : '1';
            write_runlog(DEBUG1,
                "[%s][line:%d] bitIndex = %u dnLastReadOnly:%u, dnCurrReadOnly: %u,  bitsString[bitIndex] = [%c].\n",
                __FUNCTION__, __LINE__, bitIndex, dnLastReadOnly, dnCurrReadOnly, bitsString[bitIndex - 1]);
            if (dnLastReadOnly != dnCurrReadOnly) {
                char command[CM_MAX_COMMAND_LEN] = {0};
                if (dnCurrReadOnly == READONLY_OFF) {
                    rci = snprintf_s(command,
                        CM_MAX_COMMAND_LEN,
                        CM_MAX_COMMAND_LEN - 1,
                        g_readWriteSetCmd,
                        g_dynamicNodeReadOnlyInfo[i].nodeName,
                        g_dynamicNodeReadOnlyInfo[i].dataNode[j].dataNodePath);
                } else if (dnCurrReadOnly == READONLY_ON) {
                    rci = snprintf_s(command,
                        CM_MAX_COMMAND_LEN,
                        CM_MAX_COMMAND_LEN - 1,
                        g_readOnlySetCmd,
                        g_dynamicNodeReadOnlyInfo[i].nodeName,
                        g_dynamicNodeReadOnlyInfo[i].dataNode[j].dataNodePath);
                }
                securec_check_intval(rci, (void)rci);
                write_runlog(LOG, "[%s][line:%d] DN command:[%s].\n", __FUNCTION__, __LINE__, command);

                rc = system(command);
                if (rc == 0) {
                    rc = snprintf_s(instanceName, sizeof(instanceName), sizeof(instanceName) - 1,
                        "%s_%u", "dn", dnInstanceId);
                    securec_check_intval(rc, (void)rc);
                    write_runlog(DEBUG1, "[%s][line:%d] DN instanceName:[%s].\n", __FUNCTION__, __LINE__, instanceName);

                    if (dnCurrReadOnly == READONLY_ON && dnLastReadOnly == READONLY_OFF) {
                        write_runlog(DEBUG1,
                            "[%s][line:%d] DN ALM_AT_Fault instanceName = %s, alarmIndex: %u.\n",
                            __FUNCTION__, __LINE__, instanceName, alarmIndex);
                        ReportReadOnlyAlarm(ALM_AT_Fault, instanceName, alarmIndex++);
                    } else if (dnCurrReadOnly == READONLY_OFF && dnLastReadOnly == READONLY_ON) {
                        write_runlog(DEBUG1,
                            "[%s][line:%d] DN ALM_AT_Resume instanceName = %s, alarmIndex: %u.\n",
                            __FUNCTION__, __LINE__, instanceName, alarmIndex);
                        ReportReadOnlyAlarm(ALM_AT_Resume, instanceName, alarmIndex++);
                    }
                    g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastReadOnly = dnCurrReadOnly;
                    g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly = READONLY_OFF;
                } else {
                    write_runlog(WARNING,
                        "[%s][line:%d] DN command:[%s] failed, rc=%d, errno=%d.\n",
                        __FUNCTION__, __LINE__, command, rc, errno);
                }
            } else {
                alarmIndex++;
            }
        }
    }
    bool ddbResult = IsNeedSyncDdb();
    if (!ddbResult) {
        write_runlog(LOG, "[%s][line:%d] in single node cluster, don't save value to ddb.\n", __FUNCTION__, __LINE__);
    } else {
        /* One hex char is 4 bit, and the minimum bitindex is 4 */
        while (bitIndex % 4 != 0) {
            bitsString[bitIndex++] = '0';
        }
        bitsString[bitIndex] = '\0';
        if (strlen(bitsString) == 0) {
            write_runlog(logLevel, "bitsString len is 0.\n");
            return;
        }
        write_runlog(logLevel,
            "[%s][line:%d] bitsString = %s, bitIndex = %u alarmIndex: %u.\n",
            __FUNCTION__, __LINE__, bitsString, bitIndex, alarmIndex);
        status_t st = SetNodeReadOnlyStatusToDdb(bitsString, logLevel);
        if (st != CM_SUCCESS) {
            write_runlog(ERROR, "[%s][line:%d] ddb set failed.\n", __FUNCTION__, __LINE__);
        }
    }
}

/**
 * @brief Set the Node Instance Read Only Status object
 * 
 * @param  instanceType     My Param doc
 * @param  instanceId       My Param doc
 * @param  readonly         My Param doc
 */
void SetNodeInstanceReadOnlyStatus(int instanceType, uint32 instanceId, uint32 readonly)
{
    uint32 i;
    uint32 j;
    write_runlog(DEBUG1,
        "[%s][line:%d] instanceType:%d, instanceId:%u, readonly:%u \n",
        __FUNCTION__, __LINE__, instanceType, instanceId, readonly);
    for (i = 0; i < g_node_num; i++) {
        switch (instanceType) {
            case INSTANCE_TYPE_DATANODE:
                for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
                    if (instanceId == DATANODE_ALL) {
                        g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly = readonly;
                    } else if (instanceId == g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId) {
                        g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly = readonly;
                        write_runlog(DEBUG1,
                            "[%s][line:%d]  instanceId:%u currReadOnly:%u.\n",
                            __FUNCTION__, __LINE__,
                            g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId,
                            g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly);
                        return;
                    }
                }
                break;
            case INSTANCE_TYPE_COORDINATE:
                if (g_node[i].coordinate == 1) {
                    if (instanceId == DATANODE_ALL) {
                        g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly = readonly;
                    } else if (instanceId == g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId) {
                        g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly = readonly;
                        write_runlog(DEBUG1,
                            "[%s][line:%d]  instanceId:%u currReadOnly:%u.\n",
                            __FUNCTION__, __LINE__,
                            g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId,
                            g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly);
                        return;
                    }
                }
                break;
            default:
                write_runlog(ERROR,
                    "[%s][line:%d] Input invalid ! instanceType:%d!\n", __FUNCTION__, __LINE__, instanceType);
                break;
        }
    }
}

/**
 * @brief Initialization dynamic node ReadOnly status info.
 * 
 * @return int 
 */
int InitNodeReadonlyInfo()
{
    uint32 i = 0;
    uint32 j = 0;

    if (g_dynamicNodeReadOnlyInfo == NULL) {
        g_dynamicNodeReadOnlyInfo = (DynamicNodeReadOnlyInfo*)malloc(sizeof(DynamicNodeReadOnlyInfo) * CM_NODE_MAXNUM);
        if (g_dynamicNodeReadOnlyInfo == NULL) {
            write_runlog(ERROR, "[%s][line:%d] g_dynamicNodeReadOnlyInfo malloc failed!\n", __FUNCTION__, __LINE__);
            return OUT_OF_MEMORY;
        }
    }
    errno_t rcs = memset_s(g_dynamicNodeReadOnlyInfo, sizeof(DynamicNodeReadOnlyInfo) * CM_NODE_MAXNUM, 0,
        sizeof(DynamicNodeReadOnlyInfo) * CM_NODE_MAXNUM);
    securec_check_errno(rcs, FREE_AND_RESET(g_dynamicNodeReadOnlyInfo));

    for (i = 0; i < g_node_num; ++i) {
        rcs = strncpy_s(g_dynamicNodeReadOnlyInfo[i].nodeName, CM_NODE_NAME, g_node[i].nodeName, CM_NODE_NAME - 1);
        securec_check_errno(rcs, FREE_AND_RESET(g_dynamicNodeReadOnlyInfo));
        g_dynamicNodeReadOnlyInfo[i].currLogDiskPreAlarm = PRE_ALARM_OFF;
        g_dynamicNodeReadOnlyInfo[i].lastLogDiskPreAlarm = PRE_ALARM_OFF;
        g_dynamicNodeReadOnlyInfo[i].dataNodeCount = g_node[i].datanodeCount;
        g_dynamicNodeReadOnlyInfo[i].logDiskUsage = 0;
        for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId = g_node[i].datanode[j].datanodeId;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].currPreAlarm = PRE_ALARM_OFF;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastPreAlarm = PRE_ALARM_OFF;
            /* default off */
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].dataDiskUsage = 0;
            rcs = strncpy_s(g_dynamicNodeReadOnlyInfo[i].dataNode[j].dataNodePath, CM_PATH_LENGTH,
                g_node[i].datanode[j].datanodeLocalDataPath, CM_PATH_LENGTH - 1);
            securec_check_errno(rcs, FREE_AND_RESET(g_dynamicNodeReadOnlyInfo));
        }
        if (g_node[i].coordinate == 1) {
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId = g_node[i].coordinateId;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.currPreAlarm = PRE_ALARM_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastPreAlarm = PRE_ALARM_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.dataDiskUsage = 0;
            rcs = strncpy_s(g_dynamicNodeReadOnlyInfo[i].coordinateNode.dataNodePath, CM_PATH_LENGTH,
                g_node[i].DataPath, CM_PATH_LENGTH - 1);
            securec_check_errno(rcs, FREE_AND_RESET(g_dynamicNodeReadOnlyInfo));
        }
    }
    return 0;
}

/**
 * @brief set the readonly stgatus by instanceId, if it's DN, we will set each one in it's group
 * 
 * @param instanceId  which instance will be set
 */
static void SetNodeReadOnlyStatusByInstanceId(uint32 instanceId, int logLevel)
{
    for (uint32 i = 0; i < g_dynamic_header->relationCount; i++) {
        uint32 groupDnNum = (uint32)g_instance_role_group_ptr[i].count;
        cm_instance_role_group *curRoleGroup = &g_instance_role_group_ptr[i];

        if (curRoleGroup->instanceMember[0].instanceType == INSTANCE_TYPE_DATANODE) {
            write_runlog(logLevel, "[%s][line:%d] groupDnNum=%u, instanceId=%u\n",
                __FUNCTION__, __LINE__, groupDnNum, instanceId);
            bool existGroupMember = false;
            for (uint32 j = 0; j < groupDnNum; j++) {
                if (instanceId == curRoleGroup->instanceMember[j].instanceId) {
                    existGroupMember = true;
                    break;
                }
            }
            if (existGroupMember) {
                for (uint32 j = 0; j < groupDnNum; j++) {
                    uint32 dnInstanceId = curRoleGroup->instanceMember[j].instanceId;
                    SetNodeInstanceReadOnlyStatus(INSTANCE_TYPE_DATANODE, dnInstanceId, READONLY_ON);
                }
            }
        } else if (curRoleGroup->instanceMember[0].instanceType == INSTANCE_TYPE_COORDINATE) {
            for (uint32 j = 0; j < groupDnNum; j++) {
                uint32 cnInstanceId = curRoleGroup->instanceMember[j].instanceId;
                if (instanceId == cnInstanceId) {
                    SetNodeInstanceReadOnlyStatus(INSTANCE_TYPE_COORDINATE, cnInstanceId, READONLY_ON);
                }
            }
        }
    }
}

/**
 * @brief Check and set instance to readonly if it's usage exceed the threshold
 */
void CheckAndSetStorageThresholdReadOnlyAlarm(int logLevel)
{
    uint32 readonlyThreshold = (uint32)strtol(g_enableSetReadOnlyThreshold, NULL, 10);
    bool allHealth = true;

    write_runlog(logLevel, "[%s][line:%d] check storage read only start, threshold=%u \n",
        __FUNCTION__, __LINE__, readonlyThreshold);

    // init CN/DN cur readonly status
    SetNodeInstanceReadOnlyStatus(INSTANCE_TYPE_COORDINATE, DATANODE_ALL, READONLY_OFF);
    SetNodeInstanceReadOnlyStatus(INSTANCE_TYPE_DATANODE, DATANODE_ALL, READONLY_OFF);

    for (uint32 i = 0; i < g_node_num; i++) {
        DynamicNodeReadOnlyInfo *curNodeInfo = &g_dynamicNodeReadOnlyInfo[i];

        /* CN */
        if (g_node[i].coordinate == 1) {
            write_runlog(DEBUG1, "[%s][line:%d] instanceId=%u, usage=%u\n",
                __FUNCTION__, __LINE__, curNodeInfo->coordinateNode.instanceId,
                curNodeInfo->coordinateNode.dataDiskUsage);

            if (curNodeInfo->coordinateNode.dataDiskUsage >= readonlyThreshold) {
                isNeedCancel = true;
                write_runlog(logLevel, "[%s][line:%d] Set database to read only mode, instanceId=%u, usage=%u\n",
                    __FUNCTION__, __LINE__,
                    curNodeInfo->coordinateNode.instanceId, curNodeInfo->coordinateNode.dataDiskUsage);
                SetNodeReadOnlyStatusByInstanceId(curNodeInfo->coordinateNode.instanceId, logLevel);
                allHealth = false;
            }
        }

        /* DN */
        for (uint32 j = 0; j < curNodeInfo->dataNodeCount; j++) {
            DataNodeReadOnlyInfo *curDn = &curNodeInfo->dataNode[j];
            write_runlog(DEBUG1, "[%s][line:%d] instanceId=%u, usage=%u\n",
                __FUNCTION__, __LINE__, curDn->instanceId, curDn->dataDiskUsage);
            if (curDn->dataDiskUsage >= readonlyThreshold) {
                isNeedCancel = true;
                write_runlog(logLevel, "[%s][line:%d] Set database to read only mode, instanceId=%u, usage=%u\n",
                    __FUNCTION__, __LINE__, curDn->instanceId, curDn->dataDiskUsage);
                SetNodeReadOnlyStatusByInstanceId(curDn->instanceId, logLevel);
                allHealth = false;
            }
        }
    }

    if (allHealth) {
        isNeedCancel = false;
    }

    SaveNodeReadOnlyConfig(logLevel);
}

/**
 * @brief Set the Pre Alarm For Node Instance object
 * 
 * @param  instanceId       My Param doc
 */
void SetPreAlarmForNodeInstance(uint32 instanceId) 
{
    uint32 i = 0;
    uint32 j = 0;

    for (i = 0; i < g_node_num; i++) {
        if (instanceId == DATANODE_ALL) {
            if (g_node[i].coordinate == 1) {
                g_dynamicNodeReadOnlyInfo[i].coordinateNode.currPreAlarm = PRE_ALARM_OFF;
            }
            for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
                g_dynamicNodeReadOnlyInfo[i].dataNode[j].currPreAlarm = PRE_ALARM_OFF;
            }
        } else if (IS_DN_INSTANCEID(instanceId)) {
            for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
                if (instanceId == g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId) {
                    g_dynamicNodeReadOnlyInfo[i].dataNode[j].currPreAlarm = PRE_ALARM_ON;
                    return;
                }
            }
        } else if (IS_CN_INSTANCEID(instanceId)) {
            if (instanceId == g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId) {
                g_dynamicNodeReadOnlyInfo[i].coordinateNode.currPreAlarm = PRE_ALARM_ON;
                return;
            }
        } else {
            write_runlog(LOG, "[%s][line:%d] instanceId[%u] is invalid!\n", __FUNCTION__, __LINE__, instanceId);
        }
    }
    return;
}

/**
 * @brief 
 * 
 */
void ReportPreAlarmForNodeInstance()
{
    int rcs = 0;
    uint32 i = 0;
    uint32 j = 0;
    uint32 dnCount = 0;
    uint32 cnCount = 0;

    for (i = 0; i < g_node_num; i++) {
        char instanceName[INSTANCE_NAME_LENGTH] = {'\0'};
        if (g_node[i].coordinate == 1) {
            uint32 cnCurrPreAlarm = g_dynamicNodeReadOnlyInfo[i].coordinateNode.currPreAlarm;
            uint32 cnLastPreAlarm = g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastPreAlarm;
            rcs = snprintf_s(instanceName,
                INSTANCE_NAME_LENGTH,
                INSTANCE_NAME_LENGTH - 1,
                "cn_%d",
                g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId);
            securec_check_intval(rcs, (void)rcs);
            if (cnCurrPreAlarm == PRE_ALARM_ON && cnLastPreAlarm == PRE_ALARM_OFF) {
                ReportStorageThresholdPreAlarm(ALM_AT_Fault, instanceName, PRE_ALARM_CN, cnCount++);
            } else if (cnCurrPreAlarm == PRE_ALARM_OFF && cnLastPreAlarm == PRE_ALARM_ON) {
                ReportStorageThresholdPreAlarm(ALM_AT_Resume, instanceName, PRE_ALARM_CN, cnCount++);
            } else {
                cnCount++;
            }
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastPreAlarm = cnCurrPreAlarm;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.currPreAlarm = PRE_ALARM_OFF;
        }
        for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
            uint32 dnCurrPreAlarm = g_dynamicNodeReadOnlyInfo[i].dataNode[j].currPreAlarm;
            uint32 dnLastPreAlarm = g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastPreAlarm;
            rcs = snprintf_s(instanceName,
                INSTANCE_NAME_LENGTH,
                INSTANCE_NAME_LENGTH - 1,
                "dn_%d",
                g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId);
                securec_check_intval(rcs, (void)rcs);
            if (dnCurrPreAlarm == PRE_ALARM_ON && dnLastPreAlarm == PRE_ALARM_OFF) {
                ReportStorageThresholdPreAlarm(ALM_AT_Fault, instanceName, PRE_ALARM_DN, dnCount++);
            } else if (dnCurrPreAlarm == PRE_ALARM_OFF && dnLastPreAlarm == PRE_ALARM_ON) {
                ReportStorageThresholdPreAlarm(ALM_AT_Resume, instanceName, PRE_ALARM_DN, dnCount++);
            } else {
                dnCount++;
            }
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastPreAlarm = dnCurrPreAlarm;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].currPreAlarm = PRE_ALARM_OFF;
        }
    }
    return;
}

/**
 * @brief Set the Pre Alarm For Log Disk Instance object
 * 
 * @param  nodeName         My Param doc
 */
void SetPreAlarmForLogDiskInstance(const char* nodeName) 
{
    uint32 i = 0;

    for (i = 0; i < g_node_num; i++) {
        if (strcmp(nodeName, "ALL_LOGDISK") == 0) {
            g_dynamicNodeReadOnlyInfo[i].currLogDiskPreAlarm = PRE_ALARM_OFF;
        } else if (strcmp(nodeName, g_node[i].nodeName) == 0) {
            g_dynamicNodeReadOnlyInfo[i].currLogDiskPreAlarm = PRE_ALARM_ON;
            break;
        }
    }
    return;
}

/**
 * @brief 
 * 
 */
void RepostPreAlarmForLogDiskInstance()
{
    uint32 i = 0;
    int rcs = 0;
    char instanceName[INSTANCE_NAME_LENGTH] = {'\0'};
    for (i = 0; i < g_node_num; i++) {
        uint32 logDiskCurrPreAlarm = g_dynamicNodeReadOnlyInfo[i].currLogDiskPreAlarm;
        uint32 logDiskLastPreAlarm = g_dynamicNodeReadOnlyInfo[i].lastLogDiskPreAlarm;
        rcs = snprintf_s(instanceName,
            INSTANCE_NAME_LENGTH,
            INSTANCE_NAME_LENGTH - 1,
            "LogDisk on %s",
            g_dynamicNodeReadOnlyInfo[i].nodeName);
        securec_check_intval(rcs, (void)rcs);

        if (logDiskCurrPreAlarm == PRE_ALARM_ON && logDiskLastPreAlarm == PRE_ALARM_OFF) {
            ReportStorageThresholdPreAlarm(ALM_AT_Fault, instanceName, PRE_ALARM_LOG, i);
        } else if (logDiskCurrPreAlarm == PRE_ALARM_OFF && logDiskLastPreAlarm == PRE_ALARM_ON) {
            ReportStorageThresholdPreAlarm(ALM_AT_Resume, instanceName, PRE_ALARM_LOG, i);
        }
        g_dynamicNodeReadOnlyInfo[i].lastLogDiskPreAlarm = logDiskCurrPreAlarm;
        g_dynamicNodeReadOnlyInfo[i].currLogDiskPreAlarm = PRE_ALARM_OFF;
    }
}

/**
 * @brief Node usage check for preAlarm
 */
void PreAlarmForNodeThreshold(int logLevel)
{
    const uint32 preAlarmThreshhold = (uint32)(strtol(g_enableSetReadOnlyThreshold, NULL, 10) * 4 / 5);
    
    write_runlog(logLevel, "[%s][line:%d] [Disk usage] Starting check for disk alarm, preAlarmThreshhold=%u.\n",
        __FUNCTION__, __LINE__, preAlarmThreshhold);

    // init curAlarm
    SetPreAlarmForLogDiskInstance("ALL_LOGDISK");
    SetPreAlarmForNodeInstance(DATANODE_ALL);

    for (uint32 i = 0; i < g_node_num; i++) {
        DynamicNodeReadOnlyInfo *curNodeInfo = &g_dynamicNodeReadOnlyInfo[i];
        
        /* log usage */
        if (curNodeInfo->logDiskUsage >= preAlarmThreshhold) {
            write_runlog(logLevel, "[%s][line:%d] [logDisk usage] Alarm threshold reached, nodeName=%s, usage=%u.\n",
                __FUNCTION__, __LINE__, curNodeInfo->nodeName, curNodeInfo->logDiskUsage);
            SetPreAlarmForLogDiskInstance(curNodeInfo->nodeName);
        }

        /* CN */
        if (g_node[i].coordinate == 1) {
            if (curNodeInfo->coordinateNode.dataDiskUsage >= preAlarmThreshhold) {
                write_runlog(logLevel, 
                    "[%s][line:%d] [dataDisk usage] Alarm threshold reached, instanceId=%u, usage=%u\n",
                    __FUNCTION__, __LINE__,
                    curNodeInfo->coordinateNode.instanceId, curNodeInfo->coordinateNode.dataDiskUsage);
                SetPreAlarmForNodeInstance(curNodeInfo->coordinateNode.instanceId);
            }
        }

        /* DN */
        for (uint32 j = 0; j < curNodeInfo->dataNodeCount; j++) {
            DataNodeReadOnlyInfo *curDn = &curNodeInfo->dataNode[j];
            if (curDn->dataDiskUsage >= preAlarmThreshhold) {
                write_runlog(logLevel, 
                    "[%s][line:%d] [dataDisk usage] Alarm threshold reached, instanceId=%u, usage=%u\n",
                    __FUNCTION__, __LINE__, curDn->instanceId, curDn->dataDiskUsage);
                SetPreAlarmForNodeInstance(curDn->instanceId);
            }
        }
    }

    ReportPreAlarmForNodeInstance();
    RepostPreAlarmForLogDiskInstance();

    write_runlog(logLevel, "[%s][line:%d] [Disk usage] Check for disk alarm done.\n", __FUNCTION__, __LINE__);
}

/**
 * @brief cmserver check self disk condition
 */
static bool CmserverCheckDisc(const char* path)
{
    FILE* fd = NULL;
    char write_test_file[MAXPGPATH] = {0};
    int rc;
    char buf[2] = {0};

    Assert(path != NULL);
    rc = snprintf_s(
        write_test_file, sizeof(write_test_file), sizeof(write_test_file) - 1, "%s/cms_disc_readonly_test", path);
    securec_check_intval(rc, (void)rc);
    canonicalize_path(write_test_file);

    errno = 0;
    fd = fopen(write_test_file, "w");
    if (fd != NULL) {
        errno = 0;
        if (fwrite("1", sizeof("1"), 1, fd) != 1) {
            if (errno == EROFS || errno == EIO) {
                fclose(fd);
                write_runlog(LOG, "could not write disc test file: %m\n");
                (void)remove(write_test_file);
                return false;
            }
        }
        fclose(fd);
        fd = fopen(write_test_file, "r");
        if (fd != NULL) {
            if (fread(buf, sizeof("1"), 1, fd) != 1) {
                if (errno == ENOSPC) {
                    fclose(fd);
                    write_runlog(LOG, "could not read disc test file: %m\n");
                    (void)remove(write_test_file);
                    return false;
                }
            }
            fclose(fd);
        }
        (void)remove(write_test_file);
        return true;
    } else {
        int save_errno = errno;
        write_runlog(LOG, "could not open disc test file: %m\n");

        if (save_errno == EROFS || save_errno == EACCES || save_errno == ENOENT || save_errno == EIO) {
            return false;
        }

        return true;
    }
}


/**
 * @brief check storage threshold and set database mode
 * 
 * @param  arg              My Param doc
 * @return void* 
 */
void* StorageDetectMain(void* arg)
{
    /*
     * if modify status from read only to read write manual,
     * 12 hours later will continue check status,12 hours is system
     * recovery time;
     */
    int ret = InitNodeReadonlyInfo();
    int checkTimes = 0;
    int logLevel = DEBUG1;
    const uint32 default_check_interval = 10;
    if (ret != 0) {
        write_runlog(
            ERROR,
            "[%s][line:%d] initialize datanode readonly status failed! error:%d\n",
            __FUNCTION__, __LINE__, ret);
        return NULL;
    }

    g_readOnlySetCmd = "gs_guc reload -Z datanode -N %s -D %s -c 'default_transaction_read_only = on'";
    g_readWriteSetCmd = "gs_guc reload -Z datanode -N %s -D %s -c 'default_transaction_read_only = off'";

    g_readOnlyCorSetCmd = "gs_guc reload -Z coordinator -N %s -D %s -c 'default_transaction_read_only = on'";
    g_readWriteCorSetCmd = "gs_guc reload -Z coordinator -N %s -D %s -c 'default_transaction_read_only = off'";

    for (;;) {
        /* 
         * timer interval should between 1 second and 2592000 seconds(30 days),top 30 days,storage threshold should
         * between 1 and 99 percent
         * Report at level LOG per 1 minute
         */
        if (checkTimes % 6 == 0) {
            logLevel = LOG;
        } else {
            logLevel = DEBUG1;
        }
        if (datastorage_threshold_check_interval >= 1 && datastorage_threshold_check_interval <= 2592000 &&
            max_datastorage_threshold_check >= datastorage_threshold_check_interval &&
            max_datastorage_threshold_check <= 2592000 &&
            (strcasecmp(g_enableSetReadOnly, "on") == 0 || strcasecmp(g_enableSetReadOnly, "yes") == 0 ||
                strcasecmp(g_enableSetReadOnly, "true") == 0 || strcasecmp(g_enableSetReadOnly, "1") == 0) &&
            strtol(g_enableSetReadOnlyThreshold, NULL, 10) > 0 &&
            strtol(g_enableSetReadOnlyThreshold, NULL, 10) <= 99) {
            write_runlog(logLevel,
                "[%s][line:%d] "
                "Parameter values of data disk check,enable_transaction_read_only=%s,"
                "datastorage_readonly_set=%s,datastorage_readwrite_set=%s,"
                "datastorage_threshold_check_interval=%d"
                ",max_datastorage_threshold_check=%d,g_enableSetReadOnlyThreshold=%s.\n",
                __FUNCTION__, __LINE__,
                g_enableSetReadOnly,
                g_readOnlySetCmd,
                g_readWriteSetCmd,
                datastorage_threshold_check_interval,
                max_datastorage_threshold_check,
                g_enableSetReadOnlyThreshold);
        } else {
            write_runlog(logLevel,
                "[%s][line:%d] "
                "Gateway is off or command is empty or threshold is not between 1 and 99 or check interval is "
                "not between 1 and 2592000 or maximum check interval "
                "is not bigger than check interval or not less than 2592000 "
                "enable_transaction_read_only=%s,datastorage_readonly_set=%s"
                ",datastorage_readwrite_set=%s,"
                "datastorage_threshold_check_interval=%d,max_datastorage_threshold_check=%d"
                ",g_enableSetReadOnlyThreshold=%s.\n",
                __FUNCTION__, __LINE__,
                g_enableSetReadOnly,
                g_readOnlySetCmd,
                g_readWriteSetCmd,
                datastorage_threshold_check_interval,
                max_datastorage_threshold_check,
                g_enableSetReadOnlyThreshold);
            cm_sleep(default_check_interval);
            continue;
        }
        write_runlog(logLevel, "[%s][line:%d] role:[%d]\n", __FUNCTION__, __LINE__, g_HA_status->local_role);

        if (g_HA_status->local_role == CM_SERVER_PRIMARY) {
            /* Pre-alarm processing */
            PreAlarmForNodeThreshold(logLevel);
            /* Read-only overrun processing */
            CheckAndSetStorageThresholdReadOnlyAlarm(logLevel);
        }

        if (!CmserverCheckDisc(g_currentNode->cmDataPath)) {
            FreeNotifyMsg();
            exit(0);
        }
        checkTimes++;
        cm_sleep((uint32)datastorage_threshold_check_interval);
    }
}

int UpdateNodeReadonlyInfo(uint32 lastNodeNum)
{
    uint32 i = 0;
    uint32 j = 0;
    errno_t rcs = 0;
    for (i = lastNodeNum; i < g_node_num; ++i) {
        rcs = strncpy_s(g_dynamicNodeReadOnlyInfo[i].nodeName, CM_NODE_NAME, g_node[i].nodeName, CM_NODE_NAME - 1);
        if (rcs != EOK) {
            write_runlog(ERROR, "[reload] [UpdateNodeReadonlyInfo] copy nodeName failed.\n");
            return -1;
        }
        g_dynamicNodeReadOnlyInfo[i].currLogDiskPreAlarm = PRE_ALARM_OFF;
        g_dynamicNodeReadOnlyInfo[i].lastLogDiskPreAlarm = PRE_ALARM_OFF;
        g_dynamicNodeReadOnlyInfo[i].dataNodeCount = g_node[i].datanodeCount;
        g_dynamicNodeReadOnlyInfo[i].logDiskUsage = 0;
        for (j = 0; j < g_dynamicNodeReadOnlyInfo[i].dataNodeCount; j++) {
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].instanceId = g_node[i].datanode[j].datanodeId;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].currPreAlarm = PRE_ALARM_OFF;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastPreAlarm = PRE_ALARM_OFF;
            /* default off */
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].lastReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].currReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].dataNode[j].dataDiskUsage = 0;
            rcs = strncpy_s(g_dynamicNodeReadOnlyInfo[i].dataNode[j].dataNodePath, CM_PATH_LENGTH,
                g_node[i].datanode[j].datanodeLocalDataPath, CM_PATH_LENGTH - 1);
            if (rcs != EOK) {
                write_runlog(ERROR, "[reload] [UpdateNodeReadonlyInfo] copy dn_dataNodePath failed.\n");
                return -1;
            }
        }
        if (g_node[i].coordinate == 1) {
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.instanceId = g_node[i].coordinateId;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.currPreAlarm = PRE_ALARM_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastPreAlarm = PRE_ALARM_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.lastReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.currReadOnly = READONLY_OFF;
            g_dynamicNodeReadOnlyInfo[i].coordinateNode.dataDiskUsage = 0;
            rcs = strncpy_s(g_dynamicNodeReadOnlyInfo[i].coordinateNode.dataNodePath, CM_PATH_LENGTH,
                g_node[i].DataPath, CM_PATH_LENGTH - 1);
            if (rcs != EOK) {
                write_runlog(ERROR, "[reload] [UpdateNodeReadonlyInfo] copy cn_dataNodePath failed.\n");
                return -1;
            }
        }
    }
    return 0;
}