/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * cm_persist.cpp
 *
 * IDENTIFICATION
 *    src/cm_persist/cm_persist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include "share_disk_lock_api.h"

static const int ARGS_FIVE_NUM = 5;
static const int ARGS_SIX_NUM = 6;
static const int CMDLOCKTIME_NO = 5;
static const int CMDTYPE_NO = 4;
static const int OFFSET_NO = 3;
static const int INSTANCEID_NO = 2;
static const int DEVICE_NO = 1;
static const int DECIMAL_BASE = 10;


int ExeLockCmd(diskLrwHandler *handler)
{
    status_t ret = CmDiskLockS(&handler->headerLock, handler->scsiDev, handler->fd);
    if (ret == CM_SUCCESS) {
        return 0;
    }
    time_t lockTime = LOCKR_LOCK_TIME(handler->headerLock);
    if (lockTime <= 0) {
        return -1;
    }

    // system function execute lock cmd result range is [0, 127], 127 and 126 maybe system command failed result
    // so get lock time valid range is [1, 125]
    // 0:get lock success;-1:get lock failed and get lock time failed;[1,125]:get lock failed but get lock time success
    return (int)lockTime;
}
int ExeForceLockCmd(diskLrwHandler *handler, int64 lockTime)
{
    return (int)CmDiskLockf(&handler->headerLock, handler->fd, lockTime);
}

typedef enum en_persist_cmd_type {
    CMD_LOCK = 0,
    CMD_FORCE_LOCK = 1,
} PERSIST_CMD_TYPE;

static void usage()
{
    (void)printf(_("cm_persist: get disk lock for the shared storage.\n\n"));
    (void)printf(_("Usage:\n"));
    (void)printf(_("  cm_persist [DEVICEPATH] [INSTANCE_ID] [OFFSET] [CMD_TYPE] [LOCK_TIME]\n"));
    (void)printf(_("[DEVICEPATH]: the path of the shared storage\n"));
    (void)printf(_("[INSTANCE_ID]: the instanceid of the process\n"));
    (void)printf(_("[OFFSET]: get disk lock on storage position\n"));
    (void)printf(_("[CMD_TYPE]: cm_persist command type\n"));
    (void)printf(_("[LOCK_TIME]: lock time only used when CMD_TYPE is 1\n"));
    (void)printf(_("-?, --help            show this help, then exit\n"));
}

static status_t GetIntValue(char *input, int64 *value)
{
    char *p = input;
    while (*p != 0) {
        if (!isdigit(*p)) {
            (void)printf(_("Input value %s invalid:\n"), input);
            return CM_ERROR;
        }
        p++;
    }
    *value = (int64)strtol(input, NULL, DECIMAL_BASE);
    return CM_SUCCESS;
}

static int ExePersistCmd(diskLrwHandler *cmsArbitrateDiskHandler, int64 cmdType, int argc, char **argv)
{
    int ret = -1;
    switch (cmdType) {
        case CMD_LOCK:
            if (argc != ARGS_FIVE_NUM) {
                break;
            }
            ret = ExeLockCmd(cmsArbitrateDiskHandler);
            break;
        case CMD_FORCE_LOCK:
            if (argc != ARGS_SIX_NUM) {
                break;
            }
            int64 lockTime;
            if (GetIntValue(argv[CMDLOCKTIME_NO], &lockTime) != CM_SUCCESS) {
                break;
            }
            ret = ExeForceLockCmd(cmsArbitrateDiskHandler, lockTime);
            break;
        default:
            break;
    }

    return ret;
}

int main(int argc, char **argv)
{
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            return 0;
        }
    }

    if (argc < ARGS_FIVE_NUM) {
        (void)printf(_("the num(%d) of parameters input is invalid.\n\n"), argc);
        usage();
        return -1;
    }

    int64 instanceId;
    int64 offset;
    int64 cmdType;
    if (GetIntValue(argv[INSTANCEID_NO], &instanceId) != CM_SUCCESS) {
        return -1;
    }
    if (GetIntValue(argv[OFFSET_NO], &offset) != CM_SUCCESS) {
        return -1;
    }
    if (GetIntValue(argv[CMDTYPE_NO], &cmdType) != CM_SUCCESS) {
        return -1;
    }

    diskLrwHandler cmsArbitrateDiskHandler;
    if (InitDiskLockHandle(&cmsArbitrateDiskHandler, argv[DEVICE_NO], (uint32)offset, instanceId) != CM_SUCCESS) {
        return -1;
    }

    int ret = ExePersistCmd(&cmsArbitrateDiskHandler, cmdType, argc, argv);
    (void)close(cmsArbitrateDiskHandler.fd);
    FREE_AND_RESET(cmsArbitrateDiskHandler.headerLock.buff);
    if (ret != (int)CM_SUCCESS) {
        (void)printf(_("Failed to get disk lock.\n\n"));
        return ret;
    }

    (void)printf(_("Success to get disk lock.\n\n"));
    return ret;
}

