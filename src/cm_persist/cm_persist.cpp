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
#include "share_disk_lock_api.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>


static const int ARGS_NUM = 4;
static const int OFFSET_NO = 3;
static const int INSTANCEID_NO = 2;
static const int DEVICE_NO = 1;
static const int DECIMAL_BASE = 10;

static void usage()
{
    (void)printf(_("cm_persist: get disk lock for the shared storage.\n\n"));
    (void)printf(_("Usage:\n"));
    (void)printf(_("  cm_persist [DEVICEPATH] [INSTANCE_ID] [OFFSET]\n"));
    (void)printf(_("[DEVICEPATH]: the path of the shared storage\n"));
    (void)printf(_("[INSTANCE_ID]: the instanceid of the process\n"));
    (void)printf(_("[OFFSET]: get disk lock on storage position\n"));
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

int main(int argc, char **argv)
{
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            return 0;
        }
    }

    if (argc != ARGS_NUM) {
        (void)printf(_("the num(%d) of parameters input is invalid.\n\n"), argc);
        usage();
        return 1;
    }

    int64 instanceId;
    int64 offset;
    if (GetIntValue(argv[INSTANCEID_NO], &instanceId) != CM_SUCCESS) {
        return 1;
    }
    if (GetIntValue(argv[OFFSET_NO], &offset) != CM_SUCCESS) {
        return 1;
    }

    diskLrwHandler cmsArbitrateDiskHandler;
    if (InitDiskLockHandle(&cmsArbitrateDiskHandler, argv[DEVICE_NO], (uint32)offset, instanceId) != CM_SUCCESS) {
        return 1;
    }

    status_t ret = ShareDiskGetDlock(&cmsArbitrateDiskHandler);
    (void)close(cmsArbitrateDiskHandler.fd);
    FREE_AND_RESET(cmsArbitrateDiskHandler.headerLock.buff);
    if (ret != CM_SUCCESS) {
        (void)printf(_("Failed to get disk lock.\n\n"));
        return 1;
    }
    
    (void)printf(_("Success to get disk lock.\n\n"));
    return 0;
}

