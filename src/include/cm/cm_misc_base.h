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
 * cm_misc_base.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_misc_base.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CM_MISC_API_H
#define CM_MISC_API_H

#include "utils/syscall_lock.h"
#include "cm/cm_elog.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_RES_NUM 16
#define HEARTBEAT_TIMEOUT 5

#define cm_close_socket    close

extern syscalllock g_cmEnvLock;

extern void cm_sleep(unsigned int sec);
extern void cm_usleep(unsigned int usec);

extern void check_input_for_security(const char *input);
extern void CheckEnvValue(const char *inputEnvValue);

extern int cm_getenv(
    const char *envVar, char *outputEnvValue, uint32 envValueLen, int elevel = -1);

extern int GetHomePath(char *outputEnvValue, uint32 envValueLen, int32 logLevel = DEBUG5);

#ifdef __cplusplus
}
#endif

bool IsSharedStorageMode();
bool IsBoolCmParamTrue(const char *param);

#endif // CM_MISC_API_H
