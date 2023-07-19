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
 * cm_cgroup.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_cgroup.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_CGROUP_H
#define CM_CGROUP_H
#ifdef ENABLE_MULTIPLE_NODES
#include <sys/types.h>
/* get the cm cgroup relpath and initialize cgroup.
 * Please note,caller should free the return value.
 */
extern char* gscgroup_cm_init();

/* make the current thread attach to cm cgroup */
extern void gscgroup_cm_attach_task(const char* relpath);
extern void gscgroup_cm_attach_task_pid(const char* relpath, pid_t tid);
#endif
#endif