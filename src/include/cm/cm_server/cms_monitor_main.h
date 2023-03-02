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
 * cms_monitor_main.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_server/cms_monitor_main.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_MONITOR_MAIN_H
#define CMS_MONITOR_MAIN_H

extern void* CM_ThreadMonitorMain(void* argp);
extern void *CM_ThreadDdbStatusCheckAndSetMain(void *argp);
extern void* CM_ThreadMonitorNodeStopMain(void* argp);
void *CheckBlackList(void *arg);

#ifdef ENABLE_MULTIPLE_NODES
void *CheckGtmModMain(void *arg);
#endif

#endif