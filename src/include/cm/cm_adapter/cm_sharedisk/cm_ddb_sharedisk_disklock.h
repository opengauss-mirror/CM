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
 * cm_ddb_sharedisk_disklock.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_adapter/cm_sharedisk/cm_ddb_sharedisk_disklock.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_DISKLOCK_H
#define CM_DISKLOCK_H

#include "c.h"
typedef unsigned char uchar;
int cm_alloc_disklock(uint64 lock_addr, int64 inst_id);
int cm_init_disklock(uint64 lock_addr, int64 inst_id);
int cm_lock_disklock(const char *scsi_dev);
int cm_unlock_disklock(const char *scsi_dev);
int cm_lockf_disklock(const char *scsi_dev, int64 lock_time);
void cm_destroy_disklock();

#endif
