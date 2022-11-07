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
 * cms_disk_check.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_server/cms_disk_check.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_DISK_CHECK_H
#define CMS_DISK_CHECK_H

#include "common/config/cm_config.h"

void* StorageDetectMain(void* arg);
bool CheckReadOnlyStatus(uint32 instanceId);
bool IsReadOnlyFinalState(uint32 instanceId, bool expectedState);
int UpdateNodeReadonlyInfo(uint32 lastNodeNum);

#endif