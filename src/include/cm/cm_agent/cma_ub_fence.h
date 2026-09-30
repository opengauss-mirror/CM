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
 * cma_ub_fence.h
 *
 * IDENTIFICATION
 *    include/cm/cm_agent/cma_ub_fence.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMA_UB_FENCE_H
#define CMA_UB_FENCE_H

#include "cm_msg.h"

#ifdef ENABLE_XALARMD
void TriggerLocalUbFenceOnXalarm(uint32 faultNodeId);
#else
static inline void TriggerLocalUbFenceOnXalarm(uint32 faultNodeId)
{
    (void)faultNodeId;
}
#endif

#endif
