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
 * cms_msg_que.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_server/cms_msg_que.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_MSG_QUE_H
#define CMS_MSG_QUE_H

#include "c.h"
#include "stringinfo.h"

enum MsgPriority {
    MsgPriHigh = 0,
    MsgPriNormal,
    MsgPriLow,
    MSG_PRI_COUNT
};

struct ConnID {
    int32 remoteType; // CM_AGENT,CM_CTL
    uint32 connSeq;
    uint32 agentNodeId;
};

struct MsgSendInfo {
    ConnID  connID;
    int32 log_level;
    uint64 procTime;
    uint8 msgProcFlag;
    char msgType;
    char procMethod;
    char reserved; // for alignment
    uint32 dataSize;
    uint64 data[0];
};

struct MsgRecvInfo {
    ConnID  connID;
    uint8 msgProcFlag;
    uint8 reserved1;
    uint8 reserved2;
    uint8 reserved3;
    CM_StringInfoData   msg;
    uint64 data[0];
};


typedef void (*wakeSenderFuncType)(void);
typedef bool (*CanProcThisMsgFunType)(void *threadInfo, const char *msgData);

void pushRecvMsg(const MsgRecvInfo* msg, MsgPriority pri);
const MsgRecvInfo *getRecvMsg(MsgPriority pri, uint32 waitTime, void *threadInfo);
void pushSendMsg(const MsgSendInfo *msg, MsgPriority pri);
const MsgSendInfo *getSendMsg();
size_t getSendMsgCount();
bool existSendMsg();
void setWakeSenderFunc(wakeSenderFuncType func);
void SetCanProcThisMsgFun(CanProcThisMsgFunType func);

#endif