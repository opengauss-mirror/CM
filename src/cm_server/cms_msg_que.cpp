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
 * cms_msg_que.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_server/cms_msg_que.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <queue>
#include <pthread.h>
#include "elog.h"
#include "cm_c.h"
#include "cm_util.h"
#include "cms_msg_que.h"


using MsgQueType = std::deque<const char *>;
using MsgQuePtr = MsgQueType*;

struct PriMsgQues {
    MsgQuePtr Ques[MSG_PRI_COUNT];
    pthread_mutex_t msg_lock;
    pthread_cond_t msg_cond;
    CMPrioMutex prioLock;
};

static PriMsgQues g_recvMsgQues;
static PriMsgQues g_sendMsgQues;

static wakeSenderFuncType wakeSenderFunc = NULL;
static CanProcThisMsgFunType CanProcThisMsgFun = NULL;

static void InitMsgQue(PriMsgQues &que)
{
    for (int i = 0; i < (int)MSG_PRI_COUNT; i++) {
        que.Ques[i] = NULL;
    }

    (void)pthread_mutex_init(&que.msg_lock, NULL);
    (void)pthread_cond_init(&que.msg_cond, NULL);
    CMPrioMutexInit(que.prioLock);
}
void InitMsgQue()
{
    InitMsgQue(g_recvMsgQues);
    InitMsgQue(g_sendMsgQues);
}

void setWakeSenderFunc(wakeSenderFuncType func)
{
    wakeSenderFunc = func;
}

void SetCanProcThisMsgFun(CanProcThisMsgFunType func)
{
    CanProcThisMsgFun = func;
}
static void pushToQue(PriMsgQues *priQue, MsgPriority pri, const char *msg)
{
    if (priQue->Ques[pri] == NULL) {
        priQue->Ques[pri] = new MsgQueType;
        if (priQue->Ques[pri] == NULL) {
            write_runlog(ERROR, "pushToQue:out of memory.\n");
            return;
        }
    }
    priQue->Ques[pri]->push_back(msg);
}

void pushRecvMsg(const MsgRecvInfo *msg, MsgPriority pri)
{
    Assert(pri >= 0 && pri < MSG_PRI_COUNT);
    PriMsgQues *priQue = &g_recvMsgQues;

    (void)CMPrioMutexLock(priQue->prioLock, CMMutexPrio::CM_MUTEX_PRIO_HIGH);
    pushToQue(priQue, pri, (const char*)msg);
    CMPrioMutexUnLock(priQue->prioLock);

    (void)pthread_cond_broadcast(&priQue->msg_cond);
}

const MsgRecvInfo *getRecvMsg(MsgPriority pri, uint32 waitTime, void *threadInfo)
{
    Assert(pri >= 0 && pri < MSG_PRI_COUNT);
    PriMsgQues *priQue = &g_recvMsgQues;
    const MsgRecvInfo *msg = NULL;
    struct timespec tv;

    (void)CMPrioMutexLock(priQue->prioLock, CMMutexPrio::CM_MUTEX_PRIO_NORMAL);

    for (int i = 0; i < (int)pri + 1; i++) {
        MsgQuePtr que = priQue->Ques[i];
        if (que == NULL) {
            continue;
        }

        MsgQueType::iterator it = que->begin();
        for (; it != que->end(); ++it) {
            if (CanProcThisMsgFun == NULL || CanProcThisMsgFun(threadInfo, *it)) {
                msg = (const MsgRecvInfo *)*it;
                (void)que->erase(it);
                break;
            }
        }

        if (msg != NULL) {
            break;
        }
    }

    CMPrioMutexUnLock(priQue->prioLock);

    if (msg == NULL && waitTime > 0) {
        (void)clock_gettime(CLOCK_REALTIME, &tv);
        tv.tv_sec = tv.tv_sec + (long long)waitTime;
        (void)pthread_mutex_lock(&priQue->msg_lock);
        (void)pthread_cond_timedwait(&priQue->msg_cond, &priQue->msg_lock, &tv);
        (void)pthread_mutex_unlock(&priQue->msg_lock);
    }

    return msg;
}

void pushSendMsg(const MsgSendInfo *msg, MsgPriority pri)
{
    Assert(pri >= 0 && pri < MSG_PRI_COUNT);
    PriMsgQues *priQue = &g_sendMsgQues;
    
    (void)CMPrioMutexLock(priQue->prioLock, CMMutexPrio::CM_MUTEX_PRIO_NORMAL);
    pushToQue(priQue, pri, (const char*)msg);
    CMPrioMutexUnLock(priQue->prioLock);
    
    if (wakeSenderFunc != NULL) {
        wakeSenderFunc();
    }
}

const MsgSendInfo *getSendMsg()
{
    const MsgSendInfo *msg = NULL;
    PriMsgQues *priQue = &g_sendMsgQues;

    uint64 now = GetMonotonicTimeMs();
    (void)CMPrioMutexLock(priQue->prioLock, CMMutexPrio::CM_MUTEX_PRIO_HIGH);

    for (int i = 0; i < (int)MSG_PRI_COUNT; i++) {
        MsgQuePtr que = priQue->Ques[i];
        if (que == NULL) {
            continue;
        }

        MsgQueType::iterator it = que->begin();
        for (; it != que->end(); ++it) {
            const MsgSendInfo* sendMsg = (MsgSendInfo*)(*it);
            if (sendMsg->procTime == 0 || sendMsg->procTime <= now) {
                msg = (const MsgSendInfo *)(*it);
                (void)que->erase(it);
                break;
            }
        }

        if (msg != NULL) {
            break;
        }
    }

    CMPrioMutexUnLock(priQue->prioLock);

    return msg;
}
bool existSendMsg()
{
    PriMsgQues *priQue = &g_sendMsgQues;
    for (int i = 0; i < (int)MSG_PRI_COUNT; i++) {
        MsgQuePtr que = priQue->Ques[i];
        if (que != NULL) {
            if (!que->empty()) {
                return true;
            }
        }
    }

    return false;
}

size_t getSendMsgCount()
{
    PriMsgQues *priQue = &g_sendMsgQues;
    size_t count = 0;

    (void)CMPrioMutexLock(priQue->prioLock, CMMutexPrio::CM_MUTEX_PRIO_HIGH);
    for (int i = 0; i < (int)MSG_PRI_COUNT; i++) {
        MsgQuePtr que = priQue->Ques[i];
        if (que != NULL) {
            count += que->size();
        }
    }
    CMPrioMutexUnLock(priQue->prioLock);

    return count;
}
