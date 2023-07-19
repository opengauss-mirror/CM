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
 * cma_mes.cpp
 *
 * IDENTIFICATION
 *    src/cm_agent/cma_mes.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cma_mes.h"

#include "mes.h"

#include "cm_config.h"
#include "cm_elog.h"
#include "cm_rhb.h"
#include "cm_cipher.h"
#include "cma_global_params.h"

#define AGENT_RHB_PORT_INC (2)
#define AGENT_RHB_MSG_BUFF_POOL_NUM (1)
#define AGENT_RHB_MSG_BUFF_QUEUE_NUM (8)
#define AGENT_RHB_BUFF_POOL_COUNT (10)
#define AGENT_RHB_BUFF_POOL_SIZE (1024)
#define AGENT_RHB_CHECK_SID (0)

static pthread_t g_rhbThread;
static const uint32 PASSWD_MAX_LEN = 64;

typedef struct RhbCtx_ {
    uint32 sid;
    uint32 instId;  // mes use index as id
    uint32 instCount;
    uint32 hbWorkThreadCount;
    uint64 instMap;
    staticNodeConfig *nodeList[MAX_RHB_NUM];  // node list to be check, it's idx as it's instId
    mes_addr_t instAddrs[MES_MAX_IP_LEN];
} RhbCtx;

static uint32 FindMinServerPort()
{
    const uint32 defaultCmsPort = 5000;
    for (uint32 i = 0; i < g_node_num; ++i) {
        if (g_node[i].cmServerLevel == 1) {
            return g_node[i].port;
        }
    }
    return defaultCmsPort;
}

static void InitAgentAddrs(
    uint32 *instCount, mes_addr_t *instAddrs, staticNodeConfig **nodeList, uint32 *curInstId, uint64 *instMap)
{
    *instCount = 0;

    char buf[MAX_LOG_BUFF_LEN] = {0};
    const uint32 maxInfoLen = 80;
    char info[maxInfoLen] = {0};
    uint32 port = FindMinServerPort();
    for (uint32 i = 0; i < g_node_num; i++) {
        if (g_node[i].node == g_nodeHeader.node) {
            *curInstId = i;
        }

        if (g_node[i].datanodeCount == 0) {
            continue;
        }
        if ((*instCount) >= MAX_RHB_NUM) {
            write_runlog(ERROR, "[InitAgentAddrs] we supported res count less than %d", MAX_RHB_NUM);
            return;
        }

        (*instMap) ^= ((uint64)1 << (*instCount));
        nodeList[(*instCount)] = &g_node[i];

        int rc = strncpy_s(instAddrs[(*instCount)].ip,
            sizeof(char) * MES_MAX_IP_LEN,
            g_node[i].datanode[0].datanodeLocalHAIP[0],
            sizeof(char) * MES_MAX_IP_LEN - 1);
        securec_check_errno(rc, (void)rc);
        instAddrs[(*instCount)].port = (uint16)port + AGENT_RHB_PORT_INC;

        int rcs =
            snprintf_s(info, maxInfoLen, maxInfoLen - 1, " [%u-%u](%s)", i, g_node[i].node, instAddrs[(*instCount)].ip);
        securec_check_intval(rcs, (void)rcs);
        rcs = strncat_s(buf, MAX_LOG_BUFF_LEN, info, strlen(info));
        securec_check_errno(rcs, (void)rcs);
        (*instCount)++;
    }

    write_runlog(LOG, "[InitAgentAddrs], detail:%s\n", buf);
}

static void InitRhbCtxByStaticConfig(RhbCtx *ctx)
{
    InitAgentAddrs(&ctx->instCount, ctx->instAddrs, ctx->nodeList, &ctx->instId, &ctx->instMap);
    ctx->hbWorkThreadCount = ctx->instCount;
}

typedef enum RhbMsgCmd_ {
    RHB_MSG_BEGIN = 0,
    RHB_MSG_HB_BC = RHB_MSG_BEGIN,  // hb broadcast
    RHB_MSG_CEIL,
} RhbMsgCmd;

static void InitTaskCmdGroup(mes_profile_t *pf)
{
    pf->task_group[MES_TASK_GROUP_ZERO] = pf->work_thread_cnt;
    pf->task_group[MES_TASK_GROUP_ONE] = 0;
    pf->task_group[MES_TASK_GROUP_TWO] = 0;
    pf->task_group[MES_TASK_GROUP_THREE] = 0;

    for (uint8 i = (uint8)RHB_MSG_BEGIN; i < (uint8)RHB_MSG_CEIL; i++) {
        mes_set_command_task_group(i, MES_TASK_GROUP_ZERO);
    }
}

static void InitBuffPool(mes_profile_t *pf)
{
    pf->buffer_pool_attr.pool_count = AGENT_RHB_MSG_BUFF_POOL_NUM;
    pf->buffer_pool_attr.queue_count = AGENT_RHB_MSG_BUFF_QUEUE_NUM;
    pf->buffer_pool_attr.buf_attr[0].count = AGENT_RHB_BUFF_POOL_COUNT;
    pf->buffer_pool_attr.buf_attr[0].size = AGENT_RHB_BUFF_POOL_SIZE;
}

static void initPfile(mes_profile_t *pf, const RhbCtx *ctx)
{
    pf->inst_id = ctx->instId;
    pf->pipe_type = MES_TYPE_TCP;
    pf->conn_created_during_init = 1;
    pf->channel_cnt = 1;
    pf->work_thread_cnt = ctx->hbWorkThreadCount;

    pf->mes_elapsed_switch = 0;

    pf->inst_cnt = ctx->instCount;
    error_t rc = memcpy_s(
        pf->inst_net_addr, sizeof(mes_addr_t) * MES_MAX_INSTANCES, ctx->instAddrs, sizeof(mes_addr_t) * MAX_RHB_NUM);
    securec_check_errno(rc, (void)rc);

    InitBuffPool(pf);
    InitTaskCmdGroup(pf);
}

// it's from CBB cm_log.h
typedef enum CbbLogLevel_ {
    LEVEL_ERROR = 0,  // error conditions
    LEVEL_WARN,       // warning conditions
    LEVEL_INFO,       // informational messages
} CbbLogLevel;

typedef enum CbbLogType_ {
    LOG_RUN = 0,
    LOG_DEBUG,
    LOG_ALARM,
    LOG_AUDIT,
    LOG_OPER,
    LOG_MEC,
    LOG_TRACE,
    LOG_PROFILE,
    LOG_COUNT  // LOG COUNT
} CbbLogType;

static void LogCallBack(int logType, int logLevel, const char *codeFileName, unsigned int codeLineNum,
    const char *moduleName, const char *fmt, ...) __attribute__((format(printf, 6, 7)));

static void LogCallBack(int logType, int logLevel, const char *codeFileName, unsigned int codeLineNum,
    const char *moduleName, const char *fmt, ...)
{
    int loglvl;
    switch (logLevel) {
        case LEVEL_ERROR:
            loglvl = ERROR;
            break;
        case LEVEL_WARN:
            loglvl = WARNING;
            break;
        case LEVEL_INFO:
            loglvl = (logType == (int)LOG_DEBUG) ? DEBUG5 : LOG;
            break;
        default:
            loglvl = LOG;
            break;
    }

    char newFmt[MAX_LOG_BUFF_LEN] = {0};
    char pathSep;
#ifdef WIN32
    pathSep = '\\';
#else
    pathSep = '/';
#endif

    const char *lastFile = strrchr(codeFileName, pathSep);
    if (lastFile == NULL) {
        lastFile = "unknow";
    }
    int32 rcs =
        snprintf_s(newFmt, MAX_LOG_BUFF_LEN, MAX_LOG_BUFF_LEN - 1, "%s [%s:%u]\n", fmt, lastFile + 1, codeLineNum);
    securec_check_intval(rcs, (void)rcs);

    va_list ap;
    va_start(ap, fmt);
    WriteRunLogv(loglvl, newFmt, ap);
    va_end(ap);
}

typedef void (*CmMesMsgProc)(mes_message_t *mgs);

typedef struct ProcessorFunc_ {
    RhbMsgCmd cmd;
    CmMesMsgProc proc;
    uint8 isEnqueue;  // Whether to let the worker thread process
    const char *desc;
} ProcessorFunc;

typedef struct Hbs_ {
    unsigned int hwl;
    time_t hbs[MAX_RHB_NUM];
} Hbs;

static Hbs g_curNodeHb = {0};

void GetHbs(time_t *hbs, unsigned int *hwl)
{
    // concurrency lock?
    *hwl = g_curNodeHb.hwl;
    errno_t rc = memcpy_s(hbs, sizeof(time_t) * (*hwl), g_curNodeHb.hbs, sizeof(time_t) * (*hwl));
    securec_check_errno(rc, (void)rc);
}

void CmaHdlRhbReq(mes_message_t *msg)
{
    write_runlog(DEBUG1, "[RHB] receive a hb msg from inst[%hhu]!\n", msg->head->src_inst);
    if (msg->head->src_inst < g_curNodeHb.hwl) {
        g_curNodeHb.hbs[msg->head->src_inst] = time(NULL);
    }
}

void CmaHdlRhbAck(mes_message_t *msg)
{
    mes_notify_broadcast_msg_recv_and_release(msg);
}

static const ProcessorFunc g_processors[RHB_MSG_CEIL] = {
    {RHB_MSG_HB_BC, CmaHdlRhbReq, CM_FALSE, "handle cma rhb broadcast message"},
};

void MesMsgProc(uint32 workThread, mes_message_t *msg)
{
    mes_message_head_t *head = msg->head;
    if (head->cmd >= (uint8)RHB_MSG_CEIL) {
        write_runlog(ERROR, "unknow cmd(%hhu) from inst:[%hhu], size:[%hu]!\n", head->cmd, head->src_inst, head->size);
        return;
    }

    const ProcessorFunc *processor = &g_processors[head->cmd];

    processor->proc(msg);
    mes_release_message_buf(msg);
}

status_t CmaRhbInit(const RhbCtx *ctx)
{
    mes_profile_t pf = {0};
    initPfile(&pf, ctx);
    g_curNodeHb.hwl = ctx->instCount;

    // regist mes log func callback
    mes_init_log();
    mes_register_log_output(LogCallBack);

    mes_register_proc_func(MesMsgProc);

    // ssl decode func
    if (IsBoolCmParamTrue(g_enableMesSsl)) {
        CM_RETURN_ERR_IF_INTERR(mes_set_param("SSL_CA", g_sslOption.ssl_para.ca_file));
        CM_RETURN_ERR_IF_INTERR(mes_set_param("SSL_KEY", g_sslOption.ssl_para.key_file));
        CM_RETURN_ERR_IF_INTERR(mes_set_param("SSL_CERT", g_sslOption.ssl_para.cert_file));
        if (g_sslOption.ssl_para.crl_file != NULL) {
            CM_RETURN_ERR_IF_INTERR(mes_set_param("SSL_CRL", g_sslOption.ssl_para.cert_file));
        }

        char notifyTime[PASSWD_MAX_LEN] = {0};
        errno_t rc = snprintf_s(notifyTime, PASSWD_MAX_LEN, PASSWD_MAX_LEN - 1, "%u", g_sslOption.expire_time);
        securec_check_intval(rc, (void)rc);
        CM_RETURN_ERR_IF_INTERR(mes_set_param("SSL_CERT_NOTIFY_TIME", notifyTime));

        char plain[PASSWD_MAX_LEN + 1] = {0};
        CM_RETURN_IFERR(cm_verify_ssl_key_pwd(plain, PASSWD_MAX_LEN, CLIENT_CIPHER));
        int32 ret = mes_set_param("SSL_PWD_PLAINTEXT", plain);

        const int32 tryTime = 3;
        for (int32 i = 0; i < tryTime; ++i) {
            rc = memset_s(plain, PASSWD_MAX_LEN + 1, 0, PASSWD_MAX_LEN + 1);
            securec_check_errno(rc, (void)rc);
        }
        CM_RETURN_ERR_IF_INTERR(ret);
        write_runlog(LOG, "enable mes ssl.\n");
    } else {
        write_runlog(WARNING, "mes ssl not enable!.\n");
    }

    for (uint32 i = (uint32)RHB_MSG_BEGIN; i < (uint32)RHB_MSG_CEIL; i++) {
        mes_set_msg_enqueue((uint32)g_processors[i].cmd, (uint32)g_processors[i].isEnqueue);
    }

    status_t ret = (status_t)mes_init(&pf);
    if (ret != CM_SUCCESS) {
        write_runlog(ERROR, "mes init failed!.\n");
        return ret;
    }

    write_runlog(LOG, "RHB mes init success!\n");
    return CM_SUCCESS;
}

static void InitMsgHead(mes_message_head_t *head, const RhbCtx *ctx)
{
    MES_INIT_MESSAGE_HEAD(head, RHB_MSG_HB_BC, 0, ctx->instId, 0, ctx->sid, 0xFFFF);
    head->size = sizeof(mes_message_head_t);
}

static void checkMesSslCertExpire()
{
    write_runlog(LOG, "start check mes ssl cert expire time.\n");
    if (mes_chk_ssl_cert_expire() != 0) {
        write_runlog(ERROR, "check mes ssl cert expire time failed.\n");
        return;
    }

    write_runlog(LOG, "check mes ssl cert expire time done.\n");
}

void CmaRhbUnInit()
{
    g_exitFlag = true;
    write_runlog(LOG, "Got exit, set g_exitFlag to true and wait rhb thread exit!\n");
    (void)pthread_join(g_rhbThread, NULL);
}

void *CmaRhbMain(void *args)
{
    thread_name = "RHB";

    RhbCtx ctx = {0};
    ctx.sid = AGENT_RHB_CHECK_SID;
    InitRhbCtxByStaticConfig(&ctx);

    if (CmaRhbInit(&ctx) != CM_SUCCESS) {
        write_runlog(FATAL, "init cma heartbeat conn by mes failed, RHB check thread will exit.\n");
        exit(1);
    }

    // for ssl cleanup
    (void)atexit(CmaRhbUnInit);

    write_runlog(LOG, "RHB check is ready to work!\n");
    mes_message_head_t head = {0};
    InitMsgHead(&head, &ctx);
    uint64 succInsts = 0;

    uint64 bcInsts = ctx.instMap & (~((uint64)0x1 << (ctx.instId)));
    int itv = 0;
    struct timespec curTime = {0, 0};
    struct timespec lastTime = {0, 0};
    for (;;) {
        if (g_exitFlag) {
            write_runlog(LOG, "Get exit flag, RHB thread will exit!\n");
            break;
        }

        (void)clock_gettime(CLOCK_MONOTONIC, &curTime);
        if (IsBoolCmParamTrue(g_enableMesSsl) &&
            (curTime.tv_sec - lastTime.tv_sec) >= (time_t)g_sslCertExpireCheckInterval) {
            checkMesSslCertExpire();
            (void)clock_gettime(CLOCK_MONOTONIC, &lastTime);
        }

        write_runlog(DEBUG1, "RHB broadcast hb to all nodes.!\n");
        mes_broadcast(ctx.sid, bcInsts, &head, &succInsts);
        if (bcInsts != succInsts) {
            write_runlog(DEBUG1,
                "bc not all success, send idx:[%llu], success status:[%llu]!\n",
                (long long unsigned int)bcInsts,
                (long long unsigned int)succInsts);
        }

        const int printItv = 5;
        if (itv++ % printItv == 0) {
            PrintRhb(g_curNodeHb.hbs, g_curNodeHb.hwl, "RHB");
        }

        CmSleep((int32)g_cmaRhbItvl);
    }

    write_runlog(LOG, "mes_uninit before exit!\n");
    mes_uninit();
    write_runlog(LOG, "RHB thread exit!\n");
    return NULL;
}

void CreateRhbCheckThreads()
{
    if (g_cmaRhbItvl == 0) {
        write_runlog(LOG, "agent_rhb_interval is 0, no need rhb.\n");
        return;
    }

    if (g_currentNode->datanodeCount == 0) {
        write_runlog(LOG, "current node has no datanode, no need rhb.\n");
        return;
    }

    int err;
    if ((err = pthread_create(&g_rhbThread, NULL, CmaRhbMain, NULL)) != 0) {
        write_runlog(ERROR, "Failed to create cma mes thread %d: %d\n", err, errno);
    } else {
        write_runlog(LOG, "start rhb check thread success.\n");
    }
}
