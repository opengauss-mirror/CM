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
 * ctl_show.cpp
 *    cm_ctl show
 *
 * IDENTIFICATION
 *    src/cm_ctl/ctl_show.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm/libpq-fe.h"
#include "ctl_common.h"

#include <time.h>

#include "c.h"

#include "cm_elog.h"
#include "cm_rhb.h"
#include "cm_voting_disk.h"

extern CM_Conn* CmServer_conn;
// cm_ctl rhb print
int DoShowCommand()
{
    do_conn_cmserver(false, 0);
    if (CmServer_conn == NULL) {
        write_runlog(LOG, "show command, can't connect to cmserver.\n");
        return -1;
    }

    CmShowStatReq req = { 0 };
    req.msgType = (int)MSG_CTL_CM_RHB_STATUS_REQ;

    if (cm_client_send_msg(CmServer_conn, 'C', (char*)(&req), sizeof(CmShowStatReq)) != 0) {
        FINISH_CONNECTION_WITHOUT_EXIT();
        write_runlog(ERROR, "ctl send show rhb msg to cms failed.\n");
        (void)printf(_("ctl show send msg to cms failed.\n"));
        return 1;
    }

    GetExecCmdResult(NULL, (int)MSG_CTL_CM_RHB_STATUS_ACK);

    req.msgType = (int)MSG_CTL_CM_NODE_DISK_STATUS_REQ;
    if (cm_client_send_msg(CmServer_conn, 'C', (char*)(&req), sizeof(CmShowStatReq)) != 0) {
        FINISH_CONNECTION_WITHOUT_EXIT();
        write_runlog(ERROR, "ctl send show node disk msg to cms failed.\n");
        (void)printf(_("ctl send msg to cms failed.\n"));
        return 1;
    }

    GetExecCmdResult(NULL, (int)MSG_CTL_CM_NODE_DISK_STATUS_ACK);
    FINISH_CONNECTION_WITHOUT_EXIT();
    return 0;
}

void HandleRhbAck(CmRhbStatAck *ack)
{
    (void)printf("\n[  Network Connect State  ]\n\n");

    (void)printf("Network timeout:       %us\n", ack->timeout);

    struct tm result;
    GetLocalTime(&ack->baseTime, &result);
    const uint32 timeBufMaxLen = 128;
    char timeBuf[timeBufMaxLen] = {0};
    (void)strftime(timeBuf, timeBufMaxLen, "%Y-%m-%d %H:%M:%S", &result);
    (void)printf("Current CMServer time: %s\n", timeBuf);

    (void)printf("Network stat('Y' means connected, otherwise 'N'):\n");
    char *rs = GetRhbSimple((time_t *)ack->hbs, MAX_RHB_NUM, ack->hwl, ack->baseTime, ack->timeout);
    CM_RETURN_IF_NULL(rs);
    (void)printf("%s\n", rs);
    free(rs);
}

// |  Y  |  Y  |  Y  |  Y  |  Y  |
static char *GetNodeDiskSimple(time_t *ndHbs, uint32 hwl, time_t baseTime, uint32 timeout)
{
    const uint32 fixLen = 6;
    size_t bufLen = (fixLen + 1) * hwl + 2;
    char *buf = (char *)malloc(bufLen);
    if (buf == NULL) {
        write_runlog(ERROR, "can't alloc mem for node disk stats, needed:%u\n", (uint32)bufLen);
        return NULL;
    }
    error_t rc = memset_s(buf, bufLen, 0, bufLen);
    securec_check_errno(rc, (void)rc);

    buf[strlen(buf)] = '|';
    for (uint32 j = 0; j < hwl; j++) {
        const char *stat = IsRhbTimeout(ndHbs[j], baseTime, (int32)timeout) ? "  N  |" : "  Y  |";
        rc = strncat_s(buf, bufLen, stat, strlen(stat));
        securec_check_errno(rc, (void)rc);
    }
    PrintRhb(ndHbs, hwl, "NodeDisk");

    return buf;
}

void HandleNodeDiskAck(CmNodeDiskStatAck *ack)
{
    (void)printf("\n[  Node Disk HB State  ]\n\n");

    (void)printf("Node disk hb timeout:    %us\n", ack->timeout);

    struct tm result;
    GetLocalTime(&ack->baseTime, &result);
    const uint32 timeBufMaxLen = 128;
    char timeBuf[timeBufMaxLen] = {0};
    (void)strftime(timeBuf, timeBufMaxLen, "%Y-%m-%d %H:%M:%S", &result);
    (void)printf("Current CMServer time: %s\n", timeBuf);

    (void)printf("Node disk hb stat('Y' means connected, otherwise 'N'):\n");
    char *rs = GetNodeDiskSimple(ack->nodeDiskStats, ack->hwl, ack->baseTime, ack->timeout);
    (void)printf("%s\n", rs);
    free(rs);
}
