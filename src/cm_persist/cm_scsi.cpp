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
 * cm_scsi.cpp
 *
 * IDENTIFICATION
 *    src/cm_persist/cm_scsi.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm_scsi.h"

#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <stdio.h>
#include "securec.h"

const uint32 MOV_BIT = 32;

#define IS_BIG_ENDIAN (*(uint32 *)"\x01\x02\x03\x04" == (uint32)0x01020304)

static uint64 Htonll(uint64 val)
{
    if (!IS_BIG_ENDIAN) {
        return (((uint64)htonl((uint32)((val << MOV_BIT) >> MOV_BIT))) << MOV_BIT) |
               (uint32)htonl((uint32)(val >> MOV_BIT));
    } else {
        return val;
    }
}

static void CmSetXferData(struct sg_io_hdr *pHdr, void *data, uint32 length)
{
    if (pHdr) {
        pHdr->dxferp = data;
        pHdr->dxfer_len = length;
    }
}

static void Cmsetsensedata(struct sg_io_hdr *pHdr, uchar *data, uint32 length)
{
    if (pHdr) {
        pHdr->sbp = data;
        pHdr->mx_sb_len = length;
    }
}

// get scsi result category
int32 CmGetScsiResult(const sg_io_hdr_t *pHdr)
{
    // errors from software driver
    int32 driverStatus = pHdr->driver_status & CM_DRIVER_MASK;
    // scsi status
    int32 status = pHdr->status & 0x7e;
    // errors from host adapte
    int32 hostStatus = pHdr->host_status;

    if (hostStatus) {
        return CM_SCSI_RESULT_TRANSPORT_ERR;
    } else if (driverStatus && (driverStatus != CM_DRIVER_SENSE)) {
        return CM_SCSI_RESULT_TRANSPORT_ERR;
    } else if (driverStatus == CM_DRIVER_SENSE || status == SAM_COMMAND_TERMINATED || status == SAM_CHECK_CONDITION) {
        return CM_SCSI_RESULT_SENSE;
    } else if (status) {
        return CM_SCSI_RESULT_STATUS;
    } else {
        return CM_SCSI_RESULT_GOOD;
    }
}

void CmSetSshValue(scsi_sense_hdr_t *ssh, const uchar *sbp, int sbpLen)
{
    const int sbpFalgPos = 7;
    const int sbpAscPos = 12;
    const int sbpAscqPos = 13;
    const int sbpOffset1 = 1;
    const int sbpOffset2 = 2;
    const int sbpOffset3 = 3;
    const int sbpOffset4 = 8;

    if (ssh->responseCode >= 0x72) {
        if (sbpLen > 1) {
            ssh->senseKey = (0xf & sbp[sbpOffset1]);
        }
        if (sbpLen > sbpOffset2) {
            ssh->asc = sbp[sbpOffset2];
        }
        if (sbpLen > sbpOffset3) {
            ssh->ascq = sbp[sbpOffset3];
        }
        if (sbpLen > sbpFalgPos) {
            ssh->addLength = sbp[sbpFalgPos];
        }
    } else {
        if (sbpLen > sbpOffset2) {
            ssh->senseKey = (0xf & sbp[sbpOffset2]);
        }
        if (sbpLen > sbpFalgPos) {
            sbpLen = (sbpLen < (sbp[sbpFalgPos] + sbpOffset4)) ? sbpLen : (sbp[sbpFalgPos] + sbpOffset4);
            if (sbpLen > sbpAscPos) {
                ssh->asc = sbp[sbpAscPos];
            }
            if (sbpLen > sbpAscqPos) {
                ssh->ascq = sbp[sbpAscqPos];
            }
        }
    }
    return;
}

// normalize scsi sense descriptor
bool CmGetScsiSenseDes(scsi_sense_hdr_t *ssh, const uchar *sbp, int32 sbpLen)
{
    errno_t errcode;
    uchar respCode;
    const int sbpOffset1 = 1;
    errcode = memset_s(ssh, sizeof(scsi_sense_hdr_t), 0, sizeof(scsi_sense_hdr_t));
    if (errcode != EOK) {
        return false;
    }
    if (sbp == NULL || sbpLen < sbpOffset1) {
        return false;
    }
    respCode = 0x7f & sbp[0];
    if (respCode < 0x70 || respCode > 0x73) {
        (void)printf(_("Invalid response code %hhu"), respCode);
        return false;
    }
    ssh->responseCode = respCode;
    CmSetSshValue(ssh, sbp, sbpLen);
    return true;
}

// scsi3 vaai compare and write, just support 1 block now
int32 CmScsi3Caw(int32 fd, uint64 blockAddr, char *buff, uint32 buffLen)
{
    uchar cdb[CDB_INIT_LEN] = {0};
    uint32 blocks = 1;
    uint32 xferLen = buffLen;
    uchar senseBuffer[CM_SCSI_SENSE_LEN] = {0};
    sg_io_hdr_t hdr;
    const int cdbOffset = 2;

    errno_t errcode = memset_s(&hdr, sizeof(sg_io_hdr_t), 0, sizeof(sg_io_hdr_t));
    if (errcode != EOK) {
        return (int)CM_ERROR;
    }
    hdr.interface_id = 'S';
    hdr.flags = SG_FLAG_LUN_INHIBIT;

    CmSetXferData(&hdr, buff, xferLen);
    Cmsetsensedata(&hdr, senseBuffer, CM_SCSI_SENSE_LEN);

    cdb[0] = 0x89;
    uint64 tmp = Htonll(blockAddr);
    errcode = memcpy_s(cdb + cdbOffset, sizeof(uint64), &tmp, sizeof(uint64));
    if (errcode != EOK) {
        return (int)CM_ERROR;
    }
    cdb[CDB_POS] = (unsigned char)(blocks & 0xff);

    hdr.dxfer_direction = SG_DXFER_TO_DEV;
    hdr.cmdp = cdb;
    hdr.cmd_len = CDB_INIT_LEN;
    hdr.timeout = CM_TIMEOUT_DEFAULT;
    int32 status = ioctl(fd, SG_IO, &hdr);
    if (status < 0) {
        (void)printf(_("Sending SCSI caw command failed, status %d, errno %d.\n"), status, errno);
        return (int)CM_ERROR;
    }

    // Test pHdr->driver_status for UltraPath, in case of error the pHdr->status will be zero.
    // byte count actually written to sbp
    int32 responseLen = hdr.sb_len_wr;
    int32 result;

    result = CmGetScsiResult(&hdr);
    if (result != CM_SCSI_RESULT_GOOD) {
        return CmScsi3CawResInfo(result, responseLen, senseBuffer, hdr);
    }

    return (int)CM_SUCCESS;
}

int32 CmScsi3CawErrorInfo(const sg_io_hdr_t hdr, int32 responseLen, const uchar *senseBuffer, const int offset)
{
    scsi_sense_hdr_t ssh;
    if (responseLen > offset && CmGetScsiSenseDes(&ssh, senseBuffer, responseLen)) {
        if (ssh.senseKey == CM_SPC_SK_MISCOMPARE) {
            return CM_SCSI_ERR_MISCOMPARE;
        } else {
            (void)printf(_("SCSI caw failed, response len %d, sense key %hhu, asc %hhu, ascq %hhu."),
                responseLen, ssh.senseKey, ssh.asc, ssh.ascq);
            return (int32)CM_ERROR;
        }
    } else {
        (void)printf(_("Get scsi sense keys failed, response len %d, driver status %hu, status %hhu,host status %hu, "
            "sb len wr %hhu"),
            responseLen,
            hdr.driver_status,
            hdr.status,
            hdr.host_status,
            hdr.sb_len_wr);
        return (int32)CM_ERROR;
    }
}

int32 CmScsi3CawResInfo(int32 result, int32 responseLen, const uchar *senseBuffer, const sg_io_hdr_t hdr)
{
    const int cdbOffset = 2;

    if (result == CM_SCSI_RESULT_SENSE) {
        return CmScsi3CawErrorInfo(hdr, responseLen, senseBuffer, cdbOffset);
    } else if (result == CM_SCSI_RESULT_TRANSPORT_ERR) {
        return CmScsi3CawErrorInfo(hdr, responseLen, senseBuffer, 0);
    } else {
        (void)printf(_("Get scsi sense keys failed, scsi result %d, driver status %hu, status %hhu, host status %hu, "
            "sb len wr %hhu."),
            result,
            hdr.driver_status,
            hdr.status,
            hdr.host_status,
            hdr.sb_len_wr);
        return (int32)CM_ERROR;
    }
}