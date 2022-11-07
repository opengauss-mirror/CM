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
 * cm_scsi.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_persist/cm_scsi.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_SCSI_H
#define CM_SCSI_H
#include <stddef.h>
#include <scsi/sg.h>
#define _(x) x

#define CM_SCSI_SENSE_LEN (64)
#define CM_SCSI_XFER_DATA (512)
#define CM_SCSI_TIMEOUT (60)  // secs
#define CM_DEF_BLOCK_SIZE (512)
#define CM_MAX_VENDOR_LEN (64)
#define CM_MAX_WWN_LEN (64)
#define CM_MAX_PRODUCT_LEN (30)
#define CM_MAX_ARRAY_SN_LEN (64)
#define CM_MAX_LUNID_LEN (11)
#define CM_HW_ARRAY_SN_LEN (21)

#define SAM_CHECK_CONDITION (0x02)
#define SAM_RESERVATION_CONFLICT (0x18)
#define SAM_COMMAND_TERMINATED (0x22)

#define CM_SPC_SK_MISCOMPARE \
    (0xe)  // the sense key indicates that the source data did not match the data read from the medium

#define CM_DRIVER_MASK (0x0f)
#define CM_DRIVER_SENSE (0x08)

#define CM_SCSI_RESULT_GOOD (0)
#define CM_SCSI_RESULT_STATUS (1)  // other than GOOD and CHECK CONDITION
#define CM_SCSI_RESULT_SENSE (2)
#define CM_SCSI_RESULT_TRANSPORT_ERR (3)

#define CM_SCSI_ERR_MISCOMPARE (-2)
#define CM_SCSI_ERR_CONFLICT (-2)

#define CM_MILLS_TIMES (1000)
#define CM_TIMEOUT_DEFAULT (10)

#define CDB_INIT_LEN (16)
#define CDB_POS (13)


#ifndef FREE_AND_RESET
#define FREE_AND_RESET(ptr)  \
    do {                     \
        if (NULL != (ptr)) { \
            free(ptr);       \
            (ptr) = NULL;    \
        }                    \
    } while (0)
#endif

typedef unsigned char uchar;
typedef signed int int32;
typedef unsigned long int uint64;
typedef long int int64;
typedef unsigned int uint32;
typedef enum en_status {
    CM_ERROR = -1,
    CM_SUCCESS = 0
} status_t;

typedef struct st_vendor_info {
    char vendor[CM_MAX_VENDOR_LEN];
    char product[CM_MAX_PRODUCT_LEN];
} vendor_info_t;

typedef struct st_array_info {
    char arraySn[CM_MAX_ARRAY_SN_LEN];
} array_info_t;

typedef struct st_lun_info {
    char lunWwn[CM_MAX_WWN_LEN];
    int32 lunId;
} lun_info_t;

typedef struct st_inquiry_data {
    vendor_info_t vendorInfo;
    array_info_t arrayInfo;
    lun_info_t lunInfo;
} inquiry_data_t;

// SCSI sense header
typedef struct st_scsi_sense_hdr {
    uchar responseCode;
    uchar senseKey;
    uchar asc;
    uchar ascq;
    uchar res4;
    uchar res5;
    uchar res6;
    uchar addLength;
} scsi_sense_hdr_t;

// scsi3 register/reserve/release/clear/preempt
// return : CM_TIMEDOUT/GS_SUCCESS/GS_ERROR/CM_SCSI_ERR_MISCOMPARE
int32 CmScsi3Caw(int32 fd, uint64 blockAddr, char *buff, uint32 buffLen);
int32 CmScsi3CawResInfo(int32 result, int32 responseLen, const uchar *senseBuffer, const sg_io_hdr_t hdr);

#endif
