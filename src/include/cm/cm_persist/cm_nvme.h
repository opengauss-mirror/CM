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
 * cm_nvme.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_persist/cm_nvme.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_NVME_H
#define CM_NVME_H
#define _(x) x

typedef unsigned char uchar;
typedef unsigned char uint8;
typedef unsigned short uint16;
typedef signed int int32;
typedef unsigned long int uint64;
typedef long int int64;
typedef unsigned int uint32;

struct nvme_user_io {
    uint8    opcode;
    uint8    flags;
    uint16    control;
    uint16    nblocks;
    uint16    rsvd;
    uint64    metadata;
    uint64    addr;
    uint64    slba;
    uint32    dsmgmt;
    uint32    reftag;
    uint16    apptag;
    uint16    appmask;
};

enum {
    /*
     * Generic Command Status:
     */
    CM_NVME_SC_SUCCESS         = 0x0,
    CM_NVME_SC_INVALID_OPCODE      = 0x1,
    CM_NVME_SC_INVALID_FIELD       = 0x2,
    CM_NVME_SC_CMDID_CONFLICT      = 0x3,
    CM_NVME_SC_DATA_XFER_ERROR     = 0x4,
    CM_NVME_SC_POWER_LOSS      = 0x5,
    CM_NVME_SC_INTERNAL        = 0x6,
    CM_NVME_SC_ABORT_REQ       = 0x7,
    CM_NVME_SC_ABORT_QUEUE     = 0x8,
    CM_NVME_SC_FUSED_FAIL      = 0x9,
    CM_NVME_SC_FUSED_MISSING       = 0xa,
    CM_NVME_SC_INVALID_NS      = 0xb,
    CM_NVME_SC_CMD_SEQ_ERROR       = 0xc,
    CM_NVME_SC_SGL_INVALID_LAST    = 0xd,
    CM_NVME_SC_SGL_INVALID_COUNT   = 0xe,
    CM_NVME_SC_SGL_INVALID_DATA    = 0xf,
    CM_NVME_SC_SGL_INVALID_METADATA    = 0x10,
    CM_NVME_SC_SGL_INVALID_TYPE    = 0x11,
    CM_NVME_SC_CMB_INVALID_USE     = 0x12,
    CM_NVME_SC_PRP_INVALID_OFFSET  = 0x13,
    CM_NVME_SC_ATOMIC_WRITE_UNIT_EXCEEDED = 0x14,
    CM_NVME_SC_OPERATION_DENIED    = 0x15,
    CM_NVME_SC_SGL_INVALID_OFFSET  = 0x16,

    CM_NVME_SC_INCONSISTENT_HOST_ID = 0x18,
    CM_NVME_SC_KEEP_ALIVE_EXPIRED  = 0x19,
    CM_NVME_SC_KEEP_ALIVE_INVALID  = 0x1A,
    CM_NVME_SC_PREEMPT_ABORT       = 0x1B,
    CM_NVME_SC_SANITIZE_FAILED     = 0x1C,
    CM_NVME_SC_SANITIZE_IN_PROGRESS    = 0x1D,

    CM_NVME_SC_NS_WRITE_PROTECTED  = 0x20,
    CM_NVME_SC_CMD_INTERRUPTED     = 0x21,
    CM_NVME_SC_TRANSIENT_TRANSPORT = 0x22,

    CM_NVME_SC_LBA_RANGE       = 0x80,
    CM_NVME_SC_CAP_EXCEEDED        = 0x81,
    CM_NVME_SC_NS_NOT_READY        = 0x82,
    CM_NVME_SC_RESERVATION_CONFLICT    = 0x83,
    CM_NVME_SC_FORMAT_IN_PROGRESS  = 0x84,

    /*
     * Command Specific Status:
     */
    CM_NVME_SC_CQ_INVALID      = 0x100,
    CM_NVME_SC_QID_INVALID     = 0x101,
    CM_NVME_SC_QUEUE_SIZE      = 0x102,
    CM_NVME_SC_ABORT_LIMIT     = 0x103,
    CM_NVME_SC_ABORT_MISSING       = 0x104,
    CM_NVME_SC_ASYNC_LIMIT     = 0x105,
    CM_NVME_SC_FIRMWARE_SLOT       = 0x106,
    CM_NVME_SC_FIRMWARE_IMAGE      = 0x107,
    CM_NVME_SC_INVALID_VECTOR      = 0x108,
    CM_NVME_SC_INVALID_LOG_PAGE    = 0x109,
    CM_NVME_SC_INVALID_FORMAT      = 0x10a,
    CM_NVME_SC_FW_NEEDS_CONV_RESET = 0x10b,
    CM_NVME_SC_INVALID_QUEUE       = 0x10c,
    CM_NVME_SC_FEATURE_NOT_SAVEABLE    = 0x10d,
    CM_NVME_SC_FEATURE_NOT_CHANGEABLE  = 0x10e,
    CM_NVME_SC_FEATURE_NOT_PER_NS  = 0x10f,
    CM_NVME_SC_FW_NEEDS_SUBSYS_RESET   = 0x110,
    CM_NVME_SC_FW_NEEDS_RESET      = 0x111,
    CM_NVME_SC_FW_NEEDS_MAX_TIME   = 0x112,
    CM_NVME_SC_FW_ACTIVATE_PROHIBITED  = 0x113,
    CM_NVME_SC_OVERLAPPING_RANGE   = 0x114,
    CM_NVME_SC_NS_INSUFFICIENT_CAP = 0x115,
    CM_NVME_SC_NS_ID_UNAVAILABLE   = 0x116,
    CM_NVME_SC_NS_ALREADY_ATTACHED = 0x118,
    CM_NVME_SC_NS_IS_PRIVATE       = 0x119,
    CM_NVME_SC_NS_NOT_ATTACHED     = 0x11a,
    CM_NVME_SC_THIN_PROV_NOT_SUPP  = 0x11b,
    CM_NVME_SC_CTRL_LIST_INVALID   = 0x11c,
    CM_NVME_SC_DEVICE_SELF_TEST_IN_PROGRESS = 0x11d,
    CM_NVME_SC_BP_WRITE_PROHIBITED = 0x11e,
    CM_NVME_SC_INVALID_CTRL_ID     = 0x11f,
    CM_NVME_SC_INVALID_SECONDARY_CTRL_STATE = 0x120,
    CM_NVME_SC_INVALID_NUM_CTRL_RESOURCE   = 0x121,
    CM_NVME_SC_INVALID_RESOURCE_ID = 0x122,
    CM_NVME_SC_PMR_SAN_PROHIBITED  = 0x123,
    CM_NVME_SC_ANA_INVALID_GROUP_ID = 0x124,
    CM_NVME_SC_ANA_ATTACH_FAIL     = 0x125,

    /*
     * I/O Command Set Specific - NVM commands:
     */
    CM_NVME_SC_BAD_ATTRIBUTES      = 0x180,
    CM_NVME_SC_INVALID_PI      = 0x181,
    CM_NVME_SC_READ_ONLY       = 0x182,
    CM_NVME_SC_ONCS_NOT_SUPPORTED  = 0x183,

    /*
     * I/O Command Set Specific - Fabrics commands:
     */
    CM_NVME_SC_CONNECT_FORMAT      = 0x180,
    CM_NVME_SC_CONNECT_CTRL_BUSY   = 0x181,
    CM_NVME_SC_CONNECT_INVALID_PARAM   = 0x182,
    CM_NVME_SC_CONNECT_RESTART_DISC    = 0x183,
    CM_NVME_SC_CONNECT_INVALID_HOST    = 0x184,

    CM_NVME_SC_DISCOVERY_RESTART   = 0x190,
    CM_NVME_SC_AUTH_REQUIRED       = 0x191,

    /*
     * Media and Data Integrity Errors:
     */
    CM_NVME_SC_WRITE_FAULT     = 0x280,
    CM_NVME_SC_READ_ERROR      = 0x281,
    CM_NVME_SC_GUARD_CHECK     = 0x282,
    CM_NVME_SC_APPTAG_CHECK        = 0x283,
    CM_NVME_SC_REFTAG_CHECK        = 0x284,
    CM_NVME_SC_COMPARE_FAILED      = 0x285,
    CM_NVME_SC_ACCESS_DENIED       = 0x286,
    CM_NVME_SC_UNWRITTEN_BLOCK     = 0x287,

    /*
     * Path-related Errors:
     */
    CM_NVME_SC_ANA_PERSISTENT_LOSS = 0x301,
    CM_NVME_SC_ANA_INACCESSIBLE    = 0x302,
    CM_NVME_SC_ANA_TRANSITION      = 0x303,

    CM_NVME_SC_CRD         = 0x1800,
    CM_NVME_SC_DNR         = 0x4000,
};


#define nvme_cmd_write 0x01
#define nvme_cmd_compare 0x05

#define NVME_IOCTL_ID        _IO('N', 0x40)
#define NVME_IOCTL_SUBMIT_IO    _IOW('N', 0x42, struct nvme_user_io)

int32 CmNVMeCaw(int32 fd, uint64 blockAddr, char *buff, uint32 buffLen);
const char* CmNVMeStatusMessage(uint32 status);
#endif
