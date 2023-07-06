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
 * cm_nvme.cpp
 *
 * IDENTIFICATION
 *    src/cm_persist/cm_nvme.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm_nvme.h"

#include <sys/ioctl.h>
#include <stdio.h>

#define CM_DEF_BLOCK_SIZE (512)
#define CM_NVME_SC_SUCCESS 0x0
#define CM_NVME_ERR_MISCOMPARE (-2)

typedef enum en_status {
    CM_ERROR = -1,
    CM_SUCCESS = 0
} status_t;

// NVMe vaai compare and write, just support 1 block now
int32 CmNVMeCaw(int32 fd, uint64 blockAddr, char *buff, uint32 buffLen)
{
    int32 status;
    struct nvme_user_io io = {0};

    io.opcode     = nvme_cmd_compare;
    io.slba       = blockAddr;
    io.addr       = (uint64) buff;
    status =  ioctl(fd, NVME_IOCTL_SUBMIT_IO, &io);
    if (status == CM_NVME_SC_COMPARE_FAILED) {
		(void)printf(_("Sending NVMe compare command in caw failed, %s(%#x)."), CmNVMeStatusMessage(status), status);
        return CM_NVME_ERR_MISCOMPARE;
    } else if (status != CM_NVME_SC_SUCCESS) {
        (void)printf(_("Sending NVMe compare command in caw failed, %s(%#x)."), CmNVMeStatusMessage(status), status);
        return CM_ERROR;
    }

    io.opcode = nvme_cmd_write;
    io.addr = (uint64)(buff + CM_DEF_BLOCK_SIZE);
    status =  ioctl(fd, NVME_IOCTL_SUBMIT_IO, &io);
    if (status != CM_NVME_SC_SUCCESS) {
        (void)printf(_("Sending NVMe write command in caw failed, %s(%#x)."), CmNVMeStatusMessage(status), status);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

const char* CmNVMeStatusMessage(uint32 status)
{
    switch (status & 0x3ff) {
    case CM_NVME_SC_SUCCESS:
        return "SUCCESS: The command completed successfully";
    case CM_NVME_SC_INVALID_OPCODE:
        return "INVALID_OPCODE: The associated command opcode field is not valid";
    case CM_NVME_SC_INVALID_FIELD:
        return "INVALID_FIELD: A reserved coded value or an unsupported value in a defined field";
    case CM_NVME_SC_CMDID_CONFLICT:
        return "CMDID_CONFLICT: The command identifier is already in use";
    case CM_NVME_SC_DATA_XFER_ERROR:
        return "DATA_XFER_ERROR: Error while trying to transfer the data or metadata";
    case CM_NVME_SC_POWER_LOSS:
        return "POWER_LOSS: Command aborted due to power loss notification";
    case CM_NVME_SC_INTERNAL:
        return "INTERNAL: The command was not completed successfully due to an internal error";
    case CM_NVME_SC_ABORT_REQ:
        return "ABORT_REQ: The command was aborted due to a Command Abort request";
    case CM_NVME_SC_ABORT_QUEUE:
        return "ABORT_QUEUE: The command was aborted due to a Delete I/O Submission Queue request";
    case CM_NVME_SC_FUSED_FAIL:
        return "FUSED_FAIL: The command was aborted due to the other command in a fused operation failing";
    case CM_NVME_SC_FUSED_MISSING:
        return "FUSED_MISSING: The command was aborted due to a Missing Fused Command";
    case CM_NVME_SC_INVALID_NS:
        return "INVALID_NS: The namespace or the format of that namespace is invalid";
    case CM_NVME_SC_CMD_SEQ_ERROR:
        return "CMD_SEQ_ERROR: The command was aborted due to a protocol violation in a multicommand sequence";
    case CM_NVME_SC_SGL_INVALID_LAST:
        return "SGL_INVALID_LAST: The command includes an invalid SGL Last Segment or SGL Segment descriptor.";
    case CM_NVME_SC_SGL_INVALID_COUNT:
        return "SGL_INVALID_COUNT: There is an SGL Last Segment descriptor or an SGL Segment descriptor in a"
               " location other than the last descriptor of a segment based on the length indicated.";
    case CM_NVME_SC_SGL_INVALID_DATA:
        return "SGL_INVALID_DATA: This may occur if the length of a Data SGL is too short.";
    case CM_NVME_SC_SGL_INVALID_METADATA:
        return "SGL_INVALID_METADATA: This may occur if the length of a Metadata SGL is too short";
    case CM_NVME_SC_SGL_INVALID_TYPE:
        return "SGL_INVALID_TYPE: The type of an SGL Descriptor is a type that is not supported by the controller.";
    case CM_NVME_SC_CMB_INVALID_USE:
        return "CMB_INVALID_USE: The attempted use of the Controller Memory Buffer is not supported by the controller.";
    case CM_NVME_SC_PRP_INVALID_OFFSET:
        return "PRP_INVALID_OFFSET: The Offset field for a PRP entry is invalid.";
    case CM_NVME_SC_ATOMIC_WRITE_UNIT_EXCEEDED:
        return "ATOMIC_WRITE_UNIT_EXCEEDED: The length specified exceeds the atomic write unit size.";
    case CM_NVME_SC_OPERATION_DENIED:
        return "OPERATION_DENIED: The command was denied due to lack of access rights.";
    case CM_NVME_SC_SGL_INVALID_OFFSET:
        return "SGL_INVALID_OFFSET: The offset specified in a descriptor is invalid.";
    case CM_NVME_SC_INCONSISTENT_HOST_ID:
        return "INCONSISTENT_HOST_ID: The NVM subsystem detected the simultaneous use of 64-bit and 128-bit Host"
               " Identifier values on different controllers.";
    case CM_NVME_SC_KEEP_ALIVE_EXPIRED:
        return "KEEP_ALIVE_EXPIRED: The Keep Alive Timer expired.";
    case CM_NVME_SC_KEEP_ALIVE_INVALID:
        return "KEEP_ALIVE_INVALID: The Keep Alive Timeout value specified is invalid.";
    case CM_NVME_SC_PREEMPT_ABORT:
        return "PREEMPT_ABORT: The command was aborted due to a Reservation Acquire command with the Reservation"
               " Acquire Action (RACQA) set to 010b (Preempt and Abort).";
    case CM_NVME_SC_SANITIZE_FAILED:
        return "SANITIZE_FAILED: The most recent sanitize operation failed and no recovery actions has been"
               " successfully completed";
    case CM_NVME_SC_SANITIZE_IN_PROGRESS:
        return "SANITIZE_IN_PROGRESS: The requested function is prohibited while a sanitize operation is in progress";
    case CM_NVME_SC_LBA_RANGE:
        return "LBA_RANGE: The command references a LBA that exceeds the size of the namespace";
    case CM_NVME_SC_NS_WRITE_PROTECTED:
        return "NS_WRITE_PROTECTED: The command is prohibited while the namespace is write protected by the host.";
    case CM_NVME_SC_TRANSIENT_TRANSPORT:
        return "TRANSIENT_TRANSPORT: A transient transport error was detected.";
    case CM_NVME_SC_CAP_EXCEEDED:
        return "CAP_EXCEEDED: The execution of the command has caused the capacity of the namespace to be exceeded";
    case CM_NVME_SC_NS_NOT_READY:
        return "NS_NOT_READY: The namespace is not ready to be accessed as a result of a condition other than a"
               " condition that is reported as an Asymmetric Namespace Access condition";
    case CM_NVME_SC_RESERVATION_CONFLICT:
        return "RESERVATION_CONFLICT: The command was aborted due to a conflict with a reservation held on"
               " the accessed namespace";
    case CM_NVME_SC_FORMAT_IN_PROGRESS:
        return "FORMAT_IN_PROGRESS: A Format NVM command is in progress on the namespace.";
    case CM_NVME_SC_CQ_INVALID:
        return "CQ_INVALID: The Completion Queue identifier specified in the command does not exist";
    case CM_NVME_SC_QID_INVALID:
        return "QID_INVALID: The creation of the I/O Completion Queue failed due to an invalid queue identifier"
               " specified as part of the command. An invalid queue identifier is one that is currently in use"
               " or one that is outside the range supported by the controller";
    case CM_NVME_SC_QUEUE_SIZE:
        return "QUEUE_SIZE: The host attempted to create an I/O Completion Queue with an invalid number of entries";
    case CM_NVME_SC_ABORT_LIMIT:
        return "ABORT_LIMIT: The number of concurrently outstanding Abort commands has exceeded the limit indicated"
               " in the Identify Controller data structure";
    case CM_NVME_SC_ABORT_MISSING:
        return "ABORT_MISSING: The abort command is missing";
    case CM_NVME_SC_ASYNC_LIMIT:
        return "ASYNC_LIMIT: The number of concurrently outstanding Asynchronous Event Request commands"
               " has been exceeded";
    case CM_NVME_SC_FIRMWARE_SLOT:
        return "FIRMWARE_SLOT: The firmware slot indicated is invalid or read only. This error is indicated if the"
               " firmware slot exceeds the number supported";
    case CM_NVME_SC_FIRMWARE_IMAGE:
        return "FIRMWARE_IMAGE: The firmware image specified for activation is invalid and not loaded"
               " by the controller";
    case CM_NVME_SC_INVALID_VECTOR:
        return "INVALID_VECTOR: The creation of the I/O Completion Queue failed due to an invalid interrupt vector"
               " specified as part of the command";
    case CM_NVME_SC_INVALID_LOG_PAGE:
        return "INVALID_LOG_PAGE: The log page indicated is invalid. This error condition is also returned if a"
               " reserved log page is requested";
    case CM_NVME_SC_INVALID_FORMAT:
        return "INVALID_FORMAT: The LBA Format specified is not supported. This may be due to various conditions";
    case CM_NVME_SC_FW_NEEDS_CONV_RESET:
        return "FW_NEEDS_CONVENTIONAL_RESET: The firmware commit was successful, however, activation of the firmware"
               " image requires a conventional reset";
    case CM_NVME_SC_INVALID_QUEUE:
        return "INVALID_QUEUE: This error indicates that it is invalid to delete the I/O Completion Queue specified."
               " The typical reason for this error condition is that there is an associated I/O Submission Queue"
               " that has not been deleted.";
    case CM_NVME_SC_FEATURE_NOT_SAVEABLE:
        return "FEATURE_NOT_SAVEABLE: The Feature Identifier specified does not support a saveable value";
    case CM_NVME_SC_FEATURE_NOT_CHANGEABLE:
        return "FEATURE_NOT_CHANGEABLE: The Feature Identifier is not able to be changed";
    case CM_NVME_SC_FEATURE_NOT_PER_NS:
        return "FEATURE_NOT_PER_NS: The Feature Identifier specified is not namespace specific. The Feature Identifier"
               " settings apply across all namespaces";
    case CM_NVME_SC_FW_NEEDS_SUBSYS_RESET:
        return "FW_NEEDS_SUBSYSTEM_RESET: The firmware commit was successful, however, activation of the firmware image"
               " requires an NVM Subsystem";
    case CM_NVME_SC_FW_NEEDS_RESET:
        return "FW_NEEDS_RESET: The firmware commit was successful; however, the image specified does not support being"
               " activated without a reset";
    case CM_NVME_SC_FW_NEEDS_MAX_TIME:
        return "FW_NEEDS_MAX_TIME_VIOLATION: The image specified if activated immediately would exceed"
               " the Maximum Time for Firmware Activation (MTFA) value reported in Identify Controller."
               " To activate the firmware, the Firmware Commit command needs to be re-issued"
               " and the image activated using a reset";
    case CM_NVME_SC_FW_ACTIVATE_PROHIBITED:
        return "FW_ACTIVATION_PROHIBITED: The image specified is being prohibited from activation by the controller"
               " for vendor specific reasons";
    case CM_NVME_SC_OVERLAPPING_RANGE:
        return "OVERLAPPING_RANGE: This error is indicated if the firmware image has overlapping ranges";
    case CM_NVME_SC_NS_INSUFFICIENT_CAP:
        return "NS_INSUFFICIENT_CAPACITY: Creating the namespace requires more free space than is currently available."
               " The Command Specific Information field of the Error Information Log specifies"
               " the total amount of NVM capacity required to create the namespace in bytes";
    case CM_NVME_SC_NS_ID_UNAVAILABLE:
        return "NS_ID_UNAVAILABLE: The number of namespaces supported has been exceeded";
    case CM_NVME_SC_NS_ALREADY_ATTACHED:
        return "NS_ALREADY_ATTACHED: The controller is already attached to the namespace specified";
    case CM_NVME_SC_NS_IS_PRIVATE:
        return "NS_IS_PRIVATE: The namespace is private and is already attached to one controller";
    case CM_NVME_SC_NS_NOT_ATTACHED:
        return "NS_NOT_ATTACHED: The request to detach the controller could not be completed because"
               " the controller is not attached to the namespace";
    case CM_NVME_SC_THIN_PROV_NOT_SUPP:
        return "THIN_PROVISIONING_NOT_SUPPORTED: Thin provisioning is not supported by the controller";
    case CM_NVME_SC_CTRL_LIST_INVALID:
        return "CONTROLLER_LIST_INVALID: The controller list provided is invalid";
    case CM_NVME_SC_DEVICE_SELF_TEST_IN_PROGRESS:
        return "DEVICE_SELF_TEST_IN_PROGRESS: The controller or NVM subsystem already has"
               " a device self-test operation in process.";
    case CM_NVME_SC_BP_WRITE_PROHIBITED:
        return "BOOT PARTITION WRITE PROHIBITED: The command is trying to modify a Boot Partition while it is locked";
    case CM_NVME_SC_INVALID_CTRL_ID:
        return "INVALID_CTRL_ID: An invalid Controller Identifier was specified.";
    case CM_NVME_SC_INVALID_SECONDARY_CTRL_STATE:
        return "INVALID_SECONDARY_CTRL_STATE: The action requested for the secondary controller is invalid based"
               " on the current state of the secondary controller and its primary controller.";
    case CM_NVME_SC_INVALID_NUM_CTRL_RESOURCE:
        return "INVALID_NUM_CTRL_RESOURCE: The specified number of Flexible Resources is invalid";
    case CM_NVME_SC_INVALID_RESOURCE_ID:
        return "INVALID_RESOURCE_ID: At least one of the specified resource identifiers was invalid";
    case CM_NVME_SC_ANA_INVALID_GROUP_ID:
        return "ANA_INVALID_GROUP_ID: The specified ANA Group Identifier (ANAGRPID) is not supported"
               " in the submitted command.";
    case CM_NVME_SC_ANA_ATTACH_FAIL:
        return "ANA_ATTACH_FAIL: The controller is not attached to the namespace as a result of an ANA condition";
    case CM_NVME_SC_BAD_ATTRIBUTES:
        return "BAD_ATTRIBUTES: Bad attributes were given";
    case CM_NVME_SC_WRITE_FAULT:
        return "WRITE_FAULT: The write data could not be committed to the media";
    case CM_NVME_SC_READ_ERROR:
        return "READ_ERROR: The read data could not be recovered from the media";
    case CM_NVME_SC_GUARD_CHECK:
        return "GUARD_CHECK: The command was aborted due to an end-to-end guard check failure";
    case CM_NVME_SC_APPTAG_CHECK:
        return "APPTAG_CHECK: The command was aborted due to an end-to-end application tag check failure";
    case CM_NVME_SC_REFTAG_CHECK:
        return "REFTAG_CHECK: The command was aborted due to an end-to-end reference tag check failure";
    case CM_NVME_SC_COMPARE_FAILED:
        return "COMPARE_FAILED: The command failed due to a miscompare during a Compare command";
    case CM_NVME_SC_ACCESS_DENIED:
        return "ACCESS_DENIED: Access to the namespace and/or LBA range is denied due to lack of access rights";
    case CM_NVME_SC_UNWRITTEN_BLOCK:
        return "UNWRITTEN_BLOCK: The command failed due to an attempt to read from an LBA range containing"
               " a deallocated or unwritten logical block";
    case CM_NVME_SC_ANA_PERSISTENT_LOSS:
        return "ASYMMETRIC_NAMESPACE_ACCESS_PERSISTENT_LOSS: The requested function (e.g., command)"
               " is not able to be performed as a result of the relationship between the controller"
               " and the namespace being in the ANA Persistent Loss state";
    case CM_NVME_SC_ANA_INACCESSIBLE:
        return "ASYMMETRIC_NAMESPACE_ACCESS_INACCESSIBLE: The requested function (e.g., command)"
               " is not able to be performed as a result of the relationship between the controller"
               " and the namespace being in the ANA Inaccessible state";
    case CM_NVME_SC_ANA_TRANSITION:
        return "ASYMMETRIC_NAMESPACE_ACCESS_TRANSITION: The requested function (e.g., command)"
               " is not able to be performed as a result of the relationship between the controller"
               " and the namespace transitioning between Asymmetric Namespace Access states";
    case CM_NVME_SC_CMD_INTERRUPTED:
        return "CMD_INTERRUPTED: Command processing was interrupted and the controller is unable"
               " to successfully complete the command. The host should retry the command.";
    case CM_NVME_SC_PMR_SAN_PROHIBITED:
        return "Sanitize Prohibited While Persistent Memory Region is Enabled: A sanitize operation"
               " is prohibited while the Persistent Memory Region is enabled.";
    default:
        return "Unknown";
    }
}
