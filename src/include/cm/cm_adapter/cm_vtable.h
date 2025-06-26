/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * DSS is licensed under Mulan PSL v2.
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
 * dss_vtable.h
 *
 *
 * IDENTIFICATION
 *    src/include/cm/cm_adapter/cm_vtable.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __CM_VTABLE_H__
#define __CM_VTABLE_H__

#include "stdint.h"
#include "stdbool.h"
#include <time.h>
#include <limits.h>

#define LIB_VTABLE_NAME "libbio_bdev.so"
#define LIB_VTABLE_PLUS_NAME "libvTable.so"

// BIO
typedef enum {
    RET_CACHE_OK = 0,            // successful
    RET_CACHE_PROTECTED = 1,     // cache write protected
    RET_CACHE_ERROR = 2,         // unknown error code
    RET_CACHE_EPERM = 3,         // input parameter is incorrect
    RET_CACHE_BUSY = 4,          // cache busy, need outer retry
    RET_CACHE_NEED_RETRY = 5,    // need retry
    RET_CACHE_NOT_READY = 6,     // retry is not required
    RET_CACHE_NOT_FOUND = 7,     // not found this key
    RET_CACHE_CONFLICT = 8,      // key conflict
    RET_CACHE_MISS = 9,          // cache miss
    RET_CACHE_NO_SPACE = 10,     // cache capacity not enough
    RET_CACHE_UNAVAILABLE = 11,  // cache service unavailable
    RET_CACHE_EXCEED_QUOTA = 12, // exceed cache quota limit
    RET_CACHE_PT_FAULT = 13,     // cache partition fault
    RET_CACHE_READ_EXCEED = 14,  // read limit is exceeded
    RET_CACHE_EXISTS = 15,       // cache already exists
    RET_CACHE_BUTT
} CResult;

typedef enum {
    LOCAL_AFFINITY = 1, // data local affinity
    GLOBAL_BALANCE = 2, // data global balance
    AFFINITY_BUTT
} AffinityStrategy;

typedef enum {
    WRITE_BACK = 1,
    WRITE_THROUGH = 2,
    STRATEGY_BUTT
} WriteStrategy;

typedef enum {
    CONVERGENCE,
    SEPARATES
} WorkerMode;

#define MAX_KEY_SIZE (256)
#define LOCATION_SIZE (2)
typedef void (*BioLoadCallback)(void *context, int32_t result);

typedef struct {
    char key[MAX_KEY_SIZE];
    uint32_t size;
    time_t time;
} ObjStat;

typedef struct {
    uint64_t location[LOCATION_SIZE];
} ObjLocation;

typedef struct {
    uint64_t tenantId;
    AffinityStrategy affinity;
    WriteStrategy strategy;
} CacheDescriptor;

#define CACHE_SPACE_ADDRESS_SIZE (2)
#define CACHE_SPACE_DEC_SIZE (64)

typedef struct {
    uint64_t address;
    uint32_t size;
} CacheAddress;

typedef struct {
    uint8_t allocLoc;
    uint16_t addressNum;
    uint16_t descriptorSize;
    ObjLocation loc;
    CacheAddress address[CACHE_SPACE_ADDRESS_SIZE];
    char descriptorInfo[CACHE_SPACE_DEC_SIZE];
} CacheSpaceInfo;

typedef enum {
    STDOUT_TYPE,
    FILE_TYPE,
    STDERR_TYPE
} LogType;

typedef struct {
    LogType logType;                   // STDOUT_TYPE/FILE_TYPE/STDERR_TYPE
    char logFilePath[PATH_MAX];        // log file path, if log type use FILE_TYPE, need to set this param
    uint8_t enable;                    // switch
    char certificationPath[PATH_MAX];  // certification path
    char caCerPath[PATH_MAX];          // caCer path
    char caCrlPath[PATH_MAX];          // caCer path
    char privateKeyPath[PATH_MAX];     // private key path
    char privateKeyPassword[PATH_MAX]; // private key password
    char hseKfsMasterPath[PATH_MAX];   // hseceasy kfs master path
    char hseKfsStandbyPath[PATH_MAX];  // hseceasy kfs standby path
} ClientOptionsConfig;

// vtable
void Exit();
void BdevExit();
CResult Initialize(WorkerMode mode, ClientOptionsConfig *optConf);
CResult CreateVolume(uint16_t volumeType, uint64_t cap, uint32_t alignedSize, uint64_t* volumeId);
CResult DestroyVolume(uint64_t volumeId);
CResult Write(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
CResult Read(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
CResult Append(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);

// vtable Adapter
int VtableAdapterAppend(uint64_t lunId, uint64_t offset, uint64_t length, const char* value);
int VtableAdapterInit();

// DSS
typedef struct st_vtable_func {
    bool symbolnited;
    bool isInitialize;
    void* handle;
    void (*BdevExit)(void);
    CResult (*Initialize)(WorkerMode mode, ClientOptionsConfig *optConf);
    CResult (*CreateVolume)(uint16_t volumeType, uint64_t cap, uint32_t alignedSize, uint64_t* volumeId);
    CResult (*DestroyVolume)(uint64_t volumeId);
    CResult (*Write)(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
    CResult (*Read)(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
    CResult (*VtableAdapterInit)();
    CResult (*VtableAdapterAppend)(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
} vtable_func_t;

#ifdef __cplusplus
extern "C" {
#endif
extern vtable_func_t g_vtable_func;
int vtable_func_init();
void VtableExit(void);
int VtableInitAgain();
int VtableInitialize(WorkerMode mode, ClientOptionsConfig *optConf);
int VtableCreateVolume(uint16_t volumeType, uint64_t cap, uint32_t alignedSize, uint64_t* volumeId);
int VtableDestroyVolume(uint64_t volumeId);
int VtableWrite(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
int VtableRead(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
int VtableAppend(uint64_t volumeId, uint64_t offset, uint32_t length, char *value);
int cm_init_vtable(void);

#ifdef __cplusplus
}
#endif

#endif
