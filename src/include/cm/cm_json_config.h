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
* cm_json_config.h
*
*
* IDENTIFICATION
*    include/cm/cm_json_config.h
*
* -------------------------------------------------------------------------
*/

#ifndef CM_CM_JSON_CONFIG_H
#define CM_CM_JSON_CONFIG_H

#include "cjson/cJSON.h"
#include "cm_c.h"

#define CM_JSON_NOT_EXIST 1
#define CM_JSON_OPEN_ERROR 2
#define CM_JSON_GET_LEN_ERROR 3
#define CM_JSON_OUT_OF_MEMORY 4
#define CM_JSON_READ_ERROR 5

#define CM_IS_READ_JSON_FAIL(ret) (((ret) != 0) && ((ret) != CM_JSON_NOT_EXIST))

#define CM_JSON_STR_LEN 1024

typedef void (*CmJsonLogOutput)(int logLevel, const char *format, ...) __attribute__((format(printf, 2, 3)));

typedef struct CusResInstConfSt {
    int nodeId;
    int resInstId;
    char resArgs[CM_JSON_STR_LEN];
} CusResInstConf;

typedef struct CusResConfJson {
    char resName[CM_JSON_STR_LEN];
    char resScript[CM_JSON_STR_LEN];
    int checkInterval;
    int timeOut;
    int restartDelay;
    int restartPeriod;
    int restartTimes;
    struct {
        CusResInstConf *conf;
        uint32 count;
    } instance;
} AppCusResConfJson, DnCusResConfJson;

typedef struct BaseIpListConfSt {
    int instId;
    char baseIp[CM_JSON_STR_LEN];
} BaseIpListConf;

typedef enum CusResTypeEn {
    CUSTOM_RESOURCE_UNKNOWN,
    CUSTOM_RESOURCE_APP,
    CUSTOM_RESOURCE_DN,
} CusResType;

typedef struct OneCusResConfJsonSt {
    CusResType resType;  // resources_type (APP,DN)
    union {
        AppCusResConfJson appResConf;  // APP
        DnCusResConfJson dnResConf;    // DN
    };
} OneCusResConfJson;

typedef struct CmConfJsonSt {
    struct {
        OneCusResConfJson *conf;
        uint32 count;
    } resource;  // resource
} CmConfJson;

extern CmConfJson *g_confJson;

void ParseRootJson(const cJSON *root, CmConfJson *cmConf);
int ReadConfJsonFile(const char *jsonFile);
void SetReadJsonConfWriteLog(CmJsonLogOutput logFunc);
bool IsConfJsonEmpty();
cJSON *ReadJsonFile(const char *jsonPath, int *err);

#endif  // CM_CM_JSON_CONFIG_H
