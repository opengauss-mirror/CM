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
* ctl_res.cpp
*    cm_ctl res --add
*    cm_ctl res --edit
*    cm_ctl res --del
*
* IDENTIFICATION
*    src/cm_ctl/ctl_res.cpp
*
* -------------------------------------------------------------------------
*/
#include "cjson/cJSON.h"
#include "cm/libpq-fe.h"
#include "cm_json_config.h"
#include "ctl_common.h"

static char g_jsonFile[CM_PATH_LENGTH] = {0};

typedef void (*ProcessConfJson)(cJSON * const confObj, const char *key, const char *value);
typedef status_t (*DelArrayFuc)(cJSON *array, char *instAttr);
typedef status_t (*AddRes)(const ResOption *resCtx);
typedef status_t (*EditRes)(const ResOption *resCtx);
typedef status_t (*DelRes)(const ResOption *resCtx);
typedef status_t (*CheckRes)();

typedef struct ResOperMapT {
    ResType type;
    AddRes add;
    EditRes edit;
    DelRes del;
    CheckRes check;
} ResOperMap;

// APP, DN
static status_t AddResToJsonCore(const ResOption *resCtx);
static status_t EditResInJsonCore(const ResOption *resCtx);
static status_t DelResInJsonCore(const ResOption *resCtx);
static status_t CheckResInJsonCore();

static void ProcessInstAttrConfJson(cJSON * const confObj, const char *key, const char *value);
static status_t DelInstFromArrayJson(cJSON *array, char *instAttr);
static status_t ResAddInst(
    const ResOption *resCtx, const char *resName, ProcessConfJson processFuc = ProcessInstAttrConfJson);
static status_t ResDelInst(const ResOption *resCtx, const char *resName, DelArrayFuc fuc = DelInstFromArrayJson);
static status_t CheckAppResInfo(cJSON *resItem, const char *resName);
static status_t CheckDnResInfo(cJSON *resItem, const char *resName);

ResTypeMap g_resTypeMap[RES_TYPE_CEIL] = {{RES_TYPE_UNKOWN, "APP", "instances", CheckAppResInfo},
    {RES_TYPE_APP, "APP", "instances", CheckAppResInfo},
    {RES_TYPE_DN, "DN", NULL, CheckDnResInfo},
};

static ResOperMap g_resOperMap[RES_TYPE_CEIL] = {
    {RES_TYPE_UNKOWN, AddResToJsonCore, EditResInJsonCore, DelResInJsonCore, CheckResInJsonCore},
    {RES_TYPE_APP, AddResToJsonCore, EditResInJsonCore, DelResInJsonCore, CheckResInJsonCore},
    {RES_TYPE_DN, AddResToJsonCore, EditResInJsonCore, DelResInJsonCore, CheckResInJsonCore},
};

static const char *g_editModeStr[RES_EDIT_CEIL] = {"unkown edit", "add res inst", "del res inst", "add edit inst"};

static bool IsValueNumber(const char *value)
{
    if (value == NULL) {
        return false;
    }
    if (value[0] == '-') {
        if (strlen(value) > 1) {
            return (CM_is_str_all_digit(value + 1) == 0);
        }
        return false;
    }

    return (CM_is_str_all_digit(value) == 0);
}

static status_t CreateEmptyJsonFile(const char *fileName)
{
    char newFileName[MAX_PATH_LEN] = {0};
    int ret = snprintf_s(newFileName, MAX_PATH_LEN, MAX_PATH_LEN, "%s", fileName);
    securec_check_intval(ret, (void)ret);
    
    FILE *fp = fopen(newFileName, "a");
    if (fp == NULL) {
        write_runlog(ERROR, "create file \"%s\" failed, errno is %s.\n", newFileName, gs_strerror(errno));
        return CM_ERROR;
    }

    (void)fclose(fp);

    if (chmod(newFileName, S_IRUSR | S_IWUSR) == -1) {
        write_runlog(ERROR, "chmod file \"%s\" failed.\n", newFileName);
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

static status_t WriteJsonFile(const cJSON *root, char *jsonPath)
{
    FILE *fp = fopen(jsonPath, "w+");
    if (fp == NULL) {
        CM_RETURN_IFERR(CreateEmptyJsonFile(jsonPath));
        fp = fopen(jsonPath, "w+");
        if (fp == NULL) {
            write_runlog(ERROR, "could not open file \"%s\". errno is %s \n", jsonPath, gs_strerror(errno));
            return CM_ERROR;
        }
    }
    if (fseek(fp, 0, SEEK_SET) != 0) {
        (void)fclose(fp);
        return CM_ERROR;
    }
    char *jsonStr = cJSON_Print(root);
    CM_RETERR_IF_NULL_EX(jsonStr, (void)fclose(fp));
    size_t jsonStrLen = strlen(jsonStr);
    write_runlog(DEBUG1, "new res conf json str len is (%zu).\n", jsonStrLen);
    if (fwrite(jsonStr, jsonStrLen, 1, fp) != 1) {
        write_runlog(ERROR, "could not write file \"%s\": %s.\n", jsonPath, gs_strerror(errno));
        (void)fclose(fp);
        cJSON_free(jsonStr);
        return CM_ERROR;
    }
    cJSON_free(jsonStr);
    
    if (fsync(fileno(fp)) != 0) {
        write_runlog(ERROR, "could not fsync file \"%s\": %s.\n", jsonPath, gs_strerror(errno));
        (void)fclose(fp);
        return CM_ERROR;
    }
    
    (void)fclose(fp);
    return CM_SUCCESS;
}

static status_t SplitKeyAndValue(cJSON *obj, char *str, ProcessConfJson processFuc)
{
    if (CM_IS_EMPTY_STR(str)) {
        write_runlog(WARNING, "res_attr exist null kv pair, please check.\n");
        return CM_SUCCESS;
    }
    char *left = NULL;
    char *para = strtok_r(str, "=", &left);
    if (CM_IS_EMPTY_STR(para)) {
        write_runlog(ERROR, "res_attr irregular, parameter is null, please check.\n");
        return CM_ERROR;
    }
    char *value = strtok_r(NULL, "=", &left);
    if (CM_IS_EMPTY_STR(value)) {
        write_runlog(ERROR, "res_attr irregular, lost \'=\' or value is null, please check.\n");
        return CM_ERROR;
    }
    processFuc(obj, para, value);
    return CM_SUCCESS;
}

static status_t SplitResAttr(cJSON *obj, char *resAttr, ProcessConfJson fuc)
{
    char *left = NULL;
    char *oneAttr = strtok_r(resAttr, ",", &left);
    CM_RETURN_IFERR(SplitKeyAndValue(obj, oneAttr, fuc));
    while (!CM_IS_EMPTY_STR(left)) {
        oneAttr = strtok_r(NULL, ",", &left);
        CM_RETURN_IFERR(SplitKeyAndValue(obj, oneAttr, fuc));
    }
    
    return CM_SUCCESS;
}

static bool CompareResType(const char *value, uint32 *index)
{
    if (value == NULL) {
        write_runlog(ERROR, "value is NULL.\n");
        return false;
    }
    char resTypeStr[MAX_PATH_LEN] = {0};
    errno_t rc;
    uint32 arrLen = (uint32)(sizeof(g_resTypeMap) / sizeof(g_resTypeMap[0]));
    char tmpStr[MAX_PATH_LEN] = {0};
    for (uint32 i = 0; i < arrLen; ++i) {
        if (strcmp(value, g_resTypeMap[i].typeStr) == 0) {
            *index = i;
            return true;
        }
        if (i == 0) {
            rc = snprintf_s(
                tmpStr, MAX_PATH_LEN, MAX_PATH_LEN - 1, "%s-%s", g_resTypeMap[i].typeStr, g_resTypeMap[i].value);
        } else {
            rc = snprintf_s(
                tmpStr, MAX_PATH_LEN, MAX_PATH_LEN - 1, ", %s-%s", g_resTypeMap[i].typeStr, g_resTypeMap[i].value);
        }
        securec_check_intval(rc, (void)rc);
        rc = strcat_s(resTypeStr, MAX_PATH_LEN, tmpStr);
        securec_check_errno(rc, (void)rc);
    }
    write_runlog(DEBUG1, "cannot find resType[%s] in g_resTypeMap[%s].\n", value, resTypeStr);
    return false;
}

void ProcessResAttrConfJson(cJSON * const confObj, const char *key, const char *value)
{
    if (IsValueNumber(value)) {
        (void)cJSON_AddNumberToObject(confObj, key, (const double)CmAtol(value, -1));
    } else {
        (void)cJSON_AddStringToObject(confObj, key, value);
    }
    uint32 index;
    if ((strcmp(key, "resources_type") == 0) && CompareResType(value, &index)) {
        if (g_resTypeMap[index].value == NULL) {
            return;
        }
        (void)cJSON_AddArrayToObject(confObj, g_resTypeMap[index].value);
    }
}

cJSON *ParseResAttr(const char *resName, char *resAttr)
{
    cJSON *resObj = cJSON_CreateObject();
    if (!cJSON_IsObject(resObj)) {
        write_runlog(ERROR, "create new res json obj failed, add res failed.\n");
        cJSON_Delete(resObj);
        return NULL;
    }
    if (cJSON_AddStringToObject(resObj, "name", resName) == NULL) {
        write_runlog(ERROR, "add name info to new res json obj failed, add res failed.\n");
        cJSON_Delete(resObj);
        return NULL;
    }
    if (SplitResAttr(resObj, resAttr, ProcessResAttrConfJson) != CM_SUCCESS) {
        write_runlog(ERROR, "parse res attr failed, add res failed.\n");
        cJSON_Delete(resObj);
        return NULL;
    }
    return resObj;
}

static status_t CanDoAddRes(const ResOption *resCtx)
{
    if (CM_IS_EMPTY_STR(resCtx->resName)) {
        write_runlog(ERROR, "res_name is null, cannot do add res.\n");
        return CM_ERROR;
    }
    if (CM_IS_EMPTY_STR(resCtx->resAttr)) {
        write_runlog(ERROR, "res_attr is null, cannot do add res.\n");
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static status_t CanDoDelRes(const ResOption *resCtx)
{
    if (CM_IS_EMPTY_STR(resCtx->resName)) {
        write_runlog(ERROR, "res_name is null, cannot do del res.\n");
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static cJSON *CreateNewResJsonObj()
{
    cJSON *root = cJSON_CreateObject();
    if (!cJSON_IsObject(root)) {
        write_runlog(ERROR, "create new res json obj failed.\n");
        cJSON_Delete(root);
        return NULL;
    }
    cJSON *resArray = cJSON_AddArrayToObject(root, "resources");
    if (!cJSON_IsArray(resArray)) {
        write_runlog(ERROR, "create new res json array failed.\n");
        cJSON_Delete(root);
        return NULL;
    }
    return root;
}

static status_t AddNewResToJsonObj(const cJSON * const root, cJSON *newRes)
{
    CM_RETURN_IF_FALSE(cJSON_IsObject(root));
    CM_RETURN_IF_FALSE(cJSON_IsObject(newRes));
    cJSON *resArray = cJSON_GetObjectItem(root, "resources");
    if (!cJSON_IsArray(resArray)) {
        write_runlog(ERROR, "json obj in \"%s\" incorrect format.\n", g_jsonFile);
        return CM_ERROR;
    }
    if (!cJSON_AddItemToArray(resArray, newRes)) {
        write_runlog(ERROR, "add new res info to json failed.\n");
        return CM_ERROR;
    }
    
    return CM_SUCCESS;
}

static cJSON *GetResJsonFromFile(const char *jsonFile, bool canCreateFile)
{
    int err = 0;
    cJSON *root = ReadJsonFile(jsonFile, &err);
    if (!cJSON_IsObject(root)) {
        if (root != NULL) {
            cJSON_Delete(root);
        }
        if (canCreateFile) {
            root = CreateNewResJsonObj();
        } else {
            write_runlog(ERROR, "read res conf json \"%s\" failed, err=%d.\n", jsonFile, err);
            root = NULL;
        }
    }
    return root;
}

static inline void DeleteAllJson(cJSON *root, cJSON *res)
{
    cJSON_Delete(root);
    cJSON_Delete(res);
}

// command: cm_ctl --add --res_name --res_attr
static status_t AddResToJsonCore(const ResOption *resCtx)
{
    cJSON *root = GetResJsonFromFile(g_jsonFile, true);
    CM_RETERR_IF_NULL(root);
    cJSON *newRes = ParseResAttr(resCtx->resName, resCtx->resAttr);
    CM_RETERR_IF_NULL_EX(newRes, cJSON_Delete(root));
    CM_RETURN_IFERR_EX(AddNewResToJsonObj(root, newRes), DeleteAllJson(root, newRes));
    CM_RETURN_IFERR_EX(WriteJsonFile(root, g_jsonFile), cJSON_Delete(root));
    cJSON_Delete(root);
    return CM_SUCCESS;
}

static status_t CanDoEditResInst(const ResOption *resCtx)
{
    const char *editMode = g_editModeStr[resCtx->editMode];
    const char *editAttr = NULL;
    switch (resCtx->editMode) {
        case RES_ADD_INST_CONF:
            editAttr = resCtx->addInstStr;
            break;
        case RES_DEL_INST_CONF:
            editAttr = resCtx->delInstStr;
            break;
        case RES_EDIT_RES_CONF:
            editAttr = resCtx->resAttr;
            break;
        default:;
    }
    if (editAttr == NULL) {
        write_runlog(ERROR, "attr is null, cannot do %s.\n", editMode);
        return CM_ERROR;
    }
    return CM_SUCCESS;
}

static int GetValueIntFromCJson(const cJSON *object, const char *infoKey)
{
    cJSON *objValue = cJSON_GetObjectItem(object, infoKey);
    if (!cJSON_IsNumber(objValue)) {
        write_runlog(ERROR, "(%s) object is not number.\n", infoKey);
        return -1;
    }
    if (objValue->valueint < 0) {
        write_runlog(ERROR, "get invalid objValue(%d) from cJson, by key(%s).\n", objValue->valueint, infoKey);
        return -1;
    }
    return objValue->valueint;
}

static char *GetValueStrFromCJson(const cJSON *object, const char *infoKey)
{
    cJSON *objValue = cJSON_GetObjectItem(object, infoKey);
    if (!cJSON_IsString(objValue)) {
        write_runlog(ERROR, "(%s) object is not string.\n", infoKey);
        return NULL;
    }
    if (objValue->valuestring == NULL || objValue->valuestring[0] == '\0') {
        write_runlog(ERROR, "(%s) object is null.\n", infoKey);
        return NULL;
    }
    return objValue->valuestring;
}

cJSON *GetArrayFromObj(const cJSON *obj, const char *arrName)
{
    cJSON *array = cJSON_GetObjectItem(obj, arrName);
    if (!cJSON_IsArray(array)) {
        write_runlog(ERROR, "\"%s\" not exit array: %s.\n", g_jsonFile, arrName);
        return NULL;
    }
    return array;
}

cJSON *GetResFromArray(cJSON *resArray, const char *resName)
{
    cJSON *resItem;
    cJSON_ArrayForEach(resItem, resArray) {
        char *valueStr = GetValueStrFromCJson(resItem, "name");
        if (valueStr == NULL) {
            continue;
        }
        if (strcmp(valueStr, resName) == 0) {
            break;
        }
    }
    if (resItem == NULL) {
        write_runlog(ERROR, "no res(%s) info in \"%s\".\n", resName, g_jsonFile);
    }
    
    return resItem;
}

static void ProcessInstAttrConfJson(cJSON * const confObj, const char *key, const char *value)
{
    if ((strcmp(key, "node_id") == 0 || strcmp(key, "res_instance_id") == 0) && IsValueNumber(value)) {
        (void)cJSON_AddNumberToObject(confObj, key, (const double)CmAtol(value, -1));
    } else if (strcmp(key, "res_args") == 0) {
        (void)cJSON_AddStringToObject(confObj, key, value);
    } else {
        write_runlog(WARNING, "\"%s=%s\" is invalid for inst json conf, cannot edit json file.\n", key, value);
    }
}

void ProcessEditAttrConfJson(cJSON *const confObj, const char *key, const char *value)
{
    if (IsValueNumber(value)) {
        (void)cJSON_ReplaceItemInObject(confObj, key, cJSON_CreateNumber((const double)CmAtol(value, -1)));
    } else {
        (void)cJSON_ReplaceItemInObject(confObj, key, cJSON_CreateString(value));
    }
}

static cJSON *ParseInstAttr(char *instAttr, ProcessConfJson processFuc)
{
    cJSON *instObj = cJSON_CreateObject();
    if (!cJSON_IsObject(instObj)) {
        write_runlog(ERROR, "create new res inst json obj failed, edit res failed.\n");
        cJSON_Delete(instObj);
        return NULL;
    }
    if (SplitResAttr(instObj, instAttr, processFuc) != CM_SUCCESS) {
        write_runlog(ERROR, "parse res attr failed, edit res failed.\n");
        cJSON_Delete(instObj);
        return NULL;
    }
    return instObj;
}

static status_t AddNewInstToArrayJson(cJSON *array, cJSON *newInst)
{
    CM_RETURN_IF_FALSE(cJSON_IsArray(array));
    CM_RETURN_IF_FALSE(cJSON_IsObject(newInst));
    if (!cJSON_AddItemToArray(array, newInst)) {
        write_runlog(ERROR, "add new newInst info to json failed.\n");
        return CM_ERROR;
    }

    return CM_SUCCESS;
}

static status_t DelInstFromArrayJson(cJSON *array, char *instAttr)
{
    CM_RETURN_IF_FALSE(cJSON_IsArray(array));
    cJSON *delInst = ParseInstAttr(instAttr, ProcessInstAttrConfJson);
    if (!cJSON_IsObject(delInst)) {
        write_runlog(ERROR, "\"%s\" is invalid for inst json conf.\n", instAttr);
        cJSON_Delete(delInst);
        return CM_ERROR;
    }
    int delNodeId = GetValueIntFromCJson(delInst, "node_id");
    int delResInstId = GetValueIntFromCJson(delInst, "res_instance_id");

    for (int i = 0; i < cJSON_GetArraySize(array); ++i) {
        cJSON *resItem = cJSON_GetArrayItem(array, i);
        int nodeId = GetValueIntFromCJson(resItem, "node_id");
        int resInstId = GetValueIntFromCJson(resItem, "res_instance_id");
        if (nodeId == delNodeId && resInstId == delResInstId) {
            cJSON_DeleteItemFromArray(array, i);
            cJSON_Delete(delInst);
            return CM_SUCCESS;
        }
    }
    write_runlog(ERROR, "res inst(%s) info do not exist, delete inst info from json failed.\n", instAttr);
    cJSON_Delete(delInst);
    return CM_ERROR;
}

static status_t DelResFromJsonObj(const cJSON *const root, const char *resName)
{
    CM_RETURN_IF_FALSE(cJSON_IsObject(root));
    cJSON *resArray = cJSON_GetObjectItem(root, "resources");
    if (!cJSON_IsArray(resArray)) {
        write_runlog(ERROR, "json obj in \"%s\" incorrect format.\n", g_jsonFile);
        return CM_ERROR;
    }
    int32 arraySize = cJSON_GetArraySize(resArray);
    for (int32 i = 0; i < arraySize; i++) {
        cJSON *resItem = cJSON_GetArrayItem(resArray, i);
        char *valueStr = GetValueStrFromCJson(resItem, "name");
        if (valueStr == NULL) {
            continue;
        }
        if (strcmp(valueStr, resName) == 0) {
            cJSON_DeleteItemFromArray(resArray, i);
            return CM_SUCCESS;
        }
    }
    write_runlog(ERROR, "res info(\"name = %s\") do not exist, delete res info from json failed.\n", resName);
    return CM_ERROR;
}

static status_t GetNumberFromJson(const cJSON * const resItem, const char *resName, const char *checkKey, int32 *value)
{
    cJSON *objValue = cJSON_GetObjectItem(resItem, checkKey);
    if (objValue != NULL) {
        if (!cJSON_IsNumber(objValue)) {
            write_runlog(WARNING, "res(%s) %s object is not a number.\n", resName, checkKey);
            return CM_ERROR;
        }
    } else {
        write_runlog(WARNING, "res(%s) has no %s object.\n", resName, checkKey);
        return CM_ERROR;
    }

    if (!IsResConfValid(checkKey, objValue->valueint)) {
        write_runlog(WARNING, "res(%s) \"%s\" is valid.\n", resName, checkKey);
        return CM_ERROR;
    }
    if (value != NULL) {
        *value = objValue->valueint;
    }
    return CM_SUCCESS;
}

void CheckResOptionalInfo(cJSON *resItem, const char *resName, const char *checkKey)
{
    (void)GetNumberFromJson(resItem, resName, checkKey, NULL);
}

static status_t CheckResInst(cJSON *instItem, const char *resName)
{
    cJSON *nodeId = cJSON_GetObjectItem(instItem, "node_id");
    if (!cJSON_IsNumber(nodeId)) {
        write_runlog(ERROR, "(%s) instances node_id is not number.\n", resName);
        return CM_ERROR;
    }
    bool isValid = false;
    for (uint32 i = 0; i < g_node_num; i++) {
        if (nodeId->valueint == (int)g_node[i].node) {
            isValid = true;
            break;
        }
    }
    if (!isValid) {
        write_runlog(ERROR, "(%s) instances node_id is not valid.\n", resName);
        return CM_ERROR;
    }
    cJSON *resInstValue = cJSON_GetObjectItem(instItem, "res_instance_id");
    if (!cJSON_IsNumber(resInstValue) || resInstValue->valueint < 0) {
        write_runlog(ERROR, "(%s) res_instance_id object is not valid number.\n", resName);
        return CM_ERROR;
    }
    resInstValue = cJSON_GetObjectItem(instItem, "res_args");
    if (!cJSON_IsString(resInstValue)) {
        write_runlog(ERROR, "(%s) res_args object is not string.\n", resName);
        return CM_ERROR;
    }
    check_input_for_security(resInstValue->valuestring);

    return CM_SUCCESS;
}

static status_t CheckResName(
    const cJSON *resItem, char (*resName)[CM_MAX_RES_NAME], uint32 maxCnt, uint32 *curCnt, const char **curResName)
{
    cJSON *objName = cJSON_GetObjectItem(resItem, "name");
    if (!cJSON_IsString(objName)) {
        write_runlog(ERROR, "res name(%s) object is not string.\n", objName->valuestring);
        return CM_ERROR;
    }
    if (strlen(objName->valuestring) >= CM_MAX_RES_NAME) {
        write_runlog(ERROR,
            "res name(%s) length exceeds the maximuml, maximuml length is (%d).\n",
            objName->valuestring,
            CM_MAX_RES_NAME);
        return CM_ERROR;
    }
    uint32 resNameCount = *curCnt;
    if (resNameCount >= maxCnt) {
        write_runlog(ERROR, "res count exceeds the maximuml, maximuml count is (%u).\n", maxCnt);
        return CM_ERROR;
    }
    errno_t rc = strcpy_s(resName[resNameCount], CM_MAX_RES_NAME, objName->valuestring);
    securec_check_errno(rc, (void)rc);
    *curResName = resName[resNameCount];
    for (uint32 i = 0; i < resNameCount; i++) {
        if (strcmp(resName[resNameCount], resName[i]) == 0) {
            write_runlog(ERROR, "res name(%s) object already exists.\n", objName->valuestring);
            return CM_ERROR;
        }
    }
    ++(*curCnt);
    return CM_SUCCESS;
}

static void GetAllRestypeStr(char *typeStr, uint32 maxlen)
{
    errno_t rc;
    uint32 arrLen = (uint32)(sizeof(g_resTypeMap) / sizeof(g_resTypeMap[0]));
    char tmpStr[MAX_PATH_LEN] = {0};
    for (uint32 i = 0; i < arrLen; ++i) {
        if (g_resTypeMap[i].type == RES_TYPE_UNKOWN) {
            continue;
        }
        if (strlen(typeStr) + strlen(g_resTypeMap[i].typeStr) >= maxlen) {
            return;
        }
        if (i == 0) {
            rc = snprintf_s(tmpStr, MAX_PATH_LEN, MAX_PATH_LEN - 1, "\"%s\"", g_resTypeMap[i].typeStr);
        } else {
            rc = snprintf_s(tmpStr, MAX_PATH_LEN, MAX_PATH_LEN - 1, ", \"%s\"", g_resTypeMap[i].typeStr);
        }
        securec_check_intval(rc, (void)rc);
        rc = strcat_s(typeStr, maxlen, tmpStr);
        securec_check_errno(rc, (void)rc);
    }
}

static status_t GetResTypeIndex(const cJSON * const resItem, const char *resName, uint32 *index)
{
    cJSON *objValue = cJSON_GetObjectItem(resItem, "resources_type");
    if (!cJSON_IsString(objValue)) {
        write_runlog(ERROR, "(%s) resources_type object is not string.\n", resName);
        return CM_ERROR;
    }
    if (CompareResType(objValue->valuestring, index)) {
        return CM_SUCCESS;
    }
    char allResName[MAX_PATH_LEN] = {0};
    GetAllRestypeStr(allResName, MAX_PATH_LEN);
    write_runlog(ERROR, "(%s) resources_type object(%s) is not in %s.\n", resName, objValue->string, allResName);
    return CM_ERROR;
}

static status_t CheckStringValidInJson(const cJSON * const resItem, const char *key, const char *resName, int logLevel)
{
    cJSON *objValue = cJSON_GetObjectItem(resItem, key);
    if (!cJSON_IsString(objValue)) {
        write_runlog(logLevel, "(%s) %s object is not string.\n", resName, key);
        return CM_ERROR;
    }
    check_input_for_security(objValue->valuestring);
    return CM_SUCCESS;
}

static status_t CheckAppDnCommResInfo(cJSON *resItem, const char *resName)
{
    (void)CheckStringValidInJson(resItem, "script", resName, DEBUG1);

    CheckResOptionalInfo(resItem, resName, "check_interval");
    CheckResOptionalInfo(resItem, resName, "time_out");
    CheckResOptionalInfo(resItem, resName, "restart_delay");
    CheckResOptionalInfo(resItem, resName, "restart_period");
    CheckResOptionalInfo(resItem, resName, "restart_times");

    return CM_SUCCESS;
}

static status_t CheckAppResInfo(cJSON *resItem, const char *resName)
{
    cJSON *instArray = cJSON_GetObjectItem(resItem, "instances");
    if (!cJSON_IsArray(instArray)) {
        write_runlog(ERROR, "(%s) resource_type=\"APP\", \"instances\" doest not exists.\n", resName);
        return CM_ERROR;
    }
    cJSON *instItem;
    cJSON_ArrayForEach(instItem, instArray) {
        CM_RETURN_IFERR(CheckResInst(instItem, resName));
    }
    return CheckAppDnCommResInfo(resItem, resName);
}

static status_t CheckDnResInfo(cJSON *resItem, const char *resName)
{
    cJSON *instArray = cJSON_GetObjectItem(resItem, "instances");
    if (cJSON_IsArray(instArray)) {
        cJSON *instItem;
        cJSON_ArrayForEach(instItem, instArray) {
            CM_RETURN_IFERR(CheckResInst(instItem, resName));
        }
    }
    return CheckAppDnCommResInfo(resItem, resName);
}

static status_t CheckResFromArray(cJSON *resArray)
{
    cJSON *resItem;
    const uint32 maxResCnt = CM_MAX_RES_COUNT + CM_MAX_VIP_COUNT;
    char resName[maxResCnt][CM_MAX_RES_NAME];
    uint32 resNameCount = 0;
    const char *curResName;
    uint32 curIndex = 0;
    CheckResInfo check;
    cJSON_ArrayForEach(resItem, resArray) {
        CM_RETURN_IFERR(CheckResName(resItem, resName, maxResCnt, &resNameCount, &curResName));

        CM_RETURN_IFERR(GetResTypeIndex(resItem, curResName, &curIndex));

        check = g_resTypeMap[curIndex].check;

        // resource may not be checked.
        if (check == NULL) {
            continue;
        }
        CM_RETURN_IFERR(check(resItem, curResName));
    }
    return CM_SUCCESS;
}

static cJSON *GetConfJsonArray(cJSON *resItem, const char *resName)
{
    cJSON *instArray = cJSON_GetObjectItem(resItem, resName);
    if (!cJSON_IsArray(instArray)) {
        instArray = cJSON_AddArrayToObject(resItem, resName);
    }
    return instArray;
}

// command: cm_ctl --edit --res_name --add_inst
static status_t ResAddInst(const ResOption *resCtx, const char *resName, ProcessConfJson processFuc)
{
    cJSON *root = GetResJsonFromFile(g_jsonFile, false);
    CM_RETERR_IF_NULL(root);
    cJSON *resArray = GetArrayFromObj(root, "resources");
    CM_RETURN_IF_FALSE_EX(cJSON_IsArray(resArray), cJSON_Delete(root));
    cJSON *resItem = GetResFromArray(resArray, resCtx->resName);
    CM_RETERR_IF_NULL_EX(resItem, cJSON_Delete(root));
    cJSON *instArray = GetConfJsonArray(resItem, resName);
    CM_RETURN_IF_FALSE_EX(cJSON_IsArray(instArray), cJSON_Delete(root));
    cJSON *newInst = ParseInstAttr(resCtx->addInstStr, processFuc);
    CM_RETERR_IF_NULL_EX(newInst, cJSON_Delete(root));

    CM_RETURN_IFERR_EX(AddNewInstToArrayJson(instArray, newInst), DeleteAllJson(root, newInst));
    CM_RETURN_IFERR_EX(WriteJsonFile(root, g_jsonFile), cJSON_Delete(root));
    cJSON_Delete(root);

    return CM_SUCCESS;
}

// command: cm_ctl --edit --res_name --del_inst
static status_t ResDelInst(const ResOption *resCtx, const char *resName, DelArrayFuc fuc)
{
    cJSON *root = GetResJsonFromFile(g_jsonFile, false);
    CM_RETERR_IF_NULL(root);
    cJSON *resArray = GetArrayFromObj(root, "resources");
    CM_RETURN_IF_FALSE_EX(cJSON_IsArray(resArray), cJSON_Delete(root));
    cJSON *resItem = GetResFromArray(resArray, resCtx->resName);
    CM_RETERR_IF_NULL_EX(resItem, cJSON_Delete(root));
    cJSON *instArray = cJSON_GetObjectItem(resItem, resName);
    CM_RETURN_IF_FALSE_EX(cJSON_IsArray(instArray), cJSON_Delete(root));

    CM_RETURN_IFERR_EX(fuc(instArray, resCtx->delInstStr), cJSON_Delete(root));
    CM_RETURN_IFERR_EX(WriteJsonFile(root, g_jsonFile), cJSON_Delete(root));
    cJSON_Delete(root);

    return CM_SUCCESS;
}

// command: cm_ctl --edit --res_name --res_attr
static status_t ResEditInst(const ResOption *resCtx)
{
    cJSON *root = GetResJsonFromFile(g_jsonFile, false);
    CM_RETERR_IF_NULL(root);
    cJSON *resArray = GetArrayFromObj(root, "resources");
    CM_RETURN_IF_FALSE_EX(cJSON_IsArray(resArray), cJSON_Delete(root));
    cJSON *resItem = GetResFromArray(resArray, resCtx->resName);
    CM_RETERR_IF_NULL_EX(resItem, cJSON_Delete(root));

    CM_RETURN_IFERR_EX(SplitResAttr(resItem, resCtx->resAttr, ProcessEditAttrConfJson), cJSON_Delete(root));
    CM_RETURN_IFERR_EX(WriteJsonFile(root, g_jsonFile), cJSON_Delete(root));
    cJSON_Delete(root);

    return CM_SUCCESS;
}

static status_t EditResInJsonCore(const ResOption *resCtx)
{
    switch (resCtx->editMode) {
        case RES_ADD_INST_CONF:
            return ResAddInst(resCtx, "instances");
        case RES_DEL_INST_CONF:
            return ResDelInst(resCtx, "instances");
        case RES_EDIT_RES_CONF:
            return ResEditInst(resCtx);
        case RES_EDIT_UNKNOWN:
            write_runlog(ERROR, "not input (--add_inst,--del_inst,--res_attr), please check input.\n");
            break;
        default:
            write_runlog(ERROR, "unknown edit mode(%u), cannot do edit res.\n", (uint32)resCtx->editMode);
            break;
    }

    return CM_ERROR;
}

// command: cm_ctl res --del --res_name
static status_t DelResInJsonCore(const ResOption *resCtx)
{
    cJSON *root = GetResJsonFromFile(g_jsonFile, false);
    CM_RETERR_IF_NULL(root);
    CM_RETURN_IFERR_EX(DelResFromJsonObj(root, resCtx->resName), cJSON_Delete(root));
    CM_RETURN_IFERR_EX(WriteJsonFile(root, g_jsonFile), cJSON_Delete(root));
    cJSON_Delete(root);
    return CM_SUCCESS;
}

// command: cm_ctl res --check
static status_t CheckResInJsonCore()
{
    cJSON *root = GetResJsonFromFile(g_jsonFile, false);
    CM_RETERR_IF_NULL(root);
    cJSON *resArray = cJSON_GetObjectItem(root, "resources");
    if (!cJSON_IsArray(resArray)) {
        write_runlog(ERROR, "resources do not exist.\n");
        cJSON_Delete(root);
        return CM_ERROR;
    }
    CM_RETURN_IFERR_EX(CheckResFromArray(resArray), cJSON_Delete(root));
    cJSON_Delete(root);
    return CM_SUCCESS;
}

static ResOperMap *FindOperMapByType(ResType type)
{
    int32 unknownIndex = 0;
    for (int32 i = 0; i < (int32)RES_TYPE_CEIL; ++i) {
        if (g_resOperMap[i].type == type) {
            return &(g_resOperMap[i]);
        }
        if (g_resOperMap[i].type == RES_TYPE_UNKOWN) {
            unknownIndex = i;
        }
    }
    return &(g_resOperMap[unknownIndex]);
}

static int AddResToJson(const ResOption *resCtx)
{
    if (CanDoAddRes(resCtx) != CM_SUCCESS) {
        write_runlog(ERROR, "add res(%s) fail.\n", resCtx->resName);
        return -1;
    }
    ResOperMap *oper = FindOperMapByType(resCtx->type);
    if (oper->add == NULL) {
        write_runlog(ERROR, "add res(%s) fail, bacause add oper is NULL.\n", resCtx->resName);
        return -1;
    }
    if (oper->add(resCtx) == CM_SUCCESS) {
        write_runlog(LOG, "add res(%s) success.\n", resCtx->resName);
        return 0;
    } else {
        write_runlog(ERROR, "add res(%s) fail.\n", resCtx->resName);
        return -1;
    }
}

static int EditResInJson(const ResOption *resCtx)
{
    if (resCtx->resName == NULL) {
        write_runlog(ERROR, "res_name is null, cannot do edit res.\n");
        return -1;
    }
    if (resCtx->editMode >= RES_EDIT_CEIL) {
        write_runlog(
            ERROR, "edit res(%s) fail, with unknown edit mode(%d).\n", resCtx->resName, (int32)resCtx->editMode);
        return -1;
    }
    if (CanDoEditResInst(resCtx) != CM_SUCCESS) {
        write_runlog(ERROR, "edit res(%s) fail.\n", resCtx->resName);
        return -1;
    }

    ResOperMap *oper = FindOperMapByType(resCtx->type);
    if (oper->edit == NULL) {
        write_runlog(ERROR, "edit res(%s) fail, bacause add oper is NULL.\n", resCtx->resName);
        return -1;
    }
    if (oper->edit(resCtx) == CM_SUCCESS) {
        write_runlog(LOG, "edit res(%s) success.\n", resCtx->resName);
        return 0;
    } else {
        write_runlog(ERROR, "edit res(%s) fail.\n", resCtx->resName);
        return -1;
    }
}

static int DelResInJson(const ResOption *resCtx)
{
    if (CanDoDelRes(resCtx) != CM_SUCCESS) {
        write_runlog(ERROR, "delete res(%s) fail, please check file \"%s\".\n", resCtx->resName, g_jsonFile);
        return -1;
    }
    ResOperMap *oper = FindOperMapByType(resCtx->type);
    if (oper->del == NULL) {
        write_runlog(ERROR, "del res(%s) fail, bacause add oper is NULL.\n", resCtx->resName);
        return -1;
    }
    if (oper->del(resCtx) == CM_SUCCESS) {
        write_runlog(LOG, "delete res(%s) success.\n", resCtx->resName);
        return 0;
    } else {
        write_runlog(ERROR, "delete res(%s) fail, please check file \"%s\".\n", resCtx->resName, g_jsonFile);
        return -1;
    }
}

static int CheckResInJson(const ResOption *resCtx)
{
    ResOperMap *oper = FindOperMapByType(resCtx->type);
    if (oper->check == NULL) {
        write_runlog(ERROR, "check res(%s) fail, bacause add oper is NULL.\n", resCtx->resName);
        return -1;
    }
    if (oper->check() == CM_SUCCESS) {
        write_runlog(LOG, "resource config is valid.\n");
        return 0;
    } else {
        write_runlog(ERROR, "resource config is invalid, please check file \"%s\".\n", g_jsonFile);
        return -1;
    }
}

int DoResCommand(const CtlOption *ctx)
{
    GetCmConfJsonPath(g_jsonFile, sizeof(g_jsonFile));
    switch (ctx->resOpt.mode) {
        case RES_ADD_CONF:
            return AddResToJson(&ctx->resOpt);
        case RES_EDIT_CONF:
            return EditResInJson(&ctx->resOpt);
        case RES_DEL_CONF:
            return DelResInJson(&ctx->resOpt);
        case RES_CHECK_CONF:
            return CheckResInJson(&ctx->resOpt);
        case RES_CONF_UNKNOWN:
            write_runlog(ERROR, "not input (--add,--edit,--del,--check), please check input.\n");
            break;
        default:
            write_runlog(ERROR, "unknown cm_ctl res opt %u.\n", (uint32)ctx->resOpt.mode);
            break;
    }
    return -1;
}
