/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd. All rights reserved.
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
 * dss_vtable.c
 *
 *
 * IDENTIFICATION
 *    src/common/dss_vtable.c
 *
 * -------------------------------------------------------------------------
 */

#include "cm_vtable.h"
#include "dlfcn.h"
#include "securec.h"

vtable_func_t g_vtable_func = {0};

#define RETURN_IF_ERROR(ret)          \
    do {                              \
        int _status_ = (ret);         \
        if (_status_ != 0) {          \
            return _status_;          \
        }                             \
    } while (0)

int vtable_load_symbol(char* symbol, void** sym_lib_handle)
{
    *sym_lib_handle = dlsym(g_vtable_func.handle, symbol);
    const char* dlsym_err = dlerror();
    if (dlsym_err != NULL) {
        return -1;
    }
    return 0;
}

int vtable_open_dl(void** lib_handle, char* symbol)
{
    *lib_handle = dlopen(symbol, RTLD_LAZY);
    if (*lib_handle == NULL) {
        return -1;
    }
    return 0;
}

void vtable_close_dl(void *lib_handle)
{
    (void)dlclose(lib_handle);
}

#define VTABLE_LOAD_SYMBOL_FUNC(func) vtable_load_symbol(#func, (void**)&g_vtable_func.func)

int vtable_func_init()
{
    if (g_vtable_func.symbolnited) {
        return 0;
    }

    RETURN_IF_ERROR(vtable_open_dl(&g_vtable_func.handle, (char*)LIB_VTABLE_NAME));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(BdevExit));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(Initialize));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(CreateVolume));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(DestroyVolume));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(Write));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(Read));
    vtable_close_dl(g_vtable_func.handle);

    RETURN_IF_ERROR(vtable_open_dl(&g_vtable_func.handle, (char*)LIB_VTABLE_PLUS_NAME));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(VtableAdapterAppend));
    RETURN_IF_ERROR(VTABLE_LOAD_SYMBOL_FUNC(VtableAdapterInit));
    vtable_close_dl(g_vtable_func.handle);

    g_vtable_func.symbolnited = true;
    return 0;
}

void VtableExit(void)
{
    g_vtable_func.BdevExit();
}

int VtableInitAgain()
{
    return g_vtable_func.VtableAdapterInit();
}

int VtableInitialize(WorkerMode mode, ClientOptionsConfig *optConf)
{
    return g_vtable_func.Initialize(mode, optConf);
}

int VtableCreateVolume(uint16_t volumeType, uint64_t cap, uint32_t alignedSize, uint64_t* volumeId)
{
    return g_vtable_func.CreateVolume(volumeType, cap, alignedSize, volumeId);
}

int VtableDestroyVolume(uint64_t volumeId)
{
    return g_vtable_func.DestroyVolume(volumeId);
}

int VtableWrite(uint64_t volumeId, uint64_t offset, uint32_t length, char *value)
{
    return g_vtable_func.Write(volumeId, offset, length, value);
}

int VtableRead(uint64_t volumeId, uint64_t offset, uint32_t length, char *value)
{
    return g_vtable_func.Read(volumeId, offset, length, value);
}

int VtableAppend(uint64_t volumeId, uint64_t offset, uint32_t length, char *value)
{
    return g_vtable_func.VtableAdapterAppend(volumeId, offset, length, value);
}

int cm_init_vtable(void)
{
    if (g_vtable_func.isInitialize) {
        return 0;
    }
    WorkerMode mode = SEPARATES;
    ClientOptionsConfig config;
    config.enable = false;
    config.logType = FILE_TYPE;
    char logPath[PATH_MAX] = "/var/log/turboio";
    int err = memcpy_s(config.logFilePath, sizeof(logPath), logPath, sizeof(logPath));
    RETURN_IF_ERROR(err);

    RETURN_IF_ERROR(vtable_func_init());
    RETURN_IF_ERROR(VtableInitialize(mode, &config));
    RETURN_IF_ERROR(VtableInitAgain());

    g_vtable_func.isInitialize = true;
    return 0;
}