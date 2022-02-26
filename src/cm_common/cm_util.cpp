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
 * cm_util.cpp
 *
 *
 * IDENTIFICATION
 *    src/cm_common/cm_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <sys/stat.h>
#include <securec.h>
#include "cm_util.h"
#include "cm_misc_base.h"

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

int CmMkdirP(char *path, unsigned int omode)
{
    int retval = 0;
    char *cur = path;

    mode_t oldUmask = umask(0);
    mode_t newUmask = oldUmask & ~(S_IWUSR | S_IXUSR);
    (void)umask(newUmask);
    if (cur[0] == '/') {
        ++cur;
    }
    for (int last = 0; !last; ++cur) {
        if (cur[0] == '\0') {
            last = 1;
        } else if (cur[0] != '/') {
            continue;
        }
        *cur = '\0';
        if (!last && cur[1] == '\0') {
            last = 1;
        }
        if (last) {
            (void)umask(oldUmask);
        }
        // check for pre-existing directory
        struct stat sb = {0};
        if (stat(path, &sb) == 0) {
            if (!S_ISDIR(sb.st_mode)) {
                if (last) {
                    errno = EEXIST;
                } else {
                    errno = ENOTDIR;
                }
                retval = -1;
                break;
            }
        } else if (mkdir(path, last ? (mode_t)omode : (S_IRWXU | S_IRWXG | S_IRWXO)) < 0) {
            retval = -1;
            break;
        }
        if (!last) {
            *cur = '/';
        }
    }
    (void)umask(oldUmask);
    return retval;
}

char *gs_getenv_r(const char *name)
{
    (void)pthread_mutex_lock(&g_cmEnvLock);
    char *ret = getenv(name);
    (void)pthread_mutex_unlock(&g_cmEnvLock);
    return ret;
}
