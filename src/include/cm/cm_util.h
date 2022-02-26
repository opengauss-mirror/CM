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
 * cm_util.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_util.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CM_UTIL_H
#define CM_UTIL_H
/*
 * is_absolute_path
 *
 */
#ifndef WIN32
#define IS_DIR_SEP(ch) ((ch) == '/')
#else
#define IS_DIR_SEP(ch) ((ch) == '/' || (ch) == '\\')
#endif

int CmMkdirP(char *path, unsigned int omode);
char *gs_getenv_r(const char *name);

#endif  // CM_UTIL_H