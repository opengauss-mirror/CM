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
 * cm_client_api.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_client/cm_client_api.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_CLIENT_API_H
#define CM_CLIENT_API_H

#ifdef __cplusplus
extern "C" {
#endif

#ifndef WIN32
#define CLIENT_API __attribute__ ((visibility ("default")))
#else
#define CLIENT_API __declspec(dllexport)
#endif

typedef void(*CmNotifyFunc)(void);

/*
* cm client init function, before init success, other interfaces fail to be executed.
* @param [in] instId: resource instance id, set in cm_resource.json
* @param [in] resName: resource name, len need to be shorter than 32
* @param [in] func: callback function, can be NULL
* @return 0: success; -1 failed
*/
CLIENT_API int CmInit(unsigned int instId, const char *resName, CmNotifyFunc func);

/*
* resource get instances stat list function
* @return: res status list json str
*/
CLIENT_API char *CmGetResStats();

/*
* free res status list json str
* @param [in] resStats: res status list json str
* @return 0: success; -1 failed
*/
CLIENT_API int CmFreeResStats(char *resStats);

#ifdef __cplusplus
}
#endif
#endif // CM_CLIENT_API_H
