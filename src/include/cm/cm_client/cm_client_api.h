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

#define CM_CLIENT_MAX_RES_NAME 32
#define CM_CLIENT_MAX_INSTANCES 32

typedef void(*cm_notify_func_t)(void);

typedef enum {
    CMS_RES_STAT_UNKNOWN = 0,
    CMS_RES_STAT_ONLINE  = 1,
    CMS_RES_STAT_OFFLINE = 2,
    /********************/
    CMS_RES_STAT_COUNT = 3,
} cm_res_stat;

typedef struct st_cms_res_stat_info_t {
    unsigned int node_id;
    unsigned int cm_instance_id;
    unsigned int res_instance_id;
    char res_name[CM_CLIENT_MAX_RES_NAME];
    cm_res_stat stat;
    long long instance_data;
} cms_res_stat_info_t;

typedef struct st_ms_res_stat_list_t {
    unsigned long long version;
    unsigned int master_node_id;
    unsigned int instance_count;
    cms_res_stat_info_t resStat[CM_CLIENT_MAX_INSTANCES];
} cms_res_stat_list_t;

#ifndef WIN32
/*
* cm client init function for resource
* @param [in] instance_id: resource instance id, not same with cm instance id
* @param [in] res_name: resource name
* @param [in] func: callback function
* @return 0: success; -1 failed
*/
CLIENT_API int cm_init(unsigned int instance_id, const char *res_name, cm_notify_func_t func);

/*
* resource set instance data function
* @param [in] data: resource set instance data
* @return 0: success; -1 failed
*/
CLIENT_API int cm_set_instance_data(long long data);

/*
* resource get instance stat list function
* @param [in&out] stat_list: instance status list
* @return 0: success; -1 failed
*/
CLIENT_API int cm_get_res_stat_list(cms_res_stat_list_t *stat_list);

/*
* resource set data to global data list function
* @param [in] slot_id: slot id
* @param [in] data: set data
* @param [in] size: data size
* @param [in] old_version: old list version
* @return 0: success; -1 failed
*/
CLIENT_API int cm_set_res_data(unsigned int slot_id, char *data, unsigned int size, unsigned long long old_version);

/*
* resource get data for global data list function
* @param [in] slot_id: slot id
* @param [in&out] data: get data
* @param [in] max_size: max data size
* @param [in&out] size: data size
* @param [in&out] new_version: new list version
* @return 0: success; -1 failed
*/
CLIENT_API int cm_get_res_data(unsigned int slot_id, char *data, unsigned int max_size, unsigned int *size,
    unsigned long long *new_version);
#else
static inline int cm_init(unsigned int instance_id, const char *res_name, cm_notify_func_t func)
{
    return 0;
}
static inline int cm_set_instance_data(unsigned long long data)
{
    return 0;
}
static inline int cm_get_res_stat_list(cms_res_stat_list_t *stat_list)
{
    return 0;
}
static inline int cm_set_res_data(unsigned int slot_id, char *data, unsigned int size,
    unsigned long long old_version)
{
    return 0;
}
static inline int cm_get_res_data(unsigned int slot_id, char *data, unsigned int max_size, unsigned int *size,
    unsigned long long *new_version)
{
    return 0;
}
#endif

#ifdef __cplusplus
}
#endif
#endif // CM_CLIENT_API_H
