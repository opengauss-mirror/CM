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
 * cm_ssl_base.h
 *
 *
 * IDENTIFICATION
 *    include/cm/cm_ssl_base.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CM_SSL_BASE_H
#define CM_SSL_BASE_H

#include<sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include<sys/types.h>
#include<sys/stat.h>
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#include <netinet/in.h>
#include <poll.h>
#include <stdio.h>
#include <string.h>

#include "c.h"
#include "securec.h"
#include "cm_defs.h"

#define cm_gettimeofday(a) gettimeofday(a, NULL)

#define TIMEVAL_DIFF_US(t_start, t_end) (((t_end)->tv_sec - (t_start)->tv_sec) * 1000000ULL +  \
        (t_end)->tv_usec - (t_start)->tv_usec)
#define TIMEVAL_DIFF_S(t_start, t_end)  ((t_end)->tv_sec - (t_start)->tv_sec)

#define timeval_t struct timeval

typedef volatile uint32 spinlock_t;
typedef volatile uint32 ip_spinlock_t;


#define CM_NETWORK_SEND_TIMEOUT      (uint32)5000 /* mill-seconds */
#define CM_NETWORK_IO_TIMEOUT      (uint32)1 /* mill-seconds */
#define CM_SSL_IO_TIMEOUT          (uint32)30000 /* mill-seconds */
#define CM_SSL_ACCEPT_TIMEOUT      (uint32)1 /* mill-seconds */

typedef struct st_sock_addr {
    struct sockaddr_storage addr;
    socklen_t salen;
} sock_addr_t;
typedef struct st_tcp_link {
    socket_t sock; // need to be first!
    uint8 closed; // need to be second!
    sock_addr_t remote;
    sock_addr_t local;
} tcp_link_t;

typedef struct st_ssl_ctx {
    void *reserved;
} ssl_ctx_t;

typedef struct st_ssl_socket {
    void *reserved;
} ssl_sock_t;

typedef struct st_ssl_link {
    tcp_link_t tcp;
    ssl_ctx_t *ssl_ctx;
    ssl_sock_t *ssl_sock;
} ssl_link_t;

typedef enum en_cs_pipe_type {
    CS_TYPE_TCP = 1,
    CS_TYPE_SSL = 4,
 /* direct mode, reserved */
    CS_TYPE_CEIL
} cs_pipe_type_t;

typedef union un_cs_link {
    tcp_link_t tcp;
    ssl_link_t ssl;
} cs_link_t;

typedef struct st_cs_pipe {
    cs_pipe_type_t type;
    cs_link_t link;
    uint32 options;
    uint32 version;
    int32 connect_timeout;  // ms
    int32 socket_timeout;   // ms
    int32 l_onoff;
    int32 l_linger;
} cs_pipe_t;

#pragma pack(4)
typedef struct st_text {
    char *str;
    uint32 len;
} text_t;
#pragma pack()

#define MESSAGE_BUFFER_SIZE (SIZE_M(1))
#define PADDING_BUFFER_SIZE (SIZE_K(1))
#define MAX_BATCH_SIZE      500
#define CM_PASSWD_MIN_LEN   8
#define CM_PASSWD_MAX_LEN   64
#define CM_MAX_SSL_CIPHER_LEN   (uint32)1024
#define CM_MAX_SSL_EXPIRE_THRESHOLD   (uint32)180
#define CM_MIN_SSL_EXPIRE_THRESHOLD   (uint32)7
#define CM_DEFAULT_SSL_EXPIRE_THRESHOLD 90U
#define CM_MAX_MESSAGE_BUFFER_SIZE (SIZE_M(10))

#define CS_INVALID_SOCKET (-1)

#define CM_TEXT_BEGIN(text)  ((text)->str[0])
#define CM_TEXT_FIRST(text)  ((text)->str[0])
#define CM_TEXT_SECOND(text) ((text)->str[1])
#define CM_TEXT_END(text)    ((text)->str[(text)->len - 1])
#define CM_TEXT_SECONDTOLAST(text)      (((text)->len >= 2) ? ((text)->str[(text)->len - 2]) : '\0')
#define CM_NULL_TERM(text)   \
    {                                    \
        (text)->str[(text)->len] = '\0'; \
    }
#define CM_IS_EMPTY(text) (((text)->str == NULL) || ((text)->len == 0))
#define CM_IS_QUOTE_CHAR(c1) ((c1)== '\'' || (c1) == '"' || (c1) == '`')
#define CM_IS_QUOTE_STRING(c1, c2) ((c1) == (c2) && CM_IS_QUOTE_CHAR(c1))

#define CS_WAIT_FOR_READ  1
#define CS_WAIT_FOR_WRITE 2

#define CM_BUFLEN_32             32
#define CM_BUFLEN_64             64
#define CM_BUFLEN_128            128
#define CM_BUFLEN_256            256
#define CM_BUFLEN_512            512
#define CM_BUFLEN_1K             1024
#define CM_BUFLEN_4K             4096

#define CM_POLL_WAIT               (uint32)50   /* mill-seconds */
#define CM_CONNECT_TIMEOUT         (uint32)60000 /* mill-seconds */
#define CM_SOCKET_TIMEOUT          (uint32)60000 /* mill-seconds */
#define CM_TIME_THOUSAND_UN        (uint32)1000
#define CM_HANDSHAKE_TIMEOUT       (uint32)600000 /* mill-seconds */

#define cm_str_equal(str1, str2)       (strcmp(str1, str2) == 0)

/* Remove the enclosed char or the head and the tail of the text */
#define CM_REMOVE_ENCLOSED_CHAR(text) \
    do {                              \
        ++((text)->str);              \
        (text)->len -= 2;             \
    } while (0)

static inline void cm_assert(bool condition)
{
    if (!condition) {
        *((uint32 *)NULL) = 1;
    }
}

#ifdef CM_DEBUG_VERSION
#define CM_ASSERT(expr) cm_assert((bool)(expr))
#else
#define CM_ASSERT(expr) ((void)(expr))
#endif

static inline void cm_exit(int32 exitcode)
{
    _exit(exitcode);
}

#define CM_TEXT_CLEAR(text) ((text)->len = 0)

#define CM_FILE_NAME_BUFFER_SIZE        (uint32)256
#define CM_MAX_FILE_NAME_LEN            (uint32)(CM_FILE_NAME_BUFFER_SIZE - 1)

static inline void cm_str2text(char *str, text_t *text)
{
    text->str = str;
    text->len = (str == NULL) ? 0 : (uint32)strlen(str);
}


#define UPPER(c) (((c) >= 'a' && (c) <= 'z') ? ((c) - 32) : (c))
#define LOWER(c) (((c) >= 'A' && (c) <= 'Z') ? ((c) + 32) : (c))

#ifndef ELEMENT_COUNT
#define ELEMENT_COUNT(x) ((uint32)(sizeof(x) / sizeof((x)[0])))
#endif


#endif

