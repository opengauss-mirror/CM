/* -------------------------------------------------------------------------
 *
 * pqcomm.h
 *		Definitions common to frontends and backends.
 *
 * NOTE: for historical reasons, this does not correspond to pqcomm.c.
 * pqcomm.c's routines are declared in libpq.h.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/libpq/pqcomm.h,v 1.109 2008/10/28 12:10:44 mha Exp $
 *
 * -------------------------------------------------------------------------
 */
#ifndef PQCOMM_H
#define PQCOMM_H

#include <sys/socket.h>
#include <netdb.h>
#ifdef HAVE_SYS_UN_H
#include <sys/un.h>
#endif
#include <netinet/in.h>

typedef struct {
    struct sockaddr_storage addr;
    size_t salen;
} SockAddr;

/*
 * In protocol 3.0 and later, the startup packet length is not fixed, but
 * we set an arbitrary limit on it anyway.	This is just to prevent simple
 * denial-of-service attacks via sending enough data to run the server
 * out of memory.
 */
#define MAX_STARTUP_PACKET_LENGTH 10000
/* These are the authentication request codes sent by gtm server. */
#define GTM_AUTH_REQ_OK 0       /* User is authenticated */
#define GTM_AUTH_REQ_GSS 1      /* GSSAPI without wrap() */
#define GTM_AUTH_REQ_GSS_CONT 2 /* Continue GSS exchange */

#endif /* PQCOMM_H */
