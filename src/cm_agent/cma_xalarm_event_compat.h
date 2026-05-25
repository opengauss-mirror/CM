/*
 * Copyright (c) 2026 Huawei Technologies Co.,Ltd.
 *
 * CM is licensed under Mulan PSL v2.
 * -------------------------------------------------------------------------
 *
 * cma_xalarm_event_compat.h
 *
 * Map struct alarm_msg (xalarm_get_event) to struct alarm_info so existing
 * xalarm_get* / HandleXalarm* logic can stay unchanged.
 * See openEuler sysSentry doc: xalarm_register_event / xalarm_get_event.
 * -------------------------------------------------------------------------
 */
#ifndef CMA_XALARM_EVENT_COMPAT_H
#define CMA_XALARM_EVENT_COMPAT_H

#ifdef ENABLE_XALARMD

#ifdef __cplusplus
extern "C" {
#endif

#include <xalarm/register_xalarm.h>

/*
 * Legacy libxalarm headers: void xalarm_unregister_event(struct alarm_register *r);
 * New sysSentry libxalarm:   void xalarm_unregister_event(struct alarm_register **r);
 * ptr_var must be an lvalue of type struct alarm_register * (e.g. local reg or g_xalarmEventRegister).
 */
static inline void CmaXalarmUnregisterEvent(struct alarm_register **register_holder)
{
    if (register_holder == NULL) {
        return;
    }
#ifndef CM_XALARM_LEGACY_REGISTER_ABI
    (void)xalarm_unregister_event(register_holder);
#else
    (void)xalarm_unregister_event(*register_holder);
#endif
}

#ifdef __cplusplus
}
#endif

#include "securec.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Copy fields from alarm_msg into alarm_info for use with xalarm_getid() etc.
 * Layout follows libxalarm / openEuler xalarmd documentation (alarm_info).
 */
static inline void CmaXalarmMsgToAlarmInfo(const struct alarm_msg *msg, struct alarm_info *out)
{
    if (msg == NULL || out == NULL) {
        return;
    }
    (void)memset_s(out, sizeof(*out), 0, sizeof(*out));
    out->usAlarmId = msg->usAlarmId;
    out->AlarmTime = msg->AlarmTime;
    (void)strncpy_s(out->pucParas, sizeof(out->pucParas), msg->pucParas, sizeof(out->pucParas) - 1);
    out->pucParas[sizeof(out->pucParas) - 1] = '\0';
}

#ifdef __cplusplus
}
#endif

#endif /* ENABLE_XALARMD */

#endif /* CMA_XALARM_EVENT_COMPAT_H */
