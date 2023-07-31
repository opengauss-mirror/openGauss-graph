/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * GaussAlarm_client.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/alarm/GaussAlarm_client.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef __GAUSS_ALARM_CLENT_H__
#define __GAUSS_ALARM_CLENT_H__

#ifdef __cplusplus
#if __cplusplus
extern "C" {
#endif
#endif

#ifndef CM_ALARM_TYPE_MACRO
/* �澯���� */
typedef enum tagCM_ALARM_TYPE {
    ALM_ALARM_TYPE_FAULT = 0, /* ���� */
    // ALM_ALARM_TYPE_EVENT  = 1,   /* �¼� */
    ALM_ALARM_TYPE_RESUME = 2, /* �ָ� */
    ALM_ALARM_TYPE_OPLOG = 3,  /* ������־ */
    ALM_ALARM_TYPE_EVENT = 4,  /* �¼�--> ������־ */
    ALM_ALARM_TYPE_DELETE = 5, /* ɾ�� */

} CM_ALARM_TYPE;
#define CM_ALARM_TYPE_MACRO
#endif /* #ifndef CM_ALARM_TYPE_MACRO */

#ifndef CM_SUBSYSTEM_ID_MACRO
/* �澯ģ��ID���� */
typedef enum tagCM_SUBSYSTEM_ID {
    SUBSYSID_CM = 0,      /* CM */
    SUBSYSID_PRO = 1,     /* protocol */
    SUBSYSID_TS = 2,      /* TSģ�� */
    SUBSYSID_CA = 3,      /* CA */
    SUBSYSID_MDS = 4,     /* MDS */
    SUBSYSID_DS = 5,      /* DS*/
    SUBSYSID_DLM = 6,     /* BASE DLM*/
    SUBSYSID_MONC = 7,    /* MONC */
    SUBSYSID_TRNS = 8,    /* TRNS */
    SUBSYSID_NVCACHE = 9, /* NVCACHE */
    SUBSYSID_PMA = 10,    /* PMA */
    SUBSYSID_BASE = 11,   /* BASE */
    SUBSYSID_MONS = 12,   /* MONS */
    SUBSYSID_NOFS = 13,   /* NOFS */

    SUBSYSID_SQL = 20, /* ��˹���ݿ� */
    SUBSYSID_HD = 21,  /* hadoop */
    SUBSYSID_MDM = 22, /* �ļ�ϵͳԪ���ݹ��� */

    SUBSYSID_BUTT /* ���ֵ */
} CM_SUBSYSTEM_ID;
#define CM_SUBSYSTEM_ID_MACRO
#endif /*# ifndef CM_SUBSYSTEM_ID_MACRO */

/**
���Ʒ���͸澯.
alarmMsg����󳤶�Ϊ1024���������ֻᱻ�ض�
����ֵ�� 0 �ɹ�����0 ʧ��
*/
int Gauss_alarm_report(int moduleID, long long alarmID, CM_ALARM_TYPE type, char* alarmMsg, int msgLength);

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif

#endif  //__GAUSS_ALARM_CLENT_H__