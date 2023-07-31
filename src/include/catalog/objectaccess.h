/*
 * objectaccess.h
 *
 *		Object access hooks.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#ifndef OBJECTACCESS_H
#define OBJECTACCESS_H

/*
 * Object access hooks are intended to be called just before or just after
 * performing certain actions on a SQL object.	This is intended as
 * infrastructure for security or logging pluggins.
 *
 * OAT_POST_CREATE should be invoked just after the object is created.
 * Typically, this is done after inserting the primary catalog records and
 * associated dependencies.
 *
 * OAT_DROP should be invoked just before deletion of objects; typically
 * deleteOneObject(). Its arguments are packed within ObjectAccessDrop.
 *
 * Other types may be added in the future.
 */
typedef enum ObjectAccessType
{
	OAT_POST_CREATE,
	OAT_DROP,
	OAT_POST_ALTER,
} ObjectAccessType;

typedef struct
{
	/*
	 * This identifier is used when system catalog takes two IDs to identify a
	 * particular tuple of the catalog. It is only used when the caller want
	 * to identify an entry of pg_inherits, pg_db_role_setting or
	 * pg_user_mapping. Elsewhere, InvalidOid should be set.
	 */
	Oid			auxiliary_id;

	/*
	 * If this flag is set, the user hasn't requested that the object be
	 * altered, but we're doing it anyway for some internal reason.
	 * Permissions-checking hooks may want to skip checks if, say, we're alter
	 * the constraints of a temporary heap during CLUSTER.
	 */
	bool		is_internal;
} ObjectAccessPostAlter;

/*
 * Arguments of OAT_DROP event
 */
typedef struct
{
	/*
	 * Flags to inform extensions the context of this deletion. Also see
	 * PERFORM_DELETION_* in dependency.h
	 */
	int			dropflags;
} ObjectAccessDrop;

/*
 * Hook, and a macro to invoke it.
 */
typedef void (*object_access_hook_type) (ObjectAccessType access,
													 Oid classId,
													 Oid objectId,
													 int subId,
													 void *arg);

extern THR_LOCAL PGDLLIMPORT object_access_hook_type object_access_hook;

#define InvokeObjectAccessHook(access,classId,objectId,subId,arg)	\
	do {															\
		if (object_access_hook)										\
			(*object_access_hook)((access),(classId),				\
								  (objectId),(subId),(arg));		\
	} while(0)

extern void RunObjectPostAlterHook(Oid classId, Oid objectId, int subId,
					   Oid auxiliaryId, bool is_internal);

#define InvokeObjectPostAlterHook(classId,objectId,subId)			\
	InvokeObjectPostAlterHookArg((classId),(objectId),(subId),		\
								 InvalidOid,false)

#define InvokeObjectPostAlterHookArg(classId,objectId,subId,		\
									 auxiliaryId,is_internal)		\
	do {															\
		if (object_access_hook)										\
			RunObjectPostAlterHook((classId),(objectId),(subId),	\
								   (auxiliaryId),(is_internal));	\
	} while(0)

#define ObjectAddressSubSet(addr, class_id, object_id, object_sub_id) \
	do { \
		(addr).classId = (class_id); \
		(addr).objectId = (object_id); \
		(addr).objectSubId = (object_sub_id); \
	} while (0)
	
#define ObjectAddressSet(addr, class_id, object_id) 				\
	ObjectAddressSubSet(addr, class_id, object_id, 0)
	
#endif   /* OBJECTACCESS_H */
