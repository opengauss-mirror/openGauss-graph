/*-------------------------------------------------------------------------
 *
 * aclchk_internal.h
 *
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/aclchk_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ACLCHK_INTERNAL_H
#define ACLCHK_INTERNAL_H

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

/*
 * The information about one Grant/Revoke statement, in internal format: object
 * and grantees names have been turned into Oids, the privilege list is an AclMode bitmask.
 * If 'privileges' is ACL_NO_RIGHTS (the 0 value) and and 'ddl_privileges' is ACL_NO_DDL_RIGHTS and
 * all_privs is true, 'privileges' and 'ddl_privileges' will be internally set to the right kind of
 * ACL_ALL_RIGHTS_* and ACL_ALL_DDL_RIGHTS_* respectively, depending on the object type
 * (NB - this will modify the InternalGrant struct!)
 *
 * Note: 'all_privs', 'privileges' and 'ddl_privileges' represent object-level privileges only.
 * There might also be column-level privilege specifications, which are
 * represented in col_privs and col_ddl_privs (this is a list of untransformed AccessPriv nodes).
 * Column privileges are only valid for objtype ACL_OBJECT_RELATION.
 */
typedef struct {
    bool is_grant;
    GrantObjectType objtype;
    List* objects;
    bool all_privs;
    AclMode privileges;
    AclMode ddl_privileges;
    List* col_privs;
    List* col_ddl_privs;
    List* grantees;
    bool grant_option;
    DropBehavior behavior;
} InternalGrant;


#endif							/* ACLCHK_INTERNAL_H */
