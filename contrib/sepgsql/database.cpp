/* -------------------------------------------------------------------------
 *
 * contrib/sepgsql/database.c
 *
 * Routines corresponding to database objects
 *
 * Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/dependency.h"
#include "catalog/pg_database.h"
#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "commands/seclabel.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "sepgsql.h"

/*
 * sepgsql_database_post_create
 *
 * This routine assigns a default security label on a newly defined
 * database, and check permission needed for its creation.
 */
void sepgsql_database_post_create(Oid databaseId, const char* dtemplate)
{
    Relation rel;
    ScanKeyData skey;
    SysScanDesc sscan;
    HeapTuple tuple;
    char* tcontext = NULL;
    char* ncontext = NULL;
    char audit_name[NAMEDATALEN + 20];
    ObjectAddress object;
    Form_pg_database datForm;

    /*
     * Oid of the source database is not saved in pg_database catalog, so we
     * collect its identifier using contextual information. If NULL, its
     * default is "template1" according to createdb().
     */
    if (!dtemplate)
        dtemplate = "template1";

    object.classId = DatabaseRelationId;
    object.objectId = get_database_oid(dtemplate, false);
    object.objectSubId = 0;

    tcontext = sepgsql_get_label(object.classId, object.objectId, object.objectSubId);

    /*
     * check db_database:{getattr} permission
     */
    snprintf(audit_name, sizeof(audit_name), "database %s", dtemplate);
    sepgsql_avc_check_perms_label(tcontext, SEPG_CLASS_DB_DATABASE, SEPG_DB_DATABASE__GETATTR, audit_name, true);

    /*
     * Compute a default security label of the newly created database based on
     * a pair of security label of client and source database.
     *
     * XXX - uncoming version of libselinux supports to take object name to
     * handle special treatment on default security label.
     */
    rel = heap_open(DatabaseRelationId, AccessShareLock);

    ScanKeyInit(&skey, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(databaseId));

    sscan = systable_beginscan(rel, DatabaseOidIndexId, true, SnapshotSelf, 1, &skey);
    tuple = systable_getnext(sscan);
    if (!HeapTupleIsValid(tuple))
        elog(ERROR, "catalog lookup failed for database %u", databaseId);

    datForm = (Form_pg_database)GETSTRUCT(tuple);

    ncontext = sepgsql_compute_create(sepgsql_get_client_label(), tcontext, SEPG_CLASS_DB_DATABASE);

    /*
     * check db_database:{create} permission
     */
    snprintf(audit_name, sizeof(audit_name), "database %s", NameStr(datForm->datname));
    sepgsql_avc_check_perms_label(ncontext, SEPG_CLASS_DB_DATABASE, SEPG_DB_DATABASE__CREATE, audit_name, true);

    systable_endscan(sscan);
    heap_close(rel, AccessShareLock);

    /*
     * Assign the default security label on the new database
     */
    object.classId = DatabaseRelationId;
    object.objectId = databaseId;
    object.objectSubId = 0;

    SetSecurityLabel(&object, SEPGSQL_LABEL_TAG, ncontext);

    pfree(ncontext);
    pfree(tcontext);
}

/*
 * sepgsql_database_drop
 *
 * It checks privileges to drop the supplied database
 */
void sepgsql_database_drop(Oid databaseId)
{
    ObjectAddress object;
    char* audit_name = NULL;

    /*
     * check db_database:{drop} permission
     */
    object.classId = DatabaseRelationId;
    object.objectId = databaseId;
    object.objectSubId = 0;
    audit_name = getObjectDescription(&object);

    sepgsql_avc_check_perms(&object, SEPG_CLASS_DB_DATABASE, SEPG_DB_DATABASE__DROP, audit_name, true);
    pfree(audit_name);
}

/*
 * sepgsql_database_relabel
 *
 * It checks privileges to relabel the supplied database with the `seclabel'
 */
void sepgsql_database_relabel(Oid databaseId, const char* seclabel)
{
    ObjectAddress object;
    char* audit_name = NULL;

    object.classId = DatabaseRelationId;
    object.objectId = databaseId;
    object.objectSubId = 0;
    audit_name = getObjectDescription(&object);

    /*
     * check db_database:{setattr relabelfrom} permission
     */
    sepgsql_avc_check_perms(
        &object, SEPG_CLASS_DB_DATABASE, SEPG_DB_DATABASE__SETATTR | SEPG_DB_DATABASE__RELABELFROM, audit_name, true);

    /*
     * check db_database:{relabelto} permission
     */
    sepgsql_avc_check_perms_label(seclabel, SEPG_CLASS_DB_DATABASE, SEPG_DB_DATABASE__RELABELTO, audit_name, true);
    pfree(audit_name);
}
