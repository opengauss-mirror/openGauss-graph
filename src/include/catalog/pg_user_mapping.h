/* -------------------------------------------------------------------------
 *
 * pg_user_mapping.h
 *	  definition of the system "user mapping" relation (pg_user_mapping)
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_user_mapping.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PG_USER_MAPPING_H
#define PG_USER_MAPPING_H

#include "catalog/genbki.h"

/* ----------------
 *		pg_user_mapping definition.  cpp turns this into
 *		typedef struct FormData_pg_user_mapping
 * ----------------
 */
#define UserMappingRelationId	1418
#define UserMappingRelation_Rowtype_Id 11647

CATALOG(pg_user_mapping,1418) BKI_SCHEMA_MACRO
{
	Oid			umuser;	    /* Id of the user, InvalidOid if PUBLIC 
                                             * is wanted */
	Oid			umserver;   /* server of this mapping */

#ifdef CATALOG_VARLEN                       /* variable-length fields start here */
	text		umoptions[1];       /* user mapping options */
#endif
} FormData_pg_user_mapping;

/* ----------------
 *		Form_pg_user_mapping corresponds to a pointer to a tuple with
 *		the format of pg_user_mapping relation.
 * ----------------
 */
typedef FormData_pg_user_mapping *Form_pg_user_mapping;

/* ----------------
 *		compiler constants for pg_user_mapping
 * ----------------
 */
#define Natts_pg_user_mapping				3
#define Anum_pg_user_mapping_umuser			1
#define Anum_pg_user_mapping_umserver		2
#define Anum_pg_user_mapping_umoptions		3

extern const char* sensitiveOptionsArray[];
extern const int sensitiveArrayLength;

#endif   /* PG_USER_MAPPING_H */

