/*
 * Copyright (c) 2022 tju dbgroup.
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
 * -------------------------------------------------------------------------
 *
 * gs_sparql_prefix.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_sparql_prefix.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_SPARQL_PREFIX_H
#define GS_SPARQL_PREFIX_H

#include "catalog/genbki.h"

#define SparqlPrefixRelationOid 1061

/* ----------------
 *        gs_sparql_prefix definition.  cpp turns this into
 *        typedef struct FormData_gs_sparql_prefix
 * ----------------
 */
CATALOG(gs_sparql_prefix,1061) BKI_SCHEMA_MACRO
{
    NameData prefix;
} FormData_gs_sparql_prefix;


/* ----------------
 *		Form_gs_sparql_prefix corresponds to a pointer to a tuple with
 *		the format of gs_sparql_prefix relation.
 * ----------------
 */
typedef FormData_gs_sparql_prefix* Form_gs_sparql_prefix;


/* ----------------
 *		compiler constants for gs_label
 * ----------------
 */

#define Natts_gs_sparql_prefix	1
#define Anum_gs_sparql_prefix	1

DATA(insert OID = 1 ("test"));

#endif