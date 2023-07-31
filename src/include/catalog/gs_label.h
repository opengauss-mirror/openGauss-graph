/*-------------------------------------------------------------------------
 *
 * This file has referenced pg_class.h
 *	  definition of the system "label" relation (gs_label)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/gs_label.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef GS_LABEL_H
#define GS_LABEL_H

#include "catalog/genbki.h"

/* ----------------
 *		gs_label definition.  cpp turns this into
 *		typedef struct FormData_gs_label
 * ----------------
 */
#define LabelRelationId	8145

CATALOG(gs_label,8145) BKI_SCHEMA_MACRO
{
	NameData	labname;		/* label name */
	Oid			graphid;		/* graph oid */
	int4		labid;			/* label ID in a graph */
	Oid			relid;			/* table oid under the label */
	char		labkind;		/* see LABEL_KIND_XXX constants below */
} FormData_gs_label;

/* ----------------
 *		Form_gs_label corresponds to a pointer to a tuple with
 *		the format of gs_label relation.
 * ----------------
 */
typedef FormData_gs_label *Form_gs_label;

/* ----------------
 *		compiler constants for gs_label
 * ----------------
 */

#define Natts_gs_label			5
#define Anum_gs_label_labname	1
#define Anum_gs_label_graphid	2
#define Anum_gs_label_labid		3
#define Anum_gs_label_relid		4
#define Anum_gs_label_labkind	5

#define LABEL_KIND_VERTEX	'v'
#define LABEL_KIND_EDGE		'e'

#endif   /* GS_LABEL_H */
