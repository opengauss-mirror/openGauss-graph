/*-------------------------------------------------------------------------
 *
 * gs_graphmeta.h
 *	  definition of the system "graphmeta" relation (gs_graphmeta)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/gs_graphmeta.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef GS_GRAPHMETA_H
#define GS_GRAPHMETA_H

#include "catalog/genbki.h"
// #include "utils/graph.h"
#include "pgstat.h"

/* ----------------
 *		gs_graphmeta definition.  cpp turns this into
 *		typedef struct FormData_gs_graphmeta
 * ----------------
 */
#define GraphMetaRelationId	8155

CATALOG(gs_graphmeta,8155) BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
	Oid			graph;			/* graph oid */
	int2		edge;			/* edge label id */
	int2		start;			/* start vertex label id */
	int2		end;			/* end vertex label id */
	int8		edgecount;		/* # of edge between start and end */
} FormData_gs_graphmeta;

/* ----------------
 *		Form_ag_graphmeta corresponds to a pointer to a tuple with
 *		the format of ag_graphmeta relation.
 * ----------------
 */
typedef FormData_gs_graphmeta *Form_gs_graphmeta;

/* ----------------
 *		compiler constants for ag_graphmeta
 * ----------------
 */

#define Natts_gs_graphmeta			5
#define Anum_gs_graphmeta_graph		1
#define Anum_gs_graphmeta_edge		2
#define Anum_gs_graphmeta_start		3
#define Anum_gs_graphmeta_end		4
#define Anum_gs_graphmeta_edgecount	5

typedef struct GsStat_key
{
	Oid		graph;
	Labid	edge;
	Labid	start;
	Labid	end;
} GsStat_key;

typedef struct GsStat_GraphMeta
{
	struct GsStat_key	key;

	PgStat_Counter		edges_inserted;
	PgStat_Counter		edges_deleted;
} GsStat_GraphMeta;

#endif   /* GS_GRAPHMETA_H */