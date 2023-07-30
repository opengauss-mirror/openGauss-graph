/*-------------------------------------------------------------------------
 *
 * gs_graph.h
 *	  definition of the system "graph" relation (gs_graph)
 *	  along with the relation's initial contents.
 *
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * src/include/catalog/gs_graph.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
// #ifdef GS_GRAPH
#ifndef GS_GRAPH_H
#define GS_GRAPH_H

#include "catalog/genbki.h"

/* ----------------
 *		gs_graph definition.  cpp turns this into
 *		typedef struct FormData_gs_graph
 * ----------------
 */
#define GraphRelationId	8140

CATALOG(gs_graph,8140) BKI_SCHEMA_MACRO
{
	NameData	graphname;
	Oid			nspid;
} FormData_gs_graph;

/* ----------------
 *		Form_gs_graph corresponds to a pointer to a tuple with
 *		the format of gs_graph relation.
 * ----------------
 */
typedef FormData_gs_graph *Form_gs_graph;

/* ----------------
 *		compiler constants for gs_graph
 * ----------------
 */

#define Natts_gs_graph					2
#define Anum_gs_graph_graphname			1
#define Anum_gs_graph_nspid				2

#endif   /* GS_GRAPH_H */
// #endif /* GS_GRAPH */