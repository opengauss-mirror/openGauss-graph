/*
 * gs_graph_fn.h
 *	  prototypes for functions in backend/catalog/gs_graph.c
 *
 * Copyright (c) 2016 by Bitnine Global, Inc.
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_graph_fn.h
 */
#ifndef GS_GRAPH_FN_H
#define GS_GRAPH_FN_H

#include "nodes/parsenodes.h"

extern THR_LOCAL char *graph_path;
extern bool enableGraphDML;

extern char *get_graph_path(bool lookup_cache);
extern Oid get_graph_path_oid(void);

extern Oid GraphCreate(CreateGraphStmt *stmt, const char *queryString);

#endif	/* GS_GRAPH_FN_H */
