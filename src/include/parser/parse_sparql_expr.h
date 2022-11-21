/* -------------------------------------------------------------------------
 *
 * parse_startwith.cpp
 *    Implement start with related modules in transform state
 *
 * Portions Copyright (c) 2022, tjudb group
 *
 *
 * IDENTIFICATION
 *    src/include/parser/parse_sparql_expr.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PARSE_SPARQL_EXPR_H
#define PARSE_SPARQL_EXPR_H

#include "parser/parse_node.h"

/* GUC variable (enable/disable null properties) */
extern bool allow_null_properties;

extern Query* transformSparqlLoadStmt(ParseState *pstate, SparqlLoadStmt* LoadStmt);
extern Query* transformSparqlSelectStmt(ParseState* pstate, SparqlSelectStmt* stmt);
extern Query* transformSparqlInsertStmt(ParseState* pstate, SparqlInsertStmt* stmt);
extern Query* transformSparqlDeleteDataStmt(ParseState* pstate, SparqlDeleteDataStmt* stmt);
#define NUMERIC_TAG "^\\d+$"
#define default_loc -1

#endif  /* PARSE_SPARQL_EXPR_H */