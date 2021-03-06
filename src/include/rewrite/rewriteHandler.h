/* -------------------------------------------------------------------------
 *
 * rewriteHandler.h
 *		External interface to query rewriter.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/rewrite/rewriteHandler.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef REWRITEHANDLER_H
#define REWRITEHANDLER_H

#include "utils/relcache.h"
#include "nodes/parsenodes.h"

extern List* QueryRewrite(Query* parsetree);
extern void AcquireRewriteLocks(Query* parsetree, bool forUpdatePushedDown);
extern Node* build_column_default(Relation rel, int attrno, bool isInsertCmd = false);
extern List* pull_qual_vars(Node* node, int varno = 0, int flags = 0, bool nonRepeat = false);
extern void rewriteTargetListMerge(Query* parsetree, Index result_relation, List* range_table);

#ifdef PGXC
extern List* QueryRewriteCTAS(Query* parsetree);
extern List* QueryRewriteRefresh(Query *parsetree);
#endif

#endif /* REWRITEHANDLER_H */
