/* -------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeSeqscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODESEQSCAN_H
#define NODESEQSCAN_H

#include "nodes/execnodes.h"

extern SeqScanState* ExecInitSeqScan(SeqScan* node, EState* estate, int eflags);
extern TupleTableSlot* ExecSeqScan(SeqScanState* node);
extern void ExecEndSeqScan(SeqScanState* node);
extern void ExecSeqMarkPos(SeqScanState* node);
extern void ExecSeqRestrPos(SeqScanState* node);
extern void ExecReScanSeqScan(SeqScanState* node);
extern void InitScanRelation(SeqScanState* node, EState* estate, int eflags);
extern RangeScanInRedis reset_scan_qual(Relation currHeapRel, ScanState *node, bool isRangeScanInRedis = false);

extern ExprState *ExecInitVecExpr(Expr *node, PlanState *parent);

#ifdef GS_GRAPH
extern void ExecNextSeqScanContext(SeqScanState *node);
extern void ExecPrevSeqScanContext(SeqScanState *node);
#endif
#endif /* NODESEQSCAN_H */
