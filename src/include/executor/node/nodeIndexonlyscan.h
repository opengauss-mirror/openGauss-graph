/* -------------------------------------------------------------------------
 *
 * nodeIndexonlyscan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeIndexonlyscan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEINDEXONLYSCAN_H
#define NODEINDEXONLYSCAN_H

#include "nodes/execnodes.h"

extern IndexOnlyScanState* ExecInitIndexOnlyScan(IndexOnlyScan* node, EState* estate, int eflags);
extern TupleTableSlot* ExecIndexOnlyScan(IndexOnlyScanState* node);
extern void ExecEndIndexOnlyScan(IndexOnlyScanState* node);
extern void ExecIndexOnlyMarkPos(IndexOnlyScanState* node);
extern void ExecIndexOnlyRestrPos(IndexOnlyScanState* node);
extern void ExecReScanIndexOnlyScan(IndexOnlyScanState* node);
extern bool ExecGPIGetNextPartRelation(IndexOnlyScanState* node, IndexScanDesc indexScan);
extern bool ExecCBIFixHBktRel(IndexScanDesc indexScan, Buffer *vmbuffer);
extern void StoreIndexTuple(TupleTableSlot* slot, IndexTuple itup, TupleDesc itupdesc);

#ifdef GS_GRAPH
extern void ExecNextIndexOnlyScanContext(IndexOnlyScanState *node);
extern void ExecPrevIndexOnlyScanContext(IndexOnlyScanState *node);
#endif
#endif /* NODEINDEXONLYSCAN_H */
