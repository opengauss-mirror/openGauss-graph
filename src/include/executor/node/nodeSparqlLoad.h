/* -------------------------------------------------------------------------
 *
 * nodeSparqlLoad.h
 *
 * Portions Copyright (c) 2022, tjudb group
 *
 * src/include/executor/nodeSparqlLoad.h
 *
 * -------------------------------------------------------------------------
 */

#include "nodes/execnodes.h"

extern TupleTableSlot* ExecSparqlLoad(SparqlLoadState* node);

extern SparqlLoadState* ExecInitSparqlLoad(SparqlLoadPlan* node, EState* estate, int eflags);

extern void ExecEndSparqlLoad(SparqlLoadState* node);