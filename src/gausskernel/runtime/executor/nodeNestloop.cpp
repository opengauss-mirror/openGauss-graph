/* -------------------------------------------------------------------------
 *
 * nodeNestloop.cpp
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeNestloop.cpp
 *
 * -------------------------------------------------------------------------
 *
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeNestloop.h"
#include "executor/exec/execStream.h"
#include "utils/memutils.h"
#include "executor/node/nodeHashjoin.h"

#ifdef GS_GRAPH
#include "executor/nodeModifyGraph.h"
#include "executor/tuptable.h"
typedef struct NestLoopContext
{
	dlist_node	list;
	TupleTableSlot *outer_tupleslot;
	HeapTuple	outer_tuple;
} NestLoopContext;
#endif
static void MaterialAll(PlanState* node)
{
    if (IsA(node, MaterialState)) {
        ((MaterialState*)node)->materalAll = true;

        /*
         * Call ExecProcNode to material all the inner tuple first.
         */
        ExecProcNode(node);

        /* early free the left tree of Material. */
        ExecEarlyFree(outerPlanState(node));

        /* early deinit consumer in left tree of Material.
         * It should be noticed that we can not do early deinit 
         * within predpush.
         */
        if (node != NULL && !CheckParamWalker(node)) {
            ExecEarlyDeinitConsumer(node);
        }
    }
}

/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecNestLoop(NestLoopState* node)
{
    TupleTableSlot* outer_tuple_slot = NULL;
    TupleTableSlot* inner_tuple_slot = NULL;
    ListCell* lc = NULL;

    /*
     * get information from the node
     */
    ENL1_printf("getting info from node");

    NestLoop* nl = (NestLoop*)node->js.ps.plan;
    List* joinqual = node->js.joinqual;
    List* otherqual = node->js.ps.qual;
    PlanState* outer_plan = outerPlanState(node);
    PlanState* inner_plan = innerPlanState(node);
    ExprContext* econtext = node->js.ps.ps_ExprContext;

    /*
     * Check to see if we're still projecting out tuples from a previous join
     * tuple (because there is a function-returning-set in the projection
     * expressions).  If so, try to project another one.
     */
    if (node->js.ps.ps_TupFromTlist) {
        ExprDoneCond is_done;

        TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &is_done);
        if (is_done == ExprMultipleResult)
            return result;
        /* Done with that source tuple... */
        node->js.ps.ps_TupFromTlist = false;
    }

    /*
     * Reset per-tuple memory context to free any expression evaluation
     * storage allocated in the previous tuple cycle.  Note this can't happen
     * until we're done projecting out tuples from a join tuple.
     */
    ResetExprContext(econtext);

    /*
     * Ok, everything is setup for the join so now loop until we return a
     * qualifying join tuple.
     */
    ENL1_printf("entering main loop");

    if (node->nl_MaterialAll) {
        MaterialAll(inner_plan);
        node->nl_MaterialAll = false;
    }

    for (;;) {
#ifdef GS_GRAPH
        CommandId	svCid = InvalidCommandId;
#endif        
        /*
         * If we don't have an outer tuple, get the next one and reset the
         * inner scan.
         */
        if (node->nl_NeedNewOuter) {
            ENL1_printf("getting new outer tuple");
            outer_tuple_slot = ExecProcNode(outer_plan);
            /*
             * if there are no more outer tuples, then the join is complete..
             */
            if (TupIsNull(outer_tuple_slot)) {
                ExecEarlyFree(inner_plan);
                ExecEarlyFree(outer_plan);

                EARLY_FREE_LOG(elog(LOG,
                    "Early Free: NestLoop is done "
                    "at node %d, memory used %d MB.",
                    (node->js.ps.plan)->plan_node_id,
                    getSessionMemoryUsageMB()));

                ENL1_printf("no outer tuple, ending join");

                return NULL;
            }

            ENL1_printf("saving new outer tuple information");
            econtext->ecxt_outertuple = outer_tuple_slot;
            node->nl_NeedNewOuter = false;
            node->nl_MatchedOuter = false;

            /*
             * fetch the values of any outer Vars that must be passed to the
             * inner scan, and store them in the appropriate PARAM_EXEC slots.
             */
            foreach (lc, nl->nestParams) {
                NestLoopParam* nlp = (NestLoopParam*)lfirst(lc);
                int paramno = nlp->paramno;
                ParamExecData* prm = NULL;

                prm = &(econtext->ecxt_param_exec_vals[paramno]);
                /* Param value should be an OUTER_VAR var */
                Assert(IsA(nlp->paramval, Var));
                Assert(nlp->paramval->varno == OUTER_VAR);
                Assert(nlp->paramval->varattno > 0);
                Assert(outer_tuple_slot != NULL && outer_tuple_slot->tts_tupleDescriptor != NULL);
                /* Get the Table Accessor Method*/
                prm->value = tableam_tslot_getattr(outer_tuple_slot, nlp->paramval->varattno, &(prm->isnull));
                /*
                 * the following two parameters are called when there exist
                 * join-operation with column table (see ExecEvalVecParamExec).
                 */
                prm->valueType = outer_tuple_slot->tts_tupleDescriptor->tdtypeid;
                prm->isChanged = true;
                /* Flag parameter value as changed */
                inner_plan->chgParam = bms_add_member(inner_plan->chgParam, paramno);
            }

            /*
             * now rescan the inner plan
             */
            ENL1_printf("rescanning inner plan");
            ExecReScan(inner_plan);
        }

        /*
         * we have an outerTuple, try to get the next inner tuple.
         */
        ENL1_printf("getting new inner tuple");

#ifdef GS_GRAPH
        if (node->js.jointype == JOIN_CYPHER_MERGE ||
			node->js.jointype == JOIN_CYPHER_DELETE)
		{
			svCid = inner_plan->state->es_snapshot->curcid;
			inner_plan->state->es_snapshot->curcid = node->nl_graphwrite_cid;
		}
#endif        

        /*
         * If inner plan is mergejoin, which does not cache data,
         * but will early free the left and right tree's caching memory.
         * When rescan left tree, may fail.
         */
        bool orig_value = inner_plan->state->es_skip_early_free;
        if (!IsA(inner_plan, MaterialState))
            inner_plan->state->es_skip_early_free = true;

        inner_tuple_slot = ExecProcNode(inner_plan);

        inner_plan->state->es_skip_early_free = orig_value;
        econtext->ecxt_innertuple = inner_tuple_slot;

#ifdef GS_GRAPH
        if (svCid != InvalidCommandId)
			inner_plan->state->es_snapshot->curcid = svCid;
#endif

        if (TupIsNull(inner_tuple_slot)) {
            ENL1_printf("no inner tuple, need new outer tuple");

            node->nl_NeedNewOuter = true;

#ifdef GS_GRAPH
            if (!node->nl_MatchedOuter && (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_CYPHER_MERGE ||
				                            node->js.jointype == JOIN_CYPHER_DELETE || node->js.jointype == JOIN_ANTI ||
                                              node->js.jointype == JOIN_LEFT_ANTI_FULL)) {
#else
            if (!node->nl_MatchedOuter && (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI ||
                                              node->js.jointype == JOIN_LEFT_ANTI_FULL)) {
#endif
                /*
                 * We are doing an outer join and there were no join matches
                 * for this outer tuple.  Generate a fake join tuple with
                 * nulls for the inner tuple, and return it if it passes the
                 * non-join quals.
                 */
                econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

                ENL1_printf("testing qualification for outer-join tuple");

                if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                    /*
                     * qualification was satisfied so we project and return
                     * the slot containing the result tuple using
                     * function ExecProject.
                     */
                    ExprDoneCond is_done;

                    ENL1_printf("qualification succeeded, projecting tuple");

                    TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &is_done);

                    if (is_done != ExprEndResult) {
                        node->js.ps.ps_TupFromTlist = (is_done == ExprMultipleResult);
                        return result;
                    }
                } else
                    InstrCountFiltered2(node, 1);
            }

            /*
             * Otherwise just return to top of loop for a new outer tuple.
             */
            continue;
        }

        /*
         * at this point we have a new pair of inner and outer tuples so we
         * test the inner and outer tuples to see if they satisfy the node's
         * qualification.
         *
         * Only the joinquals determine MatchedOuter status, but all quals
         * must pass to actually return the tuple.
         */
        ENL1_printf("testing qualification");

        if (ExecQual(joinqual, econtext, false)) {
            node->nl_MatchedOuter = true;

            /* In an antijoin, we never return a matched tuple */
            if (node->js.jointype == JOIN_ANTI || node->js.jointype == JOIN_LEFT_ANTI_FULL) {
                node->nl_NeedNewOuter = true;
                continue; /* return to top of loop */
            }

            /*
             * In a semijoin, we'll consider returning the first match, but
             * after that we're done with this outer tuple.
             */
            if (node->js.jointype == JOIN_SEMI)
                node->nl_NeedNewOuter = true;

#ifdef GS_GRAPH
            if (node->js.single_match)
				node->nl_NeedNewOuter = true;
#endif

            if (otherqual == NIL || ExecQual(otherqual, econtext, false)) {
                /*
                 * qualification was satisfied so we project and return the
                 * slot containing the result tuple using ExecProject().
                 */
                ExprDoneCond is_done;

                ENL1_printf("qualification succeeded, projecting tuple");

                TupleTableSlot* result = ExecProject(node->js.ps.ps_ProjInfo, &is_done);

                if (is_done != ExprEndResult) {
                    node->js.ps.ps_TupFromTlist = (is_done == ExprMultipleResult);
                    /*
                     * @hdfs
                     * Optimize plan by informational constraint.
                     */
                    if (((NestLoop*)(node->js.ps.plan))->join.optimizable) {
                        node->nl_NeedNewOuter = true;
                    }

                    return result;
                }
            } else
                InstrCountFiltered2(node, 1);
        } else
            InstrCountFiltered1(node, 1);

        /*
         * Tuple fails qual, so free per-tuple memory and try again.
         */
        ResetExprContext(econtext);

        ENL1_printf("qualification failed, looping");
    }
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState* ExecInitNestLoop(NestLoop* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    NL1_printf("ExecInitNestLoop: %s\n", "initializing node");

    /*
     * create state structure
     */
    NestLoopState* nlstate = makeNode(NestLoopState);
#ifdef GS_GRAPH
    CommandId	svCid = InvalidCommandId;
#endif
    nlstate->js.ps.plan = (Plan*)node;
    nlstate->js.ps.state = estate;
    nlstate->nl_MaterialAll = node->materialAll;

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &nlstate->js.ps);

    /*
     * initialize child expressions
     */
    nlstate->js.ps.targetlist = (List*)ExecInitExpr((Expr*)node->join.plan.targetlist, (PlanState*)nlstate);
    nlstate->js.ps.qual = (List*)ExecInitExpr((Expr*)node->join.plan.qual, (PlanState*)nlstate);
    nlstate->js.jointype = node->join.jointype;
    nlstate->js.joinqual = (List*)ExecInitExpr((Expr*)node->join.joinqual, (PlanState*)nlstate);
    Assert(node->join.nulleqqual == NIL);

    /*
     * initialize child nodes
     *
     * If we have no parameters to pass into the inner rel from the outer,
     * tell the inner child that cheap rescans would be good.  If we do have
     * such parameters, then there is no point in REWIND support at all in the
     * inner child, because it will always be rescanned with fresh parameter
     * values.
     */
    outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
    if (node->nestParams == NIL)
        eflags |= EXEC_FLAG_REWIND;
    else
        eflags &= ~EXEC_FLAG_REWIND;

#ifdef GS_GRAPH
    if (node->join.jointype == JOIN_CYPHER_MERGE ||
		node->join.jointype == JOIN_CYPHER_DELETE)
	{
		/*
		 * Modify the CID to see the graph pattern created by MERGE CREATE
		 * or to not see it deleted by DELETE.
		 */
		nlstate->nl_graphwrite_cid =
						estate->es_snapshot->curcid + MODIFY_CID_NLJOIN_MATCH;

		svCid = estate->es_snapshot->curcid;
		estate->es_snapshot->curcid = nlstate->nl_graphwrite_cid;
	}
#endif

    innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &nlstate->js.ps);

#ifdef GS_GRAPH
    /*
	 * detect whether we need only consider the first matching inner tuple
	 */
	nlstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);
#endif

    switch (node->join.jointype) {
        case JOIN_INNER:
        case JOIN_SEMI:
#ifdef GS_GRAPH
        case JOIN_VLE:
#endif        
            break;
        case JOIN_LEFT:
        case JOIN_ANTI:
#ifdef GS_GRAPH
        case JOIN_CYPHER_MERGE:
		case JOIN_CYPHER_DELETE:
#endif
        case JOIN_LEFT_ANTI_FULL:
            nlstate->nl_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(nlstate)));
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmodule(MOD_EXECUTOR),
                    errmsg("unrecognized join type: %d when initializing nestLoop", (int)node->join.jointype)));
    }

    /*
     * initialize tuple type and projection info
     * the result in this case would hold only virtual data.
     */
    ExecAssignResultTypeFromTL(&nlstate->js.ps, TAM_HEAP);

    ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

#ifdef GS_GRAPH
    dlist_init(&nlstate->ctxs_head);
	nlstate->prev_ctx_node = &nlstate->ctxs_head.head;
#endif

    /*
     * finally, wipe the current outer tuple clean.
     */
    nlstate->js.ps.ps_TupFromTlist = false;
    nlstate->nl_NeedNewOuter = true;
    nlstate->nl_MatchedOuter = false;

    NL1_printf("ExecInitNestLoop: %s\n", "node initialized");

    return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndNestLoop(NestLoopState* node)
{
    NL1_printf("ExecEndNestLoop: %s\n", "ending node processing");

    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->js.ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

#ifdef GS_GRAPH
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &node->ctxs_head)
	{
		NestLoopContext *ctx;

		dlist_delete(iter.cur);

		ctx = dlist_container(NestLoopContext, list, iter.cur);
		pfree(ctx);
	}
	node->prev_ctx_node = &node->ctxs_head.head;
#endif

    /*
     * close down subplans
     */
    ExecEndNode(outerPlanState(node));
    ExecEndNode(innerPlanState(node));

    NL1_printf("ExecEndNestLoop: %s\n", "node processing ended");
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void ExecReScanNestLoop(NestLoopState* node)
{
    PlanState* outer_plan = outerPlanState(node);
    PlanState* inner_plan = innerPlanState(node);
    PlanState ps = node->js.ps;

    /*
     * If outer_plan->chgParam is not null then plan will be automatically
     * re-scanned by first ExecProcNode.
     */
    if (outer_plan->chgParam == NULL)
        ExecReScan(outer_plan);

    /*
     * Under recursive-stream condition, need to rescan
     * both outer_plan and inner_plan.
     */
    if (IS_PGXC_DATANODE && EXEC_IN_RECURSIVE_MODE(ps.plan) && ((ps.state)->es_recursive_next_iteration)) {
        ExecReScan(inner_plan);
        node->nl_MaterialAll = ((NestLoop*)ps.plan)->materialAll;
    }

    /*
     * inner_plan is re-scanned for each new outer tuple and MUST NOT be
     * re-scanned from here or you'll get troubles from inner index scans when
     * outer Vars are used as run-time keys...
     */
    node->js.ps.ps_TupFromTlist = false;
    node->nl_NeedNewOuter = true;
    node->nl_MatchedOuter = false;
}
#ifdef GS_GRAPH
void ExecNextNestLoopContext(NestLoopState *node)
{
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	dlist_node *ctx_node;
	NestLoopContext *ctx;
	TupleTableSlot *slot;

	/*
	 * This nested loop is supposed to be for vertex-edge join to get vertices
	 * for graphpath results, and that means it is the top most (and only)
	 * innerPlan of NestLoopVLE. Since it is already executed at this point,
	 * its chgParam is already NULL. So, we don't need to manage chgParam.
	 */
	Assert(node->js.ps.chgParam == NULL);
	Assert(node->js.jointype == JOIN_INNER);

	/* get the current context */
	if (dlist_has_next(&node->ctxs_head, node->prev_ctx_node))
	{
		ctx_node = dlist_next_node(&node->ctxs_head, node->prev_ctx_node);
		ctx = dlist_container(NestLoopContext, list, ctx_node);
	}
	else
	{
		ctx = (NestLoopContext*)palloc(sizeof(*ctx));
		ctx_node = &ctx->list;

		dlist_push_tail(&node->ctxs_head, ctx_node);
	}

	slot = econtext->ecxt_outertuple;
	if (TTS_IS_HEAPTUPLE(slot))
	{
		HeapTupleTableSlot *heapTupleTableSlot = (HeapTupleTableSlot *) slot;

		/*
		 * If tts_tuple is the same with the stored one, remove it from the
		 * slot to keep this copy from ExecStoreTuple()/ExecClearTuple() in
		 * the next nested loop.
		 *
		 * This can happen when there is a matched result with the tuple.
		 */
		if (heapTupleTableSlot->tuple == ctx->outer_tuple)
		{
			slot->tts_isempty |= TTS_FLAG_EMPTY;
			slot->tts_nvalid = 0;
			ItemPointerSetInvalid(&heapTupleTableSlot->tuple->t_self);
		}
	}
	else
	{
		/*
		 * Keep the latest outer tuple slot to 1) set ecxt_outertuple to it
		 * later when continuing the current nested loop after the end of the
		 * next nested loop, and 2) use the original slot rather than a
		 * temporary slot that requires extra resources.
		 * We get the slot through ecxt_outertuple instead of
		 * outerPlanState(node)->ps_ResultTupleSlot because
		 * outerPlanState(node) can be AppendState.
		 */
		ctx->outer_tupleslot = slot;
		/*
		 * We need to copy and store the current outer tuple here because;
		 * 1) there might be a chance to unpin the underlying buffer that the
		 *    slot relies on while doing ExecStoreTuple() in the next nested
		 *    loop, and
		 * 2) when continuing the current nested loop later, the inner plan
		 *    needs the right outer variables that are in the slot.
		 * The tuple has to be stored in CurrentMemoryContext.
		 */
		ctx->outer_tuple = ExecCopySlotHeapTuple(slot);
	}
	/*
	 * We don't need to care about the inner plan and nl_NeedNewOuter because
	 * the next execution of the current nested loop must execute the inner
	 * plan.
	 */

	/* make the current context previous context */
	node->prev_ctx_node = ctx_node;

	/*
	 * We don't have to restore the current outer tuple slot because it will be
	 * filled with values of the first scan result of the outer plan.
	 */

	ExecNextContext(outerPlanState(node));
	ExecNextContext(innerPlanState(node));

	node->nl_NeedNewOuter = true;
}

void ExecPrevNestLoopContext(NestLoopState *node)
{
	NestLoop   *nl = (NestLoop *) node->js.ps.plan;
	ExprContext *econtext = node->js.ps.ps_ExprContext;
	dlist_node *ctx_node;
	NestLoopContext *ctx;
	TupleTableSlot *slot;
	ListCell   *lc;

	/*
	 * We don't have to store the current outer tuple slot because of the same
	 * reason above.
	 */

	/* if chgParam is not NULL, free it now */
	if (node->js.ps.chgParam != NULL)
	{
		bms_free(node->js.ps.chgParam);
		node->js.ps.chgParam = NULL;
	}

	/* make the previous context current context */
	ctx_node = node->prev_ctx_node;
	Assert(ctx_node != &node->ctxs_head.head);

	if (dlist_has_prev(&node->ctxs_head, ctx_node))
		node->prev_ctx_node = dlist_prev_node(&node->ctxs_head, ctx_node);
	else
		node->prev_ctx_node = &node->ctxs_head.head;

	ctx = dlist_container(NestLoopContext, list, ctx_node);
	slot = ctx->outer_tupleslot;
	econtext->ecxt_outertuple = slot;
	/*
	 * Pass true to shouldFree here because the tuple must be freed when
	 * ExecStoreTuple(), ExecClearTuple(), or ExecResetTupleTable() is called.
	 */
	ExecForceStoreHeapTuple(ctx->outer_tuple, slot, true);
	/* restore outer variables */
	foreach(lc, nl->nestParams)
	{
		NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
		int			paramno = nlp->paramno;
		ParamExecData *prm;

		prm = &(econtext->ecxt_param_exec_vals[paramno]);
		/* Param value should be an OUTER_VAR var */
		Assert(IsA(nlp->paramval, Var));
		Assert(nlp->paramval->varno == OUTER_VAR);
		Assert(nlp->paramval->varattno > 0);
		prm->value = slot_getattr(slot,
								  nlp->paramval->varattno,
								  &(prm->isnull));
	}

	ExecPrevContext(outerPlanState(node));
	ExecPrevContext(innerPlanState(node));

	/*
	 * The next execution of the current nested loop must execute the inner
	 * plan.
	 */
	node->nl_NeedNewOuter = false;
}
#endif