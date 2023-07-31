/* -------------------------------------------------------------------------
 *
 * streamplan_single.cpp
 *      functions related to stream plan.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/plan/streamplan_single.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include <pthread.h>
#include "access/heapam.h"
#include "access/transam.h"
#include "access/hash.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "catalog/pgxc_node.h"
#include "commands/explain.h"
#include "commands/tablecmds.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/dataskew.h"
#include "optimizer/nodegroups.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "optimizer/tlist.h"
#include "parser/parse_collate.h"
#include "parser/parse_coerce.h"
#include "parser/parse_clause.h"
#include "parser/parse_merge.h"
#include "parser/parse_node.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgxc/groupmgr.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/poolmgr.h"
#include "pgxc/poolutils.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "rewrite/rewriteHandler.h"
#include "nodes/pg_list.h"
#include "securec.h"
#include "lz4.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"

void set_default_stream()
{
    /* initdb could not use smp */
    if (IsInitdb) {
        u_sess->opt_cxt.is_stream = false;
        u_sess->opt_cxt.is_stream_support = false;
    } else {
        u_sess->opt_cxt.is_stream = (u_sess->opt_cxt.query_dop > 1);
        u_sess->opt_cxt.is_stream_support = (u_sess->opt_cxt.query_dop > 1);
    }
}

int2vector* get_baserel_distributekey_no(Oid relid)
{
    /* while smp is not allowed, no need to generate distribute key */
    if (!check_stream_support()) {
        return NULL;
    }
    AttrNumber attnum  = 1;
    while (true) {
        HeapTuple tp;
        Form_pg_attribute att_tup;
        tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
        if (!HeapTupleIsValid(tp)) {
            attnum = 0;
            ReleaseSysCache(tp);
            break;
        }
        att_tup = (Form_pg_attribute)GETSTRUCT(tp);
        if (!att_tup->attisdropped) {
            ReleaseSysCache(tp);
            break;
        }
        ++attnum;
        ReleaseSysCache(tp);
    }
    if (attnum == 0)
        return NULL;
    int2 col[1] = { attnum };
    int2vector* attnumVec = buildint2vector(col, 1);
    return attnumVec;
}

/* in build_simple_rel used. Put it all back. Record it */
List* build_baserel_distributekey(RangeTblEntry* rte, int relindex)
{
    if (IS_PGXC_DATANODE || !IS_PGXC_COORDINATOR || !rte->relid || get_rel_relkind(rte->relid) != RELKIND_RELATION)
        return NIL;

    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NIL;
}

Plan* make_simple_RemoteQuery(Plan* lefttree, PlannerInfo* root, bool is_subplan, ExecNodes* target_exec_nodes)
{
    if (NULL == root->glob)
        ereport(ERROR,
            (errmodule(MOD_OPT),
                errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                (errmsg("Could not find globle planner info when make simple remote query."))));

    if (root->glob->insideRecursion)
        return lefttree;

    if (is_execute_on_coordinator(lefttree) || is_execute_on_allnodes(lefttree))
        return lefttree;

    if (lefttree->dop > 1) {
        lefttree = create_local_gather(lefttree);
    }

    return lefttree;
}

void add_remote_subplan(PlannerInfo* root, RemoteQuery* result_node)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

ExecNodes* get_plan_max_ExecNodes(Plan* lefttree, List* subplans)
{
    ExecNodes* final_exec_nodes = NULL;

    final_exec_nodes = makeNode(ExecNodes);
    final_exec_nodes->nodeList = NIL;
    final_exec_nodes->baselocatortype = LOCATOR_TYPE_REPLICATED;
    final_exec_nodes->accesstype = RELATION_ACCESS_READ;
    final_exec_nodes->primarynodelist = NIL;
    final_exec_nodes->en_expr = NULL;
    final_exec_nodes->nodeList = lefttree->exec_nodes->nodeList;

    /* Set Distribution */
    Distribution* distribution = ng_convert_to_distribution(final_exec_nodes->nodeList);
    ng_set_distribution(&final_exec_nodes->distribution, distribution);

    return final_exec_nodes;
}

bool is_replicated_plan(Plan* plan)
{
    return false;
}

bool is_hashed_plan(Plan* plan)
{
    if (IsA(plan, Stream)) {
        if (is_broadcast_stream((Stream*)plan) || is_gather_stream((Stream*)plan))
            return false;
        else if (is_redistribute_stream((Stream*)plan))
            return true;
    } else if (plan->exec_nodes != NULL)
        return IsLocatorDistributedByHash(plan->exec_nodes->baselocatortype);

    return false;
}

bool is_rangelist_plan(Plan* plan)
{
    return false;
}

ExecNodes* stream_merge_exec_nodes(Plan* lefttree, Plan* righttree, bool push_nodelist)
{
    return lefttree->exec_nodes;
}

char* CompressSerializedPlan(const char* plan_string, int* cLen)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
char* DecompressSerializedPlan(const char* comp_plan_string, int cLen, int oLen)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

void SerializePlan(Plan* node, PlannedStmt* planned_stmt, StringInfoData* str, int num_stream, int num_gather)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

Plan* mark_distribute_dml(
    PlannerInfo* root, Plan** sourceplan, ModifyTable* mt_plan, List** resultRelations, List* mergeActionList)
{
    Plan* subplan = *sourceplan;
    /* We should avoid the ModifyTable Exec on Datanode When it followd by BaseResult */
    if (is_single_baseresult_plan(subplan)) {
        inherit_plan_locator_info((Plan*)mt_plan, *sourceplan);
    }
    return (Plan*)mt_plan;
}

/*
 * Just execute the plan as a replicateion
 */
static void mark_distribute_setop_allnodes(Plan* plan)
{
    plan->distributed_keys = NIL;
    plan->exec_type = EXEC_ON_ALL_NODES;
    Distribution* distribution = ng_get_default_computing_group_distribution();
    plan->exec_nodes = ng_convert_to_exec_nodes(distribution, LOCATOR_TYPE_REPLICATED, RELATION_ACCESS_READ);
}

/*
 * We should get distribute key for union all for two case:
 * 1. All subplan are hash and there have distkey;
 * 2. Some subplan are hash which have distkey and some are replication.
 */
static List* get_distkey_for_unionall(List** subPlanKeyArray, int subPlanNum)
{
    List* common_diskey = NIL;

    for (int i = 0; i < subPlanNum; i++) {
        if (subPlanKeyArray[i] != NIL) {
            if (common_diskey == NIL)
                common_diskey = subPlanKeyArray[i];
            else if (!equal(common_diskey, subPlanKeyArray[i]))
                return NIL;
        }
    }

    /* All the distkey is same, we should get first for setop. */
    return common_diskey;
}

/* For unionall case, with no replicated plan, we should judge if replicated plan can be redistributed */
bool judge_redistribute_setop_support(PlannerInfo* root, List* subplanlist, Bitmapset* redistributePlanSet)
{
    Index subPlanIndex = 0;
    ListCell* lc = NULL;

    if (bms_is_empty(redistributePlanSet))
        return true;

    foreach (lc, subplanlist) {
        if (bms_is_member(subPlanIndex, redistributePlanSet)) {
            Plan* subPlan = (Plan*)lfirst(lc);
            if (NULL != make_distkey_for_append(root, subPlan))
                return true;
            else
                return false;
        }
        subPlanIndex++;
    }
    return true;
}

static bool redistributeInfo(PlannerInfo* root, List* subPlans, Plan* plan, List** redistributeKey,
    Bitmapset** redistributePlanSet, Distribution** redistributeDistribution, bool isunionall, bool canDiskeyChange)
{
    ListCell* cell = NULL;
    Plan* subPlan = NULL;
    int subPlanNum = list_length(subPlans);
    Cost* subPlanCostArray = NULL;
    List** subPlanKeyArray = NULL;
    List* subPlanKeyIndex = NULL;
    int subPlanIndex = 0;
    List* redistributeKeyIndex = NULL;
    Bitmapset* redistributePlanSetCopy = NULL;
    bool result = true;
    bool norediskeyplan = false, redistributedplan = false;

    /*
     * This is the case that no columns needed by append,
     * but with lower level of colstore table.
     */
    if (plan->targetlist == NIL && !isunionall)
        return false;

    subPlanCostArray = (Cost*)palloc0(sizeof(Cost) * subPlanNum);
    subPlanKeyArray = (List**)palloc0(sizeof(List*) * subPlanNum);
    subPlanIndex = 0;

    /* We get the best target node group for set op */
    Distribution* target_distribution = ng_get_best_setop_distribution(subPlans, isunionall, root->is_correlated);

    /*
     * Find redistribute key for each subplan.
     *
     * We have three kinds of subplan here.
     *	(1) replicate plan.
     *	(2) redistributed plan.
     *	(3) non-replicate plan that has no distribute key possible
     *
     * Note, we can't have all replicate plan in subplans here, since it has been handled earlier.
     * Note, there may be all replicate plan in subplans, if their exec nodes have no overlap.
     *
     * For union all, we need replicate plan to be redistributed, and find a common redistribute
     * key as possible (not a must). For non-union all, we need to find a common redistribute
     * key for all the plans.
     */
    foreach (cell, subPlans) {
        subPlan = (Plan*)lfirst(cell);
        Distribution* current_distribution = ng_get_dest_distribution(subPlan);

        /* When the target and the current are all distributed by single node, ignore the distribution keys. */
        if (!ng_is_single_node_group_distribution(current_distribution)) {
            subPlanKeyIndex = distributeKeyIndex(root, subPlan->distributed_keys, subPlan->targetlist);

            if (NULL == subPlanKeyIndex) {
                if (!isunionall) {
                    redistributePlanSetCopy = bms_add_member(redistributePlanSetCopy, subPlanIndex);
                }

                if (canDiskeyChange) {
                    ExecNodes* en = IsA(subPlan, Stream) ? ((Stream*)subPlan)->consumer_nodes : subPlan->exec_nodes;
                    bool partial_single_node =
                        (list_length(en->nodeList) == 1 && bms_num_members(target_distribution->bms_data_nodeids) > 1);

                    /*
                     * Whether a stream has been added for the plan only executes on one datanode
                     * of multi-datanode group, we should redistribute it beforehand. Or it'll lead
                     * duplicate results when pushing down exec_nodes to other datanodes, and we
                     * can't prevent exec_nodes pushing down since executor needs all the consumer
                     * to be same in one thread, or it'll hang.
                     */
                    if (partial_single_node) {
                        ListCell* lc = NULL;
                        List* distkeys = NIL;

                        /*
                         * Since there's no stats info in append rel, we can only roughly
                         * choose the distribute key to do redistribute
                         */
                        foreach (lc, subPlan->targetlist) {
                            TargetEntry* tle = (TargetEntry*)lfirst(lc);
                            if (IsTypeDistributable(exprType((Node*)tle->expr))) {
                                distkeys = list_make1(tle->expr);
                                break;
                            }
                        }
                        if (distkeys != NIL) {
                            /* Found a valid distribute key, so use it */
                            subPlan = make_stream_plan(root, subPlan, distkeys, 1.0, target_distribution);
                            subPlanKeyArray[subPlanIndex] =
                                distributeKeyIndex(root, subPlan->distributed_keys, subPlan->targetlist);
                        } else {
                            /*
                             * No suitable distribute key, and we don't support distribute on
                             * roundrobin, so make a const to distribute on it. NOTE. We know
                             * it's not a good idea, but no way in the moment. Can improve later
                             */
                            Const* con = makeConst(INT4OID, -1, InvalidOid, -2, (Datum)0, true, false);
                            distkeys = list_make1(con);
                            subPlan = make_stream_plan(root, subPlan, distkeys, 1.0, target_distribution);
                        }
                        redistributedplan = true;
                        lfirst(cell) = subPlan;
                    } else
                        norediskeyplan = true;
                }
            } else {
                subPlanKeyArray[subPlanIndex] = subPlanKeyIndex;
                redistributedplan = true;
            }
        }

        if (!isunionall) {
            unsigned int producer_num_datanodes = ng_get_dest_num_data_nodes(subPlan);
            unsigned int consumer_num_datanodes = bms_num_members(target_distribution->bms_data_nodeids);

            if (consumer_num_datanodes == 0) {
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        errcode(ERRCODE_DIVISION_BY_ZERO),
                        (errmsg("consumer_num_datanodes should not be zero"))));
            }

            subPlanCostArray[subPlanIndex] =
                ng_calculate_setop_branch_stream_cost(subPlan, producer_num_datanodes, consumer_num_datanodes);
        }

        subPlanIndex++;
    }

    /* Sepcial process for union all. */
    if (isunionall) {
        /* if all the plans are hashed plan and in same node group, common distribute key is possible */
        if (!norediskeyplan) {
            redistributeKeyIndex = get_distkey_for_unionall(subPlanKeyArray, subPlanNum);
        } else if (!redistributedplan && !judge_redistribute_setop_support(root, subPlans, redistributePlanSetCopy)) {
            /* if no hashed plan, no distribute key is possible */
            result = false;
        }
    } else {
        /*
         * Find no distribute key for subPlan original, we should generate distribute key from max
         * cost plan as the distribute key.
         */
        if (!redistributedplan) {
            redistributeKeyIndex = get_max_cost_distkey_for_nulldistkey(root, subPlans, subPlanNum, subPlanCostArray);
            if (redistributeKeyIndex == NIL)
                result = false;
        } else {
            /*
             * There has distribute key for subPlan original,
             * use subPlanKeyArray as the distribute key.
             */
            redistributeKeyIndex = get_max_cost_distkey_for_hasdistkey(
                root, subPlans, subPlanNum, subPlanKeyArray, subPlanCostArray, &redistributePlanSetCopy);
        }
    }

    pfree_ext(subPlanCostArray);
    pfree_ext(subPlanKeyArray);

    *redistributePlanSet = redistributePlanSetCopy;
    *redistributeKey = redistributeKeyIndex;
    *redistributeDistribution = target_distribution;

    return result;
}

static ExecNodes* append_merge_exec_nodes(List* subplans, bool is_distributed)
{
    ListCell* lc = NULL;
    ExecNodes* merged_en = (ExecNodes*)makeNode(ExecNodes);
    Distribution* merged_distribution = NULL;

    foreach (lc, subplans) {
        Plan* subplan = (Plan*)lfirst(lc);
        ExecNodes* en = subplan->exec_nodes;

        if (IsA(subplan, Stream))
            en = ((Stream*)subplan)->consumer_nodes;

        merged_en->nodeList = list_merge_int(merged_en->nodeList, en->nodeList);

        /*
         * There are two callers of this function
         * (1) mark_distribute_setop : the en->distribution may not the same
         * (2) mark_distribute_setop_distribution : the en->distribution should be the same
         * So, we could not do this assert : ng_is_same_group(&merged_en->distribution, &en->distribution)
         */
        if (is_distributed &&
            (merged_distribution != NULL && !ng_is_same_group(merged_distribution, &en->distribution))) {
                elog(ERROR, "The distribution of merged and exec node are not the same\n"
                            "merged distribution is %s\n"
                            "supblan distribution is %s",
                            dist_to_str(merged_distribution),
                            dist_to_str(&en->distribution));
            }

        Distribution* new_merged_distribution = ng_get_union_distribution(merged_distribution, &en->distribution);
        DestroyDistribution(merged_distribution);
        merged_distribution = new_merged_distribution;
    }
    ng_set_distribution(&merged_en->distribution, merged_distribution);

    foreach (lc, subplans) {
        Plan* subplan = (Plan*)lfirst(lc);
        ExecNodes* en = subplan->exec_nodes;

        /*
         * If the subplan contains stream, we should pushdown the merged exec_nodes.
         * The reason why we do this is because different exec_nodes between top plan node and
         * stream consumer_nodes may cause hang up.
         *
         * And we must pushdown merged exec_nodes when subplan->dop > 1. because when add local
         * stream, we need the plan node on both sides of the local stream node have the same exec nodes.
         * pushdown merged exec_nodes can guarantee this.
         */
        if (!contain_special_plan_node(subplan, T_Stream, CPLN_NO_IGNORE_MATERIAL) && subplan->dop == 1) {
            continue;
        }
        if (IsA(subplan, Stream))
            en = ((Stream*)subplan)->consumer_nodes;
        if (list_length(merged_en->nodeList) > list_length(en->nodeList)) {
            /*
             * Use a deep copy of 'merged_en', in case the subplan's
             * baselocatortype was changed by the assignment of
             * merged_en->baselocatortype.
             */
            ExecNodes* temp_execnodes = (ExecNodes*)copyObject(merged_en);
            temp_execnodes->baselocatortype = en->baselocatortype;
            pushdown_execnodes(subplan, temp_execnodes, true);
        }
    }

    merged_en->baselocatortype = LOCATOR_TYPE_HASH;
    return merged_en;
}

static void mark_distribute_setop_distribution(PlannerInfo* root, Node* node, Plan* plan, List* subPlans,
    Bitmapset* redistributePlanSet, List* redistributeKey, Distribution* redistributeDistribution)
{
    ListCell* cell = NULL;
    List* newSubPlans = NIL;
    Plan* subPlan = NULL;
    Index subPlanIndex = 0;
    TargetEntry* teEntry = NULL;
    MergeAppend* mergeAppend = NULL;
    Append* append = NULL;
    ListCell* attnumCell = NULL;
    AttrNumber attnum;
    RecursiveUnion* recursiveUnionPlan = NULL;
    Distribution *newDistribution = redistributeDistribution;

    if (IsA(node, MergeAppend)) {
        mergeAppend = (MergeAppend*)node;
    } else if (IsA(node, Append)) {
        AssertEreport(IsA(node, Append), MOD_OPT, "The node is NOT a Append");

        append = (Append*)node;
    } else if (IsA(node, RecursiveUnion)) {
        recursiveUnionPlan = (RecursiveUnion*)node;
    }

    if (!bms_is_empty(redistributePlanSet)) {
        foreach (cell, subPlans) {
            subPlan = (Plan*)lfirst(cell);
            List* distribute_keys = NIL;
            List* subplandistkey = redistributeKey;

            /*
             * There are four cases enter the else branch below:
             * 1. All subplan are hash and there are no distkey;
             * 2. All subplan are hash which some have distkey and some have no distkey;
             * 3. Some subplan are hash which  involve two cases above-mentioned and some are replication.
             * We will choose distkey for replication of union all.
             */
            if (subplandistkey == NIL)
                subplandistkey = make_distkey_for_append(root, subPlan);

            /*
             * Add distribute node
             */
            foreach (attnumCell, subplandistkey) {
                attnum = lfirst_int(attnumCell);
                if ((attnum - 1) >= list_length(subPlan->targetlist)) {
                    elog(ERROR, "attnum overflow the length of subplan targetlist");
                }
                teEntry = (TargetEntry*)list_nth(subPlan->targetlist, attnum - 1);
                distribute_keys = lappend(distribute_keys, teEntry->expr);
            }

            if (bms_is_member(subPlanIndex, redistributePlanSet)) {
                /*
                 * If both sub plan are replicate plan and we could not get distribute keys for them,
                 * we need to broadcast both of them to a single datanode from redistributeDistribution
                 */
                bool noDistribute_keys = (distribute_keys == NIL &&
                                          bms_num_members(redistributeDistribution->bms_data_nodeids) > 1);
                if (noDistribute_keys) {
                    newDistribution = ng_get_random_single_dn_distribution(redistributeDistribution);
                }

                Plan* newplan = subPlan;

                if (root->is_correlated && SUBQUERY_PREDPUSH(root))
                    elog(ERROR, "Can not add stream operator on to parameterize plan.");


                /* Make stream node of redistribute. */
                bool partial_single_node = bms_num_members (newDistribution->bms_data_nodeids) == 1 &&
                                           bms_num_members(redistributeDistribution->bms_data_nodeids) > 1 &&
                                           distribute_keys == NIL;
                if (partial_single_node) {
                    /*
                     * If a stream plan only executes on one datanode of multi-datanode group,
                     * we should redistribute it. Or it'll lead duplicate results when pushing
                     * down exec_nodes to other datanodes
                     */
                    const int typeMod = -1;
                    const int typeLen = -2;
                    Const *con = makeConst(INT4OID, typeMod, InvalidOid, typeLen, (Datum)0, true, false);
                    newplan = make_stream_plan(root, subPlan, list_make1(con), 0, redistributeDistribution);
                } else {
                    newplan = make_stream_plan(root, subPlan, distribute_keys, 0, newDistribution);
                    /* We should use the original redistributeDistribution as the 
                     * stream->consumer_nodes->distribution
                     * to make sure all the subplans of append has the same distributeion.
                     */
                    if (newDistribution != redistributeDistribution) {
                        ng_copy_distribution(&((Stream *)newplan)->consumer_nodes->distribution,
                                             redistributeDistribution);
                    }
                }

                if (IsA(newplan, Stream)) {
                    Stream *streamNode = (Stream *)newplan;
                    streamNode->is_sorted = IsA(node, MergeAppend) ? true : false;
                }

                if (PointerIsValid(mergeAppend)) {
                    newSubPlans = lappend(newSubPlans,
                        make_sort(root,
                            newplan,
                            mergeAppend->numCols,
                            mergeAppend->sortColIdx,
                            mergeAppend->sortOperators,
                            mergeAppend->collations,
                            mergeAppend->nullsFirst,
                            -1));
                } else {
                    newSubPlans = lappend(newSubPlans, newplan);
                }
            } else {
                newSubPlans = lappend(newSubPlans, subPlan);
            }

            subPlanIndex++;
        }

        if (PointerIsValid(mergeAppend)) {
            mergeAppend->mergeplans = newSubPlans;
        } else if (PointerIsValid(recursiveUnionPlan)) {
            const int invalidLen = 2;
            AssertEreport(list_length(newSubPlans) == invalidLen, MOD_OPT, "Invalid subplan length");

            Plan* recursive_base_plan = (Plan*)recursiveUnionPlan;

            recursive_base_plan->lefttree = (Plan*)linitial(newSubPlans);
            recursive_base_plan->righttree = (Plan*)lsecond(newSubPlans);
        } else {
            AssertEreport(PointerIsValid(append), MOD_OPT, "The append is NULL");

            append->appendplans = newSubPlans;
        }
        subPlans = newSubPlans;
    }

    foreach (attnumCell, redistributeKey) {
        attnum = lfirst_int(attnumCell);
        if (list_length(plan->targetlist) < attnum) {
            elog(ERROR, "target list is too short");
        }
        teEntry = (TargetEntry*)list_nth(plan->targetlist, attnum - 1);
        plan->distributed_keys = lappend(plan->distributed_keys, teEntry->expr);
    }

    plan->exec_type = EXEC_ON_DATANODES;
    plan->exec_nodes = append_merge_exec_nodes(subPlans, true);
}


void mark_distribute_setop(PlannerInfo* root, Node* node, bool isunionall, bool canDiskeyChange)
{
    List* subPlans = NIL;
    ListCell* planCell = NULL;
    Plan* plan = NULL;
    Bitmapset* execAllNodesPlanSet = NULL;
    Index subPlanIndex = 0;
    bool execOnCoords = false;

    if (IsA(node, Append)) {
        Append* appendPlan = (Append*)node;

        subPlans = appendPlan->appendplans;
        plan = &(appendPlan->plan);
    } else if (IsA(node, RecursiveUnion)) {
        RecursiveUnion* recursive_union_plan = (RecursiveUnion*)node;
        plan = &recursive_union_plan->plan;

        subPlans = lappend(subPlans, plan->lefttree);
        subPlans = lappend(subPlans, plan->righttree);
    } else {
        MergeAppend* mergeAppendPlan = NULL;

        AssertEreport(IsA(node, MergeAppend), MOD_OPT, "The node is NOT a MergeAppend");

        mergeAppendPlan = (MergeAppend*)node;

        subPlans = mergeAppendPlan->mergeplans;
        plan = &(mergeAppendPlan->plan);
    }

    AssertEreport(PointerIsValid(subPlans), MOD_OPT, "The subPlan is NULL");
    AssertEreport(list_length(subPlans) >= 1, MOD_OPT, "The list length of subplan is 0");

    foreach (planCell, subPlans) {
        Plan* subPlan = (Plan*)lfirst(planCell);

        if (is_execute_on_coordinator(subPlan)) {
            execOnCoords = true;
            break;
        }
    }

    if (execOnCoords) {
        if (SUBQUERY_IS_PARAM(root) && root->is_correlated) {
            elog(ERROR, "Can not add stream operator on to parameterize plan.");
        }
    } else {
        subPlanIndex = 0;
        foreach (planCell, subPlans) {
            Plan* subPlan = (Plan*)lfirst(planCell);

            /* make each branch replicated if there are subplan exprs in one branches */
            if (root->is_correlated && !(SUBQUERY_PREDPUSH(root))) {
                if (contain_special_plan_node(subPlan, T_Stream)) {
                    subPlan = materialize_finished_plan(subPlan, true, root->glob->vectorized);
                }
                lfirst(planCell) = subPlan;
            }

            if (is_execute_on_allnodes(subPlan)) {
                execAllNodesPlanSet = bms_add_member(execAllNodesPlanSet, subPlanIndex);

                subPlanIndex++;
                continue;
            }

            if (subPlan->exec_nodes->nodeList == NIL) {
                elog(DEBUG1, "[mark_distribute_setop] empty node list");
            }

            AssertEreport(PointerIsValid(subPlan->exec_nodes), MOD_OPT, "The subPlan's exec_nodes is NULL");

            subPlanIndex++;
        }

        if (bms_num_members(execAllNodesPlanSet) == list_length(subPlans)) {
            mark_distribute_setop_allnodes(plan);
        } else {
            /*
             * Just redistribute subplans if there is no less than one subplan
             * that is distributed
             */
            Bitmapset* redistributePlanSet = NULL;
            List* redistributeKey = NIL;
            Distribution* redistributeDistribution = NULL;
            bool result = false;

            /*
             * get redistribute information
             * return true if succeed; else return fail
             */
            result = redistributeInfo(root,
                subPlans,
                plan,
                &redistributeKey,
                &redistributePlanSet,
                &redistributeDistribution,
                isunionall,
                canDiskeyChange);

            if (result) {
                mark_distribute_setop_distribution(
                    root, node, plan, subPlans, redistributePlanSet, redistributeKey, redistributeDistribution);
            } else {
                /*
                 * Add remote query node on top of each subplan if fail
                 * to get redistribute information
                 */
                mark_distribute_setop_remotequery(root, node, plan, subPlans);
            }
        }

        if (PointerIsValid(execAllNodesPlanSet)) {
            pfree_ext(execAllNodesPlanSet);
        }
    }
}

// the name is_stream_support is used in function check_stream_support, which used as condition
void mark_stream_unsupport()
{
    u_sess->opt_cxt.is_stream_support = false;
    ereport(ERROR, (errmodule(MOD_OPT), (errcode(ERRCODE_STREAM_NOT_SUPPORTED), errmsg("mark_stream_unsupport."))));
}

void materialize_remote_query(Plan* result_plan, bool* materialized, bool sort_to_store)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

void SerializePlan(
    Plan* node, PlannedStmt* planned_stmt, StringInfoData* str, int num_stream, int num_gather, bool push_subplan)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

