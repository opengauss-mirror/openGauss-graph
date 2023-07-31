/* -------------------------------------------------------------------------
 *
 * analyzejoins.cpp
 *	  Routines for simplifying joins after initial query analysis
 *
 * While we do a great deal of join simplification in prep/prepjointree.c,
 * certain optimizations cannot be performed at that stage for lack of
 * detailed information about the query.  The routines here are invoked
 * after initsplan.c has done its work, and can do additional join removal
 * and simplification steps based on the information extracted.  The penalty
 * is that we have to work harder to clean up after ourselves when we modify
 * the query, since the derived data structures have to be updated too.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/plan/analyzejoins.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "utils/lsyscache.h"

/* local functions */
static bool join_is_removable(PlannerInfo* root, SpecialJoinInfo* sjinfo);
static void remove_rel_from_query(PlannerInfo* root, int relid, Relids joinrelids);
static List* remove_rel_from_joinlist(List* joinlist, int relid, int* nremoved);
static bool rel_supports_distinctness(PlannerInfo* root, RelOptInfo* rel);
static bool rel_is_distinct_for(PlannerInfo* root, RelOptInfo* rel, List* clause_list);
static bool check_column_uniqueness(List* groupClause, List* targetList, List* colnos, List* opids);
static Oid distinct_col_search(int colno, List* colnos, List* opids);
#ifdef GS_GRAPH
static bool is_innerrel_unique_for(PlannerInfo *root,
								   Relids joinrelids,
								   Relids outerrelids,
								   RelOptInfo *innerrel,
								   JoinType jointype,
								   List *restrictlist);
#endif
/*
 * remove_useless_joins
 *		Check for relations that don't actually need to be joined at all,
 *		and remove them from the query.
 *
 * We are passed the current joinlist and return the updated list.	Other
 * data structures that have to be updated are accessible via "root".
 */
List* remove_useless_joins(PlannerInfo* root, List* joinlist)
{
    ListCell* lc = NULL;
    ListCell* pnext = NULL;

    /*
     * We are only interested in relations that are left-joined to, so we can
     * scan the join_info_list to find them easily.
     */
restart:
    for (lc = list_head(root->join_info_list); lc != NULL; lc = pnext) {
        SpecialJoinInfo* sjinfo = (SpecialJoinInfo*)lfirst(lc);
        pnext = lnext(lc);
        int innerrelid;
        int nremoved;

        /* Skip if not removable */
        if (!join_is_removable(root, sjinfo))
            continue;

        /*
         * Currently, join_is_removable can only succeed when the sjinfo's
         * righthand is a single baserel.  Remove that rel from the query and
         * joinlist.
         */
        innerrelid = bms_singleton_member(sjinfo->min_righthand);

        remove_rel_from_query(root, innerrelid, bms_union(sjinfo->min_lefthand, sjinfo->min_righthand));

        /* We verify that exactly one reference gets removed from joinlist */
        nremoved = 0;
        joinlist = remove_rel_from_joinlist(joinlist, innerrelid, &nremoved);
        if (nremoved != 1)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("failed to find relation %d in joinlist when remove relation from joinlist", innerrelid))));

        /*
         * We can delete this SpecialJoinInfo from the list too, since it's no
         * longer of interest.
         */
        root->join_info_list = list_delete_ptr(root->join_info_list, sjinfo);

        /*
         * Restart the scan.  This is necessary to ensure we find all
         * removable joins independently of ordering of the join_info_list
         * (note that removal of attr_needed bits may make a join appear
         * removable that did not before).	Also, since we just deleted the
         * current list cell, we'd have to have some kluge to continue the
         * list scan anyway.
         */
        goto restart;
    }

    return joinlist;
}

/*
 * clause_sides_match_join
 *	  Determine whether a join clause is of the right form to use in this join.
 *
 * We already know that the clause is a binary opclause referencing only the
 * rels in the current join.  The point here is to check whether it has the
 * form "outerrel_expr op innerrel_expr" or "innerrel_expr op outerrel_expr",
 * rather than mixing outer and inner vars on either side.	If it matches,
 * we set the transient flag outer_is_left to identify which side is which.
 */
static inline bool clause_sides_match_join(RestrictInfo* rinfo, Relids outerrelids, Relids innerrelids)
{
    if (bms_is_subset(rinfo->left_relids, outerrelids) && bms_is_subset(rinfo->right_relids, innerrelids)) {
        /* lefthand side is outer */
        rinfo->outer_is_left = true;
        return true;
    } else if (bms_is_subset(rinfo->left_relids, innerrelids) && bms_is_subset(rinfo->right_relids, outerrelids)) {
        /* righthand side is outer */
        rinfo->outer_is_left = false;
        return true;
    }
    return false; /* no good for these input relations */
}

/*
 * join_is_removable
 *	  Check whether we need not perform this special join at all, because
 *	  it will just duplicate its left input.
 *
 * This is true for a left join for which the join condition cannot match
 * more than one inner-side row.  (There are other possibly interesting
 * cases, but we don't have the infrastructure to prove them.)  We also
 * have to check that the inner side doesn't generate any variables needed
 * above the join.
 */
static bool join_is_removable(PlannerInfo* root, SpecialJoinInfo* sjinfo)
{
    int innerrelid;
    RelOptInfo* innerrel = NULL;
    Relids joinrelids;
    List* clause_list = NIL;
    ListCell* l = NULL;
    int attroff;

    /*
     * Must be a non-delaying left join to a single baserel, else we aren't
     * going to be able to do anything with it.
     */
    if ((sjinfo->jointype != JOIN_LEFT && sjinfo->jointype != JOIN_LEFT_ANTI_FULL) || sjinfo->delay_upper_joins ||
        bms_membership(sjinfo->min_righthand) != BMS_SINGLETON)
        return false;

    innerrelid = bms_singleton_member(sjinfo->min_righthand);
    innerrel = find_base_rel(root, innerrelid);

    /*
     * Before we go to the effort of checking whether any innerrel variables
     * are needed above the join, make a quick check to eliminate cases in
     * which we will surely be unable to prove uniqueness of the innerrel.
     */
    if (!rel_supports_distinctness(root, innerrel))
        return false;

    /* Compute the relid set for the join we are considering */
    joinrelids = bms_union(sjinfo->min_lefthand, sjinfo->min_righthand);

    /*
     * We can't remove the join if any inner-rel attributes are used above the
     * join.
     *
     * Note that this test only detects use of inner-rel attributes in higher
     * join conditions and the target list.  There might be such attributes in
     * pushed-down conditions at this join, too.  We check that case below.
     *
     * As a micro-optimization, it seems better to start with max_attr and
     * count down rather than starting with min_attr and counting up, on the
     * theory that the system attributes are somewhat less likely to be wanted
     * and should be tested last.
     */
    for (attroff = innerrel->max_attr - innerrel->min_attr; attroff >= 0; attroff--) {
        if (!bms_is_subset(innerrel->attr_needed[attroff], joinrelids))
            return false;
    }

    /*
     * Similarly check that the inner rel isn't needed by any PlaceHolderVars
     * that will be used above the join.  We only need to fail if such a PHV
     * actually references some inner-rel attributes; but the correct check
     * for that is relatively expensive, so we first check against ph_eval_at,
     * which must mention the inner rel if the PHV uses any inner-rel attrs.
     */
    foreach (l, root->placeholder_list) {
        PlaceHolderInfo* phinfo = (PlaceHolderInfo*)lfirst(l);

        if (bms_is_subset(phinfo->ph_needed, joinrelids))
            continue; /* PHV is not used above the join */
        if (!bms_overlap(phinfo->ph_eval_at, innerrel->relids))
            continue; /* it definitely doesn't reference innerrel */
        if (bms_overlap(pull_varnos((Node*)phinfo->ph_var), innerrel->relids))
            return false; /* it does reference innerrel */
    }

    /*
     * Search for mergejoinable clauses that constrain the inner rel against
     * either the outer rel or a pseudoconstant.  If an operator is
     * mergejoinable then it behaves like equality for some btree opclass, so
     * it's what we want.  The mergejoinability test also eliminates clauses
     * containing volatile functions, which we couldn't depend on.
     */
    foreach (l, innerrel->joininfo) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);

        /*
         * If it's not a join clause for this outer join, we can't use it.
         * Note that if the clause is pushed-down, then it is logically from
         * above the outer join, even if it references no other rels (it might
         * be from WHERE, for example).
         */
        if (restrictinfo->is_pushed_down || !bms_equal(restrictinfo->required_relids, joinrelids)) {
            /*
             * If such a clause actually references the inner rel then join
             * removal has to be disallowed.  We have to check this despite
             * the previous attr_needed checks because of the possibility of
             * pushed-down clauses referencing the rel.
             */
            if (bms_is_member(innerrelid, restrictinfo->clause_relids))
                return false;
            continue; /* else, ignore; not useful here */
        }

        /* Ignore if it's not a mergejoinable clause */
        if (!restrictinfo->can_join || restrictinfo->mergeopfamilies == NIL)
            continue; /* not mergejoinable */

        /*
         * Check if clause has the form "outer op inner" or "inner op outer",
         * and if so mark which side is inner.
         */
        if (!clause_sides_match_join(restrictinfo, sjinfo->min_lefthand, innerrel->relids))
            continue; /* no good for these input relations */

        /* OK, add to list */
        clause_list = lappend(clause_list, restrictinfo);
    }

    /*
     * Now that we have the relevant equality join clauses, try to prove the
     * innerrel distinct.
     */
    if (rel_is_distinct_for(root, innerrel, clause_list))
        return true;

    /*
     * Some day it would be nice to check for other methods of establishing
     * distinctness.
     */
    return false;
}

/*
 * Remove the target relid from the planner's data structures, having
 * determined that there is no need to include it in the query.
 *
 * We are not terribly thorough here.  We must make sure that the rel is
 * no longer treated as a baserel, and that attributes of other baserels
 * are no longer marked as being needed at joins involving this rel.
 * Also, join quals involving the rel have to be removed from the joininfo
 * lists, but only if they belong to the outer join identified by joinrelids.
 */
static void remove_rel_from_query(PlannerInfo* root, int relid, Relids joinrelids)
{
    RelOptInfo* rel = find_base_rel(root, relid);
    List* joininfos = NIL;
    Index rti;
    ListCell* l = NULL;
    ListCell* nextl = NULL;

    /*
     * Mark the rel as "dead" to show it is no longer part of the join tree.
     * (Removing it from the baserel array altogether seems too risky.)
     */
    rel->reloptkind = RELOPT_DEADREL;

    /*
     * Remove references to the rel from other baserels' attr_needed arrays.
     */
    for (rti = 1; rti < (unsigned int)root->simple_rel_array_size; rti++) {
        RelOptInfo* otherrel = root->simple_rel_array[rti];
        int attroff;

        /* there may be empty slots corresponding to non-baserel RTEs */
        if (otherrel == NULL)
            continue;

        AssertEreport(otherrel->relid == rti, MOD_OPT, "RelOptInfo Index Incorrect.");

        /* no point in processing target rel itself */
        if (otherrel == rel)
            continue;

        for (attroff = otherrel->max_attr - otherrel->min_attr; attroff >= 0; attroff--) {
            otherrel->attr_needed[attroff] = bms_del_member(otherrel->attr_needed[attroff], relid);
        }
    }

    /*
     * Likewise remove references from SpecialJoinInfo data structures.
     *
     * This is relevant in case the outer join we're deleting is nested inside
     * other outer joins: the upper joins' relid sets have to be adjusted. The
     * RHS of the target outer join will be made empty here, but that's OK
     * since caller will delete that SpecialJoinInfo entirely.
     */
    foreach (l, root->join_info_list) {
        SpecialJoinInfo* sjinfo = (SpecialJoinInfo*)lfirst(l);

        sjinfo->min_lefthand = bms_del_member(sjinfo->min_lefthand, relid);
        sjinfo->min_righthand = bms_del_member(sjinfo->min_righthand, relid);
        sjinfo->syn_lefthand = bms_del_member(sjinfo->syn_lefthand, relid);
        sjinfo->syn_righthand = bms_del_member(sjinfo->syn_righthand, relid);
    }

    /*
     * Likewise remove references from LateralJoinInfo data structures.
     *
     * If we are deleting a LATERAL subquery, we can forget its
     * LateralJoinInfo altogether.  Otherwise, make sure the target is not
     * included in any lateral_lhs set.  (It probably can't be, since that
     * should have precluded deciding to remove it; but let's cope anyway.)
     */
    for (l = list_head(root->lateral_info_list); l != NULL; l = nextl)
    {
        LateralJoinInfo *ljinfo = (LateralJoinInfo *) lfirst(l);

        nextl = lnext(l);
        if (ljinfo->lateral_rhs == (Index)relid)
            root->lateral_info_list = list_delete_ptr(root->lateral_info_list,
                                                      ljinfo);
        else
            ljinfo->lateral_lhs = bms_del_member(ljinfo->lateral_lhs, relid);
    }

    /*
     * Likewise remove references from PlaceHolderVar data structures.
     *
     * Here we have a special case: if a PHV's eval_at set is just the target
     * relid, we want to leave it that way instead of reducing it to the empty
     * set.  An empty eval_at set would confuse later processing since it
     * would match every possible eval placement.
     */
    foreach (l, root->placeholder_list) {
        PlaceHolderInfo* phinfo = (PlaceHolderInfo*)lfirst(l);

        phinfo->ph_eval_at = bms_del_member(phinfo->ph_eval_at, relid);
        if (bms_is_empty(phinfo->ph_eval_at)) /* oops, belay that */
            phinfo->ph_eval_at = bms_add_member(phinfo->ph_eval_at, relid);

        phinfo->ph_needed = bms_del_member(phinfo->ph_needed, relid);
    }

    /*
     * Remove any joinquals referencing the rel from the joininfo lists.
     *
     * In some cases, a joinqual has to be put back after deleting its
     * reference to the target rel.  This can occur for pseudoconstant and
     * outerjoin-delayed quals, which can get marked as requiring the rel in
     * order to force them to be evaluated at or above the join.  We can't
     * just discard them, though.  Only quals that logically belonged to the
     * outer join being discarded should be removed from the query.
     *
     * We must make a copy of the rel's old joininfo list before starting the
     * loop, because otherwise remove_join_clause_from_rels would destroy the
     * list while we're scanning it.
     */
    joininfos = list_copy(rel->joininfo);
    foreach (l, joininfos) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);

        remove_join_clause_from_rels(root, rinfo, rinfo->required_relids);

        if (rinfo->is_pushed_down || !bms_equal(rinfo->required_relids, joinrelids)) {
            /* Recheck that qual doesn't actually reference the target rel */
            AssertEreport(!bms_is_member(relid, rinfo->clause_relids), MOD_OPT, "");
            /*
             * The required_relids probably aren't shared with anything else,
             * but let's copy them just to be sure.
             */
            rinfo->required_relids = bms_copy(rinfo->required_relids);
            rinfo->required_relids = bms_del_member(rinfo->required_relids, relid);
            distribute_restrictinfo_to_rels(root, rinfo);
        }
    }
}

/*
 * Remove any occurrences of the target relid from a joinlist structure.
 *
 * It's easiest to build a whole new list structure, so we handle it that
 * way.  Efficiency is not a big deal here.
 *
 * *nremoved is incremented by the number of occurrences removed (there
 * should be exactly one, but the caller checks that).
 */
static List* remove_rel_from_joinlist(List* joinlist, int relid, int* nremoved)
{
    List* result = NIL;
    ListCell* jl = NULL;

    foreach (jl, joinlist) {
        Node* jlnode = (Node*)lfirst(jl);

        if (IsA(jlnode, RangeTblRef)) {
            int varno = ((RangeTblRef*)jlnode)->rtindex;

            if (varno == relid)
                (*nremoved)++;
            else
                result = lappend(result, jlnode);
        } else if (IsA(jlnode, List)) {
            /* Recurse to handle subproblem */
            List* sublist = NIL;

            sublist = remove_rel_from_joinlist((List*)jlnode, relid, nremoved);
            /* Avoid including empty sub-lists in the result */
            if (sublist != NIL)
                result = lappend(result, sublist);
        } else {
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_UNEXPECTED_NODE_STATE),
                    (errmsg("unrecognized joinlist node type when remove relation from joinlist: %d",
                        (int)nodeTag(jlnode)))));
        }
    }

    return result;
}

/*
 * rel_supports_distinctness
 *		Could the relation possibly be proven distinct on some set of columns?
 *
 * This is effectively a pre-checking function for rel_is_distinct_for().
 * It must return TRUE if rel_is_distinct_for() could possibly return TRUE
 * with this rel, but it should not expend a lot of cycles.  The idea is
 * that callers can avoid doing possibly-expensive processing to compute
 * rel_is_distinct_for()'s argument lists if the call could not possibly
 * succeed.
 */
static bool rel_supports_distinctness(PlannerInfo* root, RelOptInfo* rel)
{
    /*
     * We only handle two cases here:
     * 1. base rel with a unique index
     * 2. subquery with aggreations
     * We can improve later when we can propagate uniqueness
     */
    if (rel->reloptkind != RELOPT_BASEREL)
        return false;
    if (rel->rtekind == RTE_RELATION) {
        /*
         * For a plain relation, we only know how to prove uniqueness by
         * reference to unique indexes.  Make sure there's at least one
         * suitable unique index.  It must be immediately enforced, and if
         * it's a partial index, it must match the query.  (Keep these
         * conditions in sync with relation_has_unique_index_for!)
         */
        ListCell* lc = NULL;

        foreach (lc, rel->indexlist) {
            IndexOptInfo* ind = (IndexOptInfo*)lfirst(lc);

            if (ind->unique && ind->immediate && (ind->indpred == NIL || ind->predOK))
                return true;
        }
    } else if (rel->rtekind == RTE_SUBQUERY) {
        Query* subquery = root->simple_rte_array[rel->relid]->subquery;

        /* Check if the subquery has any qualities that support distinctness */
        if (query_supports_distinctness(subquery))
            return true;
    }
    /* We have no proof rules for any other rtekinds. */
    return false;
}

/*
 * rel_is_distinct_for
 *		Does the relation return only distinct rows according to clause_list?
 *
 * clause_list is a list of join restriction clauses involving this rel and
 * some other one.  Return true if no two rows emitted by this rel could
 * possibly join to the same row of the other rel.
 *
 * The caller must have already determined that each condition is a
 * mergejoinable equality with an expression in this relation on one side, and
 * an expression not involving this relation on the other.  The transient
 * outer_is_left flag is used to identify which side references this relation:
 * left side if outer_is_left is false, right side if it is true.
 *
 * Note that the passed-in clause_list may be destructively modified!  This
 * is OK for current uses, because the clause_list is built by the caller for
 * the sole purpose of passing to this function.
 */
static bool rel_is_distinct_for(PlannerInfo* root, RelOptInfo* rel, List* clause_list)
{
    /*
     * We could skip a couple of tests here if we assume all callers checked
     * rel_supports_distinctness first, but it doesn't seem worth taking any
     * risk for.
     */
    if (rel->reloptkind != RELOPT_BASEREL)
        return false;
    if (rel->rtekind == RTE_RELATION) {
        /*
         * Examine the indexes to see if we have a matching unique index.
         * relation_has_unique_index_for automatically adds any usable
         * restriction clauses for the rel, so we needn't do that here.
         */
        if (relation_has_unique_index_for(root, rel, clause_list, NIL, NIL))
            return true;
    } else if (rel->rtekind == RTE_SUBQUERY) {
        Index relid = rel->relid;
        Query* subquery = root->simple_rte_array[relid]->subquery;
        List* colnos = NIL;
        List* opids = NIL;
        ListCell* l = NULL;

        /*
         * Build the argument lists for query_is_distinct_for: a list of
         * output column numbers that the query needs to be distinct over, and
         * a list of equality operators that the output columns need to be
         * distinct according to.
         *
         * (XXX we are not considering restriction clauses attached to the
         * subquery; is that worth doing?)
         */
        foreach (l, clause_list) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(l);
            Oid op;
            Var* var = NULL;

            /*
             * Get the equality operator we need uniqueness according to.
             * (This might be a cross-type operator and thus not exactly the
             * same operator the subquery would consider; that's all right
             * since query_is_distinct_for can resolve such cases.)  The
             * caller's mergejoinability test should have selected only
             * OpExprs.
             */
            Assert(IsA(rinfo->clause, OpExpr));
            op = ((OpExpr*)rinfo->clause)->opno;

            /* caller identified the inner side for us */
            if (rinfo->outer_is_left)
                var = (Var*)get_rightop(rinfo->clause);
            else
                var = (Var*)get_leftop(rinfo->clause);

            if (var != NULL) {
                /* try to find compatible var */
                var = locate_distribute_var((Expr*)var);
            }

            /*
             * If inner side isn't a Var referencing a subquery output column,
             * this clause doesn't help us.
             */
            if ((var == NULL) || !IsA(var, Var) || var->varno != relid || var->varlevelsup != 0) {
                continue;
            }

            colnos = lappend_int(colnos, var->varattno);
            opids = lappend_oid(opids, op);
        }

        if (query_is_distinct_for(subquery, colnos, opids))
            return true;
    }
    return false;
}

/*
 * query_supports_distinctness - could the query possibly be proven distinct
 *		on some set of output columns?
 *
 * This is effectively a pre-checking function for query_is_distinct_for().
 * It must return TRUE if query_is_distinct_for() could possibly return TRUE
 * with this query, but it should not expend a lot of cycles.  The idea is
 * that callers can avoid doing possibly-expensive processing to compute
 * query_is_distinct_for()'s argument lists if the call could not possibly
 * succeed.
 */
bool query_supports_distinctness(Query* query)
{
    if (query->distinctClause != NIL || query->groupClause != NIL || query->hasAggs || query->havingQual ||
        query->setOperations)
        return true;

    return false;
}

/*
 * query_is_distinct_for - does query never return duplicates of the
 *		specified columns?
 *
 * query is a not-yet-planned subquery (in current usage, it's always from
 * a subquery RTE, which the planner avoids scribbling on).
 *
 * colnos is an integer list of output column numbers (resno's).  We are
 * interested in whether rows consisting of just these columns are certain
 * to be distinct.  "Distinctness" is defined according to whether the
 * corresponding upper-level equality operators listed in opids would think
 * the values are distinct.  (Note: the opids entries could be cross-type
 * operators, and thus not exactly the equality operators that the subquery
 * would use itself.  We use equality_ops_are_compatible() to check
 * compatibility.  That looks at btree or hash opfamily membership, and so
 * should give trustworthy answers for all operators that we might need
 * to deal with here.)
 */
bool query_is_distinct_for(Query* query, List* colnos, List* opids)
{
    ListCell* l = NULL;
    Oid opid;

    Assert(list_length(colnos) == list_length(opids));

    /*
     * A set-returning function in the query's targetlist can result in
     * returning duplicate rows, if the SRF is evaluated after the
     * de-duplication step; so we play it safe and say "no" if there are any
     * SRFs.  (We could be certain that it's okay if SRFs appear only in the
     * specified columns, since those must be evaluated before de-duplication;
     * but it doesn't presently seem worth the complication to check that.)
     */
    if (expression_returns_set((Node*)query->targetList))
        return false;

    /*
     * DISTINCT (including DISTINCT ON) guarantees uniqueness if all the
     * columns in the DISTINCT clause appear in colnos and operator semantics
     * match.
     */
    if (query->distinctClause != NIL) {
        if (check_column_uniqueness(query->distinctClause, query->targetList, colnos, opids))
            return true;
    }

    /*
     * Similarly, GROUP BY guarantees uniqueness if all the grouped columns
     * appear in colnos and operator semantics match.
     */
    if (query->groupClause != NIL && query->groupingSets == NIL) {
        if (check_column_uniqueness(query->groupClause, query->targetList, colnos, opids))
            return true;
    } else if (query->groupingSets != NIL) {
        /*
         * If we have grouping sets with expressions, we probably don't have
         * uniqueness and analysis would be hard. Punt.
         */
        if (query->groupClause != NIL)
            return false;

        /*
         * If we have no groupClause (therefore no grouping expressions), we
         * might have one or many empty grouping sets. If there's just one,
         * then we're returning only one row and are certainly unique. But
         * otherwise, we know we're certainly not unique.
         */
        bool isTrue = list_length(query->groupingSets) == 1 &&
            ((GroupingSet*)linitial(query->groupingSets))->kind == GROUPING_SET_EMPTY;
        if (isTrue)
            return true;
        else
            return false;
    } else {
        /*
         * If we have no GROUP BY, but do have aggregates or HAVING, then the
         * result is at most one row so it's surely unique, for any operators.
         */
        if (query->hasAggs || query->havingQual)
            return true;
    }

    /*
     * UNION, INTERSECT, EXCEPT guarantee uniqueness of the whole output row,
     * except with ALL.
     */
    if (query->setOperations != NULL) {
        SetOperationStmt* topop = (SetOperationStmt*)query->setOperations;

        Assert(IsA(topop, SetOperationStmt));
        Assert(topop->op != SETOP_NONE);

        if (!topop->all) {
            ListCell* lg = NULL;

            /* We're good if all the nonjunk output columns are in colnos */
            lg = list_head(topop->groupClauses);
            foreach (l, query->targetList) {
                TargetEntry* tle = (TargetEntry*)lfirst(l);
                SortGroupClause* sgc = NULL;

                if (tle->resjunk)
                    continue; /* ignore resjunk columns */

                /* non-resjunk columns should have grouping clauses */
                Assert(lg != NULL);
                sgc = (SortGroupClause*)lfirst(lg);
                lg = lnext(lg);

                opid = distinct_col_search(tle->resno, colnos, opids);
                if (!OidIsValid(opid) || !equality_ops_are_compatible(opid, sgc->eqop))
                    break; /* exit early if no match */
            }
            if (l == NULL) /* had matches for all? */
                return true;
        }
    }

    /*
     * XXX Are there any other cases in which we can easily see the result
     * must be distinct?
     *
     * If you do add more smarts to this function, be sure to update
     * query_supports_distinctness() to match.
     */
    return false;
}

/*
 * check_column_uniqueness - subroutine for query_is_distinct_for
 *
 * Given group clause and targetlist, find if all the aggregated columns
 * in colnos, return true if so, else false.
 */
static bool check_column_uniqueness(List* groupClause, List* targetList, List* colnos, List* opids)
{
    ListCell* l = NULL;
    Oid opid;

    foreach (l, groupClause) {
        SortGroupClause* sgc = (SortGroupClause*)lfirst(l);
        TargetEntry* tle = get_sortgroupclause_tle(sgc, targetList);

        opid = distinct_col_search(tle->resno, colnos, opids);
        if (!OidIsValid(opid) || !equality_ops_are_compatible(opid, sgc->eqop))
            break; /* exit early if no match */
    }
    if (l == NULL) /* had matches for all? */
        return true;

    return false;
}

/*
 * distinct_col_search - subroutine for query_is_distinct_for
 *
 * If colno is in colnos, return the corresponding element of opids,
 * else return InvalidOid.  (Ordinarily colnos would not contain duplicates,
 * but if it does, we arbitrarily select the first match.)
 */
static Oid distinct_col_search(int colno, List* colnos, List* opids)
{
    ListCell* lc1 = NULL;
    ListCell* lc2 = NULL;

    forboth(lc1, colnos, lc2, opids)
    {
        if (colno == lfirst_int(lc1))
            return lfirst_oid(lc2);
    }
    return InvalidOid;
}

#ifdef GS_GRAPH
/*
 * innerrel_is_unique
 *	  Check if the innerrel provably contains at most one tuple matching any
 *	  tuple from the outerrel, based on join clauses in the 'restrictlist'.
 *
 * We need an actual RelOptInfo for the innerrel, but it's sufficient to
 * identify the outerrel by its Relids.  This asymmetry supports use of this
 * function before joinrels have been built.  (The caller is expected to
 * also supply the joinrelids, just to save recalculating that.)
 *
 * The proof must be made based only on clauses that will be "joinquals"
 * rather than "otherquals" at execution.  For an inner join there's no
 * difference; but if the join is outer, we must ignore pushed-down quals,
 * as those will become "otherquals".  Note that this means the answer might
 * vary depending on whether IS_OUTER_JOIN(jointype); since we cache the
 * answer without regard to that, callers must take care not to call this
 * with jointypes that would be classified differently by IS_OUTER_JOIN().
 *
 * The actual proof is undertaken by is_innerrel_unique_for(); this function
 * is a frontend that is mainly concerned with caching the answers.
 * In particular, the force_cache argument allows overriding the internal
 * heuristic about whether to cache negative answers; it should be "true"
 * if making an inquiry that is not part of the normal bottom-up join search
 * sequence.
 */
bool
innerrel_is_unique(PlannerInfo *root,
				   Relids joinrelids,
				   Relids outerrelids,
				   RelOptInfo *innerrel,
				   JoinType jointype,
				   List *restrictlist,
				   bool force_cache)
{
	MemoryContext old_context;
	ListCell   *lc;

	/* Certainly can't prove uniqueness when there are no joinclauses */
	if (restrictlist == NIL)
		return false;

	/*
	 * Make a quick check to eliminate cases in which we will surely be unable
	 * to prove uniqueness of the innerrel.
	 */
	if (!rel_supports_distinctness(root, innerrel))
		return false;

	/*
	 * Query the cache to see if we've managed to prove that innerrel is
	 * unique for any subset of this outerrel.  We don't need an exact match,
	 * as extra outerrels can't make the innerrel any less unique (or more
	 * formally, the restrictlist for a join to a superset outerrel must be a
	 * superset of the conditions we successfully used before).
	 */
	foreach(lc, innerrel->unique_for_rels)
	{
		Relids		unique_for_rels = (Relids) lfirst(lc);

		if (bms_is_subset(unique_for_rels, outerrelids))
			return true;		/* Success! */
	}

	/*
	 * Conversely, we may have already determined that this outerrel, or some
	 * superset thereof, cannot prove this innerrel to be unique.
	 */
	foreach(lc, innerrel->non_unique_for_rels)
	{
		Relids		unique_for_rels = (Relids) lfirst(lc);

		if (bms_is_subset(outerrelids, unique_for_rels))
			return false;
	}

	/* No cached information, so try to make the proof. */
	if (is_innerrel_unique_for(root, joinrelids, outerrelids, innerrel,
							   jointype, restrictlist))
	{
		/*
		 * Cache the positive result for future probes, being sure to keep it
		 * in the planner_cxt even if we are working in GEQO.
		 *
		 * Note: one might consider trying to isolate the minimal subset of
		 * the outerrels that proved the innerrel unique.  But it's not worth
		 * the trouble, because the planner builds up joinrels incrementally
		 * and so we'll see the minimally sufficient outerrels before any
		 * supersets of them anyway.
		 */
		old_context = MemoryContextSwitchTo(root->planner_cxt);
		innerrel->unique_for_rels = lappend(innerrel->unique_for_rels,
											bms_copy(outerrelids));
		MemoryContextSwitchTo(old_context);

		return true;			/* Success! */
	}
	else
	{
		/*
		 * None of the join conditions for outerrel proved innerrel unique, so
		 * we can safely reject this outerrel or any subset of it in future
		 * checks.
		 *
		 * However, in normal planning mode, caching this knowledge is totally
		 * pointless; it won't be queried again, because we build up joinrels
		 * from smaller to larger.  It is useful in GEQO mode, where the
		 * knowledge can be carried across successive planning attempts; and
		 * it's likely to be useful when using join-search plugins, too. Hence
		 * cache when join_search_private is non-NULL.  (Yeah, that's a hack,
		 * but it seems reasonable.)
		 *
		 * Also, allow callers to override that heuristic and force caching;
		 * that's useful for reduce_unique_semijoins, which calls here before
		 * the normal join search starts.
		 */
		if (force_cache || root->join_search_private)
		{
			old_context = MemoryContextSwitchTo(root->planner_cxt);
			innerrel->non_unique_for_rels =
				lappend(innerrel->non_unique_for_rels,
						bms_copy(outerrelids));
			MemoryContextSwitchTo(old_context);
		}

		return false;
	}
}

/*
 * is_innerrel_unique_for
 *	  Check if the innerrel provably contains at most one tuple matching any
 *	  tuple from the outerrel, based on join clauses in the 'restrictlist'.
 */
static bool
is_innerrel_unique_for(PlannerInfo *root,
					   Relids joinrelids,
					   Relids outerrelids,
					   RelOptInfo *innerrel,
					   JoinType jointype,
					   List *restrictlist)
{
	List	   *clause_list = NIL;
	ListCell   *lc;

	/*
	 * Search for mergejoinable clauses that constrain the inner rel against
	 * the outer rel.  If an operator is mergejoinable then it behaves like
	 * equality for some btree opclass, so it's what we want.  The
	 * mergejoinability test also eliminates clauses containing volatile
	 * functions, which we couldn't depend on.
	 */
	foreach(lc, restrictlist)
	{
		RestrictInfo *restrictinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * As noted above, if it's a pushed-down clause and we're at an outer
		 * join, we can't use it.
		 */
		if (IS_OUTER_JOIN(jointype) &&
			RINFO_IS_PUSHED_DOWN(restrictinfo, joinrelids))
			continue;

		/* Ignore if it's not a mergejoinable clause */
		if (!restrictinfo->can_join ||
			restrictinfo->mergeopfamilies == NIL)
			continue;			/* not mergejoinable */

		/*
		 * Check if clause has the form "outer op inner" or "inner op outer",
		 * and if so mark which side is inner.
		 */
		if (!clause_sides_match_join(restrictinfo, outerrelids,
									 innerrel->relids))
			continue;			/* no good for these input relations */

		/* OK, add to list */
		clause_list = lappend(clause_list, restrictinfo);
	}

	/* Let rel_is_distinct_for() do the hard work */
	return rel_is_distinct_for(root, innerrel, clause_list);
}

#endif