/* -------------------------------------------------------------------------
 *
 * joinrels.cpp
 *	  Routines to determine which relations should be joined
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/joinrels.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "miscadmin.h"
#include "optimizer/dataskew.h"
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif
#ifdef STREAMPLAN
#include "optimizer/streamplan.h"
#endif

static void make_rels_by_clause_joins(PlannerInfo* root, RelOptInfo* old_rel, ListCell* other_rels);
static void make_rels_by_clauseless_joins(PlannerInfo* root, RelOptInfo* old_rel, ListCell* other_rels);
static bool has_join_restriction(PlannerInfo* root, RelOptInfo* rel);
static bool has_legal_joinclause(PlannerInfo* root, RelOptInfo* rel);
static bool is_dummy_rel(const RelOptInfo* rel);
static void mark_dummy_rel(RelOptInfo* rel);
static bool restriction_is_constant_false(List* restrictlist, bool only_pushed_down);

/*
 * join_search_one_level
 *	  Consider ways to produce join relations containing exactly 'level'
 *	  jointree items.  (This is one step of the dynamic-programming method
 *	  embodied in standard_join_search.)  Join rel nodes for each feasible
 *	  combination of lower-level rels are created and returned in a list.
 *	  Implementation paths are created for each such joinrel, too.
 *
 * level: level of rels we want to make this time
 * root->join_rel_level[j], 1 <= j < level, is a list of rels containing j items
 *
 * The result is returned in root->join_rel_level[level].
 */
void join_search_one_level(PlannerInfo* root, int level)
{
    List** joinrels = root->join_rel_level;
    ListCell* r = NULL;
    int k;

    AssertEreport(joinrels[level] == NIL, MOD_OPT_JOIN, "Join rel list is NIL");

    /* Set join_cur_level so that new joinrels are added to proper list */
    root->join_cur_level = level;

    /*
     * First, consider left-sided and right-sided plans, in which rels of
     * exactly level-1 member relations are joined against initial relations.
     * We prefer to join using join clauses, but if we find a rel of level-1
     * members that has no join clauses, we will generate Cartesian-product
     * joins against all initial rels not already contained in it.
     */
    foreach (r, joinrels[level - 1]) {
        RelOptInfo* old_rel = (RelOptInfo*)lfirst(r);
        bool isTrue = old_rel->joininfo != NIL || old_rel->has_eclass_joins 
                                               || has_join_restriction(root, old_rel);
        if (isTrue) {
            /*
             * There are join clauses or join order restrictions relevant to
             * this rel, so consider joins between this rel and (only) those
             * initial rels it is linked to by a clause or restriction.
             *
             * At level 2 this condition is symmetric, so there is no need to
             * look at initial rels before this one in the list; we already
             * considered such joins when we were at the earlier rel.  (The
             * mirror-image joins are handled automatically by make_join_rel.)
             * In later passes (level > 2), we join rels of the previous level
             * to each initial rel they don't already include but have a join
             * clause or restriction with.
             */
            ListCell* other_rels = NULL;

            if (level == 2) /* consider remaining initial rels */
                other_rels = lnext(r);
            else /* consider all initial rels */
                other_rels = list_head(joinrels[1]);

            make_rels_by_clause_joins(root, old_rel, other_rels);
        } else {
            /*
             * Oops, we have a relation that is not joined to any other
             * relation, either directly or by join-order restrictions.
             * Cartesian product time.
             *
             * We consider a cartesian product with each not-already-included
             * initial rel, whether it has other join clauses or not.  At
             * level 2, if there are two or more clauseless initial rels, we
             * will redundantly consider joining them in both directions; but
             * such cases aren't common enough to justify adding complexity to
             * avoid the duplicated effort.
             */
            make_rels_by_clauseless_joins(root, old_rel, list_head(joinrels[1]));
        }
    }

    /*
     * Now, consider "bushy plans" in which relations of k initial rels are
     * joined to relations of level-k initial rels, for 2 <= k <= level-2.
     *
     * We only consider bushy-plan joins for pairs of rels where there is a
     * suitable join clause (or join order restriction), in order to avoid
     * unreasonable growth of planning time.
     */
    for (k = 2;; k++) {
        int other_level = level - k;

        /*
         * Since make_join_rel(x, y) handles both x,y and y,x cases, we only
         * need to go as far as the halfway point.
         */
        if (k > other_level)
            break;

        foreach (r, joinrels[k]) {
            RelOptInfo* old_rel = (RelOptInfo*)lfirst(r);
            ListCell* other_rels = NULL;
            ListCell* r2 = NULL;

            /*
             * We can ignore relations without join clauses here, unless they
             * participate in join-order restrictions --- then we might have
             * to force a bushy join plan.
             */
            if (old_rel->joininfo == NIL && !old_rel->has_eclass_joins && !has_join_restriction(root, old_rel))
                continue;

            if (k == other_level)
                other_rels = lnext(r); /* only consider remaining rels */
            else
                other_rels = list_head(joinrels[other_level]);

            for_each_cell(r2, other_rels)
            {
                RelOptInfo* new_rel = (RelOptInfo*)lfirst(r2);

                if (!bms_overlap(old_rel->relids, new_rel->relids)) {
                    /*
                     * OK, we can build a rel of the right level from this
                     * pair of rels.  Do so if there is at least one relevant
                     * join clause or join order restriction.
                     */
                    if (have_relevant_joinclause(root, old_rel, new_rel) ||
                        have_join_order_restriction(root, old_rel, new_rel)) {
                        (void)make_join_rel(root, old_rel, new_rel);
                    }
                }
            }
        }
    }

    /* ----------
     * Last-ditch effort: if we failed to find any usable joins so far, force
     * a set of cartesian-product joins to be generated.  This handles the
     * special case where all the available rels have join clauses but we
     * cannot use any of those clauses yet.  This can only happen when we are
     * considering a join sub-problem (a sub-joinlist) and all the rels in the
     * sub-problem have only join clauses with rels outside the sub-problem.
     * An example is
     *
     *		SELECT ... FROM a INNER JOIN b ON TRUE, c, d, ...
     *		WHERE a.w = c.x and b.y = d.z;
     *
     * If the "a INNER JOIN b" sub-problem does not get flattened into the
     * upper level, we must be willing to make a cartesian join of a and b;
     * but the code above will not have done so, because it thought that both
     * a and b have joinclauses.  We consider only left-sided and right-sided
     * cartesian joins in this case (no bushy).
     * ----------
     */
    if (joinrels[level] == NIL) {
        /*
         * This loop is just like the first one, except we always
         * call make_rels_by_clauseless_joins().
         */
        foreach (r, joinrels[level - 1]) {
            RelOptInfo* old_rel = (RelOptInfo*)lfirst(r);

            make_rels_by_clauseless_joins(root, old_rel, list_head(joinrels[1]));
        }

        /* ----------
         * When special joins are involved, there may be no legal way
         * to make an N-way join for some values of N.	For example consider
         *
         * SELECT ... FROM t1 WHERE
         *	 x IN (SELECT ... FROM t2,t3 WHERE ...) AND
         *	 y IN (SELECT ... FROM t4,t5 WHERE ...)
         *
         * We will flatten this query to a 5-way join problem, but there are
         * no 4-way joins that join_is_legal() will consider legal.  We have
         * to accept failure at level 4 and go on to discover a workable
         * bushy plan at level 5.
         *
         * However, if there are no special joins then join_is_legal() should
         * never fail, and so the following sanity check is useful.
         * ----------
         */
        if (joinrels[level] == NIL && root->join_info_list == NIL &&
            root->lateral_info_list == NIL)
            ereport(ERROR,
                (errmodule(MOD_OPT),
                    errcode(ERRCODE_OPTIMIZER_INCONSISTENT_STATE),
                    (errmsg("failed to build any %d-way joins", level))));
    }
}

/*
 * make_rels_by_clause_joins
 *	  Build joins between the given relation 'old_rel' and other relations
 *	  that participate in join clauses that 'old_rel' also participates in
 *	  (or participate in join-order restrictions with it).
 *	  The join rels are returned in root->join_rel_level[join_cur_level].
 *
 * Note: at levels above 2 we will generate the same joined relation in
 * multiple ways --- for example (a join b) join c is the same RelOptInfo as
 * (b join c) join a, though the second case will add a different set of Paths
 * to it.  This is the reason for using the join_rel_level mechanism, which
 * automatically ensures that each new joinrel is only added to the list once.
 *
 * 'old_rel' is the relation entry for the relation to be joined
 * 'other_rels': the first cell in a linked list containing the other
 * rels to be considered for joining
 *
 * Currently, this is only used with initial rels in other_rels, but it
 * will work for joining to joinrels too.
 */
static void make_rels_by_clause_joins(PlannerInfo* root, RelOptInfo* old_rel, ListCell* other_rels)
{
    ListCell* l = NULL;

    for_each_cell(l, other_rels)
    {
        RelOptInfo* other_rel = (RelOptInfo*)lfirst(l);

        if (!bms_overlap(old_rel->relids, other_rel->relids) &&
            (have_relevant_joinclause(root, old_rel, other_rel) ||
                have_join_order_restriction(root, old_rel, other_rel))) {
            (void)make_join_rel(root, old_rel, other_rel);
        }
    }
}

/*
 * make_rels_by_clauseless_joins
 *	  Given a relation 'old_rel' and a list of other relations
 *	  'other_rels', create a join relation between 'old_rel' and each
 *	  member of 'other_rels' that isn't already included in 'old_rel'.
 *	  The join rels are returned in root->join_rel_level[join_cur_level].
 *
 * 'old_rel' is the relation entry for the relation to be joined
 * 'other_rels': the first cell of a linked list containing the
 * other rels to be considered for joining
 *
 * Currently, this is only used with initial rels in other_rels, but it would
 * work for joining to joinrels too.
 */
static void make_rels_by_clauseless_joins(PlannerInfo* root, RelOptInfo* old_rel, ListCell* other_rels)
{
    ListCell* l = NULL;

    for_each_cell(l, other_rels)
    {
        RelOptInfo* other_rel = (RelOptInfo*)lfirst(l);

        if (!bms_overlap(other_rel->relids, old_rel->relids)) {
            (void)make_join_rel(root, old_rel, other_rel);
        }
    }
}

/*
 * join_is_legal
 *	   Determine whether a proposed join is legal given the query's
 *	   join order constraints; and if it is, determine the join type.
 *
 * Caller must supply not only the two rels, but the union of their relids.
 * (We could simplify the API by computing joinrelids locally, but this
 * would be redundant work in the normal path through make_join_rel.)
 *
 * On success, *sjinfo_p is set to NULL if this is to be a plain inner join,
 * else it's set to point to the associated SpecialJoinInfo node.  Also,
 * *reversed_p is set TRUE if the given relations need to be swapped to
 * match the SpecialJoinInfo node.
 */
static bool join_is_legal(PlannerInfo* root, RelOptInfo* rel1, RelOptInfo* rel2, Relids joinrelids,
    SpecialJoinInfo** sjinfo_p, bool* reversed_p)
{
    SpecialJoinInfo* match_sjinfo = NULL;
    bool reversed = false;
    bool unique_exchange = false;
    bool must_be_leftjoin = false;
    bool lateral_fwd = false;
    bool lateral_rev = false;
    ListCell* l = NULL;

    /*
     * Ensure output params are set on failure return.	This is just to
     * suppress uninitialized-variable warnings from overly anal compilers.
     */
    *sjinfo_p = NULL;
    *reversed_p = false;

    /*
     * If we have any special joins, the proposed join might be illegal; and
     * in any case we have to determine its join type.	Scan the join info
     * list for matches and conflicts.
     */
    foreach (l, root->join_info_list) {
        SpecialJoinInfo* sjinfo = (SpecialJoinInfo*)lfirst(l);

        /*
         * This special join is not relevant unless its RHS overlaps the
         * proposed join.  (Check this first as a fast path for dismissing
         * most irrelevant SJs quickly.)
         */
        if (!bms_overlap(sjinfo->min_righthand, joinrelids))
            continue;

        /*
         * Also, not relevant if proposed join is fully contained within RHS
         * (ie, we're still building up the RHS).
         */
        if (bms_is_subset(joinrelids, sjinfo->min_righthand))
            continue;

        /*
         * Also, not relevant if SJ is already done within either input.
         */
        if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) && bms_is_subset(sjinfo->min_righthand, rel1->relids))
            continue;
        if (bms_is_subset(sjinfo->min_lefthand, rel2->relids) && bms_is_subset(sjinfo->min_righthand, rel2->relids))
            continue;

        /*
         * If it's a semijoin and we already joined the RHS to any other rels
         * within either input, then we must have unique-ified the RHS at that
         * point (see below).  Therefore the semijoin is no longer relevant in
         * this join path.
         */
        if (sjinfo->jointype == JOIN_SEMI) {
            if (bms_is_subset(sjinfo->syn_righthand, rel1->relids) && !bms_equal(sjinfo->syn_righthand, rel1->relids))
                continue;
            if (bms_is_subset(sjinfo->syn_righthand, rel2->relids) && !bms_equal(sjinfo->syn_righthand, rel2->relids))
                continue;
        }

        /*
         * If one input contains min_lefthand and the other contains
         * min_righthand, then we can perform the SJ at this join.
         *
         * Reject if we get matches to more than one SJ; that implies we're
         * considering something that's not really valid.
         */
        if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) && bms_is_subset(sjinfo->min_righthand, rel2->relids)) {
            if (match_sjinfo != NULL)
                return false; /* invalid join path */
            match_sjinfo = sjinfo;
            reversed = false;

            if (sjinfo->jointype == JOIN_SEMI && bms_equal(sjinfo->syn_righthand, rel2->relids) &&
                create_unique_path(root, rel2, (Path*)linitial(rel2->cheapest_total_path), sjinfo) != NULL) {
                unique_exchange = true;
            }
        } else if (bms_is_subset(sjinfo->min_lefthand, rel2->relids) &&
                   bms_is_subset(sjinfo->min_righthand, rel1->relids)) {
            if (match_sjinfo != NULL)
                return false; /* invalid join path */
            match_sjinfo = sjinfo;
            reversed = true;

            if (sjinfo->jointype == JOIN_SEMI && bms_equal(sjinfo->syn_righthand, rel1->relids) &&
                   create_unique_path(root, rel1, (Path*)linitial(rel1->cheapest_total_path), sjinfo) != NULL) {
                unique_exchange = true;
            }
        } else if (sjinfo->jointype == JOIN_SEMI && bms_equal(sjinfo->syn_righthand, rel2->relids) &&
                    ((Path*)linitial(rel2->cheapest_total_path))->param_info == NULL &&
                   create_unique_path(root, rel2, (Path*)linitial(rel2->cheapest_total_path), sjinfo) != NULL) {
            /* ----------
             * For a semijoin, we can join the RHS to anything else by
             * unique-ifying the RHS (if the RHS can be unique-ified).
             * We will only get here if we have the full RHS but less
             * than min_lefthand on the LHS.
             *
             * The reason to consider such a join path is exemplified by
             *	SELECT ... FROM a,b WHERE (a.x,b.y) IN (SELECT c1,c2 FROM c)
             * If we insist on doing this as a semijoin we will first have
             * to form the cartesian product of A*B.  But if we unique-ify
             * C then the semijoin becomes a plain innerjoin and we can join
             * in any order, eg C to A and then to B.  When C is much smaller
             * than A and B this can be a huge win.  So we allow C to be
             * joined to just A or just B here, and then make_join_rel has
             * to handle the case properly.
             *
             * Note that actually we'll allow unique-ified C to be joined to
             * some other relation D here, too.  That is legal, if usually not
             * very sane, and this routine is only concerned with legality not
             * with whether the join is good strategy.
             * ----------
             */
            if (match_sjinfo != NULL)
                return false; /* invalid join path */
            match_sjinfo = sjinfo;
            unique_exchange = true;
            reversed = false;
        } else if (sjinfo->jointype == JOIN_SEMI && bms_equal(sjinfo->syn_righthand, rel1->relids) &&
                    ((Path*)linitial(rel1->cheapest_total_path))->param_info == NULL &&
                   create_unique_path(root, rel1, (Path*)linitial(rel1->cheapest_total_path), sjinfo) != NULL) {
            /* Reversed semijoin case */
            if (match_sjinfo != NULL)
                return false; /* invalid join path */
            match_sjinfo = sjinfo;
            unique_exchange = true;
            reversed = true;
        } else {
            /*
             * Otherwise, the proposed join overlaps the RHS but isn't a valid
             * implementation of this SJ.  But don't panic quite yet: the RHS
             * violation might have occurred previously, in one or both input
             * relations, in which case we must have previously decided that
             * it was OK to commute some other SJ with this one.  If we need
             * to perform this join to finish building up the RHS, rejecting
             * it could lead to not finding any plan at all.  (This can occur
             * because of the heuristics elsewhere in this file that postpone
             * clauseless joins: we might not consider doing a clauseless join
             * within the RHS until after we've performed other, validly
             * commutable SJs with one or both sides of the clauseless join.)
             * This consideration boils down to the rule that if both inputs
             * overlap the RHS, we can allow the join --- they are either
             * fully within the RHS, or represent previously-allowed joins to
             * rels outside it.
             */
            if (bms_overlap(rel1->relids, sjinfo->min_righthand) && bms_overlap(rel2->relids, sjinfo->min_righthand))
                continue; /* assume valid previous violation of RHS */

            /*
             * The proposed join could still be legal, but only if we're
             * allowed to associate it into the RHS of this SJ.  That means
             * this SJ must be a LEFT join (not SEMI or ANTI, and certainly
             * not FULL) and the proposed join must not overlap the LHS.
             */
            if ((sjinfo->jointype != JOIN_LEFT && sjinfo->jointype != JOIN_LEFT_ANTI_FULL) ||
                bms_overlap(joinrelids, sjinfo->min_lefthand))
                return false;

            /*
             * To be valid, the proposed join must be a LEFT join; otherwise
             * it can't associate into this SJ's RHS.  But we may not yet have
             * found the SpecialJoinInfo matching the proposed join, so we
             * can't test that yet.  Remember the requirement for later.
             */
            must_be_leftjoin = true;
        }
    }

    /*
     * Fail if violated any SJ's RHS and didn't match to a LEFT SJ: the
     * proposed join can't associate into an SJ's RHS.
     *
     * Also, fail if the proposed join's predicate isn't strict; we're
     * essentially checking to see if we can apply outer-join identity 3, and
     * that's a requirement.  (This check may be redundant with checks in
     * make_outerjoininfo, but I'm not quite sure, and it's cheap to test.)
     */
    if (must_be_leftjoin &&
        (match_sjinfo == NULL ||
            (match_sjinfo->jointype != JOIN_LEFT && match_sjinfo->jointype != JOIN_LEFT_ANTI_FULL) ||
            !match_sjinfo->lhs_strict))
        return false; /* invalid join path */

    /*
     * We also have to check for constraints imposed by LATERAL references.
     * The proposed rels could each contain lateral references to the other,
     * in which case the join is impossible.  If there are lateral references
     * in just one direction, then the join has to be done with a nestloop
     * with the lateral referencer on the inside.  If the join matches an SJ
     * that cannot be implemented by such a nestloop, the join is impossible.
     */
    lateral_fwd = lateral_rev = false;
    foreach(l, root->lateral_info_list)
    {
        LateralJoinInfo *ljinfo = (LateralJoinInfo *) lfirst(l);

        if (bms_is_member(ljinfo->lateral_rhs, rel2->relids) &&
            bms_overlap(ljinfo->lateral_lhs, rel1->relids))
        {
            /* has to be implemented as nestloop with rel1 on left */
            if (lateral_rev)
                return false;   /* have lateral refs in both directions */
            lateral_fwd = true;
            if (!bms_is_subset(ljinfo->lateral_lhs, rel1->relids))
                return false;   /* rel1 can't compute the required parameter */
            if (match_sjinfo &&
                ((reversed && !unique_exchange) || match_sjinfo->jointype == JOIN_FULL))
                return false;   /* not implementable as nestloop */
        }
        if (bms_is_member(ljinfo->lateral_rhs, rel1->relids) &&
            bms_overlap(ljinfo->lateral_lhs, rel2->relids))
        {
            /* has to be implemented as nestloop with rel2 on left */
            if (lateral_fwd)
                return false;   /* have lateral refs in both directions */
            lateral_rev = true;
            if (!bms_is_subset(ljinfo->lateral_lhs, rel2->relids))
                return false;   /* rel2 can't compute the required parameter */
            if (match_sjinfo &&
                ((!reversed && !unique_exchange) || match_sjinfo->jointype == JOIN_FULL))
                return false;   /* not implementable as nestloop */
        }
    }

    /* Otherwise, it's a valid join */
    *sjinfo_p = match_sjinfo;
    *reversed_p = reversed;
    return true;
}

/*
 * make_join_rel
 *	   Find or create a join RelOptInfo that represents the join of
 *	   the two given rels, and add to it path information for paths
 *	   created with the two rels as outer and inner rel.
 *	   (The join rel may already contain paths generated from other
 *	   pairs of rels that add up to the same set of base rels.)
 *
 * NB: will return NULL if attempted join is not valid.  This can happen
 * when working with outer joins, or with IN or EXISTS clauses that have been
 * turned into joins.
 */
RelOptInfo* make_join_rel(PlannerInfo* root, RelOptInfo* rel1, RelOptInfo* rel2)
{
    Relids joinrelids;
    SpecialJoinInfo* sjinfo = NULL;
    bool reversed = false;
    SpecialJoinInfo sjinfo_data;
    RelOptInfo* joinrel = NULL;
    List* restrictlist = NIL;

    /* We should never try to join two overlapping sets of rels. */
    AssertEreport(!bms_overlap(rel1->relids, rel2->relids), MOD_OPT_JOIN, "rel sets are overlapped");

    /* Construct Relids set that identifies the joinrel. */
    joinrelids = bms_union(rel1->relids, rel2->relids);

    /* Check validity and determine join type. */
    if (!join_is_legal(root, rel1, rel2, joinrelids, &sjinfo, &reversed)) {
        /* invalid join path */
        bms_free_ext(joinrelids);
        return NULL;
    }

    /* Swap rels if needed to match the join info. */
    if (reversed) {
        RelOptInfo* trel = rel1;

        rel1 = rel2;
        rel2 = trel;
    }

    /*
     * If it's a plain inner join, then we won't have found anything in
     * join_info_list.	Make up a SpecialJoinInfo so that selectivity
     * estimation functions will know what's being joined.
     */
    if (sjinfo == NULL) {
        sjinfo = &sjinfo_data;
        sjinfo->type = T_SpecialJoinInfo;
        sjinfo->min_lefthand = rel1->relids;
        sjinfo->min_righthand = rel2->relids;
        sjinfo->syn_lefthand = rel1->relids;
        sjinfo->syn_righthand = rel2->relids;
        sjinfo->jointype = JOIN_INNER;
        /* we don't bother trying to make the remaining fields valid */
        sjinfo->lhs_strict = false;
        sjinfo->delay_upper_joins = false;
        sjinfo->join_quals = NIL;
    }

    /* identify we need cache the var ratio for join rel selectivity. */
    sjinfo->varratio_cached = true;

    /*
     * Find or build the join RelOptInfo, and compute the restrictlist that
     * goes with this particular joining.
     */
    joinrel = build_join_rel(root, joinrelids, rel1, rel2, sjinfo, &restrictlist);

    /*
     * If we've already proven this join is empty, we needn't consider any
     * more paths for it.
     */
    if (is_dummy_rel(joinrel)) {
        bms_free_ext(joinrelids);
        return joinrel;
    }

    /* query mem: change the work_mem of joinrel according to its num rels */
    int work_mem_orig = u_sess->opt_cxt.op_work_mem;
    int esti_op_mem_orig = root->glob->estiopmem;
    if (root->glob->minopmem > 0 && bms_num_members(root->all_baserels) != bms_num_members(joinrel->relids)) {
        root->glob->estiopmem =
            (int)(root->glob->estiopmem /
                  ceil(LOG2((double)bms_num_members(root->all_baserels) / bms_num_members(joinrel->relids))));
        root->glob->estiopmem = Max(root->glob->minopmem, root->glob->estiopmem);
        u_sess->opt_cxt.op_work_mem = Min(root->glob->estiopmem, OPT_MAX_OP_MEM);
        AssertEreport(u_sess->opt_cxt.op_work_mem > 0, MOD_OPT_JOIN, "work memory is 0");
    }

    /*
     * Consider paths using each rel as both outer and inner.  Depending on
     * the join type, a provably empty outer or inner rel might mean the join
     * is provably empty too; in which case throw away any previously computed
     * paths and mark the join as dummy.  (We do it this way since it's
     * conceivable that dummy-ness of a multi-element join might only be
     * noticeable for certain construction paths.)
     *
     * Also, a provably constant-false join restriction typically means that
     * we can skip evaluating one or both sides of the join.  We do this by
     * marking the appropriate rel as dummy.  For outer joins, a
     * constant-false restriction that is pushed down still means the whole
     * join is dummy, while a non-pushed-down one means that no inner rows
     * will join so we can treat the inner rel as dummy.
     *
     * We need only consider the jointypes that appear in join_info_list, plus
     * JOIN_INNER.
     */
    switch (sjinfo->jointype) {
        case JOIN_INNER:
            if (is_dummy_rel(rel1) || is_dummy_rel(rel2) || restriction_is_constant_false(restrictlist, false)) {
                mark_dummy_rel(joinrel);
                break;
            }
            add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_INNER, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_INNER, sjinfo, restrictlist);
            break;
        case JOIN_LEFT:
        case JOIN_LEFT_ANTI_FULL:
            if (is_dummy_rel(rel1) || restriction_is_constant_false(restrictlist, true)) {
                mark_dummy_rel(joinrel);
                break;
            }
            if (restriction_is_constant_false(restrictlist, false) &&
                bms_is_subset(rel2->relids, sjinfo->syn_righthand))
                mark_dummy_rel(rel2);
            add_paths_to_joinrel(root, joinrel, rel1, rel2, sjinfo->jointype, sjinfo, restrictlist);
            add_paths_to_joinrel(root,
                joinrel,
                rel2,
                rel1,
                (sjinfo->jointype == JOIN_LEFT) ? JOIN_RIGHT : JOIN_RIGHT_ANTI_FULL,
                sjinfo,
                restrictlist);
            break;
        case JOIN_FULL:
            if ((is_dummy_rel(rel1) && is_dummy_rel(rel2)) || restriction_is_constant_false(restrictlist, true)) {
                mark_dummy_rel(joinrel);
                break;
            }

            add_paths_to_joinrel(root, joinrel, rel1, rel2, sjinfo->jointype, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, sjinfo->jointype, sjinfo, restrictlist);

            /*
             * If there are join quals that aren't mergeable or hashable, we
             * may not be able to build any valid plan.  Complain here so that
             * we can give a somewhat-useful error message.  (Since we have no
             * flexibility of planning for a full join, there's no chance of
             * succeeding later with another pair of input rels.)
             */
            if (joinrel->pathlist == NIL && !IS_STREAM)
                ereport(ERROR,
                    (errmodule(MOD_OPT),
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg(
                                "FULL JOIN is only supported with merge-joinable or hash-joinable join conditions"))));

            break;
        case JOIN_SEMI:

            /*
             * We might have a normal semijoin, or a case where we don't have
             * enough rels to do the semijoin but can unique-ify the RHS and
             * then do an innerjoin (see comments in join_is_legal).  In the
             * latter case we can't apply JOIN_SEMI joining.
             */
            if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) &&
                bms_is_subset(sjinfo->min_righthand, rel2->relids)) {
                if (is_dummy_rel(rel1) || is_dummy_rel(rel2) || restriction_is_constant_false(restrictlist, false)) {
                    mark_dummy_rel(joinrel);
                    break;
                }
                add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_SEMI, sjinfo, restrictlist);
                add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_RIGHT_SEMI, sjinfo, restrictlist);
            }

            /*
             * If we know how to unique-ify the RHS and one input rel is
             * exactly the RHS (not a superset) we can consider unique-ifying
             * it and then doing a regular join.  (The create_unique_path
             * check here is probably redundant with what join_is_legal did,
             * but if so the check is cheap because it's cached.  So test
             * anyway to be sure.)
             */
            if (bms_equal(sjinfo->syn_righthand, rel2->relids) &&
                create_unique_path(root, rel2, (Path*)linitial(rel2->cheapest_total_path), sjinfo) != NULL) {
                if (is_dummy_rel(rel1) || is_dummy_rel(rel2) || restriction_is_constant_false(restrictlist, false)) {
                    mark_dummy_rel(joinrel);
                    break;
                }
                add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_UNIQUE_INNER, sjinfo, restrictlist);
                add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_UNIQUE_OUTER, sjinfo, restrictlist);
            }
            break;
        case JOIN_ANTI:
            if (is_dummy_rel(rel1) || restriction_is_constant_false(restrictlist, true)) {
                mark_dummy_rel(joinrel);
                break;
            }
            if (restriction_is_constant_false(restrictlist, false) &&
                bms_is_subset(rel2->relids, sjinfo->syn_righthand))
                mark_dummy_rel(rel2);
            add_paths_to_joinrel(root, joinrel, rel1, rel2, JOIN_ANTI, sjinfo, restrictlist);
            add_paths_to_joinrel(root, joinrel, rel2, rel1, JOIN_RIGHT_ANTI, sjinfo, restrictlist);
            break;
#ifdef GS_GRAPH
        case JOIN_VLE:
            if (is_dummy_rel(rel1) || is_dummy_rel(rel2) ||
                    restriction_is_constant_false(restrictlist, false))
            {
                mark_dummy_rel(joinrel);
				break;
            }
			add_paths_to_joinrel_for_vle(root, joinrel, rel1, rel2, sjinfo, restrictlist);
			break;
        case JOIN_CYPHER_DELETE:
			if (is_dummy_rel(rel1) ||
				restriction_is_constant_false(restrictlist, true))
			{
				mark_dummy_rel(joinrel);
				break;
			}
			if (restriction_is_constant_false(restrictlist, false) &&
				bms_is_subset(rel2->relids, sjinfo->syn_righthand))
				mark_dummy_rel(rel2);
			add_paths_for_cdelete(root, joinrel, rel1, rel2, sjinfo->jointype, sjinfo, restrictlist);
			break;
#endif /* GS_GRAPH */
    }


    bms_free_ext(joinrelids);

    u_sess->opt_cxt.op_work_mem = work_mem_orig;
    root->glob->estiopmem = esti_op_mem_orig;

#ifdef STREAMPLAN
    /*
     * If there are join quals that cannot generate Stream plan, we mark it and try
     * to get a PGXC plan instead.
     * e.g. cannot broadcast hashed results for inner plan of semi join when outer
     * plan is replicated now.
     */

    if (IS_STREAM && NIL == joinrel->pathlist) {
        /*
         * We remove the useless RelOptInfo and then try other join path if the current level 
         * is not the top level.
         */
        if (ENABLE_PRED_PUSH_ALL(root) && root->join_rel_level && 
            root->join_cur_level < list_length(root->join_rel_level[1])) {
            remove_join_rel(root, joinrel);
            return NULL;
        } else {
            errno_t sprintf_rc = sprintf_s(u_sess->opt_cxt.not_shipping_info->not_shipping_reason,
                        NOTPLANSHIPPING_LENGTH,
                        "\"Full Join\" on redistribution unsupported data type");
            securec_check_ss_c(sprintf_rc, "\0", "\0");
            mark_stream_unsupport();
            mark_dummy_rel(joinrel);
        }
    }
#endif

    return joinrel;
}

/*
 * have_join_order_restriction
 *		Detect whether the two relations should be joined to satisfy
 *		a join-order restriction arising from special joins.
 *
 * In practice this is always used with have_relevant_joinclause(), and so
 * could be merged with that function, but it seems clearer to separate the
 * two concerns.  We need this test because there are degenerate cases where
 * a clauseless join must be performed to satisfy join-order restrictions.
 *
 * Note: this is only a problem if one side of a degenerate outer join
 * contains multiple rels, or a clauseless join is required within an
 * IN/EXISTS RHS; else we will find a join path via the "last ditch" case in
 * join_search_one_level().  We could dispense with this test if we were
 * willing to try bushy plans in the "last ditch" case, but that seems much
 * less efficient.
 */
bool have_join_order_restriction(PlannerInfo* root, RelOptInfo* rel1, RelOptInfo* rel2)
{
    bool result = false;
    ListCell* l = NULL;

    /*
     * If either side has a lateral reference to the other, attempt the join
     * regardless of outer-join considerations.
     */
    foreach(l, root->lateral_info_list)
    {
        LateralJoinInfo *ljinfo = (LateralJoinInfo *) lfirst(l);

        if (bms_is_member(ljinfo->lateral_rhs, rel2->relids) &&
            bms_overlap(ljinfo->lateral_lhs, rel1->relids))
            return true;
        if (bms_is_member(ljinfo->lateral_rhs, rel1->relids) &&
            bms_overlap(ljinfo->lateral_lhs, rel2->relids))
            return true;
    }

    /*
     * It's possible that the rels correspond to the left and right sides of a
     * degenerate outer join, that is, one with no joinclause mentioning the
     * non-nullable side; in which case we should force the join to occur.
     *
     * Also, the two rels could represent a clauseless join that has to be
     * completed to build up the LHS or RHS of an outer join.
     */
    foreach (l, root->join_info_list) {
        SpecialJoinInfo* sjinfo = (SpecialJoinInfo*)lfirst(l);

        /* ignore full joins --- other mechanisms handle them */
        if (sjinfo->jointype == JOIN_FULL)
            continue;

        /* Can we perform the SJ with these rels? */
        if (bms_is_subset(sjinfo->min_lefthand, rel1->relids) && bms_is_subset(sjinfo->min_righthand, rel2->relids)) {
            result = true;
            break;
        }
        if (bms_is_subset(sjinfo->min_lefthand, rel2->relids) && bms_is_subset(sjinfo->min_righthand, rel1->relids)) {
            result = true;
            break;
        }

        /*
         * Might we need to join these rels to complete the RHS?  We have to
         * use "overlap" tests since either rel might include a lower SJ that
         * has been proven to commute with this one.
         */
        if (bms_overlap(sjinfo->min_righthand, rel1->relids) && bms_overlap(sjinfo->min_righthand, rel2->relids)) {
            result = true;
            break;
        }

        /* Likewise for the LHS. */
        if (bms_overlap(sjinfo->min_lefthand, rel1->relids) && bms_overlap(sjinfo->min_lefthand, rel2->relids)) {
            result = true;
            break;
        }
    }

    /*
     * We do not force the join to occur if either input rel can legally be
     * joined to anything else using joinclauses.  This essentially means that
     * clauseless bushy joins are put off as long as possible. The reason is
     * that when there is a join order restriction high up in the join tree
     * (that is, with many rels inside the LHS or RHS), we would otherwise
     * expend lots of effort considering very stupid join combinations within
     * its LHS or RHS.
     */
    if (result) {
        if (has_legal_joinclause(root, rel1) || has_legal_joinclause(root, rel2))
            result = false;
    }

    return result;
}

/*
 * has_join_restriction
 *		Detect whether the specified relation has join-order restrictions
 *		due to being inside an outer join or an IN (sub-SELECT).
 *
 * Essentially, this tests whether have_join_order_restriction() could
 * succeed with this rel and some other one.  It's OK if we sometimes
 * say "true" incorrectly.	(Therefore, we don't bother with the relatively
 * expensive has_legal_joinclause test.)
 */
static bool has_join_restriction(PlannerInfo* root, RelOptInfo* rel)
{
    ListCell* l = NULL;

    foreach(l, root->lateral_info_list)
    {
        LateralJoinInfo *ljinfo = (LateralJoinInfo *) lfirst(l);

        if (bms_is_member(ljinfo->lateral_rhs, rel->relids) ||
            bms_overlap(ljinfo->lateral_lhs, rel->relids))
            return true;
    }

    foreach (l, root->join_info_list) {
        SpecialJoinInfo* sjinfo = (SpecialJoinInfo*)lfirst(l);

        /* ignore full joins --- other mechanisms preserve their ordering */
        if (sjinfo->jointype == JOIN_FULL)
            continue;

        /* ignore if SJ is already contained in rel */
        if (bms_is_subset(sjinfo->min_lefthand, rel->relids) && bms_is_subset(sjinfo->min_righthand, rel->relids))
            continue;

        /* restricted if it overlaps LHS or RHS, but doesn't contain SJ */
        if (bms_overlap(sjinfo->min_lefthand, rel->relids) || bms_overlap(sjinfo->min_righthand, rel->relids))
            return true;
    }

    return false;
}

/*
 * has_legal_joinclause
 *		Detect whether the specified relation can legally be joined
 *		to any other rels using join clauses.
 *
 * We consider only joins to single other relations in the current
 * initial_rels list.  This is sufficient to get a "true" result in most real
 * queries, and an occasional erroneous "false" will only cost a bit more
 * planning time.  The reason for this limitation is that considering joins to
 * other joins would require proving that the other join rel can legally be
 * formed, which seems like too much trouble for something that's only a
 * heuristic to save planning time.  (Note: we must look at initial_rels
 * and not all of the query, since when we are planning a sub-joinlist we
 * may be forced to make clauseless joins within initial_rels even though
 * there are join clauses linking to other parts of the query.)
 */
static bool has_legal_joinclause(PlannerInfo* root, RelOptInfo* rel)
{
    ListCell* lc = NULL;

    foreach (lc, root->initial_rels) {
        RelOptInfo* rel2 = (RelOptInfo*)lfirst(lc);

        /* ignore rels that are already in "rel" */
        if (bms_overlap(rel->relids, rel2->relids))
            continue;

        if (have_relevant_joinclause(root, rel, rel2)) {
            Relids joinrelids;
            SpecialJoinInfo* sjinfo = NULL;
            bool reversed = false;

            /* join_is_legal needs relids of the union */
            joinrelids = bms_union(rel->relids, rel2->relids);
            if (join_is_legal(root, rel, rel2, joinrelids, &sjinfo, &reversed)) {
                /* Yes, this will work */
                bms_free_ext(joinrelids);
                return true;
            }

            bms_free_ext(joinrelids);
        }
    }

    return false;
}

/*
 * is_dummy_rel --- has relation been proven empty?
 */
static bool is_dummy_rel(const RelOptInfo* rel)
{
    return IS_DUMMY_REL(rel);
}

/*
 * Mark a relation as proven empty.
 *
 * During GEQO planning, this can get invoked more than once on the same
 * baserel struct, so it's worth checking to see if the rel is already marked
 * dummy.
 *
 * Also, when called during GEQO join planning, we are in a short-lived
 * memory context.	We must make sure that the dummy path attached to a
 * baserel survives the GEQO cycle, else the baserel is trashed for future
 * GEQO cycles.  On the other hand, when we are marking a joinrel during GEQO,
 * we don't want the dummy path to clutter the main planning context.  Upshot
 * is that the best solution is to explicitly make the dummy path in the same
 * context the given RelOptInfo is in.
 */
static void mark_dummy_rel(RelOptInfo* rel)
{
    MemoryContext oldcontext;

    /* Already marked? */
    if (is_dummy_rel(rel))
        return;

    /* No, so choose correct context to make the dummy path in */
    oldcontext = MemoryContextSwitchTo(GetMemoryChunkContext(rel));

    /* Set dummy size estimate */
    rel->rows = 0;

    /* Evict any previously chosen paths */
    rel->pathlist = NIL;

    /* Set up the dummy path */
    Path* append_path = (Path*)create_append_path(NULL, rel, NIL, NULL);
    if (append_path != NULL) {
        add_path(NULL, rel, append_path);
    }

    /* Set or update cheapest_total_path and related fields */
    set_cheapest(rel);

    (void)MemoryContextSwitchTo(oldcontext);
}

/*
 * restriction_is_constant_false --- is a restrictlist just FALSE?
 *
 * In cases where a qual is provably constant FALSE, eval_const_expressions
 * will generally have thrown away anything that's ANDed with it.  In outer
 * join situations this will leave us computing cartesian products only to
 * decide there's no match for an outer row, which is pretty stupid.  So,
 * we need to detect the case.
 *
 * If only_pushed_down is TRUE, then consider only pushed-down quals.
 */
static bool restriction_is_constant_false(List* restrictlist, bool only_pushed_down)
{
    ListCell* lc = NULL;

    /*
     * Despite the above comment, the restriction list we see here might
     * possibly have other members besides the FALSE constant, since other
     * quals could get "pushed down" to the outer join level.  So we check
     * each member of the list.
     */
    foreach (lc, restrictlist) {
        RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

        AssertEreport(IsA(rinfo, RestrictInfo), MOD_OPT_JOIN, "Restriction information is incorrect");
        if (only_pushed_down && !rinfo->is_pushed_down)
            continue;

        if (rinfo->clause && IsA(rinfo->clause, Const)) {
            Const* con = (Const*)rinfo->clause;

            /* constant NULL is as good as constant FALSE for our purposes */
            if (con->constisnull)
                return true;
            if (!DatumGetBool(con->constvalue))
                return true;
        }
    }
    return false;
}

