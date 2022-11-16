
#include "postgres.h"

#include "access/sysattr.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_clause.h"
#include "parser/parse_cypher_utils.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "gs_const.h"

Node* getSysColumnVar(ParseState* pstate, RangeTblEntry* rte, int attnum)
{
    Var* var;

    AssertArg(attnum <= SelfItemPointerAttributeNumber && attnum >= FirstLowInvalidHeapAttributeNumber);

    var = make_var(pstate, rte, attnum, -1);

    markVarForSelectPriv(pstate, var, rte);

    return (Node*)var;
}

Node* makeTypedRowExpr(List* args, Oid typoid, int location)
{
    RowExpr* row = makeNode(RowExpr);

    row->args = args;
    row->row_typeid = typoid;
    row->row_format = COERCE_EXPLICIT_CAST;
    // row->row_format = COERCE_IMPLICIT_CAST;
    row->location = location;

    return (Node*)row;
}

Node* makeVertexExpr(ParseState* pstate, RangeTblEntry* rte, int location)
{
    Node* id;
    Node* prop_map;
    Node* tid;

    id = getColumnVar(pstate, rte, GS_ELEM_LOCAL_ID);
    prop_map = getColumnVar(pstate, rte, GS_ELEM_PROP_MAP);
    tid = getSysColumnVar(pstate, rte, SelfItemPointerAttributeNumber);

    return makeTypedRowExpr(list_make3(id, prop_map, tid), VERTEXOID, location);
}

ResTarget*
makeSimpleResTarget(char* field, char* name)
{
    ColumnRef* cref;

    cref = makeNode(ColumnRef);
    cref->fields = list_make1(makeString(pstrdup(field)));
    cref->location = -1;

    return makeResTarget((Node*)cref, name);
}

Alias*
makeAliasOptUnique(char* aliasname)
{
    aliasname = (aliasname == NULL ? genUniqueName() : pstrdup(aliasname));
    return makeAliasNoDup(aliasname, NIL);
}

/* same as makeAlias() but no pstrdup(aliasname) */
Alias*
makeAliasNoDup(char* aliasname, List* colnames)
{
    Alias* alias;

    alias = makeNode(Alias);
    alias->aliasname = aliasname;
    alias->colnames = colnames;

    return alias;
}

List* genQualifiedName(char* name1, char* name2)
{
    if (name1 == NULL)
        return list_make1(makeString(name2));
    else
        return list_make2(makeString(name1), makeString(name2));
}

Node* makeColumnRef(List* fields)
{
    ColumnRef* n = makeNode(ColumnRef);

    n->fields = fields;
    n->location = -1;
    return (Node*)n;
}

ResTarget*
makeResTarget(Node* val, char* name)
{
    ResTarget* res;

    res = makeNode(ResTarget);
    if (name != NULL)
        res->name = pstrdup(name);
    res->val = val;
    res->location = -1;

    return res;
}


List *
makeTargetListFromJoin(ParseState *pstate, RangeTblEntry *rte)
{
	List	   *targetlist = NIL;
	ListCell   *lt;
	ListCell   *ln;

	AssertArg(rte->rtekind == RTE_JOIN);

	forboth(lt, rte->joinaliasvars, ln, rte->eref->colnames)
	{
		Var		   *varnode = (Var* )lfirst(lt);
		char	   *resname = strVal(lfirst(ln));
		TargetEntry *tmp;

		tmp = makeTargetEntry((Expr *) varnode,
							  (AttrNumber) pstate->p_next_resno++,
							  resname,
							  false);
		targetlist = lappend(targetlist, tmp);
	}

	return targetlist;
}

Node* addQualUniqueEdges(ParseState* pstate, Node* qual, List* ueids, List* ueidarrs)
{
    FuncCall* arrpos;
    ListCell* le1;
    ListCell* lea1;
    FuncCall* arroverlap;

    arrpos = makeFuncCall(list_make1(makeString("array_position")), NIL, -1);

    foreach (le1, ueids) {
        Node* eid1 = (Node*)lfirst(le1);
        ListCell* le2;

        for_each_cell(le2, lnext(le1))
        {
            Node* eid2 = (Node*)lfirst(le2);
            Expr* ne;

            ne = make_op(pstate, list_make1(makeString("<>")), eid1, eid2, -1);

            qual = qualAndExpr(qual, (Node*)ne);
        }

        foreach (lea1, ueidarrs) {
            Node* eidarr = (Node*)lfirst(lea1);
            Node* arg;
            NullTest* dupcond;

            arg = ParseFuncOrColumn(pstate, arrpos->funcname,
                list_make2(eidarr, eid1),
                (FuncCall*)pstate->p_last_srf, arrpos->location, -1);

            dupcond = makeNode(NullTest);
            dupcond->arg = (Expr*)arg;
            dupcond->nulltesttype = IS_NULL;
            dupcond->argisrow = false;
            // dupcond->location = -1;

            qual = qualAndExpr(qual, (Node*)dupcond);
        }
    }

    arroverlap = makeFuncCall(list_make1(makeString("arrayoverlap")), NIL, -1);

    foreach (lea1, ueidarrs) {
        Node* eidarr1 = (Node*)lfirst(lea1);
        ListCell* lea2;

        for_each_cell(lea2, lnext(lea1))
        {
            Node* eidarr2 = (Node*)lfirst(lea2);
            Node* funcexpr;
            Node* dupcond;

            funcexpr = ParseFuncOrColumn(pstate, arroverlap->funcname,
                list_make2(eidarr1, eidarr2),
                (FuncCall*)pstate->p_last_srf, arroverlap->location, -1);

            dupcond = (Node*)makeBoolExpr(NOT_EXPR, list_make1(funcexpr), -1);

            qual = qualAndExpr(qual, dupcond);
        }
    }

    return qual;
}

Node* qualAndExpr(Node* qual, Node* expr)
{
    if (qual == NULL)
        return expr;

    if (expr == NULL)
        return qual;

    if (IsA(qual, BoolExpr)) {
        BoolExpr* bexpr = (BoolExpr*)qual;

        if (bexpr->boolop == AND_EXPR) {
            bexpr->args = lappend(bexpr->args, expr);
            return qual;
        }
    }

    return (Node*)makeBoolExpr(AND_EXPR, list_make2(qual, expr), -1);
}

ParseNamespaceItem*
findNamespaceItemForRTE(ParseState* pstate, RangeTblEntry* rte)
{
    ListCell* lni;

    foreach (lni, pstate->p_relnamespace) {
        ParseNamespaceItem* nsitem = (ParseNamespaceItem*)lfirst(lni);

        if (nsitem->p_rte == rte)
            return nsitem;
    }

    return NULL;
}

Node* getColumnVar(ParseState* pstate, RangeTblEntry* rte, char* colname)
{
    ListCell* lcn;
    int attrno;
    Var* var;

    attrno = 1;
    foreach (lcn, rte->eref->colnames) {
        const char* tmp = strVal(lfirst(lcn));

        if (strcmp(tmp, colname) == 0) {
            /*
             * NOTE: no ambiguous reference check here
             *       since all column names in `rte` are unique
             */

            var = make_var(pstate, rte, attrno, -1);

            /* require read access to the column */
            markVarForSelectPriv(pstate, var, rte);

            return (Node*)var;
        }

        attrno++;
    }

    elog(ERROR, "column \"%s\" not found (internal error)", colname);
    return NULL;
}

/* generate unique name */
char* genUniqueName(void)
{
    /* NOTE: safe unless there are more than 2^32 anonymous names at once */
    static uint32 seq = 0;

    char data[NAMEDATALEN];

    snprintf(data, sizeof(data), "<%010u>", seq++);

    return pstrdup(data);
}

TargetEntry*
makeWholeRowTarget(ParseState* pstate, RangeTblEntry* rte)
{
    int rtindex;
    Var* varnode;

    rtindex = RTERangeTablePosn(pstate, rte, NULL);

    varnode = makeWholeRowVar(rte, rtindex, 0, false);
    varnode->location = -1;

    markVarForSelectPriv(pstate, varnode, rte);

    return makeTargetEntry((Expr*)varnode,
        (AttrNumber)pstate->p_next_resno++,
        rte->eref->aliasname,
        false);
}

void addRTEtoJoinlist(ParseState* pstate, RangeTblEntry* rte, bool visible)
{
    RangeTblEntry* tmp;
    RangeTblRef* rtr;
    ParseNamespaceItem* nsitem;

    /*
     * There should be no namespace conflicts because we check a variable
     * (which becomes an alias) is duplicated. This check remains to prevent
     * future programming error.
     */
    tmp = findRTEfromNamespace(pstate, rte->eref->aliasname);
    if (tmp != NULL) {
        if (!(rte->rtekind == RTE_RELATION && rte->alias == NULL && tmp->rtekind == RTE_RELATION && tmp->alias == NULL && rte->relid != tmp->relid))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_ALIAS),
                    errmsg("variable \"%s\" specified more than once",
                        rte->eref->aliasname)));
    }

    makeExtraFromRTE(pstate, rte, &rtr, &nsitem, visible);
    pstate->p_joinlist = lappend(pstate->p_joinlist, rtr);
    pstate->p_relnamespace = lappend(pstate->p_relnamespace, nsitem);
    pstate->p_varnamespace = lappend(pstate->p_varnamespace, makeNamespaceItem(rte, false, true));
}

void makeExtraFromRTE(ParseState* pstate, RangeTblEntry* rte, RangeTblRef** rtr,
    ParseNamespaceItem** nsitem, bool visible)
{
    if (rtr != NULL) {
        RangeTblRef* _rtr;

        _rtr = makeNode(RangeTblRef);
        _rtr->rtindex = RTERangeTablePosn(pstate, rte, NULL);

        *rtr = _rtr;
    }

    if (nsitem != NULL) {
        ParseNamespaceItem* _nsitem;

        _nsitem = (ParseNamespaceItem*)palloc(sizeof(ParseNamespaceItem));
        _nsitem->p_rte = rte;
        _nsitem->p_rel_visible = visible;
        _nsitem->p_cols_visible = visible;
        _nsitem->p_lateral_only = false;
        _nsitem->p_lateral_ok = true;

        *nsitem = _nsitem;
    }
}

/* just find RTE of `refname` in the current namespace */
RangeTblEntry*
findRTEfromNamespace(ParseState* pstate, char* refname)
{
    ListCell* lni;

    if (refname == NULL)
        return NULL;

    foreach (lni, pstate->p_relnamespace) {
        ParseNamespaceItem* nsitem = (ParseNamespaceItem*)lfirst(lni);
        RangeTblEntry* rte = nsitem->p_rte;

        /* NOTE: skip all checks on `nsitem` */

        if (strcmp(rte->eref->aliasname, refname) == 0)
            return rte;
    }

    return NULL;
}

List* makeTargetListFromRTE(ParseState* pstate, RangeTblEntry* rte)
{
    List* targetlist = NIL;
    int rtindex;
    int varattno;
    ListCell* ln;
    ListCell* lt;

    AssertArg(rte->rtekind == RTE_SUBQUERY);

    rtindex = RTERangeTablePosn(pstate, rte, NULL);

    varattno = 1;
    ln = list_head(rte->eref->colnames);
    foreach (lt, rte->subquery->targetList) {
        TargetEntry* te = (TargetEntry*)lfirst(lt);
        Var* varnode;
        char* resname;
        TargetEntry* tmp;

        if (te->resjunk)
            continue;

        Assert(varattno == te->resno);

        /* no transform here, just use `te->expr` */
        varnode = makeVar(rtindex, varattno,
            exprType((Node*)te->expr),
            exprTypmod((Node*)te->expr),
            exprCollation((Node*)te->expr),
            0);

        resname = strVal(lfirst(ln));

        tmp = makeTargetEntry((Expr*)varnode,
            (AttrNumber)pstate->p_next_resno++,
            resname,
            false);
        targetlist = lappend(targetlist, tmp);

        varattno++;
        ln = lnext(ln);
    }

    return targetlist;
}