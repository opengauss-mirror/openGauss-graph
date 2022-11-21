/*
 * parse_sparql_expr.c
 * 		handle SPARQL query in parser
 *
 *  Created on: May 14, 2020
 *      Author: tju_liubaozhu
 */
#include "postgres.h"
#include <string.h>

#include "catalog/gs_graph_fn.h"
#include "catalog/gs_label.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "gs_const.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "parser/parse_rdfs.h"
#include "parser/parse_relation.h"
#include "parser/parse_sparql_expr.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
// #include "access/htup_details.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "parser/parse_cypher_utils.h"

#define VLE_COLNAME_IDS "ids"
#define VLE_LEFT_ALIAS "l"
#define VLE_RIGHT_ALIAS "r"
#define VLE_COLNAME_NEXT "next"

/*check whether vertex label exist*/
#define vertexLabelExist(pstate, labname, labloc) \
    labelExist(pstate, labname, labloc, LABEL_KIND_VERTEX, true)
/*check whether edge label exist*/
#define edgeLabelExist(pstate, labname, labloc) \
    labelExist(pstate, labname, labloc, LABEL_KIND_EDGE, false)

static bool labelExist(ParseState* pstate, char* labname, int labloc, char labkind, bool throw_);

static void addQualUniqueEdges_legcy(ParseState* pstate, List** args, List* ueids, List* ueidarrs);
static char* strim(char* str);
static char* toDowncase(char* in);

/*
 * main processes for SPARQL query transformer
 * */
static List* transformTargets(ParseState* pstate, List* targetList, List* table,
    List* attrList);
static void transformTypedTriples(ParseState* pstate, List* triList,
    List** args, List** tables, List** attrList);
static void transformTriples(ParseState* pstate, List* triList, List** args,
    List** tables, List** attrList);
static void transformVariableEdge(ParseState* pstate, List* triList, List** args,
    List** tables, List** attrList);
static void transformFilters(ParseState* pstate, List* filList, List** args,
    List* tables, List* attrList);

/*
 * deal with all kinds of arguments
 * */
static int funcType(char* funName);
static Node* transReExpr(ParseState* pstate, List* tables, List* attrList,
    Node* fnNode, char* expr, char* note);
static Node* funcArg(ParseState* pstate, Node* arg, List* tables,
    List* attrList);
static Node* transColumn(ParseState* pstate, RangeTblEntry* rte, char* attr);
static Node* transColumnEqualUri(ParseState* pstate, Node* arg, char* iri_value);
static Node* transA_Expr(ParseState* pstate, Node* a_expr, List* tables,
    List* attrList);
static Node* transBoolExpr(ParseState* pstate, Node* a_expr, List* tables,
    List* attrList);
static Node* transCast(ParseState* pstate, Node* expr, List* tables,
    List* attrList, char* type);
static RangeTblEntry* addJoin(ParseState* pstate, Node* label, Node* expr,
    bool named);
static Node* attrEq(ParseState* pstate, RangeTblEntry* target, Node* pNode,
    Node* oNode);
static void swapNode(Node** left, Node** right);

/*functions for property path*/
static void setInitialVidForVLE(ParseState* pstate, SparqlPathEl* pathEl,
    Node* vertex, SparqlPathEl* prev_pathEl, RangeTblEntry* prev_edge);
static RangeTblEntry* transformPropertyPath(ParseState* pstate,
    SparqlPathEl* pathEl);
static RangeTblEntry* transformVariablePath(ParseState* pstate);
static RangeTblEntry* transformPathSR(ParseState* pstate, SparqlPathEl* pathEl);
static RangeTblEntry* transformPathVL(ParseState* pstate, SparqlPathEl* pathEl);
static RangeTblEntry* transformVLEtoRTE(ParseState* pstate, SelectStmt* vle,
    Alias* alias);
static SelectStmt* genVLESubselect(ParseState* pstate, SparqlPathEl* pathEl);
static Node* genVLEJoinExpr(SparqlPathEl* pathEl, Node* larg, Node* rarg);
static Node* genVLELeftChild(ParseState* pstate, SparqlPathEl* pathEl);
static Node* genVLERightChild(ParseState* pstate, SparqlPathEl* pathEl);
static Node* genVLEEdgeSubselect(ParseState* pstate, SparqlPathEl* pathEl,
    char* aliasname);
static char* getEdgeColname(SparqlPathEl* pathEl, bool prev);
static Node* addEdgePath(ParseState* pstate, SparqlPathEl* prev_pathEl,
    RangeTblEntry* prev_edge, SparqlPathEl* pathEl, RangeTblEntry* edge);
static Node* addVertexPath(ParseState* pstate, RangeTblEntry* vertex,
    SparqlPathEl* pathEl, RangeTblEntry* edge, bool prev);
/*
 * get entry for nodes or attributes
 * */
static RangeTblEntry* getEntry(List* joinList, char* alias);
static Triple* getAttrEntry(List* attrList, char* name);

static Node *makeEdgeExpr(ParseState *pstate, RangeTblEntry *rte, int location);
// static void addElemQual(ParseState *pstate, AttrNumber varattno);

typedef struct {
    TargetEntry* te;
    Node* prop_map;
} ElemQualOnly;

// typedef struct
// {
// 	Index		varno;			/* of the RTE */
// 	AttrNumber	varattno;		/* in the target list */
// 	// Node	   *prop_constr;	/* property constraint of the element */
// } ElemQual;

static char* strim(char* str)
{
    char *end, *sp, *ep;
    int len;
    sp = str;
    end = str + strlen(str) - 1;
    ep = end;

    while (sp <= end && (*sp == '\"' || *sp == '<'))
        sp++;
    while (ep >= sp && (*ep == '\"' || *ep == '>'))
        ep--;
    len = (ep < sp) ? 0 : (ep - sp) + 1;
    sp[len] = '\0';
    return sp;
}

static char* toDowncase(char* in)
{
    char* re = in;
    for (; *in != '\0'; in++) {
        if ((*in) >= 'A' && (*in) <= 'Z')
            (*in) += 32;
    }
    return re;
}

inline static char*
getSparqlName(Node* n)
{
    if (n == NULL)
        return NULL;
    switch (nodeTag(n)) {
        case T_SparqlVar:
            return ((SparqlVar*)n)->name;
            break;
        case T_SparqlString:
            return strim(((SparqlString*)n)->name);
            break;
        case T_SparqlIri:
            return strim(((SparqlIri*)n)->item);
            break;
        case T_SparqlBlank:
            return ((SparqlBlank*)n)->name;
            break;
        default:
            elog(ERROR, "Type for SPARQL node is not allowed");
            return NULL;
            break;
    }
}

inline static int
getSparqlNameLoc(Node *n)
{
	if (n == NULL)
		return -1;
	switch (nodeTag(n)) {
        case T_SparqlVar:
            return ((SparqlVar*)n)->location;
            break;
        case T_SparqlString:
            return ((SparqlString*)n)->location;
            break;
        case T_SparqlIri:
            return ((SparqlIri*)n)->location;
            break;
        case T_SparqlBlank:
            return ((SparqlBlank*)n)->location;
            break;
        default:
            elog(ERROR, "Type for SPARQL node is not allowed");
            return NULL;
            break;
    }
}

static void revSubObj(Node** sNode, Node** oNode)
{
    Node* temp = *sNode;
    *sNode = *oNode;
    *oNode = temp;
}

static int funcType(char* funName)
{
    int re = 0;
    if ((strcmp(funName, "isuri") == 0) || (strcmp(funName, "isiri") == 0))
        re = 1;
    else if (strcmp(funName, "isliteral") == 0)
        re = 2;
    else if (strcmp(funName, "isnumeric") == 0)
        re = 3;
    else if (strcmp(funName, "regex") == 0)
        re = 4;
    return re;
}

static RangeTblEntry* getEntry(List* joinList, char* alias)
{
    ListCell* joinCell;
    foreach (joinCell, joinList) {
        RangeTblEntry* table = (RangeTblEntry*)lfirst(joinCell);
        if (strcmp(table->alias->aliasname, alias) == 0) {
            return table;
        }
    }
    return NULL;
}

static Triple* getAttrEntry(List* attrList, char* name)
{
    ListCell* attrCell;
    foreach (attrCell, attrList) {
        Triple* attr = (Triple*)lfirst(attrCell);
        if (strcmp(name, attr->obj) == 0) {
            return attr;
        }
    }
    return NULL;
}

Query* transformSparqlLoadStmt(ParseState* pstate, SparqlLoadStmt* LoadStmt)
{
    RangeVar* rv = LoadStmt->relation;
    Query* qry;
    Query* srcQry;
    RangeTblEntry* rte;
    ParseState* srcPState;

    // set Query tree for data source
    srcQry = makeNode(Query);
    srcQry->commandType = CMD_SELECT;
    srcPState = make_parsestate(pstate);
    rte = addRangeTableEntry(srcPState, rv, rv->alias, rv->inhOpt == INH_YES, true);
    addRTEtoJoinlist(srcPState, rte, true);

    // select all columns
    ColumnRef* n = makeNode(ColumnRef);
    memset(n, 0, sizeof(ColumnRef));
    n->type = T_ColumnRef;
    ResTarget* rt = makeNode(ResTarget);
    memset(rt, 0, sizeof(ResTarget));
    rt->type = T_ResTarget;
    n->fields = list_make1(makeNode(A_Star));
    rt->val = (Node*)n;
    srcQry->targetList = transformTargetList(srcPState, list_make1(rt));

    srcQry->rtable = srcPState->p_rtable;
    srcQry->jointree = makeFromExpr(srcPState->p_joinlist, NULL);
    assign_query_collations(srcPState, srcQry);
    free_parsestate(srcPState);

    // set Query tree for sparql load
    qry = makeNode(Query);
    qry->commandType = CMD_SPARQLLOAD;
    rte = addRangeTableEntryForSubquery(pstate, srcQry, makeAlias("*loadSrc*", NIL), false, false);
    qry->targetList = makeTargetListFromRTE(pstate, rte);
    addRTEtoJoinlist(pstate, rte, true);
    markTargetListOrigins(pstate, qry->targetList);

    qry->rtable = lappend(qry->rtable, rte);
    qry->jointree = makeFromExpr(pstate->p_joinlist, NULL);
    assign_query_collations(pstate, qry);

    return qry;
    // return srcQry;
}

static List* transformTargets(ParseState* pstate, List* targetList, List* table,
    List* attrList)
{
    List* target = NIL;
    ListCell *targetCell, *attrCell;
    RangeTblEntry* rte;
    TargetEntry* te;
    char* name;
    Node* arg;
    bool resjunk = false;

    foreach (targetCell, targetList) {
        int resno = (resjunk ? InvalidAttrNumber : pstate->p_next_resno++);
        ResTarget* tgt = (ResTarget*)lfirst(targetCell);

        switch (nodeTag(tgt->val)) {
            /*star, output all var*/
            case T_ColumnRef: {
                ListCell* rteCell;

                foreach (attrCell, attrList) {
                    Triple* attr = (Triple*)lfirst(attrCell);
                    rte = getEntry(table, attr->sub);
                    /*
                    * subject and object in one triple can be same only if
                    * it is the end of the property path
                    * in this case, the attribute should be output rather than theuri of obj node
                    * we should remove the entry for uri of obj
                    * */
                    if (strcmp(attr->sub, attr->obj) == 0)
                        list_delete(table, rte);

                    arg = transColumn(pstate, rte, attr->pre);
                    te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, attr->obj, resjunk);
                        
                    target = lappend(target, te);
                }
                foreach (rteCell, table) {
                    rte = (RangeTblEntry*)lfirst(rteCell);

                    name = tgt->name == NULL ? rte->alias->aliasname : tgt->name;
                    arg = transColumn(pstate, rte, NULL);
                    te = makeTargetEntry((Expr*)makeVertexExpr(pstate, rte, default_loc), (AttrNumber)resno, name, resjunk);   
                    // te = makeTargetEntry((Expr *) makeEdgeExpr(pstate, rte, default_loc), (AttrNumber) resno, name, resjunk);                    
                    target = lappend(target, te);
                }
                break;
            }
            /*find the column var refer to, and output it*/
            case T_SparqlVar: {
                bool processed = false;
                Triple* attr = getAttrEntry(attrList, getSparqlName(tgt->val));

                if (attr == NULL) {
                    rte = getEntry(table, getSparqlName(tgt->val));
                    if (rte == NULL)
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("target not appear"), parser_errposition(pstate, tgt->location)));
                    
                    name = tgt->name == NULL ? rte->alias->aliasname : tgt->name;
                    arg = transColumn(pstate, rte, NULL);
                    processed = true;
                } else {
                    name = tgt->name == NULL ? attr->obj : tgt->name;
                    rte = getEntry(table, attr->sub);
                    arg = transColumn(pstate, rte, attr->pre);
                    processed = true;
                }

                if (processed == false) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("some targets not appear in where clause")));
                }
                if(strcmp(rte->relname, GS_EDGE) == 0) {
                    te = makeTargetEntry((Expr *) makeEdgeExpr(pstate, rte, default_loc), (AttrNumber) resno, name, resjunk);  
                } else {
                    te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, name, resjunk);
                }
                target = lappend(target, te);
                
                break;
            }
            /*agg functions*/
            case T_FuncCall: {
                List* targs = NIL;
                Node* expr;
                FuncCall* fn = (FuncCall*)tgt->val;
                Node* funcNode;
                bool resjunk = 0;
                char* name;
                ListCell* argCell;
                foreach (argCell, ((FuncCall*)tgt->val)->args) {
                    Node* arg = (Node*)lfirst(argCell);
                    expr = funcArg(pstate, ((ResTarget*)arg)->val, table,
                        attrList);
                    targs = lappend(targs, expr);
                }
                if (fn->agg_star == true) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("star for functions not support now"), parser_errposition(pstate, fn->location)));
                }
                funcNode = ParseFuncOrColumn(pstate, fn->funcname, targs, fn, fn->location, NULL);
                if (tgt->name == NULL && !resjunk) {
                    name = FigureColname((Node*)fn);
                } else {
                    name = tgt->name;
                }

                te = makeTargetEntry((Expr*)funcNode, (AttrNumber)resno, name, resjunk);
                target = lappend(target, te);
                break;
            }
            /*math calculations*/
            case T_A_Expr: {
                arg = transA_Expr(pstate, tgt->val, table, attrList);
                if (tgt->name == NULL && !resjunk) {
                    name = "?column?";
                } else {
                    name = tgt->name;
                }
                te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, name, resjunk);
                target = lappend(target, te);
                break;
            }
            /*string constants, just add to output*/
            case T_SparqlIri:
            case T_SparqlString: {
                Value* v = makeString(getSparqlName(tgt->val));
                arg = (Node*)make_const(pstate, v, -1);
                if (tgt->name == NULL && !resjunk) {
                    name = "?column?";
                } else {
                    name = tgt->name;
                }
                te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, name, resjunk);
                target = lappend(target, te);
                break;
            }
            /*number constants, just add to output*/
            case T_A_Const:
                arg = (Node*)make_const(pstate, &((A_Const*)tgt->val)->val, -1);
                if (tgt->name == NULL && !resjunk) {
                    name = "?column?";
                } else {
                    name = tgt->name;
                }
                te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, name, resjunk);
                target = lappend(target, te);
                break;

            case T_TypeCast: {
                Oid inputType;
                int32 targetTypmod;
                Oid targetType;
                TypeCast* tc = (TypeCast*)tgt->val;
                Node* expr = (Node*)make_const(pstate, &((A_Const*)tc->arg)->val,
                    -1);
                typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);
                inputType = exprType(expr);
                arg = coerce_to_target_type(pstate, expr, inputType, targetType,
                    targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
                    tgt->location);
                if (tgt->name == NULL && !resjunk) {
                    name = "?column?";
                } else {
                    name = tgt->name;
                }
                te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, name, resjunk);
                target = lappend(target, te);
                break;
            }
            /*boolean constants, just add to output*/
            case T_BoolExpr: {
                arg = transBoolExpr(pstate, tgt->val, table, attrList);
                if (tgt->name == NULL && !resjunk) {
                    name = "?column?";
                } else {
                    name = tgt->name;
                }
                te = makeTargetEntry((Expr*)arg, (AttrNumber)resno, name, resjunk);
                target = lappend(target, te);
                break;
            }
            default: {
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("type(s) in target list not allowed"), parser_errposition(pstate, tgt->location)));
                break;
            }
        }
    }
    return target;
}

static Node* transBoolExpr(ParseState* pstate, Node* boolexpr, List* tables,
    List* attrList)
{
    Node* re;
    BoolExpr* expr = (BoolExpr*)boolexpr;
    ListCell* exprCell;
    Node* arg;
    const char* opname;
    List* args = NIL;
    switch (((BoolExpr*)boolexpr)->boolop) {
    case AND_EXPR:
        opname = "AND";
        break;
    case OR_EXPR:
        opname = "OR";
        break;
    case NOT_EXPR:
        opname = "NOT";
        break;
    default:
        elog(ERROR, "unrecognized boolean operation: %d",
            ((BoolExpr*)boolexpr)->boolop);
        opname = NULL; /* keep compiler quiet */
        break;
    }

    foreach (exprCell, expr->args) {
        Node* cell = (Node*)lfirst(exprCell);
        switch (nodeTag(cell)) {
        /*boolean expression, recursively calculate*/
        case T_BoolExpr: {
            arg = transBoolExpr(pstate, cell, tables, attrList);
            arg = coerce_to_boolean(pstate, arg, opname);
            args = lappend(args, arg);
            break;
        }
            /*var taht refer to some columns, modify the type of the var first*/
        case T_SparqlVar: {
            arg = transCast(pstate, cell, tables, attrList, "bool");
            arg = coerce_to_boolean(pstate, arg, opname);
            args = lappend(args, arg);
            break;
        }

        case T_TypeCast: {
            Oid inputType;
            int32 targetTypmod;
            Oid targetType;
            TypeCast* tc = (TypeCast*)cell;
            Node* expr = (Node*)make_const(pstate, &((A_Const*)tc->arg)->val,
                -1);
            typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);
            inputType = exprType(expr);
            arg = coerce_to_target_type(pstate, expr, inputType, targetType,
                targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
                tc->location);
            arg = coerce_to_boolean(pstate, arg, opname);
            args = lappend(args, arg);
            break;
        }

        default:
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("type(s) in math expression not allowed"), parser_errposition(pstate, expr->location)));

            break;
        }
    }

    re = (Node*)makeBoolExpr(((BoolExpr*)boolexpr)->boolop, args,
        ((BoolExpr*)boolexpr)->location);

    return re;
}

static Node* transCast(ParseState* pstate, Node* expr, List* tables,
    List* attrList, char* type)
{
    Node* re;
    Oid inputType;
    TypeName* tname = SystemTypeName(type);
    int32 targetTypmod;
    Oid targetType;
    Node* arg;
    RangeTblEntry* rte = getEntry(tables, getSparqlName(expr));
    if (rte != NULL) {
        arg = transColumn(pstate, rte, NULL);
        typenameTypeIdAndMod(pstate, tname, &targetType, &targetTypmod);
        inputType = exprType(arg);
        re = coerce_to_target_type(pstate, arg, inputType, targetType,
            targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
            ((SparqlVar*)expr)->location);
    } else if (rte == NULL) {
        Triple* attr = getAttrEntry(attrList, getSparqlName(expr));
        rte = getEntry(tables, attr->sub);
        arg = transColumn(pstate, rte, attr->pre);
        typenameTypeIdAndMod(pstate, tname, &targetType, &targetTypmod);
        inputType = exprType(arg);
        re = coerce_to_target_type(pstate, arg, inputType, targetType,
            targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
            ((SparqlVar*)expr)->location);
    }
    return re;
}
/*
 * math expression should be calculated for left tree and right tree respectively
 * the type of var should be cast first
 * */
static Node* transA_Expr(ParseState* pstate, Node* a_expr, List* tables,
    List* attrList)
{
    Node* re;
    Node* lexpr;
    Node* rexpr;
    A_Expr* expr = (A_Expr*)a_expr;

    /*left part of expression*/
    if (expr->lexpr == NULL)
        lexpr = NULL;
    else {
        switch (nodeTag(expr->lexpr)) {
        case T_A_Expr:
            lexpr = transA_Expr(pstate, expr->lexpr, tables, attrList);
            break;
        case T_SparqlVar:
            lexpr = transCast(pstate, expr->lexpr, tables, attrList, "numeric");
            break;
        case T_A_Const:
            lexpr = (Node*)make_const(pstate, &((A_Const*)expr->lexpr)->val,
                -1);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("type(s) in math expression not allowed")));
            break;
        }
    }
    /*right part of expression*/
    if (expr->rexpr == NULL) {
        rexpr = NULL;
    } else {
        switch (nodeTag(expr->rexpr)) {
        case T_A_Expr:
            rexpr = transA_Expr(pstate, expr->rexpr, tables, attrList);
            break;
        case T_SparqlVar:
            rexpr = transCast(pstate, expr->rexpr, tables, attrList, "numeric");
            break;
        case T_A_Const:
            rexpr = (Node*)make_const(pstate, &((A_Const*)expr->rexpr)->val,
                -1);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("type(s) in math expression not allowed")));
            break;
        }
    }
    re = (Node*)make_op(pstate, ((A_Expr*)a_expr)->name, lexpr, rexpr, -1);
    return re;
}

/*transform the arguments of agg functions*/
static Node* funcArg(ParseState* pstate, Node* arg, List* tables,
    List* attrList)
{
    Node* expr;
    switch (nodeTag(arg)) {
    /*var that refer to some column*/
    case T_SparqlVar: {
        RangeTblEntry* rte = getEntry(tables, getSparqlName(arg));
        if (rte != NULL)
            expr = transColumn(pstate, rte, NULL);
        else {
            Triple* t = getAttrEntry(attrList, getSparqlName(arg));
            rte = getEntry(tables, t->sub);
            expr = transColumn(pstate, rte, t->pre);
        }

        break;
    }
        /*literal constant*/
    case T_SparqlIri:
    case T_SparqlString: {
        Value* v = makeString(getSparqlName(arg));
        expr = (Node*)make_const(pstate, v, -1);
        break;
    }
        /*math expression*/
    case T_A_Expr: {
        expr = transA_Expr(pstate, arg, tables, attrList);
        break;
    }
        /*number constant*/
    case T_A_Const: {
        expr = (Node*)make_const(pstate, &((A_Const*)arg)->val, -1);
        break;
    }
    case T_TypeCast: {
        Oid inputType;
        int32 targetTypmod;
        Oid targetType;
        TypeCast* tc = (TypeCast*)arg;
        expr = (Node*)make_const(pstate, &((A_Const*)tc->arg)->val, -1);
        typenameTypeIdAndMod(pstate, tc->typname, &targetType, &targetTypmod);
        inputType = exprType(expr);
        expr = coerce_to_target_type(pstate, expr, inputType, targetType,
            targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
            tc->location);
        break;
    }
        /*boolean constant*/
    case T_BoolExpr: {
        expr = transBoolExpr(pstate, arg, tables, attrList);
        break;
    }
    default:
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("argument(s) in function not allowed or not support now")));

        return NULL;
        break;
    }
    return expr;
}
/*
 * transformer for regular expression
 * */
static Node* transReExpr(ParseState* pstate, List* tables, List* attrList,
    Node* fnNode, char* expr, char* note)
{
    Node* arg;
    Value* v;
    Node* rexpr;
    if (nodeTag(fnNode) == T_SparqlVar) {
        RangeTblEntry* rte = getEntry(tables, getSparqlName(fnNode));
        if (rte != NULL) {
            arg = transColumn(pstate, rte, NULL);
        } else {
            Triple* attr = getAttrEntry(attrList, getSparqlName(fnNode));
            rte = getEntry(tables, attr->sub);
            arg = transColumn(pstate, rte, attr->pre);
        }
        v = makeString(expr);
        rexpr = (Node*)make_const(pstate, v, default_loc);
        arg = (Node*)make_op(pstate, list_make1(makeString(note)), arg, rexpr, default_loc);
        arg = coerce_to_boolean(pstate, arg, "WHERE");
    } else {
        Node* lexpr;
        char* name = getSparqlName(fnNode);
        v = makeString(name);
        lexpr = (Node*)make_const(pstate, v, default_loc);
        v = makeString(expr);
        rexpr = (Node*)make_const(pstate, v, default_loc);
        arg = (Node*)make_op(pstate, list_make1(makeString(note)), lexpr, rexpr, default_loc);
        arg = coerce_to_boolean(pstate, arg, "WHERE");
    }
    return arg;
}

/*
 * transformer for JSON properties
 * */
static Node* transColumn(ParseState* pstate, RangeTblEntry* rte, char* attr)
{
    Node* lexpr;
    Node* rexpr;
    Value* v;
    Node *last_srf, *arg;
    char* json = "->>";

    lexpr = scanRTEForColumn(pstate, rte, GS_ELEM_PROP_MAP, default_loc);
    if (attr != NULL)
        v = makeString(attr);
    else {
        v = makeString(GS_URI);
    } 
    rexpr = (Node*)make_const(pstate, v, default_loc);
    last_srf = pstate->p_last_srf;
    arg = (Node*)make_op(pstate, list_make1(makeString(json)), lexpr, rexpr, default_loc);

    return arg;
}

/*
 * Function: construct the expression used to constrain the value of uri
 * arg is expression "x.properties->>uri" 
 * The goal is to construct an expression "x.properties->>uri = <....>"
 */
static Node* transColumnEqualUri(ParseState* pstate, Node* arg, char* iri_value){

    Node* lexpr; //"x.properties->>uri"
    Node* rexpr; //<....>
    char* op = "=";
    Value* v;
    Node* new_arg;

    lexpr = arg;
    v = makeString(iri_value);
    rexpr = (Node*)make_const(pstate, v, default_loc);
    new_arg = (Node*)make_op(pstate, list_make1(makeString(op)), lexpr, rexpr, default_loc);

    return new_arg;

}

/*
 * add the vertex table into join list
 * */
static RangeTblEntry* addJoin(ParseState* pstate, Node* label, Node* expr,
    bool named)
{
    RangeTblEntry* rte;
    RangeVar* r;
    Alias* alias;
    char* l;

    if (named)
        l = toDowncase(getSparqlName(label));
    else
        l = GS_VERTEX;

    r = makeRangeVar(get_graph_path(true), l, ((SparqlIri*)label)->location);
    r->inhOpt = INH_YES;
    alias = (expr == NULL) ? makeAliasOptUnique(NULL) : makeAliasOptUnique(getSparqlName(expr));

    rte = addRangeTableEntry(pstate, r, alias, r->inhOpt == INH_YES, true);
    addRTEtoJoinlist(pstate, rte, false);
    return rte;
}

/*
 * add edge table into join list
 * */
static Node* addEdgePath(ParseState* pstate, SparqlPathEl* prev_pathEl,
    RangeTblEntry* prev_edge, SparqlPathEl* pathEl, RangeTblEntry* edge)
{
    Node* arg;
    Node *lexpr, *rexpr;
    lexpr = getColumnVar(pstate, prev_edge, getEdgeColname(prev_pathEl, true));
    rexpr = getColumnVar(pstate, edge, getEdgeColname(pathEl, false));
    arg = (Node*)make_op(pstate, list_make1(makeString("=")), lexpr, rexpr, -1);
    arg = coerce_to_boolean(pstate, arg, "AND");
    return arg;
}

static Node* addVertexPath(ParseState* pstate, RangeTblEntry* vertex,
    SparqlPathEl* pathEl, RangeTblEntry* edge, bool prev)
{
    Node* arg;
    Node *lexpr, *rexpr;
    /* `vertex` is just a placeholder for relationships */
    if (vertex == NULL)
        return NULL;

    /*	 already done in transformPathVL()
     if (pathEl->range != NULL && prev)
     return NULL;*/

    lexpr = getColumnVar(pstate, vertex, GS_ELEM_LOCAL_ID);
    rexpr = getColumnVar(pstate, edge, getEdgeColname(pathEl, prev));
    arg = (Node*)make_op(pstate, list_make1(makeString("=")), lexpr, rexpr, -1);
    arg = coerce_to_boolean(pstate, arg, "AND");
    return arg;
}

/*
 * restrict attribute values
 * */
static Node* attrEq(ParseState* pstate, RangeTblEntry* target, Node* pNode,
    Node* oNode)
{
    Node* arg;
    Node *lexpr, *rexpr;
    Value* v;
    lexpr = scanRTEForColumn(pstate, target, GS_ELEM_PROP_MAP, ((SparqlIri*)oNode)->location);

    v = makeString(getSparqlName(pNode));
    rexpr = (Node*)make_const(pstate, v, ((SparqlIri*)pNode)->location);
    arg = (Node*)make_op(pstate, list_make1(makeString("->>")), lexpr, rexpr, ((SparqlIri*)pNode)->location);
    lexpr = arg;
    v = makeString(getSparqlName(oNode));
    rexpr = (Node*)make_const(pstate, v, ((SparqlString*)(oNode))->location);
    arg = (Node*)make_op(pstate, list_make1(makeString("=")), lexpr, rexpr, ((SparqlIri*)pNode)->location);
    arg = coerce_to_boolean(pstate, arg, "AND");
    return arg;
}

/*help handle inverse case*/
static void swapNode(Node** left, Node** right)
{
    Node* temp;
    temp = *left;
    *left = *right;
    *right = temp;
}

/*save the info of last edge*/
static void setInitialVidForVLE(ParseState* pstate, SparqlPathEl* pathEl,
    Node* vertex, SparqlPathEl* prev_pathEl, RangeTblEntry* prev_edge)
{
    ColumnRef* cref;

    /*nothing to do*/
    if (pathEl->range == NULL)
        return;
    if (vertex == NULL) {
        char* colname;

        colname = getEdgeColname(prev_pathEl, true);

        cref = makeNode(ColumnRef);
        cref->fields = list_make2(makeString(prev_edge->eref->aliasname),
            makeString(colname));
        cref->location = -1;

        pstate->p_vle_initial_vid = (Node*)cref;
        pstate->p_vle_initial_rte = prev_edge;
        return;
    }
    if (IsA(vertex, RangeTblEntry)) {
        RangeTblEntry* rte = (RangeTblEntry*)vertex;

        Assert(rte->rtekind == RTE_RELATION);

        cref = makeNode(ColumnRef);
        cref->fields = list_make2(makeString(rte->eref->aliasname),
            makeString(GS_ELEM_LOCAL_ID));
        cref->location = -1;

        pstate->p_vle_initial_vid = (Node*)cref;
        pstate->p_vle_initial_rte = rte;
        return;
    }
}

/*handle with property path*/
static RangeTblEntry*
transformPropertyPath(ParseState* pstate, SparqlPathEl* pathEl)
{
    edgeLabelExist(pstate, getSparqlName(pathEl->el), -1);
    /*edge with static length*/
    if (pathEl->range == NULL)
        return transformPathSR(pstate, pathEl);
    /*edge with variable length*/
    else
        return transformPathVL(pstate, pathEl);
    return NULL;
}

/* handle with variable path */
static RangeTblEntry*
transformVariablePath(ParseState* pstate, Node* expr)
{
    RangeTblEntry* rte;
    RangeVar* r;
    Alias* alias;
    char* varname = getSparqlName(expr);
    int varloc = getSparqlNameLoc(expr);

    alias = (expr == NULL) ? makeAliasOptUnique(NULL) : makeAliasOptUnique(varname);
    r = makeRangeVar(get_graph_path(true), GS_EDGE, -1);
    r->inhOpt = INH_YES;

    rte = addRangeTableEntry(pstate, r, alias, r->inhOpt == INH_YES, true);
    addRTEtoJoinlist(pstate, rte, false);

    return rte;
}

static Node *
makeEdgeExpr(ParseState *pstate, RangeTblEntry *rte,
			 int location)
{
	Node	   *id;
	Node	   *start;
	Node	   *end;
	Node	   *prop_map;
	Node	   *tid;

	id = getColumnVar(pstate, rte, GS_ELEM_LOCAL_ID);
	start = getColumnVar(pstate, rte, GS_START_ID);
	end = getColumnVar(pstate, rte, GS_END_ID);
	prop_map = getColumnVar(pstate, rte, GS_ELEM_PROP_MAP);
	tid = getSysColumnVar(pstate, rte, SelfItemPointerAttributeNumber);

	return makeTypedRowExpr(list_make5(id, start, end, prop_map, tid),
							EDGEOID, location);
}

/*edge with static length*/
static RangeTblEntry* transformPathSR(ParseState* pstate, SparqlPathEl* pathEl)
{
    RangeTblEntry* rte;
    RangeVar* r;
    Alias* alias;
    alias = makeAliasOptUnique(NULL);
    r = makeRangeVar(get_graph_path(true), getSparqlName(pathEl->el), -1);
    r->inhOpt = INH_YES;

    rte = addRangeTableEntry(pstate, r, alias, r->inhOpt == INH_YES, true);
    addRTEtoJoinlist(pstate, rte, false);
    return rte;
}

/*edge with variable length*/
static RangeTblEntry* transformPathVL(ParseState* pstate, SparqlPathEl* pathEl)
{
    RangeTblEntry* rte;
    SelectStmt* sel;
    Alias* alias;

    sel = genVLESubselect(pstate, pathEl);
    alias = makeAliasOptUnique(NULL);
    rte = transformVLEtoRTE(pstate, sel, alias);

    return rte;
}

/*parse the subselect query to make a intermediate relation for property path*/
static RangeTblEntry*
transformVLEtoRTE(ParseState* pstate, SelectStmt* vle, Alias* alias)
{
    ParseNamespaceItem* nsitem = NULL;
    Query* qry;
    RangeTblEntry* rte;

    Assert(!pstate->p_lateral_active);
    Assert(pstate->p_expr_kind == EXPR_KIND_NONE);

    /* make the RTE temporarily visible */
    if (pstate->p_vle_initial_rte != NULL) {
        nsitem = findNamespaceItemForRTE(pstate, pstate->p_vle_initial_rte);
        Assert(nsitem != NULL);

        nsitem->p_rel_visible = true;
    }

    pstate->p_lateral_active = true;
    pstate->p_expr_kind = EXPR_KIND_FROM_SUBSELECT;

    qry = parse_sub_analyze((Node*)vle, pstate, NULL,
        isLockedRefname(pstate, alias->aliasname), true);
    Assert(qry->commandType == CMD_SELECT);

    pstate->p_lateral_active = false;
    pstate->p_expr_kind = EXPR_KIND_NONE;

    if (nsitem != NULL)
        nsitem->p_rel_visible = false;

    rte = addRangeTableEntryForSubquery(pstate, qry, alias, true, true);
    rte->isVLE = true;
    addRTEtoJoinlist(pstate, rte, false);

    return rte;
}

/*make the intemediate relation to handle VLE*/
static SelectStmt*
genVLESubselect(ParseState* pstate, SparqlPathEl* pathEl)
{
    char* prev_colname;
    Node* prev_col;
    ResTarget* prev;
    char* curr_colname;
    Node* curr_col;
    ResTarget* curr;
    Node* ids_col;
    ResTarget* ids;
    Node* next_col;
    ResTarget* next;
    Node* id_col;
    ResTarget* id;
    List* tlist;
    Node* left;
    Node* right;
    Node* join;
    SelectStmt* sel;

    prev_colname = getEdgeColname(pathEl, false);
    prev_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, prev_colname));
    prev = makeResTarget(prev_col, prev_colname);

    curr_colname = getEdgeColname(pathEl, true);
    curr_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, curr_colname));
    curr = makeResTarget(curr_col, curr_colname);

    ids_col = makeColumnRef(genQualifiedName(VLE_LEFT_ALIAS, VLE_COLNAME_IDS));
    ids = makeResTarget(ids_col, VLE_COLNAME_IDS);

    tlist = list_make3(prev, curr, ids);

    next_col = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS,
        VLE_COLNAME_NEXT));
    next = makeResTarget(next_col, VLE_COLNAME_NEXT);

    tlist = lappend(tlist, next);

    id_col = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS, GS_ELEM_LOCAL_ID));
    id = makeResTarget(id_col, GS_ELEM_LOCAL_ID);

    tlist = lappend(tlist, id);

    left = genVLELeftChild(pstate, pathEl);
    right = genVLERightChild(pstate, pathEl);

    join = genVLEJoinExpr(pathEl, left, right);

    sel = makeNode(SelectStmt);
    sel->targetList = tlist;
    sel->fromClause = list_make1(join);

    return sel;
}

/*join the left and right part of VLE together*/
static Node*
genVLEJoinExpr(SparqlPathEl* pathEl, Node* larg, Node* rarg)
{
    A_Const* trueconst;
    TypeCast* truecond;
    A_Indices* indices;
    int minHops;
    int maxHops = -1;
    JoinExpr* n;

    trueconst = makeNode(A_Const);
    trueconst->val.type = T_String;
    trueconst->val.val.str = "t";
    trueconst->location = -1;
    truecond = makeNode(TypeCast);
    truecond->arg = (Node*)trueconst;
    truecond->typname = makeTypeNameFromNameList(genQualifiedName("pg_catalog", "bool"));
    truecond->location = -1;

    indices = (A_Indices*)pathEl->range;
    minHops = ((A_Const*)indices->lidx)->val.val.ival;
    if (indices->uidx != NULL)
        maxHops = ((A_Const*)indices->uidx)->val.val.ival;

    n = makeNode(JoinExpr);
    n->jointype = JOIN_VLE;
    n->isNatural = false;
    n->larg = larg;
    n->rarg = rarg;
    n->usingClause = NIL;
    n->quals = (Node*)truecond;
    n->minHops = minHops;
    n->maxHops = maxHops;

    return (Node*)n;
}

/*judge the property path is from zero length or not */
static bool isZeroLengthVLE(SparqlPathEl* pathEl)
{
    A_Indices* indices;
    A_Const* lidx;

    if (pathEl == NULL)
        return false;

    if (pathEl->range == NULL)
        return false;

    indices = (A_Indices*)pathEl->range;
    lidx = (A_Const*)indices->lidx;
    return (lidx->val.val.ival == 0);
}

/*the left part of subselect fro VLE*/
static Node*
genVLELeftChild(ParseState* pstate, SparqlPathEl* pathEl)
{
    Node* vid;
    A_ArrayExpr* idarr;
    RangeSubselect* sub;
    SelectStmt* sel;
    List* colnames = NIL;
    /*
     * `vid` is NULL only if
     * (there is no previous edge of the vertex in the path
     *  and the vertex is transformed first time in the pattern)
     * and pathEl is not zero-length
     */
    vid = pstate->p_vle_initial_vid;

    if (isZeroLengthVLE(pathEl)) {
        TypeCast* ids;
        List* values;

        Assert(vid != NULL);

        idarr = makeNode(A_ArrayExpr);
        idarr->location = -1;
        ids = makeNode(TypeCast);
        ids->arg = (Node*)idarr;
        ids->typname = makeTypeName("_graphid");
        ids->location = -1;

        values = list_make3(vid, vid, ids);
        colnames = list_make3(makeString(getEdgeColname(pathEl, false)),
            makeString(getEdgeColname(pathEl, true)),
            makeString(VLE_COLNAME_IDS));
        sel = makeNode(SelectStmt);
        sel->valuesLists = list_make1(values);
    } else {
        List* prev_colname;
        Node* prev_col;
        ResTarget* prev;
        ResTarget* curr;
        Node* id;
        TypeCast* cast;
        ResTarget* ids;
        List* tlist = NIL;
        Node* from;
        List* where_args = NIL;

        prev_colname = genQualifiedName(NULL, getEdgeColname(pathEl, false));
        prev_col = makeColumnRef(prev_colname);
        prev = makeResTarget(prev_col, NULL);
        curr = makeSimpleResTarget(getEdgeColname(pathEl, true), NULL);

        id = makeColumnRef(genQualifiedName(NULL, GS_ELEM_LOCAL_ID));
        idarr = makeNode(A_ArrayExpr);
        idarr->elements = list_make1(id);
        idarr->location = -1;
        cast = makeNode(TypeCast);
        cast->arg = (Node*)idarr;
        cast->typname = makeTypeName("_graphid");
        cast->location = -1;
        ids = makeResTarget((Node*)cast, VLE_COLNAME_IDS);

        tlist = list_make3(prev, curr, ids);

        from = genVLEEdgeSubselect(pstate, pathEl, VLE_LEFT_ALIAS);

        if (vid != NULL) {
            A_Expr* vidcond;

            vidcond = makeSimpleA_Expr(AEXPR_OP, "=", vid, prev_col, -1);
            where_args = lappend(where_args, vidcond);
        }
        sel = makeNode(SelectStmt);
        sel->targetList = tlist;
        sel->fromClause = list_make1(from);
        sel->whereClause = (Node*)makeBoolExpr(AND_EXPR, where_args, -1);
    }
    sub = makeNode(RangeSubselect);
    sub->subquery = (Node*)sel;
    sub->alias = makeAliasNoDup(VLE_LEFT_ALIAS, colnames);
    return (Node*)sub;
}

/*the right part subselect to handle VLE*/
static Node*
genVLERightChild(ParseState* pstate, SparqlPathEl* pathEl)
{
    Node* colref;
    ResTarget* next;
    ResTarget* id;
    SelectStmt* sel;
    RangeSubselect* sub;
    List* tlist;
    List* from = NIL;
    List* where_args = NIL;
    ColumnRef* prev;
    ColumnRef* curr;
    A_Expr* joinqual;

    colref = makeColumnRef(
        genQualifiedName(VLE_RIGHT_ALIAS, getEdgeColname(pathEl, true)));
    next = makeResTarget(colref, VLE_COLNAME_NEXT);

    colref = makeColumnRef(genQualifiedName(VLE_RIGHT_ALIAS, GS_ELEM_LOCAL_ID));
    id = makeResTarget(colref, GS_ELEM_LOCAL_ID);
    tlist = list_make2(next, id);

    from = lappend(from, genVLEEdgeSubselect(pstate, pathEl, VLE_RIGHT_ALIAS));

    prev = makeNode(ColumnRef);
    prev->fields = genQualifiedName(VLE_LEFT_ALIAS,
        getEdgeColname(pathEl, true));
    prev->location = -1;
    curr = makeNode(ColumnRef);
    curr->fields = genQualifiedName(VLE_RIGHT_ALIAS,
        getEdgeColname(pathEl, false));
    curr->location = -1;
    joinqual = makeSimpleA_Expr(AEXPR_OP, "=", (Node*)prev, (Node*)curr, -1);
    where_args = lappend(where_args, joinqual);

    sel = makeNode(SelectStmt);
    sel->targetList = tlist;
    sel->fromClause = from;
    sel->whereClause = (Node*)makeBoolExpr(AND_EXPR, where_args, -1);

    sub = makeNode(RangeSubselect);
    sub->subquery = (Node*)sel;
    sub->alias = makeAliasNoDup(VLE_RIGHT_ALIAS, NULL);
    sub->lateral = true;

    return (Node*)sub;
}

/*meke the intemediate relation to handle VLE*/
static Node*
genVLEEdgeSubselect(ParseState* pstate, SparqlPathEl* pathEl, char* aliasname)
{
    char* typname = getSparqlName(pathEl->el);
    Alias* alias = makeAliasNoDup(aliasname, NIL);
    Node* edge;
    RangeVar* r;
    LOCKMODE lockmode;
    Relation rel;

    r = makeRangeVar(get_graph_path(true), typname, -1);
    r->inhOpt = INH_YES;

    if (isLockedRefname(pstate, aliasname))
        lockmode = RowShareLock;
    else
        lockmode = AccessShareLock;

    rel = parserOpenTable(pstate, r, lockmode);
    r->alias = alias;
    edge = (Node*)r;
    heap_close(rel, NoLock);
    return edge;
}

/*get the edge column to JOIN on*/
static char* getEdgeColname(SparqlPathEl* pathEl, bool prev)
{
    if (pathEl == NULL) {  /* the predicate is a variable */
        if (prev) {
            return GS_END_ID;
        } else {
            return GS_START_ID;
        }
    } else {
        if (prev) {
            if (pathEl->inv == TRUE)
                return GS_START_ID;
            else
                return GS_END_ID;
        } else {
            if (pathEl->inv == TRUE)
                return GS_END_ID;
            else
                return GS_START_ID;
        }
    }
    
}

/*
 * transform the triples with rdf:type property
 * */
static void transformTypedTriples(ParseState* pstate, List* triList,
    List** args, List** tables, List** attrList)
{
    ListCell* tric;
    ListCell* poc;
    ListCell* obj;

    foreach (tric, triList) {   
        SparqlTriple* tri = (SparqlTriple*)lfirst(tric);  
        Node* sNode = tri->sub;   

        if (nodeTag(sNode) == T_SparqlString) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subjects can not be string"), parser_errposition(pstate, ((SparqlString*)sNode)->location)));
        }

        foreach (poc, tri->pre_obj) {   
            SparqlPreObj* po = (SparqlPreObj*)lfirst(poc);  

            if(nodeTag(po->pre) == T_SparqlVar) {       // note: the predicate is a variable
                return;
            }

            SparqlPathOr* orPath = (SparqlPathOr*)po->pre;  
            SparqlPathAnd* andPath;

            if (list_length(orPath->orList) != 1)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("or for property path not support now")));
            andPath = (SparqlPathAnd*)linitial(orPath->orList);
            if (list_length(andPath->andList) == 1) {
                SparqlPathEl* pathEl = (SparqlPathEl*)lfirst(list_head(andPath->andList));                   
                Node* pNode = pathEl->el;

                if (strcmp(getSparqlName(pNode), RdfType) == 0 || strcmp(getSparqlName(pNode), RdfTypeIri) == 0) {                    
                    RangeTblEntry* rte;

                    if (pathEl->range != NULL || list_length(andPath->andList) != 1) {                        
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("rdf:type cannot add in property path now")));
                    }

                    foreach (obj, po->obj) {
                        Node* oNode = (Node*)lfirst(obj);
                        if (pathEl->inv)
                            revSubObj(&sNode, &oNode);

                        if (nodeTag(sNode) == T_SparqlIri && nodeTag(pNode) == T_SparqlIri                            
                            && (nodeTag(oNode) == T_SparqlIri || nodeTag(oNode) == T_SparqlString))                                
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("fact statement not allowed"), parser_errposition(pstate, ((SparqlIri*)sNode)->location)));

                        if (nodeTag(oNode) == T_SparqlIri) {
                            vertexLabelExist(pstate, toDowncase(getSparqlName(oNode)), ((SparqlIri*)oNode)->location);
                            rte = addJoin(pstate, oNode, sNode, true);
                            *tables = lappend(*tables, rte);
                        }

                        if (nodeTag(oNode) == T_SparqlVar) {
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unbound types are not support yet"), parser_errposition(pstate, ((SparqlVar*)oNode)->location)));
                        }
                        if (pathEl->inv)
                            revSubObj(&sNode, &oNode);
                    }
                }
            }
        }
    }
}

/*
 * deal with property path
 * rdf:type properties are not allowed in the middle of property path
 * NOTE: rdf:type with one length will be ignored here
 * the structure of this function is same as transformTypedTriples()
 */
static void transformTriples(ParseState* pstate, List* triList, List** args,
    List** tables, List** attrList)
{
    ListCell* tric;
    ListCell* poc;
    ListCell* obj;
    Node* arg = NULL;

    foreach (tric, triList) {
        SparqlTriple* tri = (SparqlTriple*)lfirst(tric);
        Node* sNode = tri->sub;

        if (nodeTag(sNode) == T_SparqlString) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subjects can not be string"), parser_errposition(pstate, ((SparqlString*)sNode)->location)));
        }

        foreach (poc, tri->pre_obj) {
            SparqlPreObj* po = (SparqlPreObj*)lfirst(poc);

            if(nodeTag(po->pre) == T_SparqlVar) {       // note: the predicate is a variable
                return;
            }
            
            SparqlPathOr* orPath = (SparqlPathOr*)po->pre;
            SparqlPathAnd* andPath;
            ListCell* andCell;
            List* ueids = NIL;
            List* ueidarrs = NIL;
            RangeTblEntry* prev_edge = NULL;
            SparqlPathEl* prev_pathEl = NULL;

            if (list_length(orPath->orList) != 1)
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("or for property path not support now")));

            andPath = (SparqlPathAnd*)linitial(orPath->orList);

            foreach (andCell, andPath->andList) {
                SparqlPathEl* pathEl = (SparqlPathEl*)lfirst(andCell);
                Node* pNode = pathEl->el;   // /*the IRI of the property*/
                RangeTblEntry* edge;
                RangeTblEntry* vertex;

                if (lnext(andCell) != NULL) {
                    if (strcmp(getSparqlName(pNode), RdfType) == 0) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("rdf:type not allowed in property path now")));
                    }
                    if (!edgeLabelExist(pstate,
                            toDowncase(getSparqlName(pNode)), default_loc)) {
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("property in path must refer to edge"), parser_errposition(pstate, default_loc)));
                    }

                    if (prev_edge == NULL) {
                        vertex = getEntry(*tables, getSparqlName(sNode));

                        if (vertex == NULL) {
                            vertex = addJoin(pstate, sNode, sNode, false);
                            *tables = lappend(*tables, vertex);
                        }

                        setInitialVidForVLE(pstate, pathEl, (Node*)vertex, NULL, NULL);
                        edge = transformPropertyPath(pstate, pathEl);
                        arg = addVertexPath(pstate, vertex, pathEl, edge, false);
                        if (arg != NULL)
                            *args = lappend(*args, arg);
                    } else {
                        vertex = NULL;

                        arg = addVertexPath(pstate, vertex, prev_pathEl,
                            prev_edge, true);
                        setInitialVidForVLE(pstate, pathEl, (Node*)vertex,
                            prev_pathEl, prev_edge);
                        edge = transformPropertyPath(pstate, pathEl);
                        arg = addEdgePath(pstate, prev_pathEl, prev_edge,
                            pathEl, edge);

                        if (arg != NULL)
                            *args = lappend(*args, arg);
                    }
                    if (pathEl->range == NULL) {
                        Node* eid;

                        eid = getColumnVar(pstate, edge, GS_ELEM_LOCAL_ID);
                        ueids = list_append_unique(ueids, eid);
                    } else {
                        Node* eidarr;

                        eidarr = getColumnVar(pstate, edge, VLE_COLNAME_IDS);
                        ueidarrs = list_append_unique(ueidarrs, eidarr);
                    }
                    prev_edge = edge;
                    prev_pathEl = pathEl;
                } else { /*the end of property path*/
                    RangeTblEntry* rte;
                    char* l;
                    SparqlIri* pre = (SparqlIri*)pNode;

                    if (list_length(andPath->andList) != 1
                        && strcmp(getSparqlName(pNode), RdfType) == 0) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("rdf:type not allowed in property path now")));
                    }

                    /*ignore rdf:type properties*/
                    if (strcmp(getSparqlName(pNode), RdfType) == 0)
                        continue;

                    if (nodeTag(po->pre) == T_SparqlVar) {
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unbound properties are not support yet"), parser_errposition(pstate, ((SparqlVar*)po->pre)->location)));
                    }
                    if (nodeTag(po->pre) == T_SparqlString) {
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("properties can not be string"), parser_errposition(pstate, ((SparqlString*)po->pre)->location)));
                    }

                    l = toDowncase(getSparqlName((Node*)pre));

                    foreach (obj, po->obj) {
                        RangeTblEntry* target;
                        Node* oNode = (Node*)lfirst(obj);

                        if (nodeTag(sNode) == T_SparqlIri && nodeTag(pNode) == T_SparqlIri                            
                            && (nodeTag(oNode) == T_SparqlIri || nodeTag(oNode) == T_SparqlString))                                
                            ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("fact statement not allowed"), parser_errposition(pstate, ((SparqlIri*)sNode)->location)));

                        if (nodeTag(oNode) == T_SparqlString && nodeTag(sNode) != T_SparqlIri) {
                            if (list_length(andPath->andList) == 1) {
                                target = getEntry(*tables, getSparqlName(sNode));                                    

                                if (target == NULL) {
                                    target = addJoin(pstate, sNode, sNode, false);                                        
                                    *tables = lappend(*tables, vertex);
                                }

                                if (pathEl->range == NULL) {
                                    arg = attrEq(pstate, target, pNode, oNode);
                                    *args = lappend(*args, arg);
                                } else {
                                    ereport(ERROR,
                                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("attribute not allowed with variable length"), parser_errposition(pstate, ((SparqlIri*)sNode)->location)));
                                }
                            } else {
                                target = addJoin(pstate, NULL, NULL, false);
                                arg = addVertexPath(pstate, target, prev_pathEl, prev_edge, true);                                    
                                *args = lappend(*args, arg);

                                if (pathEl->range == NULL) {
                                    arg = attrEq(pstate, target, pNode, oNode);
                                    *args = lappend(*args, arg);
                                } else {
                                    ereport(ERROR,
                                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("attribute not allowed with variable length"), parser_errposition(pstate, ((SparqlIri*)sNode)->location)));
                                }
                            }
                        } else if ((nodeTag(sNode) == T_SparqlVar) && (nodeTag(oNode) == T_SparqlVar)) {
                            if (edgeLabelExist(pstate, l, pre->location)) {
                                if (list_length(andPath->andList) == 1) {
                                    vertex = getEntry(*tables, getSparqlName(sNode));
                                        
                                    if (vertex == NULL) {
                                        vertex = addJoin(pstate, sNode, sNode,
                                            false);
                                        *tables = lappend(*tables, vertex);
                                    }

                                    setInitialVidForVLE(pstate, pathEl, (Node*)vertex, prev_pathEl, prev_edge);                                        
                                        
                                    edge = transformPropertyPath(pstate, pathEl);
                                        
                                    arg = addVertexPath(pstate, vertex, pathEl, edge, false);
                                        
                                    if (arg != NULL)
                                        *args = lappend(*args, arg);

                                    vertex = getEntry(*tables, getSparqlName(oNode));
                                        
                                    if (vertex == NULL) {
                                        vertex = addJoin(pstate, oNode, oNode, false);
                                            
                                        *tables = lappend(*tables, vertex);
                                    }
 
                                    arg = addVertexPath(pstate, vertex, pathEl, edge, true);
                                        
                                    if (arg != NULL)
                                        *args = lappend(*args, arg);

                                    if (pathEl->range == NULL) {
                                        Node* eid;

                                        eid = getColumnVar(pstate, edge, GS_ELEM_LOCAL_ID);                                           
                                        ueids = list_append_unique(ueids, eid);
                                    } else {
                                        Node* eidarr;

                                        eidarr = getColumnVar(pstate, edge, VLE_COLNAME_IDS);
                                            
                                        ueidarrs = list_append_unique(ueidarrs,  eidarr);                                           
                                    }
                                } else {
                                    vertex = NULL;
                                    arg = addVertexPath(pstate, vertex, prev_pathEl, prev_edge, true);                                       

                                    if (arg != NULL)
                                        *args = lappend(*args, arg);

                                    setInitialVidForVLE(pstate, pathEl, (Node*)vertex, prev_pathEl, prev_edge);                                       
                                    edge = transformPropertyPath(pstate, pathEl);   
                                    arg = addEdgePath(pstate, prev_pathEl, prev_edge, pathEl, edge);
    
                                    *args = lappend(*args, arg);

                                    if (pathEl->range == NULL) {
                                        Node* eid;

                                        eid = getColumnVar(pstate, edge, GS_ELEM_LOCAL_ID);                                           
                                        ueids = list_append_unique(ueids, eid);
                                    } else {
                                        Node* eidarr;

                                        eidarr = getColumnVar(pstate, edge, VLE_COLNAME_IDS);                                            
                                        ueidarrs = list_append_unique(ueidarrs, eidarr);
                                           
                                    }
                                    vertex = getEntry(*tables,
                                        getSparqlName(oNode));
                                    if (vertex == NULL) {
                                        vertex = addJoin(pstate, oNode, oNode,
                                            false);
                                        *tables = lappend(*tables, vertex);
                                    }
                                    arg = addVertexPath(pstate, vertex, pathEl,
                                        edge, true);
                                    *args = lappend(*args, arg);
                                }
                            } else {
                                Triple* t = makeNode(Triple);
                                if (list_length(andPath->andList) == 1) {
                                    if (pathEl->inv) {
                                        swapNode(&sNode, &oNode);
                                    }
                                    rte = getEntry(*tables,
                                        getSparqlName(sNode));
                                    if (rte == NULL) {
                                        rte = addJoin(pstate, sNode, sNode,
                                            false);
                                        *tables = lappend(*tables, rte);
                                    }
                                    t->sub = getSparqlName(sNode);
                                } else {
                                    if (pathEl->inv) {
                                        ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("attribute can not apply inverse")));
                                    }
                                    rte = addJoin(pstate, oNode, oNode, false);
                                    *tables = lappend(*tables, rte);
                                    arg = addVertexPath(pstate, rte,
                                        prev_pathEl, prev_edge, true);
                                    *args = lappend(*args, arg);
                                    t->sub = getSparqlName(oNode);
                                }

                                t->pre = getSparqlName(pNode);
                                t->obj = getSparqlName(oNode);
                                if (list_member(*attrList, t))
                                    ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("target duplicate")));
                                *attrList = lappend(*attrList, t);

                                if (list_length(andPath->andList) == 1
                                    && pathEl->inv) {
                                    swapNode(&sNode, &oNode);
                                }
                            }
                        }else if ((nodeTag(sNode) == T_SparqlVar)
                            && (nodeTag(oNode) == T_SparqlIri)) {
                            if (edgeLabelExist(pstate, l, pre->location)) { 
                                if (list_length(andPath->andList) == 1) {
                                    vertex = getEntry(*tables,
                                        getSparqlName(sNode));
                                    if (vertex == NULL) {
                                        vertex = addJoin(pstate, sNode, sNode,
                                            false);
                                        *tables = lappend(*tables, vertex);
                                    }
                                    setInitialVidForVLE(pstate, pathEl,
                                        (Node*)vertex, prev_pathEl,
                                        prev_edge);
                                    edge = transformPropertyPath(pstate,
                                        pathEl);
                                    arg = addVertexPath(pstate, vertex, pathEl,
                                        edge, false);
                                    if (arg != NULL)
                                        *args = lappend(*args, arg);

                                    vertex = getEntry(*tables,
                                        getSparqlName(oNode));
                                    if (vertex == NULL) {
                                        vertex = addJoin(pstate, oNode, oNode,
                                            false);
                                        *tables = lappend(*tables, vertex);
                                    }
                                    arg = addVertexPath(pstate, vertex, pathEl,
                                        edge, true);
                                    if (arg != NULL)
                                        *args = lappend(*args, arg);

                                    arg = transColumn(pstate, vertex, NULL);
                                    arg = transColumnEqualUri(pstate, arg, getSparqlName(oNode));
                                    if (arg != NULL)
                                        *args = lappend(*args, arg);

                                    if (pathEl->range == NULL) {
                                        Node* eid;

                                        eid = getColumnVar(pstate, edge,
                                            GS_ELEM_LOCAL_ID);//猜测是获取表
                                        ueids = list_append_unique(ueids, eid);
                                    } else {
                                        Node* eidarr;

                                        eidarr = getColumnVar(pstate, edge,
                                            VLE_COLNAME_IDS);
                                        ueidarrs = list_append_unique(ueidarrs,
                                            eidarr);
                                    }
                                } else {
                                    vertex = NULL;
                                    arg = addVertexPath(pstate, vertex,
                                        prev_pathEl, prev_edge, true);
                                    if (arg != NULL)
                                        *args = lappend(*args, arg);
                                    setInitialVidForVLE(pstate, pathEl,
                                        (Node*)vertex, prev_pathEl,
                                        prev_edge);
                                    edge = transformPropertyPath(pstate,
                                        pathEl);
                                    arg = addEdgePath(pstate, prev_pathEl,
                                        prev_edge, pathEl, edge);
                                    *args = lappend(*args, arg);
                                    if (pathEl->range == NULL) {
                                        Node* eid;

                                        eid = getColumnVar(pstate, edge,
                                            GS_ELEM_LOCAL_ID);
                                        ueids = list_append_unique(ueids, eid);
                                    } else {
                                        Node* eidarr;

                                        eidarr = getColumnVar(pstate, edge,
                                            VLE_COLNAME_IDS);
                                        ueidarrs = list_append_unique(ueidarrs,
                                            eidarr);
                                    }
                                    vertex = getEntry(*tables,
                                        getSparqlName(oNode));
                                    if (vertex == NULL) {
                                        vertex = addJoin(pstate, oNode, oNode,
                                            false);
                                        *tables = lappend(*tables, vertex);
                                    }
                                    arg = addVertexPath(pstate, vertex, pathEl,
                                        edge, true);
                                    *args = lappend(*args, arg);
                                }
                            } else {
                                Triple* t = makeNode(Triple);
                                if (list_length(andPath->andList) == 1) {
                                    if (pathEl->inv) {
                                        swapNode(&sNode, &oNode);
                                    }
                                    rte = getEntry(*tables,
                                        getSparqlName(sNode));
                                    if (rte == NULL) {
                                        rte = addJoin(pstate, sNode, sNode,
                                            false);
                                        *tables = lappend(*tables, rte);
                                    }
                                    t->sub = getSparqlName(sNode);
                                } else {
                                    if (pathEl->inv) {
                                        ereport(ERROR,
                                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("attribute can not apply inverse")));
                                    }
                                    rte = addJoin(pstate, oNode, oNode, false);
                                    *tables = lappend(*tables, rte);
                                    arg = addVertexPath(pstate, rte,
                                        prev_pathEl, prev_edge, true);
                                    *args = lappend(*args, arg);
                                    t->sub = getSparqlName(oNode);
                                }
                                t->pre = getSparqlName(pNode);
                                t->obj = getSparqlName(oNode);
                                if (list_member(*attrList, t))
                                    ereport(ERROR,
                                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("target duplicate")));
                                *attrList = lappend(*attrList, t);
                                if (list_length(andPath->andList) == 1
                                    && pathEl->inv) {
                                    swapNode(&sNode, &oNode);
                                }
                            }
                        } else {
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("query is meaningless or not support yet")));
                        }
                    }
                }
            }
            addQualUniqueEdges_legcy(pstate, args, ueids, ueidarrs);
        }
    }
}

/* 
 * deal with when predicate is a variable
 */
static void transformVariableEdge(ParseState* pstate, List* triList, List** args,
    List** tables, List** attrList)
{
    ListCell* tric;
    ListCell* poc;
    ListCell* obj;
    Node* arg = NULL;

    foreach (tric, triList) {
        SparqlTriple* tri = (SparqlTriple*)lfirst(tric);
        Node* sNode = tri->sub;
        RangeTblEntry* edge;
        RangeTblEntry* vertex;
        List* ueids = NIL;
        List* ueidarrs = NIL;

        if (nodeTag(sNode) == T_SparqlString) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("subjects can not be string"), parser_errposition(pstate, ((SparqlString*)sNode)->location)));
        }

        foreach (poc, tri->pre_obj) {
            SparqlPreObj* po = (SparqlPreObj*)lfirst(poc);

            if (nodeTag(po->pre) == T_SparqlPathOr) {
                return;
            }

            if(nodeTag(po->pre) != T_SparqlVar) {       
                ereport(ERROR,
                    (ERRCODE_FEATURE_NOT_SUPPORTED, errmsg("the predicate should be a variable")));
            }

            foreach(obj, po->obj) {
                RangeTblEntry* target;
                Node* oNode = (Node*)lfirst(obj);

                if ((nodeTag(sNode) == T_SparqlVar) && (nodeTag(oNode) == T_SparqlVar)) {
                    edge = transformVariablePath(pstate, (Node*)po->pre);
                    *tables = lappend(*tables, edge);     

                    /* Process the table and join expressions corresponding to the subject */
                    vertex = getEntry(*tables, getSparqlName(sNode));
                    if (vertex == NULL) {
                        vertex = addJoin(pstate, sNode, sNode, false);
                        *tables = lappend(*tables, vertex);
                    }
                    
                    arg = addVertexPath(pstate, vertex, NULL, edge, false);
                    if (arg != NULL) 
                        *args = lappend(*args, arg);

                    /* Process the table and join expressions corresponding to the object */
                    vertex = getEntry(*tables, getSparqlName(oNode));
                    if (vertex == NULL) {
                        vertex = addJoin(pstate, oNode, oNode, false);
                        *tables = lappend(*tables, vertex);
                    }
                    
                    arg = addVertexPath(pstate, vertex, NULL, edge, true);
                    if (arg != NULL) 
                        *args = lappend(*args, arg);
                }
            }
        addQualUniqueEdges_legcy(pstate, args, ueids, ueidarrs);
        }
    }
}



static void addQualUniqueEdges_legcy(ParseState* pstate, List** args, List* ueids,
    List* ueidarrs)
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

            if (ne != NULL)
                *args = lappend(*args, ne);
        }

        foreach (lea1, ueidarrs) {
            Node* eidarr = (Node*)lfirst(lea1);
            Node* arg;
            NullTest* dupcond;

            arg = ParseFuncOrColumn(pstate, arrpos->funcname, list_make2(eidarr, eid1), NULL, -1);

            dupcond = makeNode(NullTest);
            dupcond->arg = (Expr*)arg;
            dupcond->nulltesttype = IS_NULL;
            dupcond->argisrow = false;
            // dupcond->location = -1;
            if (dupcond != NULL)
                *args = lappend(*args, dupcond);
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
                list_make2(eidarr1, eidarr2), arroverlap, -1);

            dupcond = (Node*)makeBoolExpr(NOT_EXPR, list_make1(funcexpr), -1);
            if (dupcond != NULL)
                *args = lappend(*args, dupcond);
        }
    }
}

/*
 * transform filter functions
 * 		(1) ISURI ISLITERAL ISIRI REGEX etc.
 * 		(2) math expressions
 * */
static void transformFilters(ParseState* pstate, List* filList, List** args,
    List* tables, List* attrList)
{
    ListCell* filCell;
    Node* arg;
    foreach (filCell, filList) {
        Node* filNode = (Node*)lfirst(filCell);
        FuncCall* fnCall = (FuncCall*)filNode;
        switch (nodeTag(filNode)) {
        /*math expressions, cast the type first, then do the calculation*/
        case T_BoolExpr: {
            arg = transBoolExpr(pstate, filNode, tables, attrList);
            arg = coerce_to_boolean(pstate, arg, "WHERE");
            *args = lappend(*args, arg);
            break;
        }
        case T_A_Expr: {
            arg = transA_Expr(pstate, filNode, tables, attrList);
            arg = coerce_to_boolean(pstate, arg, "WHERE");
            *args = lappend(*args, arg);
            break;
        }

            /*
             * ISURI ISLITERAL ISIRI REGEX etc.
             * cast to string first, then restrict the form of the arguments
             * */
        case T_FuncCall: {
            Value* fName = (Value*)lfirst(list_head(fnCall->funcname));
            /*get the class of function */
            int tag = funcType((fName->val).str);
            Node* fnNode = (Node*)lfirst(list_head(fnCall->args));
            switch (tag) {
            /*ISURI ISIRI*/
            case 1: {
                Oid inputType;
                int32 targetTypmod;
                Oid targetType;
                A_Const* ac;
                TypeCast* tc;
                Triple* attr;
                Node* expr;
                if (nodeTag(fnNode) != T_SparqlVar) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("parameter type should be variable"), parser_errposition(pstate, fnCall->location)));
                }
                attr = getAttrEntry(attrList, getSparqlName(fnNode));
                if (attr != NULL) {
                    ac = makeNode(A_Const);
                    ac->val.type = T_String;
                    ac->val.val.str = "f";
                    ac->location = default_loc;
                } else if (getEntry(tables, getSparqlName(fnNode)) != NULL) {
                    ac = makeNode(A_Const);
                    ac->val.type = T_String;
                    ac->val.val.str = "t";
                    ac->location = default_loc;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("parameter not exist in where clause"), parser_errposition(pstate, fnCall->location)));
                }
                tc = makeNode(TypeCast);
                tc->arg = (Node*)ac;
                tc->typname = SystemTypeName("bool");
                tc->location = default_loc;
                typenameTypeIdAndMod(pstate, tc->typname, &targetType,
                    &targetTypmod);
                expr = (Node*)make_const(pstate, &((A_Const*)tc->arg)->val,
                    -1);
                inputType = exprType(expr);
                arg = coerce_to_target_type(pstate, expr, inputType, targetType,
                    targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
                    default_loc);
                arg = coerce_to_boolean(pstate, arg, "WHERE");
                *args = lappend(*args, arg);
                break;
            }
                /*ISLITERAL*/
            case 2: {
                Oid inputType;
                int32 targetTypmod;
                Oid targetType;
                A_Const* ac;
                TypeCast* tc;
                Triple* attr;
                Node* expr;
                if (nodeTag(fnNode) != T_SparqlVar) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("parameter type should be variable"), parser_errposition(pstate, fnCall->location)));
                }
                attr = getAttrEntry(attrList, getSparqlName(fnNode));
                if (attr != NULL) {
                    arg = transReExpr(pstate, tables, attrList, fnNode,
                        (char*)NUMERIC_TAG, "!~");
                    arg = coerce_to_boolean(pstate, arg, "WHERE");
                    *args = lappend(*args, arg);
                    ac = makeNode(A_Const);
                    ac->val.type = T_String;
                    ac->val.val.str = "t";
                    ac->location = default_loc;
                } else if (getEntry(tables, getSparqlName(fnNode)) != NULL) {
                    ac = makeNode(A_Const);
                    ac->val.type = T_String;
                    ac->val.val.str = "f";
                    ac->location = default_loc;
                } else {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("parameter not exist in where clause"), parser_errposition(pstate, fnCall->location)));
                }
                tc = makeNode(TypeCast);
                tc->arg = (Node*)ac;
                tc->typname = SystemTypeName("bool");
                tc->location = default_loc;
                typenameTypeIdAndMod(pstate, tc->typname, &targetType,
                    &targetTypmod);
                expr = (Node*)make_const(pstate, &((A_Const*)tc->arg)->val,
                    -1);
                inputType = exprType(expr);
                arg = coerce_to_target_type(pstate, expr, inputType, targetType,
                    targetTypmod, COERCION_EXPLICIT, COERCE_EXPLICIT_CAST,
                    default_loc);
                arg = coerce_to_boolean(pstate, arg, "WHERE");
                *args = lappend(*args, arg);
                break;
            }
                /*ISNUMERIC*/
            case 3: {
                if (fnCall->args->length > 1) {
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("argument length not allowed"), parser_errposition(pstate, fnCall->location)));
                }
                arg = transReExpr(pstate, tables, attrList, fnNode,
                    (char*)NUMERIC_TAG, "~");
                arg = coerce_to_boolean(pstate, arg, "WHERE");
                *args = lappend(*args, arg);
                break;
            }
                /*REGEX*/
            case 4: {
                char* reExp;
                if (fnCall->args->length < 2 || fnCall->args->length > 3)
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("regular expression length not allowed"), parser_errposition(pstate, fnCall->location)));
                reExp = getSparqlName((Node*)list_nth(fnCall->args, 1));
                if (fnCall->args->length == 3) {
                    char* flag = getSparqlName(
                        (Node*)list_nth(fnCall->args, 2));
                    if (strcmp(flag, "i") != 0)
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR), errmsg("regular expression flag not allowed"), parser_errposition(pstate, fnCall->location)));
                    arg = transReExpr(pstate, tables, attrList, fnNode, reExp,
                        "~*");
                } else {
                    arg = transReExpr(pstate, tables, attrList, fnNode, reExp,
                        "~");
                }
                arg = coerce_to_boolean(pstate, arg, "WHERE");
                *args = lappend(*args, arg);
                break;
            }
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("argument(s) in filter not allowed or not support now"), parser_errposition(pstate, fnCall->location)));

                break;
            }
            break;
        }
        case T_SparqlTextSearch: {
            List *targs, *targs2;
            Value *v, *vl, *vr, *val;
            Node* noderexpr;
            Node* nodelexpr;
            Node* FuncCall1lexpr;
            Node* FuncCall1rexpr;
            Node* FuncCall2lexpr;
            Node* FuncCall2rexpr;
            Node *func1, *func2;
            RangeTblEntry* rte;
            char *cn, *cnp, *json, *lkey, *rkey, *ts;
            Triple* attr;
            // Node* last_srf;
            targs = NIL;
            targs2 = NIL;

            v = makeString(
                getSparqlName((((SparqlTextSearch*)filNode)->vdictName)));
            FuncCall1lexpr = (Node*)make_const(pstate, v, default_loc);
            targs = lappend(targs, FuncCall1lexpr);

            cn = ((SparqlTextSearch*)filNode)->columnName;
            attr = getAttrEntry(attrList, cn);
            rte = getEntry(tables, attr->sub);
            nodelexpr = scanRTEForColumn(pstate, rte, GS_ELEM_PROP_MAP, default_loc, 0);

            cnp = attr->pre;
            val = makeString(cnp);
            noderexpr = (Node*)make_const(pstate, val, default_loc);

            // last_srf = pstate->p_last_srf;
            json = "->>";
            FuncCall1rexpr = (Node*)make_op(pstate, list_make1(makeString(json)), nodelexpr, noderexpr, default_loc);
            targs = lappend(targs, FuncCall1rexpr);

            lkey = "to_tsvector";
            func1 = ParseFuncOrColumn(pstate, list_make1(makeString(lkey)), targs, NULL, default_loc);

            vl = makeString(
                getSparqlName((((SparqlTextSearch*)filNode)->qdictName)));
            FuncCall2lexpr = (Node*)make_const(pstate, vl, default_loc);
            targs2 = lappend(targs2, FuncCall2lexpr);

            vr = makeString(
                getSparqlName((((SparqlTextSearch*)filNode)->tsQuery)));
            FuncCall2rexpr = (Node*)make_const(pstate, vr, default_loc);
            targs2 = lappend(targs2, FuncCall2rexpr);

            rkey = "to_tsquery";
            func2 = ParseFuncOrColumn(pstate, list_make1(makeString(rkey)), targs2, NULL, default_loc);

            ts = "@@";
            arg = (Node*)make_op(pstate, list_make1(makeString(ts)), func1, func2, default_loc);

            arg = coerce_to_boolean(pstate, arg, "WHERE");
            *args = lappend(*args, arg);

            break;
        }
        default: {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("argument(s) in filter not allowed or not support now"), parser_errposition(pstate, fnCall->location)));
            break;
        }
        }
    }
}

Query* transformSparqlSelectStmt(ParseState* pstate, SparqlSelectStmt* stmt)
{
    Query* qry = makeNode(Query);
    Node* qual;
    List* args = NIL;
    List* tables = NIL;
    List* attrList = NIL;

    SparqlWhere* wc = (SparqlWhere*)(stmt->whereClause);

    transformTypedTriples(pstate, wc->triList, &args, &tables, &attrList);
    transformTriples(pstate, wc->triList, &args, &tables, &attrList);
    transformVariableEdge(pstate, wc->triList, &args, &tables, &attrList);
    transformFilters(pstate, wc->filterList, &args, tables, attrList);

    qual = (Node*)makeBoolExpr(AND_EXPR, args, default_loc);
    qry->jointree = makeFromExpr(pstate->p_joinlist, qual);
    qry->targetList = transformTargets(pstate, stmt->targetList, tables, attrList);
    markTargetListOrigins(pstate, qry->targetList);

    qry->rtable = pstate->p_rtable;
    qry->commandType = CMD_SELECT;

    /* transform window clauses after we have seen all window functions */
    qry->windowClause = transformWindowDefinitions(pstate, pstate->p_windowdefs, &qry->targetList);

    /* resolve any still-unresolved output columns as being type text */
    if (pstate->p_resolve_unknowns)
        resolveTargetListUnknowns(pstate, qry->targetList);

    qry->hasSubLinks = pstate->p_hasSubLinks;
    qry->hasWindowFuncs = pstate->p_hasWindowFuncs;
    qry->hasTargetSRFs = pstate->p_hasTargetSRFs;
    qry->hasAggs = pstate->p_hasAggs;
    if (pstate->p_hasAggs || qry->groupClause || qry->groupingSets || qry->havingQual)
        parseCheckAggregates(pstate, qry);

    assign_query_collations(pstate, qry);
    return qry;
}

/*
    SPARQL INSERT 
*/
Query* transformSparqlInsertStmt(ParseState* pstate, SparqlInsertStmt* stmt)
{
    Query* qry = makeNode(Query);
    return qry;
}

Query* transformSparqlDeleteDataStmt(ParseState* pstate, SparqlDeleteDataStmt* stmt)
{
    Query* qry = makeNode(Query);
    return qry;
}

static bool labelExist(ParseState* pstate, char* labname, int labloc,
    char labkind, bool throw_)
{
    Oid graphid;
    HeapTuple tuple;
    char* elemstr;
    Form_gs_label labtup;

    graphid = get_graph_path_oid();

    tuple = SearchSysCache2(LABELNAMEGRAPH, PointerGetDatum(labname),
        ObjectIdGetDatum(graphid));
    if (!HeapTupleIsValid(tuple)) {
        if (throw_) {
            if (labkind == LABEL_KIND_VERTEX)
                elemstr = "vertex";
            else
                elemstr = "edge";

            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("%s label \"%s\" does not exist", elemstr, labname), parser_errposition(pstate, labloc)));
        } else {
            return false;
        }
    }

    labtup = (Form_gs_label)GETSTRUCT(tuple);
    if (labtup->labkind != labkind) {
        if (labtup->labkind == LABEL_KIND_VERTEX)
            elemstr = "vertex";
        else
            elemstr = "edge";

        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("label \"%s\" is %s label", labname, elemstr), parser_errposition(pstate, labloc)));
    }

    ReleaseSysCache(tuple);

    return true;
}
