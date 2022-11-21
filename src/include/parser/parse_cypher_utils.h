/*
 * parse_cypher_expr.h
 *	  handle Cypher expressions in parser
 *
 *
 * IDENTIFICATION
 *	  src/include/parser/parse_cypher_utils.h
 */
#ifndef PARSE_CYPHER_UTILS_H
#define PARSE_CYPHER_UTILS_H

#include "nodes/parsenodes.h"
#include "parser/parse_node.h"

extern void addRTEtoJoinlist(ParseState* pstate, RangeTblEntry* rte, bool visible);

extern void makeExtraFromRTE(ParseState* pstate, RangeTblEntry* rte, RangeTblRef** rtr,
    ParseNamespaceItem** nsitem, bool visible);

extern List* makeTargetListFromRTE(ParseState* pstate, RangeTblEntry* rte);

/* just find RTE of `refname` in the current namespace */
extern RangeTblEntry* findRTEfromNamespace(ParseState* pstate, char* refname);
extern TargetEntry* makeWholeRowTarget(ParseState* pstate, RangeTblEntry* rte);

extern Node* getColumnVar(ParseState* pstate, RangeTblEntry* rte, char* colname);
extern List* genQualifiedName(char* name1, char* name2);
extern ParseNamespaceItem* findNamespaceItemForRTE(ParseState* pstate, RangeTblEntry* rte);
extern Node* addQualUniqueEdges(ParseState* pstate, Node* qual, List* ueids, List* ueidarrs);
extern Node* makeColumnRef(List* fields);
extern ResTarget* makeResTarget(Node* val, char* name);
extern List* makeTargetListFromJoin(ParseState* pstate, RangeTblEntry* rte);

extern Node* qualAndExpr(Node* qual, Node* expr);
extern ResTarget* makeSimpleResTarget(char* field, char* name);
extern Node* getSysColumnVar(ParseState* pstate, RangeTblEntry* rte, int attnum);
extern Node* makeVertexExpr(ParseState* pstate, RangeTblEntry* rte, int location);
extern Node* makeTypedRowExpr(List* args, Oid typoid, int location);

extern Alias* makeAliasNoDup(char* aliasname, List* colnames);
extern Alias* makeAliasOptUnique(char* aliasname);

extern char* genUniqueName(void);

#endif