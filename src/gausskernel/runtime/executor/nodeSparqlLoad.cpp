/*
 * nodeModifyGraph.c
 *	  routines to handle ModifyGraph nodes.
 *
 * Copyright by tjudb group
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSparqlLoad.cpp
 */

#include "postgres.h"

#include "executor/executor.h"
#include "executor/node/nodeSparqlLoad.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "access/heapam.h"
#include "catalog/gs_graph_fn.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_opfamily.h"
#include "commands/graphcmds.h"
#include "commands/sequence.h"
#include "fmgr.h"
#include "nodes/makefuncs.h"
#include "parser/parse_cypher_expr.h"
#include "utils/int16.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"

#include <fstream>

#define MAX_LABEL_NAME_LEN 64
#define UNKOWN_TYPE_VLABEL "unkown"

typedef struct LabelStatus {
    Relation rel;
    uint16 labid;
    LabelKind kind;
    uint64 rowno;
    uint64 startno;
} LabelStatus;

typedef struct TriplesSameSubject {
    char* subject;
    List* types;
    List* props; /* key val key val .... */
    List* edges; /* pre obj pre obj .... */
} TriplesSameSubject;

typedef struct HashEntryNameToStatus {
    char name[MAX_LABEL_NAME_LEN];
    LabelStatus* status;
} HashEntryNameToStatus;

typedef struct HashEntryIdToStatus {
    uint16 labelid;
    LabelStatus* status;
} HashEntryIdToStatus;

static void finalize(SparqlLoadState* state);

static bool isRdfType(const char* pre)
{
    return strcmp("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", pre) == 0;
}

static bool isLiteral(const char* obj)
{
    Assert(obj != NULL);
    /*
     * in dbpedia dataset, the literal may be "<"
     */
    return obj[0] != '<' || obj[1] == '\0';
}

/*
 * remove the prefix of a uri
 * for example:
 * input: <http://xmlns.com/foaf/0.1/name>
 * ouput: type
 */
static char* removePrefix(char* s)
{
    int len = strlen(s);
    char* p = &s[len - 1];
    Assert(*p == '>');
    *p = '\0';
    p--;
    while (p > s && (*p != '#' && *p != '/')) {
        p--;
    }
    return p + 1;
}

static char* removeHeadTail(char* s)
{
    int len = strlen(s);
    Assert(len > 2);
    s[len - 1] = '\0';
    return s + 1;
}

static char* toLower(char* s)
{
    for (char* p = s; *p; p++) {
        *p = tolower(*p);
    }
    return s;
}

static uint16 createLabel(char* name, LabelKind labelKind)
{
    /*
     * sql used only for debug purpose
     */
    char sql[64];
    snprintf(sql, sizeof(sql), "CREATE LABEL %s", name);
    CreateLabelStmt* stmt = makeNode(CreateLabelStmt);
    stmt->labelKind = labelKind;
    stmt->relation = makeRangeVar(NULL, name, -1);
    stmt->if_not_exists = true;
    CreateLabelCommand(stmt, sql, NULL);
    uint16 labid = get_labname_labid(name, get_graph_path_oid());
    return labid;
}

static LabelStatus* getLabelStatusByName(SparqlLoadState* state, char* name, LabelKind labKind)
{
    HTAB* name2label = state->name2label;
    HTAB* id2label = state->id2label;
    bool found;
    HashEntryNameToStatus* entry = (HashEntryNameToStatus*)hash_search(name2label, name, HASH_ENTER, &found);
    /*
     * create label if not exist
     */
    if (!found) {
        MemoryContext old = MemoryContextSwitchTo(state->ps.nodeContext);
        uint16 labid = createLabel(name, labKind);
        Oid relid = get_labid_relid(get_graph_path_oid(), labid);

        /*
         * set label status
         */
        LabelStatus* status = (LabelStatus*)palloc0(sizeof(LabelStatus));
        status->labid = labid;
        status->rel = heap_open(relid, RowExclusiveLock);
        status->kind = labKind;

        char seqname[64];
        snprintf(seqname, sizeof(seqname), "%s.%s_id_seq", get_graph_path(false), name);
        status->startno = DatumGetInt64(DirectFunctionCall1(nextval, CStringGetTextDatum(seqname)));
        status->rowno = status->startno;

        entry->status = status;

        /*
         * we need an id to elabel status lookup table
         * for insertEdge function
         */
        if (labKind == LABEL_EDGE) {
            bool flag = false;
            HashEntryIdToStatus* idEntry = (HashEntryIdToStatus*)hash_search(id2label, &labid, HASH_ENTER, &flag);

            Assert(!flag);
            idEntry->status = entry->status;
        }

        MemoryContextSwitchTo(old);
    }

    {
        int natts = entry->status->rel->rd_att->natts;
        if ((natts == 2 && labKind == LABEL_EDGE) || (natts == 4 && labKind == LABEL_VERTEX)) {
            ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Label \"%s\" exists in both the ELABEL and the VLABEL", name)));
        }
    }
    
    return entry->status;
}

static LabelStatus* getLabelStatusByLabelId(SparqlLoadState* state, uint16 labid)
{
    HTAB* id2label = state->id2label;
    bool found = false;
    HashEntryIdToStatus* entry = (HashEntryIdToStatus*)hash_search(id2label, &labid, HASH_FIND, &found);

    /*
     * hash entries of id2label should be created in function getLabelStatusByName
     */
    Assert(found);
    return entry->status;
}

/*
 * insert vertex, and return its graphid
 */
static Graphid insertVertex(LabelStatus* vstat, List* props)
{
    Graphid id;
    GraphidSet(&id, vstat->labid, ++vstat->rowno);

    /*
     * transform props into jsonb
     */
    JsonbParseState* jbstate = NULL;
    JsonbValue* jb;
    pushJsonbValue(&jbstate, WJB_BEGIN_OBJECT, NULL);
    ListCell* lc = list_head(props);
    while (lc != NULL) {
        JsonbValue vjv, kjv;
        char *key, *val;

        key = (char*)lfirst(lc);
        kjv.type = jbvString;
        kjv.string.val = key;
        kjv.string.len = strlen(key);
        kjv.estSize = sizeof(JEntry) + kjv.string.len;
        pushJsonbValue(&jbstate, WJB_KEY, &kjv);

        lc = lnext(lc);
        Assert(lc != NULL);

        val = (char*)lfirst(lc);
        vjv.type = jbvString;
        vjv.string.val = val;
        vjv.string.len = strlen(val);
        vjv.estSize = sizeof(JEntry) + vjv.string.len;
        pushJsonbValue(&jbstate, WJB_VALUE, &vjv);

        lc = lnext(lc);
    }
    jb = pushJsonbValue(&jbstate, WJB_END_OBJECT, NULL);

    /*
     * construct tuple and insert
     */
    Datum values[2] = { GraphidGetDatum(id), JsonbPGetDatum(JsonbValueToJsonb(jb)) };
    bool nulls[2] = { 0 };
    HeapTuple tuple = heap_form_tuple(RelationGetDescr(vstat->rel), values, nulls);
    simple_heap_insert(vstat->rel, tuple);
    heap_freetuple(tuple);
    return id;
}

static void insertUri2id(SparqlLoadState* state, char* uri, Graphid id)
{
    Tuplesortstate* uri2id = state->uri2id;
    // uri, id
    TupleDesc tupdesc = state->uri2idDesc;

    TupleTableSlot* slot = MakeSingleTupleTableSlot(tupdesc);
    memset(slot->tts_isnull, 0, sizeof(bool) * tupdesc->natts);
    slot->tts_isempty = false;
    slot->tts_values[0] = CStringGetTextDatum(uri);
    slot->tts_values[1] = GraphidGetDatum(id);
    tuplesort_puttupleslot(uri2id, slot);
}

static void insertEdgesTemp(SparqlLoadState* state, Graphid id, List* edges)
{
    Tuplesortstate* edgesTmp = state->edgesTmp;
    TupleDesc tupdesc = state->edgesTmpDesc;
    TupleTableSlot* slot = MakeSingleTupleTableSlot(tupdesc);
    memset(slot->tts_isnull, 0, sizeof(bool) * tupdesc->natts);
    slot->tts_isempty = false;

    // start_id, end_uri, label_id
    slot->tts_values[0] = GraphidGetDatum(id);

    ListCell* lc = list_head(edges);
    while (lc != NULL) {
        char* pre = (char*)lfirst(lc);
        LabelStatus* estat = getLabelStatusByName(state, pre, LABEL_EDGE);
        slot->tts_values[2] = UInt16GetDatum(estat->labid);

        lc = lnext(lc);
        Assert(lc != NULL);
        char* obj = (char*)lfirst(lc);
        slot->tts_values[1] = CStringGetTextDatum(obj);

        tuplesort_puttupleslot(edgesTmp, slot);
        lc = lnext(lc);
    }
}

static void handleVertexData(SparqlLoadState* state, TriplesSameSubject& tss)
{
    if (list_length(tss.types) == 0) {
        tss.types = lappend(tss.types, pstrdup(UNKOWN_TYPE_VLABEL));
    }

    /*
     * add subject uri into props
     */
    tss.props = list_concat(tss.props, list_make2(pstrdup("uri"), tss.subject));

    LabelStatus* vstat = getLabelStatusByName(state, (char*)linitial(tss.types), LABEL_VERTEX);
    Graphid id = insertVertex(vstat, tss.props);
    insertUri2id(state, tss.subject, id);
    insertEdgesTemp(state, id, tss.edges);
}

static void insertEdge(SparqlLoadState* state, Graphid start, Graphid end, uint16 labelid)
{
    Datum values[4];
    bool nulls[4] = {};
    Graphid id;
    JsonbParseState* jbstate = NULL;
    JsonbValue* jb;

    // build an empty jsonb object
    pushJsonbValue(&jbstate, WJB_BEGIN_OBJECT, NULL);
    jb = pushJsonbValue(&jbstate, WJB_END_OBJECT, NULL);

    LabelStatus* estat = getLabelStatusByLabelId(state, labelid);
    Assert(estat->labid == labelid);
    GraphidSet(&id, labelid, ++estat->rowno);
    values[0] = GraphidGetDatum(id);
    values[1] = GraphidGetDatum(start);
    values[2] = GraphidGetDatum(end);
    values[3] = JsonbPGetDatum(JsonbValueToJsonb(jb));
    HeapTuple tuple = heap_form_tuple(RelationGetDescr(estat->rel), values, nulls);
    simple_heap_insert(estat->rel, tuple);
    heap_freetuple(tuple);
}

/*
 * edge:  start_id, end_uri, label_id
 * uri2id:  uri, id
 *
 * texteq is faster than text_cmp since it can return false directly
 * if the length of the strings are unequal
 */
static bool isUriEqual(TupleTableSlot* edge, TupleTableSlot* ui)
{
    bool isNull = false;
    Datum end_uri = slot_getattr(edge, 2, &isNull);
    Datum uri = slot_getattr(ui, 1, &isNull);
    Datum equal = DirectFunctionCall2(texteq, end_uri, uri);
    return DatumGetBool(equal);
}

/*
 * edge:  start_id, end_uri, label_id
 * uri2id:  uri, id
 */
static int uriCmp(TupleTableSlot* edge, TupleTableSlot* ui)
{
    bool isNull = false;
    text* end_uri = DatumGetTextPP(slot_getattr(edge, 2, &isNull));
    text* uri = DatumGetTextPP(slot_getattr(ui, 1, &isNull));
    return text_cmp(end_uri, uri, DEFAULT_COLLATION_OID);
}

static void constructEdge(SparqlLoadState* state, TupleTableSlot* edge, TupleTableSlot* ui)
{
    bool isNull;
    Graphid start = DatumGetGraphid(slot_getattr(edge, 1, &isNull));
    int16 labid = DatumGetUInt16(slot_getattr(edge, 3, &isNull));
    Graphid end;

    if (ui == NULL) {
        char* subject = TextDatumGetCString(slot_getattr(edge, 2, &isNull));
        LabelStatus* unkownLabelStat = getLabelStatusByName(state, pstrdup(UNKOWN_TYPE_VLABEL), LABEL_VERTEX);
        end = insertVertex(unkownLabelStat, list_make2(pstrdup("uri"), subject));
    } else {
        end = DatumGetGraphid(slot_getattr(ui, 2, &isNull));
    }
    insertEdge(state, start, end, labid);
}

static void constructEdges(SparqlLoadState* state)
{
    Tuplesortstate* edgesTmp = state->edgesTmp;
    Tuplesortstate* uri2id = state->uri2id;
    TupleTableSlot* edge = MakeSingleTupleTableSlot(state->edgesTmpDesc);
    TupleTableSlot* ui = MakeSingleTupleTableSlot(state->uri2idDesc);

    tuplesort_performsort(edgesTmp);
    tuplesort_performsort(uri2id);
    /*
     * edgesTemp left merge join uri2id
     * on edgesTemp.end_uri = uri2id.uri
     *
     */
    MemoryContext old = MemoryContextSwitchTo(state->eCtx);
    tuplesort_gettupleslot(uri2id, true, ui, NULL);
    tuplesort_gettupleslot(edgesTmp, true, edge, NULL);
    while (!ui->tts_isempty && !edge->tts_isempty) {
        while (!ui->tts_isempty && !edge->tts_isempty) {
            int cmp = uriCmp(edge, ui);
            if (cmp == 0)
                break;
            else if (cmp < 0) {
                constructEdge(state, edge, ui);
                tuplesort_gettupleslot(edgesTmp, true, edge, NULL);
            } else {
                tuplesort_gettupleslot(uri2id, true, ui, NULL);
            }
        }

        while (!ui->tts_isempty && !edge->tts_isempty && isUriEqual(edge, ui)) {
            constructEdge(state, edge, ui);
            tuplesort_gettupleslot(edgesTmp, true, edge, NULL);
        }
        MemoryContextReset(CurrentMemoryContext);
    }

    while (!edge->tts_isempty) {
        constructEdge(state, edge, NULL);
        tuplesort_gettupleslot(edgesTmp, true, edge, NULL);
        MemoryContextReset(CurrentMemoryContext);
    }
    MemoryContextSwitchTo(old);
}

TupleTableSlot* ExecSparqlLoad(SparqlLoadState* state)
{
    TriplesSameSubject tss {};
    tss.subject = "";

    /*
     * we employ two vertexContexs to avoid redundant copy
     */
    MemoryContext old = MemoryContextSwitchTo(state->vCtx1);
    MemoryContext vertexContext = state->vCtx2;

    for (;;) {
        TupleTableSlot* slot;
        slot = ExecProcNode(state->src_plan);

        if (TupIsNull(slot))
            break;

        bool isNull;
        char* subject = TextDatumGetCString(slot_getattr(slot, 1, &isNull));
        char* predicate = TextDatumGetCString(slot_getattr(slot, 2, &isNull));
        Datum od = slot_getattr(slot, 3, &isNull);
        char* object = "";
        if (!isNull) {
            object = TextDatumGetCString(od);
        }

        subject = removeHeadTail(subject);

        if (strcmp(subject, tss.subject) != 0) {
            if (tss.subject[0] != '\0') {
                handleVertexData(state, tss);
            }
            tss.subject = subject;
            tss.edges = tss.props = tss.types = NIL;

            /*
             * we only reset the previous vertex context, so memroy in current MemoryContext is still available
             */
            MemoryContextReset(vertexContext);
            vertexContext = MemoryContextSwitchTo(vertexContext);
        }

        if (isRdfType(predicate)) {
            tss.types = lappend(tss.types, toLower(removePrefix(object)));
        } else if (isLiteral(object)) {
            tss.props = lappend(tss.props, removePrefix(predicate));
            tss.props = lappend(tss.props, object);
        } else {
            tss.edges = lappend(tss.edges, toLower(removePrefix(predicate)));
            tss.edges = lappend(tss.edges, removeHeadTail(object));
        }
    }
    handleVertexData(state, tss);
    MemoryContextSwitchTo(old);

    constructEdges(state);

    finalize(state);
    return NULL;
}

SparqlLoadState* ExecInitSparqlLoad(SparqlLoadPlan* node, EState* estate, int eflags)
{
    SparqlLoadState* state = makeNode(SparqlLoadState);
    state->ps.plan = (Plan*)node;
    state->ps.state = estate;
    estate->es_sparqlloadstats = (SparqlLoadStats*)palloc0(sizeof(SparqlLoadStats));
    memset(estate->es_sparqlloadstats, 0, sizeof(SparqlLoadStats));

    ExecInitResultTupleSlot(estate, &state->ps);
    state->src_plan = ExecInitNode(node->src_plan, estate, eflags);

    /*
     * allocate memory context
     */
    state->vCtx1 = AllocSetContextCreate(state->ps.nodeContext, "Vertex1", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    state->vCtx2 = AllocSetContextCreate(state->ps.nodeContext, "Vertex2", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);
    state->eCtx = AllocSetContextCreate(state->ps.nodeContext, "Edge", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * init hash tabel for name to LabelStatus
     */
    HASHCTL info = { 0 };
    info.keysize = MAX_LABEL_NAME_LEN;
    info.entrysize = sizeof(HashEntryNameToStatus);
    info.hash = string_hash;
    info.hcxt = CurrentMemoryContext;
    state->name2label = hash_create("name to label status lookup table", 8192, &info, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /*
     * init hash tabel for labid to LabelStatus
     */
    info.keysize = sizeof(uint16);
    info.entrysize = sizeof(HashEntryIdToStatus);
    info.hash = uint16_hash;
    info.hcxt = CurrentMemoryContext;
    state->id2label = hash_create("int to label status lookup table", 8192, &info, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

    /*
     * init uri2id tuple sort state
     */
    TupleDesc uri2idDesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(uri2idDesc, (AttrNumber)1, "uri", TEXTOID, -1, 0);
    TupleDescInitEntry(uri2idDesc, (AttrNumber)2, "id", GRAPHIDOID, -1, 0);
    AttrNumber attrNums[] = { 1 };
    Oid sortOps[] = { get_opfamily_member(TEXT_BTREE_FAM_OID, TEXTOID, TEXTOID, BTLessStrategyNumber) };
    Oid sortCollations[] = { DEFAULT_COLLATION_OID };
    const bool nullsFirst[] = { false };
    state->uri2id = tuplesort_begin_heap(uri2idDesc,
        1,
        attrNums,
        sortOps,
        sortCollations,
        nullsFirst,
        u_sess->attr.attr_memory.work_mem / 2,
        false);
    state->uri2idDesc = uri2idDesc;

    /*
     * init edgesTmp tuple sort state
     */
    TupleDesc edgesTmpDesc = CreateTemplateTupleDesc(3, false);
    TupleDescInitEntry(edgesTmpDesc, (AttrNumber)1, "start_id", GRAPHIDOID, -1, 0);
    TupleDescInitEntry(edgesTmpDesc, (AttrNumber)2, "end_uri", TEXTOID, -1, 0);
    TupleDescInitEntry(edgesTmpDesc, (AttrNumber)3, "label_id", INT2OID, -1, 0);

    attrNums[0] = 2;
    state->edgesTmp = tuplesort_begin_heap(edgesTmpDesc,
        1,
        attrNums,
        sortOps,
        sortCollations,
        nullsFirst,
        u_sess->attr.attr_memory.work_mem / 2,
        false);
    state->edgesTmpDesc = edgesTmpDesc;

    return state;
}

static void finalize(SparqlLoadState* state)
{
    EState* estate = state->ps.state;
    SparqlLoadStats* stats = estate->es_sparqlloadstats;

    /*
     * sequence scan all labels
     */
    HASH_SEQ_STATUS seqStatus;
    hash_seq_init(&seqStatus, state->name2label);
    HashEntryNameToStatus* entry;
    char seqname[128];
    while ((entry = (HashEntryNameToStatus*)hash_seq_search(&seqStatus)) != NULL) {
        LabelStatus* lstat = entry->status;

        /*
         * get statistics information
         */
        if (lstat->kind == LABEL_VERTEX) {
            stats->newVLabel++;
            stats->insertVertex += (lstat->rowno - lstat->startno);
        } else if (lstat->kind == LABEL_EDGE) {
            stats->newELabel++;
            stats->insertEdge += (lstat->rowno - lstat->startno);
        } else {
            // unkown label type
            Assert(false);
        }

        /*
         * close label and reindex label
         */
        Oid relid = lstat->rel->rd_id;
        heap_close(lstat->rel, RowExclusiveLock);
        ReindexRelation(relid, (REINDEX_REL_PROCESS_TOAST | REINDEX_REL_CHECK_CONSTRAINTS), REINDEX_ALL_INDEX);

        /*
         * set sequence number
         */
        snprintf(seqname, sizeof(seqname), "%s_id_seq", entry->name);
        Oid seqOid = RangeVarGetRelid(makeRangeVar(get_graph_path(false), seqname, -1), NoLock, false);
        DirectFunctionCall2(setval_oid, seqOid, DirectFunctionCall1(int16_numeric, Int128GetDatum(lstat->rowno + 1)));
    }
}

void ExecEndSparqlLoad(SparqlLoadState* state)
{
    ExecClearTuple(state->ps.ps_ResultTupleSlot);
    ExecEndNode(state->src_plan);

    MemoryContextDelete(state->vCtx1);
    MemoryContextDelete(state->vCtx2);
    MemoryContextDelete(state->eCtx);

    hash_destroy(state->name2label);
    hash_destroy(state->id2label);

    tuplesort_end(state->uri2id);
    tuplesort_end(state->edgesTmp);
}