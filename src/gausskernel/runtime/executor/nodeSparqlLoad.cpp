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

#include "executor/spi.h"
#include "executor/executor.h"
#include "executor/node/nodeSparqlLoad.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"

#include "fmgr.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "nodes/makefuncs.h"
#include "commands/sequence.h"
#include "parser/parse_cypher_expr.h"
#include "utils/int16.h"
#include "catalog/indexing.h"
#include "access/heapam.h"
#include "catalog/gs_graph_fn.h"

void CreateVertexTest(){
	// get the relation
	Oid graphoid = get_graphname_oid("lubm");
	uint16 labid = get_labname_labid("student", graphoid);
    Oid reloid = get_labid_relid(graphoid,labid);
    Relation rel = heap_open(reloid, RowExclusiveLock);

	std::unordered_map<std::string, std::string> property;
	property.emplace("name","111");
	property.emplace("gender","man");
	JsonbParseState *jpstate = NULL;
	JsonbValue *jb;
    pushJsonbValue(&jpstate, WJB_BEGIN_OBJECT, NULL);
	for(auto& k2v : property)
	{
		JsonbIterator *vji;
		JsonbValue	vjv;
		JsonbValue	kjv;
    	Jsonb	   *vjb = DatumGetJsonbP(stringToJsonbSimple(const_cast<char *>(k2v.second.c_str())));
    	vji = JsonbIteratorInit(VARDATA(vjb));
		if (JB_ROOT_IS_SCALAR(vjb))
		{
			JsonbIteratorNext(&vji, &vjv, true);
			Assert(vjv.type == jbvArray);
			JsonbIteratorNext(&vji, &vjv, true);
			vji = NULL;
		}
    	kjv.type = jbvString;
		kjv.string.val = const_cast<char *>(k2v.first.c_str());
		kjv.string.len = strlen(kjv.string.val);
		kjv.estSize = sizeof(JEntry) + kjv.string.len;
    	pushJsonbValue(&jpstate, WJB_KEY, &kjv);
    	if (vji == NULL)
		{
			Assert(jpstate->contVal.type == jbvObject);
			vjv.estSize = sizeof(JEntry) + vjv.string.len;
			pushJsonbValue(&jpstate, WJB_VALUE, &vjv);
		}
	}
	jb = pushJsonbValue(&jpstate, WJB_END_OBJECT, NULL);
    
    Datum vertexProp = JsonbPGetDatum(JsonbValueToJsonb(jb));

	// construct the tuple
	HeapTuple   tuple;
    Datum values[2];
    bool nulls[2];
    Graphid id;
    GraphidSet(&id, (uint16) 4, (uint64)0);
    values[0] = UInt64GetDatum(id);
    values[1] = vertexProp;
    nulls [0] = false;
    nulls [1] = false;
    tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	// insert the tuple
	simple_heap_insert(rel, tuple);
    heap_close(rel, RowExclusiveLock);
    heap_freetuple(tuple);

	// update the sequence
	int128 num = 30; // total number of a relation
	Oid seqReloid = RangeVarGetRelid(makeRangeVar("network", "student_id_seq", -1), NoLock, false);
	DirectFunctionCall3(setval3_oid,seqReloid,DirectFunctionCall1(int16_numeric,Int128GetDatum(num)),true);
}

void RunQuery(int &&flag, const char *keyword, const char *name)
{
	char		sqlcmd[128];
	switch (flag)
	{
	case 1:
		// create vlabel or elabel
		if(keyword == "VLABEL"){
			snprintf(sqlcmd, sizeof(sqlcmd), "CREATE VLABEL \"%s\"", name);
			SPI_exec(sqlcmd, 0);
		}else if(keyword == "ELABEL"){
			snprintf(sqlcmd, sizeof(sqlcmd), "CREATE ELABEL \"%s\"", name);
			SPI_exec(sqlcmd, 0);
		}else{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("the keyword %s is not \"VLABEL\" or \"ELABEL\"", keyword)));
		}
		break;
	case 2:
		// rebuild the index
		snprintf(sqlcmd, sizeof(sqlcmd), "REINDEX TABLE %s", name);
		SPI_exec(sqlcmd, 0);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("The flag %d is invalid", flag)));
		break;
	}
}

void ConstuctTupleAndInsert(std::vector<Datum> &values, Relation &rel)
{
	HeapTuple   tuple;
	bool 	nulls[values.size()] = {0};
    tuple = heap_form_tuple(RelationGetDescr(rel), values.data(), nulls);

	// insert the tuple
	simple_heap_insert(rel, tuple);
	heap_freetuple(tuple);
}

void InsertVertex(const Vertex &v, std::pair<Relation,int> &p)
{
	std::vector<Datum> values;
	values.emplace_back(UInt64GetDatum(v.id));	
	// construct the property of the vertex to insert
    JsonbParseState *jpstate = NULL;
	JsonbValue *jb;
    pushJsonbValue(&jpstate, WJB_BEGIN_OBJECT, NULL);
	for(auto& k2v : v.property)
	{
		JsonbIterator *vji;
		JsonbValue	vjv;
		JsonbValue	kjv;
    	Jsonb	   *vjb = DatumGetJsonbP(stringToJsonbSimple(const_cast<char *>(k2v.second.c_str())));
    	vji = JsonbIteratorInit(VARDATA(vjb));
		if (JB_ROOT_IS_SCALAR(vjb))
		{
			JsonbIteratorNext(&vji, &vjv, true);
			Assert(vjv.type == jbvArray);
			JsonbIteratorNext(&vji, &vjv, true);
			vji = NULL;
		}
    	kjv.type = jbvString;
		kjv.string.val = const_cast<char *>(k2v.first.c_str());
		kjv.string.len = strlen(kjv.string.val);
		kjv.estSize = sizeof(JEntry) + kjv.string.len;
    	pushJsonbValue(&jpstate, WJB_KEY, &kjv);
    	if (vji == NULL)
		{
			Assert(jpstate->contVal.type == jbvObject);
			vjv.estSize = sizeof(JEntry) + vjv.string.len;
			pushJsonbValue(&jpstate, WJB_VALUE, &vjv);
		}
	}
	jb = pushJsonbValue(&jpstate, WJB_END_OBJECT, NULL);
    
    Datum vertexProp = JsonbPGetDatum(JsonbValueToJsonb(jb));
	values.emplace_back(vertexProp);
	
	// construct the tuple and insert
	ConstuctTupleAndInsert(values, p.first);
	p.second++;
}

void InsertEdges(std::vector<Edge> &edges, std::unordered_map<std::string, uint64> &uri2id, std::unordered_map<std::string, std::pair<Relation, int>> &edgeType2relation)
{
	for(auto& e:edges)
	{
		uint64 startId = uri2id[e.start];
		uint64 endId = uri2id[e.end];
		std::vector<Datum> values;
		values.emplace_back(UInt64GetDatum(e.id));
		values.emplace_back(UInt64GetDatum(startId));
		values.emplace_back(UInt64GetDatum(endId));
		ConstuctTupleAndInsert(values,edgeType2relation[e.type].first);
		edgeType2relation[e.type].second++; 
	}
}

bool IsType(const std::string &pre)
{
	if(pre.size() == 0){
		return false;
	}
	if(pre.find("#type")!=std::string::npos){
		return true;
	}
	return false;
}

bool IsLiteral(const std::string &obj){
	// TextDatumGetCString will not keep the symbol \"
	if(obj[0]!='<' && obj[obj.size()-1]!='>'){
		return true;
	}else{
		return false;
	}
}

void UpdateAndCloseRelation(std::unordered_map<std::string, std::pair<Relation,int>> &vertexType2relation, std::unordered_map<std::string, std::pair<Relation,int>> &edgeType2relation, SparqlLoadStats* stats)
{
	// vertex relation
	for(auto& v2r:vertexType2relation){
		// close the relation
		heap_close(v2r.second.first, RowExclusiveLock);
		// update the sequence
		std::string seqname = v2r.first+"_id_seq";
		Oid seqReloid = RangeVarGetRelid(makeRangeVar(get_graph_path(true), const_cast<char*>(seqname.c_str()), -1), NoLock, false);
		DirectFunctionCall3(setval3_oid,seqReloid,DirectFunctionCall1(int16_numeric,Int128GetDatum(v2r.second.second)),true);
		// update the stats
		stats->insertVertex += v2r.second.second;
		// rebuild index
		std::string name = (std::string)get_graph_path(true)+"."+v2r.first;
		RunQuery(2, NULL, name.c_str());
	}
	// edge relation
	for(auto& e2r:edgeType2relation){
		// close the relation
		heap_close(e2r.second.first, RowExclusiveLock);
		// update the sequence
		std::string seqname = e2r.first+"_id_seq";
		Oid seqReloid = RangeVarGetRelid(makeRangeVar(get_graph_path(true), const_cast<char*>(seqname.c_str()), -1), NoLock, false);
		DirectFunctionCall3(setval3_oid,seqReloid,DirectFunctionCall1(int16_numeric,Int128GetDatum(e2r.second.second)),true);
		// update the stats
		stats->insertEdge += e2r.second.second;
		// rebuild index
		std::string name = (std::string)get_graph_path(true)+"."+e2r.first;
		RunQuery(2, NULL, name.c_str());
	}
}

TupleTableSlot* ExecSparqlLoad(SparqlLoadState* slstate)
{
    EState *estate = slstate->ps.state;
    SparqlLoadStats* stats = estate->es_sparqlloadstats;

	// get the graph
	// CreateVertexTest();
	Oid graphoid = get_graph_path_oid();

	std::unordered_map<std::string, uint64> uri2id;	// mapping uri to id <uri(char*),id(int)>
	std::unordered_map<std::string, std::pair<Relation,int>> vertexType2relation;	// mapping the types of all processed vertexes to relation and the number of tuples
	std::unordered_map<std::string, std::pair<Relation,int>> edgeType2relation;	// mapping the types of all processed edges to relation
	std::vector<Edge> edgeUnInserted; // all the edges to insert

	std::string lastSub = "";
	int edgeNum = 0;
	Vertex v;
	std::vector<Edge> edges;

    for (;;){
        TupleTableSlot *slot;
        slot = ExecProcNode(slstate->subplan);

        if (TupIsNull(slot))
            break;

        bool isNull;
        Datum subject_datum = slot_getattr(slot, 1, &isNull);
        Datum predicate_datum = slot_getattr(slot, 2, &isNull);
        Datum object_datum = slot_getattr(slot, 3, &isNull);

        std::string sub = TextDatumGetCString(subject_datum);
		std::string pre = TextDatumGetCString(predicate_datum);
		std::string obj = TextDatumGetCString(object_datum);

		sub = sub.substr(1,sub.size()-2);
		pre = pre.substr(1,pre.size()-2);
		// process the triple
		if(sub!=lastSub) 
		{
			if(v.property.find("uri")!=v.property.end())
			{
				// not the first line of file
				uint16 relid = get_labname_labid(v.type.c_str(), graphoid);
				GraphidSet(&v.id, (uint16) relid, (uint64)uri2id.size());
				uri2id.emplace(v.property["uri"], v.id);
				InsertVertex(v, vertexType2relation[v.type]);
				vertexType2relation[v.type].second++;
				InsertEdges(edges, uri2id, edgeType2relation);
				v.clear();
				edges.clear();
			}
			v.property.emplace("uri", sub);
		}
		if(IsType(pre)){
			// type
			obj = obj.substr(1,obj.size()-2);
			std::string type = obj.substr(obj.find("#")+1);
			std::transform(type.begin(),type.end(),type.begin(),::tolower);
			if(vertexType2relation.find(type)==vertexType2relation.end()){
				// new type
				RunQuery(1, "VLABEL", type.c_str());
				stats->newVLabel++;
				// open the relation
				uint16 relid = get_labname_labid(type.c_str(), graphoid);
				Oid reloid = get_labid_relid(graphoid, relid);
				Relation rel = heap_open(reloid, RowExclusiveLock);
				vertexType2relation.emplace(type, std::make_pair(rel,0));
			}
			v.type = type;
		}else if(IsLiteral(obj)){
			// property
			v.property.emplace(pre.substr(pre.find("#")+1), obj);
		}else{
			Edge e;
			e.type = pre.substr(pre.find("#")+1);
			std::transform(e.type.begin(),e.type.end(),e.type.begin(),::tolower);
			e.start = sub;
			e.end = obj.substr(1,obj.size()-2);
			uint16 relid;
			// edge
			if(edgeType2relation.find(e.type)==edgeType2relation.end()){
				// new edge type
				RunQuery(1, "ELABEL", e.type.c_str());
				stats->newELabel++;
				relid = get_labname_labid(e.type.c_str(), graphoid);
				// open the relation
				Oid reloid = get_labid_relid(graphoid, relid);
				Relation rel = heap_open(reloid, RowExclusiveLock);
				edgeType2relation.emplace(e.type, std::make_pair(rel,0));
			}else{
				relid = get_labname_labid(e.type.c_str(), graphoid);
			}
			GraphidSet(&e.id, (uint16) relid, (uint64)edgeNum);

			if(uri2id.find(e.end)==uri2id.end()){
				// new vertex
				edgeUnInserted.emplace_back(e);
			}else{
				// e.end = std::to_string(uri2id[e.end]);
				edges.emplace_back(e);
			}
			edgeNum++;
		}
		lastSub = sub;
    }
	// handle the case of the last group of slot
	uint16 relid = get_labname_labid(v.type.c_str(), graphoid);
	GraphidSet(&v.id, (uint16) relid, (uint64)uri2id.size());
	uri2id.emplace(v.property["uri"], v.id);
	InsertVertex(v, vertexType2relation[v.type]);
	vertexType2relation[v.type].second++;
	InsertEdges(edges, uri2id, edgeType2relation);

	InsertEdges(edgeUnInserted, uri2id, edgeType2relation);
	UpdateAndCloseRelation(vertexType2relation, edgeType2relation, stats);
	
    return NULL;
}

SparqlLoadState* ExecInitSparqlLoad(SparqlLoadPlan* node, EState* estate, int eflags)
{
    SparqlLoadState* state = makeNode(SparqlLoadState);
    state->ps.plan = (Plan*) node;
    state->ps.state = estate;
    estate->es_sparqlloadstats = (SparqlLoadStats*) palloc0(sizeof(SparqlLoadStats));
    memset(estate->es_sparqlloadstats, 0, sizeof(SparqlLoadStats));

    ExecInitResultTupleSlot(estate, &state->ps);
    state->subplan = ExecInitNode(node->subplan, estate, eflags);

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    return state;
}

void ExecEndSparqlLoad(SparqlLoadState* node){
    ExecClearTuple(node->ps.ps_ResultTupleSlot);
    ExecEndNode(node->subplan);
    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed");
}