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

#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>

#include "nodes/execnodes.h"

struct Vertex
{
    uint64 id;
    std::string type;
    std::unordered_map<std::string, std::string> property; // <property name,val>
    void clear(){
        this->id = 0;
        this->type = "";
        this->property.clear();
    }
};

struct Edge
{
    uint64 id;
    std::string type;
    std::string start; // uri
    std::string end; // uri
};

extern TupleTableSlot* ExecSparqlLoad(SparqlLoadState* node);

extern SparqlLoadState* ExecInitSparqlLoad(SparqlLoadPlan* node, EState* estate, int eflags);

extern void ExecEndSparqlLoad(SparqlLoadState* node);