#include "QueryPlan.hpp"

// The Plan Tree Node constructor
PlanTreeNode::PlanTreeNode() {}

// The Plan Tree constructor
PlanTree::PlanTree(std::set<table_t*>& tableSet) {}

// Merges two join trees into one
// and return the resulted tree
PlanTree* PlanTree::mergePlanTrees(PlanTree* left, PlanTree* right) {
    return NULL;
}

// execute the plan described by a Plan Tree
void executePlan(PlanTree* planTreePtr) {}

// Destoys a Plan Tree properly
void PlanTree::freePlanTree(PlanTree* planTreePtr) {}

// Prints a Plan Tree -- for debugging
void PlanTree::printPlanTree(PlanTree* planTreePtr) {}

// Estimates the cost of a given Plan Tree */
double PlanTree::costOfPlanTree(PlanTree* planTreePtr) {
    return 1.0;
}

// Builds a query plan with the given info
void QueryPlan::build(QueryInfo& queryInfoPtr) {
#ifdef k
    /* a map that maps each relation-ID to a pointer to it's respective table-type */
    std::map<RelationId, table_t*> tableTPtrMap;

    /* apply all filters and create a vector of table-types from the query selections */
    for (std::vector<FilterInfo>::iterator it = queryInfoPtr.filters.begin(); it != queryInfoPtr.filters.end(); ++it) {
        std::map<RelationId, table_t*>::iterator mapIt = tableTPtrMap.find(it->filterColumn.relId);

        /* if table-type for this relation-ID already in map, then update the respective table-type */
        if (mapIt != tableTPtrMap.end()) {
            Select(it, mapIt->second);
        }
        else {
            /* otherwise add the table-type's mapping to the map */
            table_t* tempTableTPtr = CreateTableTFromId(it->filterColumn.relId, it->filterColumn.binding);
            Select(it, tempTableTPtr);
            tableTPtrMap.insert(it->filterColumn.relId, tempTableTPtr);
        }
    }

    /* create a set of pointers to the table-types that are to be joined */
    std::set<table_t*> tableTPtrSet;
    for (std::vector<PredicateInfo>::iterator it = queryInfoPtr.predicates.begin(); it != queryInfoPtr.predicates.end(); ++it) {
        std::map<RelationId, table_t*>::iterator mapItLeft = tableTPtrMap.find(it->left.relId),
                                            mapItRight = tableTPtrMap.find(it->right.relId);
        /* sanity check -- REMOVE in the end */
        assert(mapItLeft != tableTPtrMap.end() && mapItRight != tableTPtrMap.end());

        tableTPtrSet.insert(mapItLeft);
        tableTPtrSet.insert(mapItRight);
    }

    /* create a Join Tree by joining all the table-types of the set */
    JoinTree* tempJoinTreePtr = constrJoinTreeFromRelations(tableTPtrSet);

    /* apply all filters */
    for (std::vector<int>::iterator it = queryInfoPtr.filters.begin() ; it != queryInfoPtr.filters.end(); ++it) {}

    /* create an array of sets of relations to be joined */

    /* create an array of Join Trees for each set of relations */

    /* merge all Join Trees to one final Join Tree */
#endif
}

// Fills the columnInfo matrix with the data of every column
void QueryPlan::fillColumnInfo(Joiner& joiner) {
    Relation* relation;
    int relationsCount = joiner.getRelationsCount(); // Get the number of relations
    int columnsCount; // Number of columns of a relation

    // For every relation get its column statistics
    for (int i = 0; i < relationsCount; i++) {
        // Get the number of columns
        relation = &(joiner.getRelation(i));
        columnsCount =  relation->columns.size();

        // Allocate a row for this relation
        columnInfos[i] = (ColumnInfo*) malloc(columnsCount * sizeof(ColumnInfo));
    }
}
