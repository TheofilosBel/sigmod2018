#include "include/QueryPlan.hpp"

/* construct a Query Plan -- do all the pre-processing that is necessary for a
  Query to be translated into a Join Tree and then start the whole process */
QueryPlan* constrQueryPlan(QueryInfo* queryInfoPtr) {
  QueryPlan* queryPlanPtr = malloc(1 * sizeof(QueryPlan));

  /*
    somehow collect relationships stats ???
  */

  /* a map that maps each relation-ID to a pointer to it's respective table-type */
  std::map<RelationId, table_t*> tableTPtrMap;

  /* apply all filters and create a vector of table-types from the query selections */
  for (std::vector<FilterInfo>::iterator it = queryInfoPtr->filters.begin(); it != queryInfoPtr->filters.end(); ++it) {
    std::map<RelationId, table_t*>::iterator mapIt = tableTPtrMap.find(it->filterColumn.relId);
    /* if table-type for this relation-ID already in map, then update the respective table-type */
    if (mapIt != tableTPtrMap.end()) {
      Select(it, mapIt->second);
    }
    /* otherwise add the table-type's mapping to the map */
    else {
      table_t* tempTableTPtr = CreateTableTFromId(it->filterColumn.relId, it->filterColumn.binding);
      Select(it, tempTableTPtr);
      tableTPtrMap.insert(it->filterColumn.relId, tempTableTPtr);
    }
  }

  /* create a set of pointers to the table-types that are to be joined */
  std::set<table_t*> tableTPtrSet;
  for (std::vector<PredicateInfo>::iterator it = queryInfoPtr->predicates.begin(); it != queryInfoPtr->predicates.end(); ++it) {
    std::map<RelationId, table_t*>::iterator mapItLeft = tableTPtrMap.find(it->left.relId),
                                            mapItRight = tableTPtrMap.find(it->right.relId);
    /* sanity check -- REMOVE in the end */
    assert(mapItLeft != tableTPtrMap.end() && mapItRight != tableTPtrMap.end());

    tableTPtrSet.insert(mapItLeft);
    tableTPtrSet.insert(mapItRight);
  }

  /* create a Join Tree by joining all the table-types of the set */
  JoinTree* tempJoinTreePtr = constrJoinTreeFromRelations(tableTPtrSet);

  /* merge all Join Trees to one final Join Tree */
  // JoinTree* tempJoinTreePtr = joinTreePtrVec.begin();
  // for (std::vector<JoinTree*>::iterator it = std::next(joinTreePtrVec.begin()); it != joinTreePtrVec.end(); ++it) {
  //   tempJoinTreePtr = constrJoinTreeFromJoinTrees(tempJoinTreePtr, *it);
  // }
  // queryPlanPtr->joinTreePtr = tempJoinTreePtr;
}

/* construct an optimal left-deep Join Tree from a given set of relations */
JoinTree* constrJoinTreeFromRelations(std::set<table_t*>& tableTPtrSet) {
  return NULL;
}

/* construct an optimal Join Tree from two given optimal Join Trees */
JoinTree* constrJoinTreeFromJoinTrees(JoinTree* joinTreePtr1, JoinTree* joinTreePtr2) {
  return NULL;
}

/* construct a Join Tree node */
JoinTreeNode* constrJoinTreeNode() {
  return NULL;
}

/* destruct a Join Tree properly */
void destrJoinTree(JoinTree* joinTreePtr) {
  ;
}

/* destruct a Join Tree node properly */
void destrJoinTreeNode(JoinTreeNode* joinTreeNodePtr) {
  ;
}

/* print a Join Tree -- for debugging */
void printJoinTree(JoinTree* joinTreePtr) {
  ;
}

/* print a Join Tree node -- for debugging */
void printJoinTreeNode(JoinTreeNode* joinTreeNodePtr) {
  ;
}

/* estimate the cost of a given Join Tree */
double joinTreeCost(JoinTree* joinTreePtr) {
  return 1.0;
}

/* estimate the cost of a given Join Tree node */
double joinTreeNodeCost(JoinTreeNode* joinTreeNodePtr) {
  return 1.0;
}
