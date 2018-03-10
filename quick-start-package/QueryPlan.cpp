#include "include/QueryPlan.hpp"

/* construct a Query Plan -- do all the pre-processing that is necessary for a
  Query to be translated into a Join Tree and then start the whole process */
QueryPlan* constrQueryPlan(QueryInfo* queryInfoPtr) {
  QueryPlan* queryPlanPtr = malloc(1 * sizeof(QueryPlan));

  /* somehow collect relationships stats */

  /* create an array of table-types from the query relations */

  /* apply all filters */
  for (std::vector<int>::iterator it = queryInfoPtr->filters.begin(); it != queryInfoPtr->filters.end(); ++it) {
    //
  }

  /* create an array of sets of relations to be joined */

  /* create an array of Join Trees, one for each set of relations */
  std::vector<JoinTree> joinTreeVec;

  /* merge all Join Trees to one final Join Tree */
  JoinTree* tempJoinTreePtr = joinTreeVec.begin();
  for (std::vector<JoinTree>::iterator it = std::next(joinTreeVec.begin()); it != joinTreeVec.end(); ++it) {
    tempJoinTreePtr = constrJoinTreeFromJoinTrees(tempJoinTreePtr, *it);
  }
  queryPlanPtr->joinTreePtr = tempJoinTreePtr;
}

/* construct an optimal left-deep Join Tree from a given set of relations */
JoinTree* constrJoinTreeFromRelations(std::vector<Relation>& relations) {
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
