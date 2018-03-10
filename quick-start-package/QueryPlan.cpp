#include "include/QueryPlan.hpp"

/* construct a Query Plan -- do all the pre-processing that is necessary for a
  Query to be translated into a Join Tree and then start the whole process */
void constrQueryPlan(QueryInfo* queryInfoPtr) {
  /* create an array of table-types from the query relations */

  /* apply all filters */
  for (std::vector<int>::iterator it = queryInfoPtr->filters.begin() ; it != queryInfoPtr->filters.end(); ++it) {
    //
  }

  /* create an array of sets of relations to be joined */

  /* create an array of Join Trees for each set of relations */

  /* merge all Join Trees to one final Join Tree */
}

/* construct an optimal left-deep Join Tree from a given set of relations */
JoinTree* constrJoinTreeFromRelations() {
  return NULL;
}

/* construct an optimal Join Tree from two given optimal Join Trees */
JoinTree* constrJoinTreeFromJoinTrees(JoinTree* joinTreePtr1, JoinTree* joinTreePtr2) {
  return NULL;
}

/* destruct a Join Tree properly */
void destrJoinTree(JoinTree* joinTreePtr) {
  ;
}

/* print a Join Tree -- for debugging */
void printJoinTree(JoinTree* joinTreePtr) {
  ;
}

/* estimate the cost of a given Join Tree */
double joinTreeCost(JoinTree* joinTreePtr) {
  return 1.0;
}
