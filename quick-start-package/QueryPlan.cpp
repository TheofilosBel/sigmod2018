#include "QueryPlan.hpp"

// The Plan Tree constructor
PlanTree::PlanTree() {}

// Merges two join trees into one
// and return the resulted tree
PlanTree* PlanTree::mergePlanTrees(PlanTree* left, PlanTree* right) {
    return NULL;
}

// Destoys a Plan Tree properly
void PlanTree::freePlanTree(PlanTree* planTreePtr) {}

// Prints a Plan Tree -- for debugging
void PlanTree::printPlanTree(PlanTree* planTreePtr) {}

// Estimates the cost of a given Plan Tree */
double PlanTree::costOfPlanTree(PlanTree* planTreePtr);

// The Query Plan constructor
QueryPlan::QueryPlan(QueryInfo* queryInfoPtr) {
    /* create an array of table-types from the query relations */

    /* apply all filters */
    for (std::vector<int>::iterator it = queryInfoPtr->filters.begin() ; it != queryInfoPtr->filters.end(); ++it) {}

    /* create an array of sets of relations to be joined */

    /* create an array of Join Trees for each set of relations */

    /* merge all Join Trees to one final Join Tree */
}