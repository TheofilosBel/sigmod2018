#include "../include/header.hpp"

using namespace std;

int main(int argc, char const *argv[])
{
    /* construct a simple query-tree */
    JTree* jTreePtr = NULL;
    jTreePtr = jTreeConstr();

    /* test query-tree by iterating through it, in order to print it */
    jTreePrintTree(jTreePtr);

    /* produce an execution plan */
    int *plan = NULL, plan_size = 0;
    plan = jTreeMakePlan(jTreePtr, &plan_size);

    /* test plan by iterating through it, in order to print it */
    jTreePrintPlan(plan, plan_size);

    /* free any dynamically allocated memory */
    jTreeDestr(jTreePtr);
    free(plan);

    return 0;
}