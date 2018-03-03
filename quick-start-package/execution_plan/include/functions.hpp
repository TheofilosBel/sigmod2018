#pragma once

#include "header.hpp"

/*          Sample query tree:

            ---D3=C3
            |
    C2=A2---|
            |
            ---A1=B2---A5>3000

*/

/* Contruct query-tree */
JTree* jTreeConstr();

/* Destruct query-tree */
void jTreeDestr(JTree* jTreePtr);

/* Print query-tree -- for debugging */
void jTreePrintTree(JTree* jTreePtr);

/* Make an execution plan out of a query-tree */
/* For now, our execution plan can be represented by a "vector" of query-tree node ID's */
int* jTreeMakePlan(JTree* jTreePtr, int* plan_size);

/* Print plan -- for debugging */
void jTreePrintPlan(int* plan, int plan_size);
