#include "./include/functions.hpp"

/*          Sample query tree:
                                             --- (7) B1<1000
                                             |
                --- (4) D3=C3--- (5) C5=B1---|
                |                            |
    (1) C2=A2---|                            --- (6) A2=b2
                |
                --- (2) A1=B2--- (3) A5>3000

*/

/* Contruct query-tree */
JTree* jTreeConstr() {
    int num_of_nodes = 0;

    JTree* jTreePtr = NULL;

    /* create the node 1 (head) of the query-tree */
    /* let C2=A2 of the above example */
    jTreePtr = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->predPtr = NULL;
    jTreePtr->predPtr = (struct PredicateInfo *) malloc(1 * sizeof(PredicateInfo));
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;
    jTreePtr->parent = NULL;

    /* create the node 2 of the query-tree */
    /* let A1=B2 of the above example */
    jTreePtr->left = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->left->parent = jTreePtr;  // remember your parent
    jTreePtr = jTreePtr->left;
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->predPtr = NULL;
    jTreePtr->predPtr = (struct PredicateInfo *) malloc(1 * sizeof(PredicateInfo));
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;

    /* create the node 3 of the query-tree */
    /* let A5>3000 of the above example */
    jTreePtr->left = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->left->parent = jTreePtr;  // remember your parent
    jTreePtr = jTreePtr->left;
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->filterPtr = (struct FilterInfo *) malloc(1 * sizeof(FilterInfo));
    jTreePtr->predPtr = NULL;
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;

    /* go back to node 2 */
    jTreePtr = jTreePtr->parent;
    /* go back to node 1 (head) */
    jTreePtr = jTreePtr->parent;

    assert(jTreePtr->parent == NULL);   // then it must be the head

    /* create the node 4 of the query-tree */
    /* let D3=C3 of the above example */
    jTreePtr->right = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->right->parent = jTreePtr;  // remember your parent
    jTreePtr = jTreePtr->right;
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->predPtr = NULL;
    jTreePtr->predPtr = (struct PredicateInfo *) malloc(1 * sizeof(PredicateInfo));
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;

    /* create the node 5 of the query-tree */
    /* let C5=B1 of the above example */
    jTreePtr->left = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->left->parent = jTreePtr;  // remember your parent
    jTreePtr = jTreePtr->left;
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->predPtr = NULL;
    jTreePtr->predPtr = (struct PredicateInfo *) malloc(1 * sizeof(PredicateInfo));
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;

    /* create the node 6 of the query-tree */
    /* let A2=B2 of the above example */
    jTreePtr->left = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->left->parent = jTreePtr;  // remember your parent
    jTreePtr = jTreePtr->left;
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->predPtr = NULL;
    jTreePtr->predPtr = (struct PredicateInfo *) malloc(1 * sizeof(PredicateInfo));
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;

    /* go back to node 5 */
    jTreePtr = jTreePtr->parent;

    /* create the node 7 of the query-tree */
    /* let B1<1000 of the above example */
    jTreePtr->right = (JTree *) malloc(1 * sizeof(JTree));
    jTreePtr->right->parent = jTreePtr;  // remember your parent
    jTreePtr = jTreePtr->right;
    jTreePtr->node_id = ++num_of_nodes;
    jTreePtr->filterPtr = NULL;
    jTreePtr->filterPtr = (struct FilterInfo *) malloc(1 * sizeof(FilterInfo));
    jTreePtr->predPtr = NULL;
    jTreePtr->left = NULL;
    jTreePtr->right = NULL;

    /* go back to node 5 */
    jTreePtr = jTreePtr->parent;
    /* go back to node 4 */
    jTreePtr = jTreePtr->parent;

    /* go back to node 1 (head) */
    jTreePtr = jTreePtr->parent;

    assert(jTreePtr->parent == NULL);   // then it must be the head

    return jTreePtr;    // essentially return the head
}

/* Destruct query-tree */
void jTreeDestr(JTree* jTreePtr) {
    /* destruct query-tree in a DFS fassion */
    JTree *currPtr = jTreePtr;
    bool from_left = false;

    while(currPtr) {
        /* if there are left children */
        if (currPtr->left) {
            currPtr = currPtr->left;
            from_left = true;
        }
        /* if there are right children */
        else if (currPtr->right) {
            currPtr = currPtr->right;
            from_left = false;
        }
        /* if there are no left or right children */
        else {
            /* clean-up node */
            free(currPtr->filterPtr);
            currPtr->filterPtr = NULL;
            free(currPtr->predPtr);
            currPtr->predPtr = NULL;
            /* essentially, not head node */
            if (currPtr->parent) {
                /* go to the parent */
                currPtr = currPtr->parent;
                /* free the correct child's node */
                if (from_left) {
                    free(currPtr->left);
                    currPtr->left = NULL;
                }
                else {
                    free(currPtr->right);
                    currPtr->right = NULL;
                }
            }
            /* essentially, head node */
            else {
                free(currPtr);
                currPtr = NULL;
            }
        }
    }
}

/* Print query-tree */
void jTreePrintTree(JTree* jTreePtr) {
    /* print query-tree in a DFS fassion */
    JTree *currPtr = jTreePtr;
    bool from_left = true, went_left = false, went_right = false;

    while(currPtr) {
        /* if you can go to the left children */
        if (!went_left && currPtr->left) {
            currPtr = currPtr->left;
            from_left = true;
            /* you may now go left or right again */
            went_left = false;
            went_right = false;
        }
        /* if you can go to the right children */
        else if (!went_right && currPtr->right) {
            currPtr = currPtr->right;
            from_left = false;
            /* you may now go left or right again */
            went_left = false;
            went_right = false;
        }
        /* if you can't go to the left or to the right children */
        else {
            /* print node ID */
            printf("%d\n", currPtr->node_id);
            /* go to the parent */
            currPtr = currPtr->parent;
            /* deside liberty of transitions */
            if (from_left) {
                went_left = true;
                went_right = false;
            }
            else {
                went_left = true;
                went_right = true;
            }
        }
    }
}

/* Make an execution plan out of a query-tree */
int* jTreeMakePlan(JTree* jTreePtr, int* plan_size, Joiner& joiner) {
    /* construct plan by iterating through the query-tree in a DFS fassion */
    *plan_size = 0;
    int* plan = NULL;

    JTree *currPtr = jTreePtr;
    bool from_left = true, went_left = false, went_right = false;

    while(currPtr) {
        /* if you can go to the left children */
        if (!went_left && currPtr->left) {
            currPtr = currPtr->left;
            from_left = true;
            /* you may now go left or right again */
            went_left = false;
            went_right = false;
        }
        /* if you can go to the right children */
        else if (!went_right && currPtr->right) {
            currPtr = currPtr->right;
            from_left = false;
            /* you may now go left or right again */
            went_left = false;
            went_right = false;
        }
        /* if you can't go to the left or to the right children */
        else {
            /* add node ID to our plan */
            (*plan_size)++;
            plan = (int *) realloc(plan, (*plan_size) * sizeof(int));
            plan[(*plan_size)-1] = currPtr->node_id;

            table_t *left_table, *right_table;

            if (currPtr->left && currPtr->left->intermediate_res) {
              if (currPtr->right->intermediate_res) {

              }
            }
            else if (currPtr->right && currPtr->right->intermediate_res) {

            }
            else {
              if (currPtr->predPtr) {
                left_table = joiner.SelectInfoToTableT(currPtr->predPtr->left);
                right_table = joiner.SelectInfoToTableT(currPtr->predPtr->right);
                currPtr->intermediate_res = joiner.join(left_table, right_table);
              } else {
                currPtr->intermediate_res = joiner.Select(currPtr->filterPtr);
              }
            }

            /* go to the parent */
            currPtr = currPtr->parent;
            /* deside liberty of transitions */
            if (from_left) {
                went_left = true;
                went_right = false;
            }
            else {
                went_left = true;
                went_right = true;
            }
        }
    }

    return plan;
}

/* Print plan -- for debugging */
void jTreePrintPlan(int* plan, int plan_size) {
    for (int i = 0; i < plan_size; i++)
        printf("%d\n", plan[i]);
}
