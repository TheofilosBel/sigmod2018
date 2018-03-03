#include "../include/header.hpp"

using namespace std;

#include <cstdio>

#ifdef __MAIN_OLD__
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

#else

void print_rec(JTree *ptr, int depth)
{
    if (ptr == NULL)
        return;
    if (ptr->node_id == -1) {

            for (int i=0; i < depth; i++)
                printf("\t");
            if (ptr->filterPtr != NULL) {
                printf("%d.%d %c %ld\n", ptr->filterPtr->filterColumn.relId,
                    ptr->filterPtr->filterColumn.colId, 
                    ptr->filterPtr->comparison, ptr->filterPtr->constant);
            print_rec(ptr->left, depth+1);
            }
            else if (ptr->predPtr != NULL) {
                printf("%d.%d = %d.%d\n", ptr->predPtr->left.relId, 
                    ptr->predPtr->left.colId, ptr->predPtr->right.relId, 
                    ptr->predPtr->right.colId);

                print_rec(ptr->left, depth+1);
                print_rec(ptr->right, depth+1);
            }
            else {
                printf(" >< \n");
                print_rec(ptr->left, depth+1);
                print_rec(ptr->right, depth+1);
            }
        }
        else {
            for (int i=0; i < depth; i++)
                printf("\t");

            printf("-- %d\n", ptr->node_id);
        }
}

int main(int argc, char  *argv[])
{

    QueryInfo *qf = new QueryInfo;
    string s("12 1 6 12|0.2=1.0&1.0=2.1&3.1=3.2&3.0<33199|2.1 0.1 0.2");
    qf->parseQuery(s);
    
  //  std::cout << "ok" << endl;
//    for (unsigned int i=0; i < qf->relationIds.size(); i++)
//          std::cout << qf->relationIds[i] << endl;;


    JTree *ptr = treegen(qf);
    int depth=0;


    int *plan = NULL, plan_size = 0;
    plan = jTreeMakePlan(ptr, &plan_size);

    /* test plan by iterating through it, in order to print it */
    jTreePrintTree(ptr);

    /* free any dynamically allocated memory */
   // jTreeDestr(ptr);
    //free(plan);

    return 0;
}




#endif