#include <stdlib.h> /* exit, perror */
#include <unistd.h> /* sysconf */
#include <errno.h>
#include "cpu_mapping.h"
/*--------------------------$WARNING$ FOR THE DEFINE CHECK cpu_mapping.h-------------------------*/



#define MAX_NODES 0
/*-----------------------------------------------WARNING HARDCODED-------------------------------*/
#ifdef MY_PC
int numas = 1;
int threads_per_numa = 4;
int numa[][4] = {0,1,2,3};
int cpu_mapping[] = {0, 1, 2, 3};
#endif

#ifdef MY_PC2
int numas = 2;
int threads_per_numa = 2;
int numa[][2] = {{0,2}, {1,3}};
int cpu_mapping[] = {0, 1, 2, 3};
#endif

#ifdef SIGMOD_1CPU/*<---------------------------ALWAYS DEFIEND IT BEFORE UPLOAD-------------------*/
int numas = 1;
int threads_per_numa = 20;
int numa[][20] = {
        {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38}
};
int cpu_mapping[] = {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38};
#endif

#ifdef SIGMOD_2CPU
int numas = 2;
int threads_per_numa = 20;
int numa[][20] = {
    {0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38},
    {1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39}
    //{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,12,13,14,15,16,17,18,19},
    //{10,11 20, 21, 22, 23, 24, 25, 26, 27, 28, 19, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39}
};
int cpu_mapping[] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,
};
#endif

#ifdef SIGMOD_2CPU2
int numas = 4;
int threads_per_numa = 10;
int numa[][10] = {
    {0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
    {1, 3, 5, 7, 9, 11, 13, 15, 17, 19},
    {20, 22, 24, 26, 28, 30, 32, 34, 36, 38},
    {21, 23, 25, 27, 29, 31, 33, 35, 37, 39}
};
int cpu_mapping[] = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,
    21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,
};
#endif
/*-----------------------------------------------END OF HARDCODED----------------------------------*/


/**
 * Returns SMT aware logical to physical CPU mapping for a given thread id.
 */
int
get_cpu_id(int thread_id)
{
    return cpu_mapping[thread_id % threads_per_numa];
}

int
get_numa_id(int mytid)
{
    int ret = 0;
    for(int i = 0; i < numas; i++)
        for(int j = 0; j < threads_per_numa; j++)
            if(numa[i][j] == mytid){
                ret = i;
                break;
            }

    return ret;
}

int
get_num_numa_regions(void)
{
    return numas;
}

int
get_numa_node_of_address(void * ptr)
{
    return MAX_NODES;
}
