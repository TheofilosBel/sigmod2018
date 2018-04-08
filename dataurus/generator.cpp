/* @version $Id: generator.c 5255 2014-07-11 15:31:34Z bcagri $ */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>              /* perror */
#include <stdlib.h>             /* posix_memalign */

#include "generator.h"          /* create_relation_*() */
#include "prj_params.h"         /* RELATION_PADDING for Parallel Radix */

/* return a random number in range [0,N] */
#define MALLOC(SZ) alloc_aligned(SZ+RELATION_PADDING) /*malloc(SZ+RELATION_PADDING)*/
#define MALLOC_64(SZ) alloc_aligned(SZ+RELATION_PADDING_64) /*malloc(SZ+RELATION_PADDING)*/
#define FREE(X,SZ) free(X)

/** An experimental feature to allocate input relations numa-local */
int numalocalize;
int nthreads;


void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size);

    if (rv) {
        perror("[ERROR] alloc_aligned() failed: out of memory");
        return 0;
    }

    return ret;
}

// ADDED ***always remember the padding for L1 misses */
relation_t *
gen_rel(int num_tuples)
{
    relation_t * r1 = (relation_t *) malloc(sizeof(relation_t));
    r1->num_tuples = num_tuples;
    r1->tuples = (tuple_t *)MALLOC(sizeof(tuple_t) * num_tuples);
    return r1;
}

// ADDED ***always remember the padding for L1 misses */
relation64_t *
gen_rel_t64(int num_tuples)
{
    relation64_t * r1 = (relation64_t *) malloc(sizeof(relation64_t));
    r1->num_tuples = num_tuples;
    r1->tuples = (tuple64_t *)MALLOC_64(sizeof(tuple64_t) * num_tuples);
    return r1;
}
