#include <sys/time.h>           /* gettimeofday */
#include <stdio.h>              /* printf */
#include "Joiner.hpp"
#include "table_t"



#ifndef NUM_RADIX_BITS
#define NUM_RADIX_BITS 14
#endif

#define NUM_PASSES 1

/** checks malloc() result */
#ifndef MALLOC_CHECK
#define MALLOC_CHECK(M)                                                 \
    if(!M){                                                             \
        printf("[ERROR] MALLOC_CHECK: %s : %d\n", __FILE__, __LINE__);  \
        perror(": malloc() failed!\n");                                 \
        exit(EXIT_FAILURE);                                             \
    }
#endif

/* #define RADIX_HASH(V)  ((V>>7)^(V>>13)^(V>>21)^V) */
#define HASH_BIT_MODULO(K, MASK, NBITS) (((K) & MASK) >> NBITS)

#ifndef NEXT_POW_2
/**
 *  compute the next number, greater than or equal to 32-bit unsigned v.
 *  taken from "bit twiddling hacks":
 *  http://graphics.stanford.edu/~seander/bithacks.html
 */
#define NEXT_POW_2(V)                           \
    do {                                        \
        V--;                                    \
        V |= V >> 1;                            \
        V |= V >> 2;                            \
        V |= V >> 4;                            \
        V |= V >> 8;                            \
        V |= V >> 16;                           \
        V++;                                    \
    } while(0)
#endif

#define MAX(X, Y) (((X) > (Y)) ? (X) : (Y))

/**
 *  This algorithm builds the hashtable using the bucket chaining idea and used
 *  in PRO implementation. Join between given two relations is evaluated using
 *  the "bucket chaining" algorithm proposed by Manegold et al. It is used after
 *  the partitioning phase, which is common for all algorithms. Moreover, R and
 *  S typically fit into L2 or at least R and |R|*sizeof(int) fits into L2 cache.
 *
 * @param R input relation R
 * @param S input relation S
 *
 * @return number of result tuples
 */
int64_t bucket_chaining_join(const relation_t * const R, const relation_t * const S, relation_t * const tmpR) {
    int * next, * bucket;
    const uint32_t numR = R->num_tuples;
    uint32_t N = numR;
    int64_t matches = 0;

    NEXT_POW_2(N);
    /* N <<= 1; */
    const uint32_t MASK = (N-1) << (NUM_RADIX_BITS);

    next   = (int*) malloc(sizeof(int) * numR);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (int*) calloc(N, sizeof(int));

    const tuple_t * const Rtuples = R->tuples;
    for(uint32_t i=0; i < numR; ){
        uint32_t idx = HASH_BIT_MODULO(R->tuples[i].key, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    const tuple_t * const Stuples = S->tuples;
    const uint32_t        numS    = S->num_tuples;

    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numS; i++ ){

        uint32_t idx = HASH_BIT_MODULO(Stuples[i].key, MASK, NUM_RADIX_BITS);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

            if(Stuples[i].key == Rtuples[hit-1].key){
                /* TODO: copy to the result buffer, we skip it */
                matches ++;
            }
        }
    }
    /* PROBE-LOOP END  */

    /* clean up temp */
    free(bucket);
    free(next);

    return matches;
}

void radix_cluster_nopadding(uint64_t ** outRel, uint64_t ** inRel, column_t *column, int row_ids_size, int R, int D) {

    uint64_t *** dst;
    uint64_t   * input;
    uint32_t   * tuples_per_cluster;
    uint32_t     i;
    uint64_t     offset;
    const uint32_t M = ((1 << D) - 1) << R;
    const uint32_t fanOut = 1 << D;
    const uint32_t ntuples = row_ids_size;

    tuples_per_cluster = (uint32_t*)calloc(fanOut, sizeof(uint32_t));
    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function
       just in case D differs from call to call. */
    dst     = (uint64_t ***)malloc(sizeof(uint64_t **) * fanOut);
    /* dst_end = (tuple_t**)malloc(sizeof(tuple_t*)*fanOut); */

    input = column->values + row_ids[0][column->binding]*sizeof(uint64_t);  // TODO FIX OUTSIDE
    /* count tuples per cluster */
    for( i=0; i < ntuples; i++ ){
        uint32_t idx = (uint32_t)(HASH_BIT_MODULO(input, M, R));
        tuples_per_cluster[idx]++;
        input += row_ids[i][column->binding]*sizeof(uint64_t);
    }

    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for ( i=0; i < fanOut; i++ ) {
        dst[i]      = outRel + offset;
        offset     += tuples_per_cluster[i] * sizeof(uint64_t**);
        /* dst_end[i]  = outRel->tuples + offset; */
    }

    input = column->values + row_ids[0][column->binding]*sizeof(uint64_t);  // TODO FIX OUTSIDE;
    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < ntuples; i++ ){
        uint32_t idx   = (uint32_t)(HASH_BIT_MODULO(input, M, R));
        *dst[idx] = *row_ids[i];
        ++dst[idx];
        input += row_ids[i][column->binding]*sizeof(uint64_t);
        /* we pre-compute the start and end of each cluster, so the following
           check is unnecessary */
        /* if(++dst[idx] >= dst_end[idx]) */
        /*     REALLOCATE(dst[idx], dst_end[idx]); */
    }

    /* clean up temp */
    /* free(dst_end); */
    free(dst);
    free(tuples_per_cluster);
}

table_t* radix_join(table_t *table_left, column_t *column_left, table_t *table_right, column_t *column_right) {

    int64_t result = 0;
    uint32_t i;

    #ifndef NO_TIMING
    struct timeval start, end;
    uint64_t timer1, timer2, timer3;
    #endif

    uint64_t **outRelR, **outRelS;

    out_row_ids_left  = (uint64_t **) malloc(sizeof(uint64_t **) * table_left->size_of_row_ids);
    out_row_ids_right = (uint64_t **) malloc(sizeof(uint64_t **) * table_right->size_of_row_ids);

    #ifndef NO_TIMING
    gettimeofday(&start, NULL);
    startTimer(&timer1);
    startTimer(&timer2);
    startTimer(&timer3);
    #endif

    /***** do the multi-pass partitioning *****/
    #if NUM_PASSES==1
    /* apply radix-clustering on relation R for pass-1 */
    radix_cluster_nopadding(out_row_ids_left, table_left->row_ids, column_left,
                                table_left->size_of_row_ids, 0, NUM_RADIX_BITS);
    free(table_left->row_ids); 
    table_left->row_ids = out_row_ids_left;

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding(out_row_ids_right, table_right->row_ids, column_right, 
                                table_right->size_of_row_ids, 0, NUM_RADIX_BITS);
    free(table_right->row_ids);
    table_right->row_ids = out_row_ids_right;

    #elif NUM_PASSES==2
    /* apply radix-clustering on relation R for pass-1 */
    radix_cluster_nopadding(outRelR, relR, 0, NUM_RADIX_BITS/NUM_PASSES);

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding(outRelS, relS, 0, NUM_RADIX_BITS/NUM_PASSES);

    /* apply radix-clustering on relation R for pass-2 */
    radix_cluster_nopadding(relR, outRelR,
                            NUM_RADIX_BITS/NUM_PASSES,
                            NUM_RADIX_BITS-(NUM_RADIX_BITS/NUM_PASSES));

    /* apply radix-clustering on relation S for pass-2 */
    radix_cluster_nopadding(relS, outRelS,
                            NUM_RADIX_BITS/NUM_PASSES,
                            NUM_RADIX_BITS-(NUM_RADIX_BITS/NUM_PASSES));

    /* clean up temporary relations */
    free(outRelR->tuples);
    free(outRelS->tuples);
    free(outRelR);
    free(outRelS);

    #else
    #error Only 1 or 2 pass partitioning is implemented, change NUM_PASSES!
    #endif


    #ifndef NO_TIMING
    stopTimer(&timer3);
    #endif

    int * R_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));
    int * S_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));

    /* compute number of tuples per cluster */
    for( i=0; i < relR->num_tuples; i++ ){
        uint32_t idx = (relR->tuples[i].key) & ((1<<NUM_RADIX_BITS)-1);
        R_count_per_cluster[idx] ++;
    }
    for( i=0; i < relS->num_tuples; i++ ){
        uint32_t idx = (relS->tuples[i].key) & ((1<<NUM_RADIX_BITS)-1);
        S_count_per_cluster[idx] ++;
    }

    /* build hashtable on inner */
    int r, s; /* start index of next clusters */
    r = s = 0;
    for( i=0; i < (1<<NUM_RADIX_BITS); i++ ){
        relation_t tmpR, tmpS;

        if(R_count_per_cluster[i] > 0 && S_count_per_cluster[i] > 0){

            tmpR.num_tuples = R_count_per_cluster[i];
            tmpR.tuples = relR->tuples + r;
            r += R_count_per_cluster[i];

            tmpS.num_tuples = S_count_per_cluster[i];
            tmpS.tuples = relS->tuples + s;
            s += S_count_per_cluster[i];

            result += bucket_chaining_join(&tmpR, &tmpS, NULL);
        }
        else {
            r += R_count_per_cluster[i];
            s += S_count_per_cluster[i];
        }
    }

    #ifndef NO_TIMING
    /* TODO: actually we're not timing build */
    stopTimer(&timer2);/* build finished */
    stopTimer(&timer1);/* probe finished */
    gettimeofday(&end, NULL);
    /* now print the timing results: */
    print_timing(timer1, timer2, timer3, relS->num_tuples, result, &start, &end);
    #endif

    /* clean-up temporary buffers */
    free(S_count_per_cluster);
    free(R_count_per_cluster);

    #if NUM_PASSES == 1
    /* clean up temporary relations */
    free(outRelR->tuples);
    free(outRelS->tuples);
    free(outRelR);
    free(outRelS);
    #endif

    return result;
}
