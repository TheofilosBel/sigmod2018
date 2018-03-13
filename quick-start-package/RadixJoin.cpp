#include "RadixJoin.hpp"

/**
 *  This algorithm builds the hashtable using the bucket chaining idea and used
 *  in PRO implementation. Join between given two relations is evaluated using
 *  the "bucket chaining" algorithm proposed by Manegold et al. It is used after
 *  the partitioning phase, which is common for all algorithms. Moreover, R and
 *  S typically fit into L2 or at least R and |R|*sizeof(int) fits into L2 cache.
 *
 * @param R input relation R->row ids
 * @param S input relation S->row ids
 *
 * @return number of result tuples
 */
int64_t bucket_chaining_join(uint64_t ** L, const column_t * column_left, const int size_left,
                             uint64_t ** R, const column_t * column_right, const int size_right, int * const tmpR) {
    int * next, * bucket;
    const uint32_t numL = size_left;
    uint32_t N = numL;
    uint64_t matches = 0;

    NEXT_POW_2(N);
    /* N <<= 1; */
    const uint64_t MASK = (N-1) << (NUM_RADIX_BITS);

    next   = (int*) malloc(sizeof(int) * numL);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (int*) calloc(N, sizeof(int));

    const uint64_t * Ltuples;
    uint64_t         index = column_left->binding;
    for(uint32_t i=0; i < numL; ){
        Ltuples = column_left->values + L[i][index]*sizeof(uint64_t*);
        uint32_t idx = HASH_BIT_MODULO(*Ltuples, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    const uint64_t * Rtuples;
    const uint32_t   numR  = size_right;
    index = column_right->binding;
    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numR; i++ ){
        Rtuples = column_right->values + R[i][index]*sizeof(uint64_t *);
        Ltuples = column_left->values + L[i][index]*sizeof(uint64_t*);
        uint32_t idx = HASH_BIT_MODULO(*Rtuples, MASK, NUM_RADIX_BITS);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){
            Ltuples = column_left->values + L[hit-1][index]*sizeof(uint64_t*);
            if(*Rtuples == *Ltuples){
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

void radix_cluster_nopadding(uint64_t ** out_rel_ids, uint64_t ** in_rel_ids, column_t *column, int row_ids_size, int R, int D) {

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


    /* count tuples per cluster */
    for( i=0; i < ntuples; i++ ){
        input = column->values + in_rel_ids[i][column->binding]*sizeof(uint64_t*);
        uint64_t idx = (uint64_t)(HASH_BIT_MODULO(*input, M, R));
        tuples_per_cluster[idx]++;
    }

    std::cerr << "HERE " << ntuples << '\n';
    flush(std::cerr);

    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for ( i=0; i < fanOut; i++ ) {
        dst[i]      = out_rel_ids + offset;
        offset     += tuples_per_cluster[i] * sizeof(uint64_t**);
    }

    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < ntuples; i++ ){
        input = column->values + in_rel_ids[i][column->binding]*sizeof(uint64_t*);
        uint64_t idx   = (uint64_t)(HASH_BIT_MODULO(*input, M, R));
        *dst[idx] = in_rel_ids[i];
        ++dst[idx];
    }

    /* clean up temp */
    /* free(dst_end); */
    free(dst);
    free(tuples_per_cluster);
}

table_t* radix_join(table_t *table_left, column_t *column_left, table_t *table_right, column_t *column_right) {

    int64_t result = 0;
    uint32_t i;

    #ifdef time
    struct timeval start, end;
    uint64_t timer1, timer2, timer3;
    #endif

    uint64_t **out_row_ids_left, **out_row_ids_right;

    out_row_ids_left  = (uint64_t **) malloc(sizeof(uint64_t **) * table_left->size_of_row_ids);
    out_row_ids_right = (uint64_t **) malloc(sizeof(uint64_t **) * table_right->size_of_row_ids);

    #ifdef time
    gettimeofday(&start, NULL);
    startTimer(&timer1);
    startTimer(&timer2);
    startTimer(&timer3);
    #endif

    /* Fix the columns */
    int index_l = -1;
    int index_r = -1;
    for (ssize_t index = 0; index < table_left->num_of_relations ; index++) {
        if (column_left->binding == table_left->relations_bindings[index]){
            index_l = index;
        }
    }
    for (ssize_t index = 0; index < table_right->num_of_relations ; index++) {
        if (column_right->binding == table_right->relations_bindings[index]){
            index_r = index;
        }
    }

    if (index_l == -1 || index_r == -1 ) std::cerr << "Error in radix : No relation mapping" << '\n';
    column_left->binding = index_l;
    column_right->binding = index_r;

    /***** do the multi-pass partitioning *****/
    #if NUM_PASSES==1
    /* apply radix-clustering on relation R for pass-1 */
    radix_cluster_nopadding(out_row_ids_left, table_left->row_ids, column_left,
                                table_left->size_of_row_ids, 0, NUM_RADIX_BITS);
    free(table_left->row_ids);
    table_left->row_ids = out_row_ids_left;
    uint64_t **row_ids_left = out_row_ids_left;

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding(out_row_ids_right, table_right->row_ids, column_right,
                                table_right->size_of_row_ids, 0, NUM_RADIX_BITS);
    free(table_right->row_ids);
    table_right->row_ids = out_row_ids_right;
    uint64_t **row_ids_right = out_row_ids_right;

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


    #ifdef time
    stopTimer(&timer3);
    #endif


    /* Why re compute */
    int * L_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));
    int * R_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));

    /* compute number of tuples per cluster */
    uint64_t **row_ids = table_left->row_ids;
    unsigned   index   = column_left->binding;
    for( i=0; i < table_left->size_of_row_ids; i++ ) {
        uint32_t idx = (column_left->values[row_ids[i][index]]) & ((1<<NUM_RADIX_BITS)-1);
        L_count_per_cluster[idx] ++;
    }
    row_ids = table_right->row_ids;
    index   = column_right->binding;
    for( i=0; i < table_right->size_of_row_ids; i++ ){
        uint32_t idx = (column_right->values[row_ids[i][index]]) & ((1<<NUM_RADIX_BITS)-1);
        R_count_per_cluster[idx] ++;
    }

    /* build hashtable on inner */
    int l, r; /* start index of next clusters */
    l = r = 0;
    for( i=0; i < (1<<NUM_RADIX_BITS); i++ ){
        uint64_t ** tmp_left;
        uint64_t ** tmp_right;

        if(L_count_per_cluster[i] > 0 && R_count_per_cluster[i] > 0){

            tmp_left =  row_ids_left + l * sizeof(uint64_t*);
            l += L_count_per_cluster[i];

            tmp_right =  row_ids_right + r * sizeof(uint64_t*);
            r += R_count_per_cluster[i];

            result += bucket_chaining_join(tmp_left, column_left, L_count_per_cluster[i],
                                            tmp_right, column_right, R_count_per_cluster[i], NULL);
        }
        else {
            l += L_count_per_cluster[i];
            r += R_count_per_cluster[i];
        }
    }

    #ifdef time
    /* TODO: actually we're not timing build */
    stopTimer(&timer2);/* build finished */
    stopTimer(&timer1);/* probe finished */
    gettimeofday(&end, NULL);
    /* now print the timing results: */
    #endif

    /* clean-up temporary buffers */
    free(L_count_per_cluster);
    free(R_count_per_cluster);

    #if NUM_PASSES == 1
    /* clean up temporary relations */
    //free(outRelR->tuples);
    //free(outRelS->tuples);
    //free(outRelR);
    //free(outRelS);
    #endif

    return NULL;
}
