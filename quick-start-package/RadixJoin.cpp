#include "RadixJoin.hpp"

using namespace std;

#define time
double timeBuildPhase = 0;
double timeProbePhase = 0;
double timePartition  = 0;

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
int64_t bucket_chaining_join(table_t *table_left, const int size_left, uint32_t starting_idx_left,
                             table_t *table_right, const int size_right, uint32_t starting_idx_right,
                             table_t * result_table) {
    uint32_t * next, * bucket;
    const uint32_t numL = size_left;
    uint32_t N = numL;
    uint64_t matches = 0;

    /* The resulting row ids */
    ssize_t  relations_num   = result_table->relations_bindings.size();
    ssize_t  relations_left  = table_left->relations_bindings.size();
    ssize_t  relations_right = table_right->relations_bindings.size();

    matrix & matched_row_ids = *result_table->relations_row_ids;
    matrix & row_ids_left    = *table_left->relations_row_ids;
    matrix & row_ids_right   = *table_right->relations_row_ids;


    NEXT_POW_2(N);

    const uint64_t MASK = (N-1) << (NUM_RADIX_BITS);
    next   = (uint32_t *) malloc(sizeof(uint32_t) * numL);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (uint32_t *) calloc(N, sizeof(uint32_t));

    const uint64_t * Ltuples = table_left->column_j->values + starting_idx_left;
    for(uint32_t i=0; i < numL; ){
        uint64_t idx = HASH_BIT_MODULO(*Ltuples, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */
        Ltuples++;
    }

    const uint64_t * Rtuples = table_right->column_j->values + starting_idx_right;
    const uint32_t   numR  = size_right;
    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numR; i++ ){

        Ltuples =  table_left->column_j->values  + starting_idx_left;
        uint64_t idx = HASH_BIT_MODULO(*Rtuples, MASK, NUM_RADIX_BITS);
        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

            /* We have a match */
            if(Rtuples[i] == Ltuples[hit-1]){

                /* Add the left table's row Ids to the new table  */
                for (uint32_t j = 0 ; j < relations_left; j++)
                    matched_row_ids[j].emplace_back(row_ids_left[j][starting_idx_left + hit-1]);

                /* Add the right table's row Ids to the new table  */
                for (uint32_t j = 0; j < relations_right; j++)
                    matched_row_ids[j + relations_left].emplace_back(row_ids_right[j][starting_idx_right + i]);
            }
        }
    }
    /* PROBE-LOOP END  */

    //std::cerr << "END" << '\n';
    //flush(std::cerr);

    /* clean up temp */
    free(next);
    free(bucket);

    return matches;
}

void radix_cluster_nopadding(matrix * out_rel_ids_ptr, table_t * table, int R, int D) {

    int  relations_num = table->relations_bindings.size();
    int  table_index   = table->column_j->table_index;
    uint64_t * dst;
    uint64_t * input;
    uint64_t * new_values;
    uint64_t * tuples_per_cluster;
    uint32_t   i;
    uint64_t   offset;
    const uint64_t M = ((1 << D) - 1) << R;
    const uint64_t fanOut = 1 << D;
    const uint32_t ntuples = (*table->relations_row_ids)[0].size();

    matrix & row_ids = *table->relations_row_ids;
    matrix & out_rel_ids = *out_rel_ids_ptr;

    tuples_per_cluster = (uint64_t *)calloc(fanOut, sizeof(uint64_t));
    new_values = new uint64_t[ntuples];
    dst        = (uint64_t *) malloc(sizeof(uint64_t *) * fanOut);

    /* count tuples per cluster */
    input = table->column_j->values;
    for(i = 0; i < ntuples; i++){
        uint64_t idx = (uint64_t)(HASH_BIT_MODULO(*input, M, R));
        tuples_per_cluster[idx]++;
        input++;
    }

    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for (i = 0; i < fanOut; i++) {
        dst[i]  = offset;
        offset += tuples_per_cluster[i];
    }

    /* copy tuples to their corresponding clusters at appropriate offsets */
    input = table->column_j->values;
    for( i=0; i < ntuples; i++ ){
        uint64_t idx   = (uint64_t)(HASH_BIT_MODULO(*input, M, R));
        for (size_t rel = 0; rel < relations_num ; rel++) {
            out_rel_ids[rel][dst[idx]] = row_ids[rel][i];
            new_values[dst[idx]]  =  *input;
        }
        ++dst[idx];
        input++;
    }

    /* Check sum the 2 arrays */
    uint64_t sum1 = 0 , sum2=0, sum3=0, sum4=0;
    for (i=0 ; i < ntuples; i++) {
        sum1 += out_rel_ids[0][i];
        sum2 += row_ids[0][i];
        sum3 += table->column_j->values[i];
        sum4 += new_values[i];
    }
    std::cerr << "SUM 1 " << sum1  << " " << out_rel_ids[0][9] << '\n';
    std::cerr << "SUM 2 " << sum2 <<  " " <<  row_ids[0][9]  <<'\n';
    std::cerr << "SUM 3 " << sum3  << " " << out_rel_ids[0][9] << '\n';
    std::cerr << "SUM 4 " << sum4 <<  " " <<  row_ids[0][9]  <<'\n';
    std::cerr << "----------------" << '\n';


    /*TODO DELETE */
    (table->intermediate_res) ? (delete[] table->column_j->values) : ((void)0);
    table->column_j->values = new_values;

    /* clean up temp */
    free(dst);
    free(tuples_per_cluster);
}

table_t* radix_join(table_t *table_left, table_t *table_right) {

    uint32_t i;
    matrix * out_row_ids_left, * out_row_ids_right;
    matrix & in_row_ids_left  = *table_left->relations_row_ids;
    matrix & in_row_ids_right = *table_right->relations_row_ids;

    /* Create 2 new matrixes to use after the pratition phase */
    out_row_ids_left  = new matrix(in_row_ids_left.size(), j_vector(in_row_ids_left[0].size()));
    out_row_ids_right = new matrix(in_row_ids_right.size(), j_vector(in_row_ids_right[0].size()));

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /***** do the multi-pass partitioning *****/

    /* apply radix-clustering on relation R for pass-1 */
    radix_cluster_nopadding(out_row_ids_left, table_left, 0, NUM_RADIX_BITS);
    delete table_left->relations_row_ids;
    table_left->relations_row_ids = out_row_ids_left;

    /* apply radix-clustering on relation S for pass-1 */
    radix_cluster_nopadding(out_row_ids_right, table_right, 0, NUM_RADIX_BITS);
    delete table_right->relations_row_ids;
    table_right->relations_row_ids = out_row_ids_right;

    //std::cerr << "END1" << '\n';
    //flush(std::cerr);

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timePartition += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif
#ifdef time
    gettimeofday(&start, NULL);
#endif

    /* Create 2^NUM_RADIX_BITS seperate batches to compute their join */
    int * L_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));
    int * R_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));

    /* compute number of tuples per cluster */
    unsigned size_l = table_left->column_j->size;
    for( i=0; i < size_l; i++ ) {
        uint64_t idx = (table_left->column_j->values[i]) & ((1<<NUM_RADIX_BITS)-1);
        L_count_per_cluster[idx] ++;
    }

    unsigned size_r =  table_right->column_j->size;;
    for( i=0; i < size_r; i++ ){
        uint64_t idx = (table_right->column_j->values[i]) & ((1<<NUM_RADIX_BITS)-1);
        R_count_per_cluster[idx]++;
    }

    /* Check sum */
    int sum1 = 0, sum2 = 0, poss = 0, sum_poss = 0;
    for (size_t i = 0; i < (1<<NUM_RADIX_BITS); i++) {
        sum1 += L_count_per_cluster[i];
        sum2 += R_count_per_cluster[i];
        if(L_count_per_cluster[i] > 0 && R_count_per_cluster[i] > 0){
            poss += (L_count_per_cluster[i] > R_count_per_cluster[i]) ? L_count_per_cluster[i] : R_count_per_cluster[i];
            sum_poss++;
        }
    }
    std::cerr << "LSum1 is " << sum1 << '\n';
    std::cerr << "RSum2 is " << sum2 << '\n';
    std::cerr << "Possible " << poss << '\n';
    std::cerr << "SUM Possible " << sum_poss << '\n';


    /* Create the result tables */
    table_t * result_table = new table_t;
    uint64_t  allocated_size = (size_l < size_r) ? (uint64_t)(size_l) : (uint64_t)(size_r);
    //std::cerr << "Allocated size " <<  allocated_size << " Size l " << size_l << " Size r " << size_r <<  '\n';

    /* update the bindings and the relations */
    result_table->column_j = new column_t;

    result_table->relations_bindings.insert(result_table->relations_bindings.end(),table_left->relations_bindings.begin(), table_left->relations_bindings.end());
    result_table->relations_bindings.insert(result_table->relations_bindings.end(),table_right->relations_bindings.begin(), table_right->relations_bindings.end());

    result_table->relation_ids.insert(result_table->relation_ids.end(),table_left->relation_ids.begin(), table_left->relation_ids.end());
    result_table->relation_ids.insert(result_table->relation_ids.end(),table_right->relation_ids.begin(), table_right->relation_ids.end());

    /* Create a new rowids table */
    int  relations_num = result_table->relations_bindings.size();
    result_table->relations_row_ids = new matrix(relations_num);
    for (size_t relation = 0; relation < relations_num; relation++) {
        result_table->relations_row_ids->operator[](relation).reserve(allocated_size);
    }
    result_table->intermediate_res = true;

#ifdef time
    gettimeofday(&end, NULL);
    timeBuildPhase += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif
#ifdef time
    gettimeofday(&start, NULL);
#endif

    /* build hashtable on inner */
    uint32_t tmp_left_idx  = 0;
    uint32_t tmp_right_idx = 0;
    uint64_t *v1 = table_left->column_j->values;
    uint64_t *v2 = table_right->column_j->values;
    uint64_t matches, sum = 0;
    for( i=0; i < (1<<NUM_RADIX_BITS); i++ ){

        if(L_count_per_cluster[i] > 0 && R_count_per_cluster[i] > 0){

            /*  bucket_chaining_join(table_left, L_count_per_cluster[i], tmp_left_idx,
                                table_right, R_count_per_cluster[i], tmp_right_idx, result_table);*/


            /* Do a nested loop */
             matches = 0;
             v1 = table_left->column_j->values + tmp_left_idx;
             v2 = table_right->column_j->values + tmp_right_idx;
             for (size_t i = 0; i < L_count_per_cluster[i]; i++) {
                 for (size_t j = 0; j < R_count_per_cluster[i]; j++) {
                     if (v1[i] == v2[j]) {
                         matches++;
                     }
                 }
             }

             if (matches > 0) {
                 sum += matches;
                 std::cerr << "One bach match is " << matches << '\n';
             }

            tmp_left_idx  += L_count_per_cluster[i];
            tmp_right_idx += R_count_per_cluster[i];
        }
        else {
            tmp_left_idx  += L_count_per_cluster[i];
            tmp_right_idx += R_count_per_cluster[i];
        }
    }

    std::cerr << "Sum is " << sum << '\n';

#ifdef time
    gettimeofday(&end, NULL);
    timeProbePhase += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    /* clean-up temporary buffers */
    free(L_count_per_cluster);
    free(R_count_per_cluster);


    return result_table;
}
