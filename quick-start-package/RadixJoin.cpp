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
int64_t bucket_chaining_join(uint64_t ** L, const column_t * column_left, const int size_left, int rel_num_left,
                             uint64_t ** R, const column_t * column_right, const int size_right, int rel_num_right,
                             table_t * result_table) {
    int * next, * bucket;
    const uint32_t numL = size_left;
    uint32_t N = numL;
    uint64_t matches = 0;

    /* The resulting row ids */
    ssize_t     size = result_table->allocated_size;
    ssize_t     vector_size = result_table->num_of_relations;
    uint64_t ** matched_row_ids = result_table->row_ids;

    NEXT_POW_2(N);
    /* N <<= 1; */
    const uint64_t MASK = (N-1) << (NUM_RADIX_BITS);

    next   = (int*) malloc(sizeof(int) * numL);
    /* posix_memalign((void**)&next, CACHE_LINE_SIZE, numR * sizeof(int)); */
    bucket = (int*) calloc(N, sizeof(int));

    const uint64_t * Ltuples;
    uint32_t         index_l = column_left->binding;
    for(uint32_t i=0; i < numL; ){
        Ltuples = column_left->values + L[i][index_l];
        uint32_t idx = HASH_BIT_MODULO(*Ltuples, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    uint64_t         new_rids_index = result_table->size_of_row_ids;
    const uint64_t * Rtuples;
    const uint32_t   numR  = size_right;
    uint32_t        index_r = column_right->binding;
    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    //std::cerr<<"----------->WILL START THE REAL BROBINGGGGGGGGGGGGGGGGGG\n";
    for(uint32_t i=0; i < numR; i++ ){
        Rtuples = column_right->values + R[i][index_r];
        uint32_t idx = HASH_BIT_MODULO(*Rtuples, MASK, NUM_RADIX_BITS);
        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){
            
            Ltuples = column_left->values + L[hit-1][index_l];

            /* We have a match */
            if(*Rtuples == *Ltuples){

                /* Reallocate in case of emergency */
                if (new_rids_index == size) {
                    size = size * 2;
                    uint64_t** new_pointer = (uint64_t **) realloc(matched_row_ids, sizeof(uint64_t*) * size);

                    if (new_pointer == NULL) std::cerr << "Error in realloc " << '\n';
                    matched_row_ids = new_pointer;

                    result_table->allocated_size = size;
                }
                //std::cerr<<"@@@@@@@@@@@@@@@@@@@@WILL START THE LOOP TO GET SEG\n";
                
                /* Create new uint64_t array */
                //std::cerr<<"current size->"<<size<<"new_rids_index->"<<new_rids_index<<"\n";
                matched_row_ids[new_rids_index] = (uint64_t *) malloc(sizeof(uint64_t *) * vector_size);
                
                //std::cerr<<"nooooooooo SEG in the loopppp@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n";f
                //std::cerr<<"########BRO WILL GET THE SEGGGGGGGGGGGGGGGGGGGGGG\n";
                /* Copy the vectors to the new ones */
                memcpy(&matched_row_ids[new_rids_index][0], &L[hit-1][0], sizeof(uint64_t) * rel_num_left);
                memcpy(&matched_row_ids[new_rids_index][rel_num_left], &R[i][0], sizeof(uint64_t) * rel_num_right);
                //std::cerr<<"NOOOOOOOOOOOOOOOOOOOOOOOOO SEGGGGGGGGGGGGGGGGGGGGGG########\n";
                new_rids_index++;
            }
        }
    }
    //std::cerr<<"WILL END THE REAL BROBINGGGGGGGGGGGGGGGGGG<-----------------\n";
    /* PROBE-LOOP END  */

    /* Update the size of the array */
    result_table->size_of_row_ids = new_rids_index;
    result_table->row_ids = matched_row_ids;
    //std::cerr << "END" << '\n';
    //flush(std::cerr);

    /* clean up temp */
    free(next);
    free(bucket);

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

    /* count tuples per cluster */
    for(i = 0; i < ntuples; i++){

        input = column->values + in_rel_ids[i][column->binding];
        uint64_t idx = (uint64_t)(HASH_BIT_MODULO(*input, M, R));
        tuples_per_cluster[idx]++;
    }

    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for (i = 0; i < fanOut; i++) {
        dst[i]  = out_rel_ids + offset;
        offset += tuples_per_cluster[i];
    }

    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < ntuples; i++ ){
        input = column->values + in_rel_ids[i][column->binding];
        uint64_t idx   = (uint64_t)(HASH_BIT_MODULO(*input, M, R));
        *dst[idx] = in_rel_ids[i];
        ++dst[idx];
    }

    /* clean up temp */
    free(dst);
    free(tuples_per_cluster);
}

table_t* radix_join(table_t *table_left, column_t *column_left, table_t *table_right, column_t *column_right) {
    std::cerr<<"RADIX STARTS*******************\n";
    uint64_t result = 0;
    uint32_t i;
    uint64_t **out_row_ids_left, **out_row_ids_right;



    out_row_ids_left  = (uint64_t **) malloc(sizeof(uint64_t **) * table_left->size_of_row_ids);
    out_row_ids_right = (uint64_t **) malloc(sizeof(uint64_t **) * table_right->size_of_row_ids);

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Fix the columns */
    //std::cerr << "Left binding before " << column_left->binding << '\n';
    //std::cerr << "Right binding before " << column_right->binding << '\n';
    int index_l = -1;
    int index_r = -1;
    for (ssize_t index = 0; index < table_left->num_of_relations ; index++) {
        if (column_left->binding == table_left->relations_bindings[index]){
            index_l = index;
            break;
        }
    }
    for (ssize_t index = 0; index < table_right->num_of_relations ; index++) {
        if (column_right->binding == table_right->relations_bindings[index]){
            index_r = index;
            break;
        }
    }

    if (index_l == -1 || index_r == -1 ) std::cerr << "Error in radix : No relation mapping" << '\n';
    column_left->binding = index_l;
    column_right->binding = index_r;

    /***** do the multi-pass partitioning *****/

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

    std::cerr << "END1" << '\n';
    flush(std::cerr);

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timePartition += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

#ifdef time
    gettimeofday(&start, NULL);
#endif

    /* Why re compute */
    int * L_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));
    int * R_count_per_cluster = (int*)calloc((1<<NUM_RADIX_BITS), sizeof(int));

    /* compute number of tuples per cluster */
    uint64_t **row_ids = table_left->row_ids;
    unsigned   index   = column_left->binding;

    for( i=0; i < table_left->size_of_row_ids; i++ ) {
        uint64_t idx = (column_left->values[row_ids[i][index]]) & ((1<<NUM_RADIX_BITS)-1);
        L_count_per_cluster[idx] ++;
    }

    row_ids = table_right->row_ids;
    index   = column_right->binding;

    for( i=0; i < table_right->size_of_row_ids; i++ ){
        uint64_t idx = (column_right->values[row_ids[i][index]]) & ((1<<NUM_RADIX_BITS)-1);
        R_count_per_cluster[idx] ++;
    }

    std::cerr << "END2" << '\n';
    flush(std::cerr);

    /* Create the result tables */
    table_t * result_table = new table_t;
    result_table->allocated_size  = (table_left->size_of_row_ids < table_right->size_of_row_ids) ?
                                            (table_left->size_of_row_ids * table_left->size_of_row_ids)   :
                                            (table_right->size_of_row_ids * table_right->size_of_row_ids) ;
    result_table->num_of_relations = table_left->num_of_relations + table_right->num_of_relations;
    result_table->size_of_row_ids  = 0;
    result_table->row_ids          = (uint64_t **) malloc(sizeof(uint64_t **) * result_table->allocated_size);


    /* upload the bindings and the relations */
    result_table->relations_bindings.insert(result_table->relations_bindings.end(),table_left->relations_bindings.begin(), table_left->relations_bindings.end());
    result_table->relations_bindings.insert(result_table->relations_bindings.end(),table_right->relations_bindings.begin(), table_right->relations_bindings.end());
    result_table->relation_ids.insert(result_table->relation_ids.end(),table_left->relation_ids.begin(), table_left->relation_ids.end());
    result_table->relation_ids.insert(result_table->relation_ids.end(),table_right->relation_ids.begin(), table_right->relation_ids.end());


#ifdef time
    gettimeofday(&end, NULL);
    timeBuildPhase += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

#ifdef time
    gettimeofday(&start, NULL);
#endif
    std::cerr<<"WILL START BUCKET CHAING HERE<----------------\n\n\n\n\n";
    /* build hashtable on inner */
    int l, r; /* start index of next clusters */
    l = r = 0;
    for( i=0; i < (1<<NUM_RADIX_BITS); i++ ){
        uint64_t ** tmp_left;
        uint64_t ** tmp_right;

        if(L_count_per_cluster[i] > 0 && R_count_per_cluster[i] > 0){

            tmp_left =  out_row_ids_left + l;
            l += L_count_per_cluster[i];

            tmp_right =  out_row_ids_right + r;
            r += R_count_per_cluster[i];
            //std::cerr<<"------------->the real chaining starqt here \n";
            bucket_chaining_join(tmp_left, column_left, L_count_per_cluster[i], table_left->num_of_relations,
                                            tmp_right, column_right, R_count_per_cluster[i], table_right->num_of_relations, result_table);
            //std::cerr<<"the real bucket chaining will exit here<--------------------\n";
        }
        else {
            l += L_count_per_cluster[i];
            r += R_count_per_cluster[i];
        }
    }

    std::cerr << "END3" << '\n';
    flush(std::cerr);

#ifdef time
    gettimeofday(&end, NULL);
    timeProbePhase += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif
    //std::cerr << "Check sum (left)" << result << '\n';

    /* clean-up temporary buffers */
    free(L_count_per_cluster);
    free(R_count_per_cluster);
    

    std::cerr <<"RADIX JOIN WILL END\n";
    return result_table;
}
