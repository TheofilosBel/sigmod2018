/**
 * @file    parallel_radix_join.c
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Sun Feb 20:19:51 2012
 * @version $Id: parallel_radix_join.c 4548 2013-12-07 16:05:16Z bcagri $
 *
 * @brief  Provides implementations for several variants of Radix Hash Join.
 *
 * (c) 2012, ETH Zurich, Systems Group
 *
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sched.h>              /* CPU_ZERO, CPU_SET */
#include <pthread.h>            /* pthread_* */
#include <stdlib.h>             /* malloc, posix_memalign */
#include <sys/time.h>           /* gettimeofday */
#include <stdio.h>              /* printf */

#include "parallel_radix_join.h"
#include "prj_params.h"         /* constant parameters */
#include "task_queue_64.h"         /* task_queue_* */
#include "barrier.h"            /* pthread_barrier_* */
#include "tuple_buffer_64.h"       /* for materialization */

/** \internal */
#ifndef BARRIER_ARRIVE
/** barrier wait macro */
#define BARRIER_ARRIVE(B,RV)                            \
    RV = pthread_barrier_wait(B);                       \
    if(RV !=0 && RV != PTHREAD_BARRIER_SERIAL_THREAD){  \
        printf("Couldn't wait on barrier\n");           \
        exit(EXIT_FAILURE);                             \
    }
#endif

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


/* just to enable compilation with g++ */
#if defined(__cplusplus)
#define restrict __restrict__
#endif

/** An experimental feature to allocate input relations numa-local */
extern int numalocalize;  /* defined in generator.c */

typedef struct arg64_t  arg64_t;
typedef struct part64_t part64_t;
typedef struct synctimer_t synctimer_t;
typedef int64_t (*JoinFunction)(const relation64_t * const,
                                const relation64_t * const,
                                relation64_t * const,
                                void * output);
/** holds the arguments passed to each thread */
struct arg64_t {
    int32_t ** histR;
    tuple64_t *  relR;
    tuple64_t *  tmpR;
    int32_t ** histS;
    tuple64_t *  relS;
    tuple64_t *  tmpS;

    int32_t numR;
    int32_t numS;
    int64_t totalR;
    int64_t totalS;

    task_queue64_t **      join_queue;
    task_queue64_t **      part_queue;

    pthread_barrier_t * barrier;
    JoinFunction        join_function;
    int64_t result;
    int32_t my_tid;
    int     nthreads;

    // # JIM/GEORGE
    //ADDITIONAL CACHED INFO HERE
    cached_t *outS;
    cached_t *outR;

    /* results of the thread */
    threadresult_t * threadresult;

    /* stats about the thread */
    int32_t        parts_processed;
    uint64_t       timer1, timer2, timer3;
    struct timeval start, end;
} __attribute__((aligned(CACHE_LINE_SIZE)));

/** holds arguments passed for partitioning */
struct part64_t {
    tuple64_t *  rel;
    tuple64_t *  tmp;
    int32_t ** hist;
    int64_t *  output;
    arg64_t   *  thrargs;
    uint64_t   total_tuples;
    uint32_t   num_tuples;
    int32_t    R;
    uint32_t   D;
    int        relidx;  /* 0: R, 1: S */
    uint32_t   padding;
} __attribute__((aligned(CACHE_LINE_SIZE)));

static void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size);

    if (rv) {
        perror("alloc_aligned() failed: out of memory");
        return 0;
    }

    return ret;
}

/** \endinternal */

/**
 * @defgroup Radix Radix Join Implementation Variants
 * @{
 */

/**
 *  This algorithm builds the hashtable using the bucket chaining idea and used
 *  in PRO implementation. Join between given two relations is evaluated using
 *  the "bucket chaining" algorithm proposed by Manegold et al. It is used after
 *  the partitioning phase, which is common for all algorithms. Moreover, R and
 *  S typically fit into L2 or at least R and |R|*sizeof(int) fits into L2 cache.
 *
 * @param R input relation R
 * @param S input relation S
 * @param output join results, if JOIN_RESULT_MATERIALIZE defined.
 *
 * @return number of result tuples
 */
int64_t
bucket_chaining_join_t64(const relation64_t * const R,
                     const relation64_t * const S,
                     relation64_t * const tmpR,
                     void * output)
{
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

    const tuple64_t * const Rtuples = R->tuples;
    for(uint32_t i=0; i < numR; ){
        uint32_t idx = HASH_BIT_MODULO(R->tuples[i].key, MASK, NUM_RADIX_BITS);
        next[i]      = bucket[idx];
        bucket[idx]  = ++i;     /* we start pos's from 1 instead of 0 */

        /* Enable the following tO avoid the code elimination
           when running probe only for the time break-down experiment */
        /* matches += idx; */
    }

    const tuple64_t * const Stuples = S->tuples;
    const uint32_t        numS    = S->num_tuples;


    chainedtuplebuffer64_t * chainedbuf = (chainedtuplebuffer64_t *) output;


    /* Disable the following loop for no-probe for the break-down experiments */
    /* PROBE- LOOP */
    for(uint32_t i=0; i < numS; i++ ){

        uint32_t idx = HASH_BIT_MODULO(Stuples[i].key, MASK, NUM_RADIX_BITS);

        for(int hit = bucket[idx]; hit > 0; hit = next[hit-1]){

            if(Stuples[i].key == Rtuples[hit-1].key){

                /* copy to the result buffer, we skip it */
                tuple64_t * joinres = cb_next_writepos_t64(chainedbuf);
                joinres->key      = Rtuples[hit-1].payload; /* R-rid */
                joinres->payload  = Stuples[i].payload;     /* S-rid */
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

/**
 * Radix clustering algorithm (originally described by Manegold et al)
 * The algorithm mimics the 2-pass radix clustering algorithm from
 * Kim et al. The difference is that it does not compute
 * prefix-sum, instead the sum (offset in the code) is computed iteratively.
 *
 * @warning This method puts padding between clusters, see
 * radix_cluster_nopadding for the one without padding.
 *
 * @param outRel [out] result of the partitioning
 * @param inRel [in] input relation
 * @param hist [out] number of tuples in each partition
 * @param R cluster bits
 * @param D radix bits per pass
 * @returns tuples per partition.
 */
void
radix_cluster_t64(relation64_t * restrict outRel,
              relation64_t * restrict inRel,
              int32_t * restrict hist,
              int R,
              int D)
{
    uint32_t i;
    uint32_t M = ((1 << D) - 1) << R;
    uint32_t offset;
    uint32_t fanOut = 1 << D;

    /* the following are fixed size when D is same for all the passes,
       and can be re-used from call to call. Allocating in this function
       just in case D differs from call to call. */
    uint32_t dst[fanOut];

    /* count tuples per cluster */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint32_t idx = HASH_BIT_MODULO(inRel->tuples[i].key, M, R);
        hist[idx]++;
    }
    offset = 0;
    /* determine the start and end of each cluster depending on the counts. */
    for ( i=0; i < fanOut; i++ ) {
        /* dst[i]      = outRel->tuples + offset; */
        /* determine the beginning of each partitioning by adding some
           padding to avoid L1 conflict misses during scatter. */
        dst[i] = offset + i * SMALL_PADDING_TUPLES_64;
        offset += hist[i];
    }

    /* copy tuples to their corresponding clusters at appropriate offsets */
    for( i=0; i < inRel->num_tuples; i++ ){
        uint32_t idx   = HASH_BIT_MODULO(inRel->tuples[i].key, M, R);
        outRel->tuples[ dst[idx] ] = inRel->tuples[i];
        ++dst[idx];
    }
}

/**
 * This function implements the radix clustering of a given input
 * relations. The relations to be clustered are defined in task64_t and after
 * clustering, each partition pair is added to the join_queue to be joined.
 *
 * @param task description of the relation to be partitioned
 * @param join_queue task queue to add join tasks after clustering
 */
void serial_radix_partition_t64(task64_t * const task,
                            task_queue64_t * join_queue,
                            const int R, const int D)
{
    int i;
    uint32_t offsetR = 0, offsetS = 0;
    const int fanOut = 1 << D;  /*(NUM_RADIX_BITS / NUM_PASSES);*/
    int32_t * outputR, * outputS;

    outputR = (int32_t*)calloc(fanOut+1, sizeof(int32_t));
    outputS = (int32_t*)calloc(fanOut+1, sizeof(int32_t));
    /* TODO: measure the effect of memset() */
    /* memset(outputR, 0, fanOut * sizeof(int32_t)); */
    radix_cluster_t64(&task->tmpR, &task->relR, outputR, R, D);

    /* memset(outputS, 0, fanOut * sizeof(int32_t)); */
    radix_cluster_t64(&task->tmpS, &task->relS, outputS, R, D);

    /* task64_t t; */
    for(i = 0; i < fanOut; i++) {
        if(outputR[i] > 0 && outputS[i] > 0) {
            task64_t * t = task_queue_get_slot_atomic_t64(join_queue);
            t->relR.num_tuples = outputR[i];
            t->relR.tuples = task->tmpR.tuples + offsetR
                             + i * SMALL_PADDING_TUPLES_64;
            t->tmpR.tuples = task->relR.tuples + offsetR
                             + i * SMALL_PADDING_TUPLES_64;
            offsetR += outputR[i];

            t->relS.num_tuples = outputS[i];
            t->relS.tuples = task->tmpS.tuples + offsetS
                             + i * SMALL_PADDING_TUPLES_64;
            t->tmpS.tuples = task->relS.tuples + offsetS
                             + i * SMALL_PADDING_TUPLES_64;
            offsetS += outputS[i];

            /* task_queue_copy_atomic(join_queue, &t); */
            task_queue_add_atomic_t64(join_queue, t);
        }
        else {
            offsetR += outputR[i];
            offsetS += outputS[i];
        }
    }
    free(outputR);
    free(outputS);
}

/**
 * This function implements the parallel radix partitioning of a given input
 * relation. Parallel partitioning is done by histogram-based relation
 * re-ordering as described by Kim et al. Parallel partitioning method is
 * commonly used by all parallel radix join algorithms.
 *
 * @param part description of the relation to be partitioned
 */
void
parallel_radix_partition_t64(part64_t * const part)
{
    const tuple64_t * restrict rel    = part->rel;
    int32_t **               hist   = part->hist;
    int64_t *       restrict output = part->output;

    const uint32_t my_tid     = part->thrargs->my_tid;
    const uint32_t nthreads   = part->thrargs->nthreads;
    const uint32_t num_tuples = part->num_tuples;

    const int32_t  R       = part->R;
    const int32_t  D       = part->D;
    const uint32_t fanOut  = 1 << D;
    const uint32_t MASK    = (fanOut - 1) << R;
    const uint32_t padding = part->padding;

    int64_t sum = 0;
    uint32_t i, j;
    int rv;

    int64_t dst[fanOut+1];

    /* compute local histogram for the assigned region of rel */
    /* compute histogram */
    int32_t * my_hist = hist[my_tid];

    for(i = 0; i < num_tuples; i++) {
        uint32_t idx = HASH_BIT_MODULO(rel[i].key, MASK, R);
        my_hist[idx] ++;
    }

    /* compute local prefix sum on hist */
    for(i = 0; i < fanOut; i++){
        sum += my_hist[i];
        my_hist[i] = sum;
    }

    /* wait at a barrier until each thread complete histograms */
    BARRIER_ARRIVE(part->thrargs->barrier, rv);

    /* determine the start and end of each cluster */
    for(i = 0; i < my_tid; i++) {
        for(j = 0; j < fanOut; j++)
            output[j] += hist[i][j];
    }
    for(i = my_tid; i < nthreads; i++) {
        for(j = 1; j < fanOut; j++)
            output[j] += hist[i][j-1];
    }

    for(i = 0; i < fanOut; i++ ) {
        output[i] += i * padding; //PADDING_TUPLES_64;
        dst[i] = output[i];
    }
    output[fanOut] = part->total_tuples + fanOut * padding; //PADDING_TUPLES_64;

    tuple64_t * restrict tmp = part->tmp;

    /* Copy tuples to their corresponding clusters */
    for(i = 0; i < num_tuples; i++ ){
        uint32_t idx = HASH_BIT_MODULO(rel[i].key, MASK, R);
        tmp[dst[idx]] = rel[i];
        ++dst[idx];
    }
}

/**
 * @defgroup SoftwareManagedBuffer Optimized Partitioning Using SW-buffers
 * @{
 */
typedef union {
    struct {
        tuple64_t tuples[CACHE_LINE_SIZE/sizeof(tuple64_t)];
    } tuples;
    struct {
        tuple64_t tuples[CACHE_LINE_SIZE/sizeof(tuple64_t) - 1];
        int64_t slot;
    } data;
} cacheline_t;

#define TUPLESPERCACHELINE_64 (CACHE_LINE_SIZE/sizeof(tuple64_t))

/** @} */
/**
 * The main thread of parallel radix join. It does partitioning in parallel with
 * other threads and during the join phase, picks up join tasks from the task
 * queue and calls appropriate JoinFunction to compute the join task.
 *
 * @param param
 *
 * @return
 */
void *
prj_thread_t64(void * param)
{
    arg64_t * args   = (arg64_t*) param;
    int32_t my_tid = args->my_tid;

    const int fanOut = 1 << (NUM_RADIX_BITS / NUM_PASSES);//PASS1RADIXBITS;
    const int R = (NUM_RADIX_BITS / NUM_PASSES);//PASS1RADIXBITS;
    const int D = (NUM_RADIX_BITS - (NUM_RADIX_BITS / NUM_PASSES));//PASS2RADIXBITS;

    uint64_t results = 0;
    int i;
    int rv;

    part64_t part;
    task64_t * task;
    task_queue64_t * part_queue;
    task_queue64_t * join_queue;

    int64_t * outputR = (int64_t *) calloc((fanOut+1), sizeof(int64_t));
    int64_t * outputS = (int64_t *) calloc((fanOut+1), sizeof(int64_t));
    //MALLOC_CHECK((outputR && outputS));

    int numaid = 0; //get_numa_id(my_tid);   # GEO / TEO
    //int numaid = get_numa_region_id(my_tid);
    part_queue = args->part_queue[numaid];
    join_queue = args->join_queue[numaid];

    args->histR[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));
    args->histS[my_tid] = (int32_t *) calloc(fanOut, sizeof(int32_t));

    /* in the first pass, partitioning is done together by all threads */

    args->parts_processed = 0;

    /* wait at a barrier until each thread starts and then start the timer */
    BARRIER_ARRIVE(args->barrier, rv);

    /********** 1st pass of multi-pass partitioning ************/
    part.R       = 0;
    part.D       = NUM_RADIX_BITS / NUM_PASSES; //PASS1RADIXBITS
    part.thrargs = args;
    part.padding = PADDING_TUPLES_64;

    /* 1. partitioning for relation R */
    // # JIM/GEORGE

    if (args->outR != NULL) {
        if (args->outR->tmp == NULL) {
            part.rel = args->relR;
            part.tmp          = args->tmpR;
            part.hist         = args->histR;        //maybe delete
            part.output       = outputR;
            part.num_tuples   = args->numR;
            part.total_tuples = args->totalR;
            part.relidx       = 0;

            parallel_radix_partition_t64(&part);

            args->outR->tmp = (void *) args->tmpR;
            args->outR->hist = args->histR;
            args->outR->output = (my_tid == 0) ? outputR : NULL;
            args->outR->num_tuples = args->numR;
            args->outR->total_tuples = args->totalR;
        }
        else {
            args->tmpR        = (tuple64_t *) args->outR->tmp;
            args->histR       = args->outR->hist;
            outputR           = args->outR->output;
            args->numR        = args->outR->num_tuples;
            args->totalR      = args->outR->total_tuples;

        }
    } else {
            part.rel = args->relR;
            part.tmp          = args->tmpR;
            part.hist         = args->histR;        //maybe delete
            part.output       = outputR;
            part.num_tuples   = args->numR;
            part.total_tuples = args->totalR;
            part.relidx       = 0;

            parallel_radix_partition_t64(&part);
    }


    /* 2. partitioning for relation S */
     // # JIM/GEORGE

    if (args->outS != NULL) {
        if (args->outS->tmp == NULL) {
            part.rel = args->relS;
            part.tmp          = args->tmpS;
            part.hist         = args->histS;       //maybe delete
            part.output       = outputS;
            part.num_tuples   = args->numS;
            part.total_tuples = args->totalS;
            part.relidx       = 1;

            parallel_radix_partition_t64(&part);

            args->outS->tmp = (void *) args->tmpS;
            args->outS->hist = args->histS;
            args->outS->output = (my_tid == 0) ? outputS : NULL;
            args->outS->num_tuples = args->numS;
            args->outS->total_tuples = args->totalS;
        }
        else {
            args->tmpS        = (tuple64_t *) args->outS->tmp;
            args->histS       = args->outS->hist;
            outputS           = args->outS->output;
            args->numS        = args->outS->num_tuples;
            args->totalS      = args->outS->total_tuples;
        }
    } else {
        part.rel = args->relS;
        part.tmp          = args->tmpS;
        part.hist         = args->histS;       //maybe delete
        part.output       = outputS;
        part.num_tuples   = args->numS;
        part.total_tuples = args->totalS;
        part.relidx       = 1;

        parallel_radix_partition_t64(&part);
    }


    /* wait at a barrier until each thread copies out */
    BARRIER_ARRIVE(args->barrier, rv);

    /********** end of 1st partitioning phase ******************/
    /* 3. first thread creates partitioning tasks for 2nd pass */
    if(my_tid == 0) {
        /* For Debugging: */
        /* int numnuma = get_num_numa_regions(); */
        /* int correct_numa_mapping = 0, wrong_numa_mapping = 0; */
        /* int counts[4] = {0, 0, 0, 0}; */
        for(i = 0; i < fanOut; i++) {
            int32_t ntupR = outputR[i+1] - outputR[i] - PADDING_TUPLES_64;
            int32_t ntupS = outputS[i+1] - outputS[i] - PADDING_TUPLES_64;

            if(ntupR > 0 && ntupS > 0) {


                /* Determine the NUMA node of each partition: */
                //void * ptr = (void*)&((args->tmpR + outputR[i])[0]);

                /* TODO:when we do the 2cpu NUMA change it */
                //int pq_idx = get_numa_node_of_address(ptr);
                int pq_idx = 0;
                /* For Debugging: */
                /* void * ptr2 = (void*)&((args->tmpS + outputS[i])[0]); */
                /* int pq_idx2 = get_numa_node_of_address(ptr2); */
                /* if(pq_idx != pq_idx2) */
                /*     wrong_numa_mapping ++; */
                /* else */
                /*     correct_numa_mapping ++; */
                /* int numanode_of_mem = get_numa_node_of_address(ptr); */
                /* int pq_idx = i / (fanOut / numnuma); */
                /* if(numanode_of_mem == pq_idx) */
                /*     correct_numa_mapping ++; */
                /* else */
                /*     wrong_numa_mapping ++; */
                /* pq_idx = numanode_of_mem; */
                /* counts[numanode_of_mem] ++; */
                /* counts[pq_idx] ++; */

                task_queue64_t * numalocal_part_queue = args->part_queue[pq_idx];

                task64_t * t = task_queue_get_slot_t64(numalocal_part_queue);

                t->relR.num_tuples = t->tmpR.num_tuples = ntupR;
                t->relR.tuples = args->tmpR + outputR[i];
                t->tmpR.tuples = args->relR + outputR[i];

                t->relS.num_tuples = t->tmpS.num_tuples = ntupS;
                t->relS.tuples = args->tmpS + outputS[i];
                t->tmpS.tuples = args->relS + outputS[i];


                task_queue_add_t64(numalocal_part_queue, t);

            }
        }

        /* DEBUG NUMA MAPPINGS */
        /* printf("Correct NUMA-mappings = %d, Wrong = %d\n", */
        /*        correct_numa_mapping, wrong_numa_mapping); */
        /* printf("Counts -- 0=%d, 1=%d, 2=%d, 3=%d\n",  */
        /*        counts[0], counts[1], counts[2], counts[3]); */
    }

    /* wait at a barrier until first thread adds all partitioning tasks */
    BARRIER_ARRIVE(args->barrier, rv);

    /************ 2nd pass of multi-pass partitioning ********************/
    /* 4. now each thread further partitions and add to join task queue **/

#if NUM_PASSES==1
    /* If the partitioning is single pass we directly add tasks from pass-1 */
    //fprintf(stderr, "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
    task_queue64_t * swap = join_queue;
    join_queue = part_queue;
    /* part_queue is used as a temporary queue for handling skewed parts */
    part_queue = swap;

#elif NUM_PASSES==2

    while((task = task_queue_get_atomic(part_queue))){

        serial_radix_partition_t64(task, join_queue, R, D);

    }

#else
#warning Only 2-pass partitioning is implemented, set NUM_PASSES to 2!
#endif

    // # JIM/GEORGE
    if (args->outR == NULL)
        free(outputR);
    if (args->outS == NULL)
        free(outputS);

    //SYNC_TIMER_STOP(&args->localtimer.sync4);
    /* wait at a barrier until all threads add all join tasks */
    BARRIER_ARRIVE(args->barrier, rv);
    /* global barrier sync point-4 */
    //SYNC_GLOBAL_STOP(&args->globaltimer->sync4, my_tid);

    chainedtuplebuffer64_t * chainedbuf = chainedtuplebuffer_init_t64();

    while((task = task_queue_get_atomic_t64(join_queue))){
        /* do the actual join. join method differs for different algorithms,
           i.e. bucket chaining, histogram-based, histogram-based with simd &
           prefetching  */
        results += args->join_function(&task->relR, &task->relS, &task->tmpR, chainedbuf);

        args->parts_processed ++;
    }

    args->result = results;
    args->threadresult->nresults = results;
    args->threadresult->threadid = my_tid;
    args->threadresult->results  = (void *) chainedbuf;

    return 0;
}

// # JIM/GEORGE


// # TEO / ORESTIS
class RadixJob_t64 : public Job {
public:
    void * arg_;
    RadixJob_t64(void * arg) :arg_(arg) {}

    ~RadixJob_t64() {}
    int Run() {
        // Run the thread function
        prj_thread_t64(arg_);
    }
};


/**
 * The template function for different joins: Basically each parallel radix join
 * has a initialization step, partitioning step and build-probe steps. All our
 * parallel radix implementations have exactly the same initialization and
 * partitioning steps. Difference is only in the build-probe step. Here are all
 * the parallel radix join implemetations and their Join (build-probe) functions:
 *
 * - PRO,  Parallel Radix Join Optimized --> bucket_chaining_join_t64()
 */
result_t *
join_init_run_t64(relation64_t * relR, relation64_t * relS, JoinFunction jf, int nthreads, struct Cacheinf& cinf, JobScheduler& js)
{
    int i, rv;
    pthread_t tid[nthreads];
    pthread_attr_t attr;
    pthread_barrier_t barrier;
    cpu_set_t set;
    arg64_t args[nthreads];

    int32_t ** histR, ** histS;
    tuple64_t * tmpRelR, * tmpRelS;
    int32_t numperthr[2];
    int64_t result = 0;

    /* task_queue64_t * part_queue, * join_queue; */
    int numnuma = 1; // get_num_numa_regions();  # GEO / TEO
    task_queue64_t * part_queue[numnuma];
    task_queue64_t * join_queue[numnuma];

    for(i = 0; i < numnuma; i++){
        part_queue[i] = task_queue_init_t64(FANOUT_PASS1);
        join_queue[i] = task_queue_init_t64((1<<NUM_RADIX_BITS));
    }

    result_t * joinresult = 0;
    joinresult = (result_t *) malloc(sizeof(result_t));
    joinresult->resultlist = (threadresult_t *) malloc(sizeof(threadresult_t)
                                                       * nthreads);

    /* allocate temporary space for partitioning */
    tmpRelR = (tuple64_t*) alloc_aligned(relR->num_tuples * sizeof(tuple64_t) +
                                       RELATION_PADDING_64);
    tmpRelS = (tuple64_t*) alloc_aligned(relS->num_tuples * sizeof(tuple64_t) +
                                       RELATION_PADDING_64);
    //MALLOC_CHECK((tmpRelR && tmpRelS));
    /** Not an elegant way of passing whether we will numa-localize, but this
        feature is experimental anyway. */
    /*if(numalocalize) {
        uint64_t numwithpad;

        numwithpad = (relR->num_tuples * sizeof(tuple64_t) +
                      RELATION_PADDING_64)/sizeof(tuple64_t);
        numa_localize(tmpRelR, numwithpad, nthreads);

        numwithpad = (relS->num_tuples * sizeof(tuple64_t) +
                      RELATION_PADDING_64)/sizeof(tuple64_t);
        numa_localize(tmpRelS, numwithpad, nthreads);
    }*/


    /* allocate histograms arrays, actual allocation is local to threads */
    histR = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
    histS = (int32_t**) alloc_aligned(nthreads * sizeof(int32_t*));
    //MALLOC_CHECK((histR && histS));

    rv = pthread_barrier_init(&barrier, NULL, nthreads);
    if(rv != 0){
        printf("[ERROR] Couldn't create the barrier\n");
        exit(EXIT_FAILURE);
    }

    pthread_attr_init(&attr);

    /* first assign chunks of relR & relS for each thread */
    numperthr[0] = relR->num_tuples / nthreads;
    numperthr[1] = relS->num_tuples / nthreads;
    for(i = 0; i < nthreads; i++){

        args[i].relR = relR->tuples + i * numperthr[0];
        args[i].tmpR = tmpRelR;
        args[i].histR = histR;

        args[i].relS = relS->tuples + i * numperthr[1];
        args[i].tmpS = tmpRelS;
        args[i].histS = histS;

        args[i].numR = (i == (nthreads-1)) ?
            (relR->num_tuples - i * numperthr[0]) : numperthr[0];
        args[i].numS = (i == (nthreads-1)) ?
            (relS->num_tuples - i * numperthr[1]) : numperthr[1];
        args[i].totalR = relR->num_tuples;
        args[i].totalS = relS->num_tuples;

        args[i].my_tid = i;
        args[i].part_queue = part_queue;
        args[i].join_queue = join_queue;

        args[i].barrier       = &barrier;
        args[i].join_function = jf;
        args[i].nthreads      = nthreads;
        args[i].threadresult  = &(joinresult->resultlist[i]);

        // # JIM/GEORGE
        if (cinf.R == NULL)
            args[i].outR = NULL;
        else
            args[i].outR = &(cinf.R[i]);

        if (cinf.S == NULL)
            args[i].outS = NULL;
        else
            args[i].outS = &(cinf.S[i]);

        // # TEO/ORESTIS
        js.Schedule(new RadixJob_t64((void*)&args[i]));
        // rv = pthread_create(&tid[i], &attr, prj_thread_t64, (void*)&args[i]);
        // if (rv){
        //     fprintf(stderr,"[ERROR] return code from pthread_create() is %d\n", rv);
        //     exit(-1);
        // }
    }

    // # TEO/ORESTIS
    js.Barrier();

    /* wait for threads to finish */
    for(i = 0; i < nthreads; i++){
        //pthread_join(tid[i], NULL);
        result += args[i].result;
    }
    joinresult->totalresults = result;
    joinresult->nthreads     = nthreads;

    // # JIM/GEORGE
    /* clean up */
    if (cinf.R == NULL)
        for(i = 0; i < nthreads; i++) {
            free(histR[i]);
        }
    if (cinf.S == NULL)
        for(i = 0; i < nthreads; i++) {
            free(histS[i]);
        }

    free(histR);
    free(histS);


    for(i = 0; i < numnuma; i++){
        task_queue_free_t64(part_queue[i]);
        task_queue_free_t64(join_queue[i]);
    }

// # JIM/GEORGE
    if (cinf.R == NULL)
        free(tmpRelR);
    if (cinf.S == NULL)
        free(tmpRelS);

    return joinresult;
}

// # JIM/GEORGE
/** \copydoc PRO */
result_t *
PRO_t64(relation64_t * relR, relation64_t * relS, int nthreads, struct Cacheinf &cinf, JobScheduler& js)
{
    return join_init_run_t64(relR, relS, bucket_chaining_join_t64, nthreads, cinf, js);
}
/** @} */
