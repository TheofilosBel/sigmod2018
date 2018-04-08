/**
 * @file    parallel_radix_join.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Sun Feb 20:09:59 2012
 * @version $Id: parallel_radix_join.h 4419 2013-10-21 16:24:35Z bcagri $
 *
 * @brief  Provides interfaces for several variants of Radix Hash Join.
 *
 * (c) 2012, ETH Zurich, Systems Group
 *
 */

#ifndef PARALLEL_RADIX_JOIN_H
#define PARALLEL_RADIX_JOIN_H

#include "types.h" /* relation_t */
#include "job_scheduler.h"

/* not timing */
#define NO_TIMING

/* if you don define it 2 passes will do*/
#define NUM_PASSES 1

/* we want to store the results */
#define JOIN_RESULT_MATERIALIZE


// # JIM/GEORGE
struct cached_t {

	void *  rel;
    void  *  tmp;
    int32_t ** hist;
    int64_t *  output;
    uint64_t   total_tuples;
    uint32_t   num_tuples;
    int32_t    R;
    uint32_t   D;
    int        relidx;  /* 0: R, 1: S */
    uint32_t   padding;
};

struct Cacheinf {
	cached_t *S;
	cached_t *R;
};

void
cache_partition_0(relation_t *r0, int nthreads, struct Cacheinf & cinf, JobScheduler & js1, JobScheduler & js2);

/**
 * PRO: Parallel Radix Join Optimized.
 *
 * The "Parallel Radix Join Optimized" implementation denoted as PRO implements
 * the parallel radix join idea by Kim et al. with further optimizations. Mainly
 * it uses the bucket chaining for the build phase instead of the
 * histogram-based relation re-ordering and performs better than other
 * variations such as PRHO, which applies SIMD and prefetching
 * optimizations.

 * @param relR  input relation R - inner relation
 * @param relS  input relation S - inner relation
 *
 * @return number of result tuples
 */
result_t *
PRO(relation_t * relR, relation_t * relS, int nthreads, struct Cacheinf&, JobScheduler&);

result_t *
PRO_t64(relation64_t * relR, relation64_t * relS, int nthreads, struct Cacheinf&, JobScheduler&);

#endif /* PARALLEL_RADIX_JOIN_H */
