#pragma once
#include <sys/time.h>           /* gettimeofday */
#include <iostream>              /* printf */
#include "Joiner.hpp"
#include "table_t.hpp"


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

int64_t bucket_chaining_join(uint64_t ** R, const column_t * column_left, const int size_left,
                             uint64_t ** S, const column_t * column_right, const int size_right, int * const tmpR);

void radix_cluster_nopadding(uint64_t ** out_rel_ids, uint64_t ** in_rel_ids, column_t *column, int row_ids_size, int R, int D);


table_t* radix_join(table_t *table_left, column_t *column_left, table_t *table_right, column_t *column_right);
