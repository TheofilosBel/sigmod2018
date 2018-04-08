/**
 * @file    types.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Tue May 22 16:43:30 2012
 * @version $Id: types.h 4419 2013-10-21 16:24:35Z bcagri $
 *
 * @brief  Provides general type definitions used by all join algorithms.
 *
 *
 */
#ifndef TYPES64_H
#define TYPES64_H

#include <stdint.h>

/**
 * @defgroup Types Common Types
 * Common type definitions used by all join implementations.
 * @{
 */

// #ifdef KEY_8B /* 64-bit key/value, 16B tuples */
// typedef int64_t intkey64_t;
// typedef int64_t value_t;
// #else /* 32-bit key/value, 8B tuples */
typedef uint64_t intkey64_t;

// #endif

typedef struct tuple64_t    tuple64_t;
typedef struct relation64_t relation64_t;

/** Type definition for a tuple, depending on KEY_8B a tuple can be 16B or 8B */
struct tuple64_t {
    intkey64_t key;
    value_t  payload;
};

/**
 * Type definition for a relation.
 * It consists of an array of tuples and a size of the relation.
 */
struct relation64_t {
  tuple64_t * tuples;
  uint64_t  num_tuples;
};

/** @} */

#endif /* TYPES_H */
