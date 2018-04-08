/**
 * @file    generator.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Fri May 18 14:05:07 2012
 * @version $Id: generator.h 4546 2013-12-07 13:56:09Z bcagri $
 *
 * @brief  Provides methods to generate data sets of various types
 *
 */

#ifndef GENERATOR_H
#define GENERATOR_H

#include "types.h"


//MINE
relation_t *
gen_rel(int num_tuples);

relation64_t *
gen_rel_t64(int num_tuples);

#endif /* GENERATOR_H */
