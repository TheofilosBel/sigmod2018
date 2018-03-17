#pragma once
#include <sys/time.h>           /* gettimeofday */
#include <iostream>              /* printf */
#include <string.h>
#include <stdlib.h>
#include "table_t.hpp"
#include "types.h"



/* Create relaton_t from table_t */
relation_t * CreateRelationT(table_t * table);
