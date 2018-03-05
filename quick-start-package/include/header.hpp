#pragma once

/* C++ libraries */
#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <assert.h>   // for debugging

/* given API */
#include "Parser.hpp"
#include "Joiner.hpp"

typedef struct JTree {
    int node_id;
    int visited;

    struct FilterInfo* filterPtr;
    struct PredicateInfo* predPtr;

    table_t *intermediate_res;

    JTree *left;
    JTree *right;
    JTree *parent;

} JTree;


/* complementary functions */
#include "functions.hpp"
