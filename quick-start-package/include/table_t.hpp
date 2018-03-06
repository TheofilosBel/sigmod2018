#pragma once

typedef std::vector<std::vector<int>> matrix;
typedef struct table_t table_t;
typedef struct column_t column_t;
typedef struct hash_entry hash_entry;
typedef struct cartesian_product cartesian_product_t;

struct hash_entry {
    uint64_t row_id;
    uint64_t index;
};

struct column_t {
    uint64_t *values;
    uint64_t  size;
    uint64_t  table_index;
    unsigned  id;
};

struct cartesian_product {
    int size_of_product;
    int size_of_left_relations;
    int size_of_right_relations;
    std::vector<int> left_relations;
    std::vector<int> right_relations;
};


struct table_t {

    /* Row Ids and relation Ids */
    std::vector<std::vector<int>>  *relations_row_ids;

    /* use it for the filtrering TODO hash map ?*/
    std::vector<int>               relation_ids;

    /* representation of cartesian product */
    cartesian_product_t            *cartesian_product;

    /* Intermediate result falg */
    bool intermediate_res;

    /* column_t pointer of column to join */
    column_t *column_j;
};
