#pragma once


typedef struct table_t table_t;
typedef struct column_t column_t;

struct column_t {
    uint64_t *values;
    uint64_t  size;
    int       table_index;
};

struct table_t {

    /* Row Ids and relation Ids */
    std::vector<std::vector<int>>  relations_row_ids;
    std::vector<int>               relation_ids;

    /* Intermediate result falg */
    bool intermediate_res;

    /* column_t pointer of column to join */
    column_t *column_j;
};
