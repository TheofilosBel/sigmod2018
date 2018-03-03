#pragma once


typedef struct table_t table_t;
typedef struct column_t column_t;
typedef struct hash_entry hash_entry;

struct hash_entry {
    uint64_t row_id;
    uint64_t index;
};

struct column_t {
    uint64_t *values;
    uint64_t  size;
    uint64_t  table_index;
};

struct table_t {

    /* Row Ids and relation Ids */
    std::vector<std::vector<int>>  relations_row_ids;

    /* use it for the filtrering */
    std::vector<int>               relation_ids;

    /* Intermediate falg */
    bool intermediate_res;

    /* column_t pointer of column to join */
    column_t *column_j;
};