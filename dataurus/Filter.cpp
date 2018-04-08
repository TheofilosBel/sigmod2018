//#include "include/Filter_tbb_types.hpp"  UNCOMENT TO USE TBB
#include "Joiner.hpp"
#include "filter_job.h"
#include "parallel_radix_join.h"
#include "prj_params.h"
#include "generator.h"

#define RANGE           100
#define RANGE_SELF_JOIN 20

/* The self Join Function */
table_t * Joiner::SelfJoin(table_t *table, PredicateInfo *predicate_ptr, columnInfoMap & cmap) {

    /* Create - Initialize a new table */
    table_t *new_table            = new table_t;
    new_table->relations_bindings = std::unordered_map<unsigned, unsigned>(table->relations_bindings);
    new_table->intermediate_res   = true;
    new_table->column_j           = new column_t;
    new_table->rels_num           = table->rels_num;

    /* Get the 2 relation rows ids vectors in referances */
    unsigned * row_ids_matrix       = table->row_ids;
    unsigned * new_row_ids_matrix   = new_table->row_ids;

    /* Get the 2 relations */
    Relation & relation_l        = getRelation(predicate_ptr->left.relId);
    Relation & relation_r        = getRelation(predicate_ptr->right.relId);

    /* Get their columns */
    uint64_t *column_values_l    = relation_l.columns[predicate_ptr->left.colId];
    uint64_t *column_values_r    = relation_r.columns[predicate_ptr->right.colId];

    /* Fint the indexes of the raltions in the table's */
    int index_l                  = -1;
    int index_r                  = -1;

    index_l = table->relations_bindings.find(predicate_ptr->left.binding)->second;
    index_r = table->relations_bindings.find(predicate_ptr->right.binding)->second;

    /* Loop all the row_ids and keep the one's matching the predicate */
    unsigned rows_number = table->tups_num;
    unsigned rels_number = table->rels_num;
    unsigned new_size = 0;

    size_t range = RANGE_SELF_JOIN;
    struct self_join_arg a[range];
    for (size_t i = 0; i < range; i++) {
        a[i].low   = (i < rows_number % range) ? i * (rows_number / range) + i : i * (rows_number / range) + rows_number % range;
        a[i].high  = (i < rows_number % range) ? a[i].low + rows_number / range + 1 :  a[i].low + rows_number / range;
        a[i].column_values_l = column_values_l;
        a[i].column_values_r = column_values_r;
        a[i].index_l = index_l;
        a[i].index_r = index_r;
        a[i].row_ids_matrix = row_ids_matrix;
        a[i].new_row_ids_matrix = new_row_ids_matrix;
        a[i].rels_number = rels_number;
        a[i].prefix = 0;
        a[i].size = 0;
        job_scheduler.Schedule(new JobSelfJoinFindSize(a[i]));
    }
    job_scheduler.Barrier();

    /* Calculate the prefix sums */
    unsigned temp = 0;
    for (size_t i = 0; i < range; i++) {
        a[i].prefix += temp;
        temp += a[i].size;
    }
    new_size = temp;

    new_row_ids_matrix = (unsigned *) malloc(sizeof(unsigned) * rels_number * new_size);
    for (size_t i = 0; i < range; i++) {
        if (a[i].size == 0) continue;
        a[i].new_row_ids_matrix = new_row_ids_matrix;
        job_scheduler.Schedule(new JobSelfJoin(a[i]));
    }
    job_scheduler.Barrier();

    new_table->row_ids = new_row_ids_matrix;
    new_table->tups_num = new_size;

    /*Delete old table_t */
    free(table->row_ids);
    delete table;

    return new_table;
}

void Joiner::Select(FilterInfo &fil_info, table_t* table, ColumnInfo* columnInfo) {

    /* Construct table  - Initialize variable */
    SelectInfo &sel_info = fil_info.filterColumn;
    uint64_t filter = fil_info.constant;
    int ch_flag;

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, filter);
        columnInfo->max = filter;
    }
    else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, filter);
        columnInfo->min = filter;
    }
    else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, filter);
        columnInfo->min = filter;
        columnInfo->max = filter;
    }
}

void Joiner::SelectEqual(table_t *table, int filter) {
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    const unsigned table_index = table->column_j->table_index;
    const unsigned rel_num = table->rels_num;
    const unsigned size = table->tups_num;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = NULL;

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;

    size_t range = RANGE;

    /* Intermediate result */
    if (inter_res) {

        struct inter_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].old_rids = old_row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobEqualInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi * rel_num);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobEqualInterFilter(a[i]));
        }
        job_scheduler.Barrier();

    }
    else {

        struct noninter_arg a[range];
        // struct noninter_arg * a = (noninter_arg *) malloc(range * sizeof(noninter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobEqualNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobEqualNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();
    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}


void Joiner::SelectGreater(table_t *table, int filter){

    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    const unsigned table_index = table->column_j->table_index;
    const unsigned rel_num = table->rels_num;
    const unsigned size = table->tups_num;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = NULL;

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;
    size_t range = RANGE;
    if (inter_res) {
        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        struct inter_arg a[range];
        // struct inter_arg * a = (inter_arg *) malloc(range * sizeof(inter_arg));
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].old_rids = old_row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobGreaterInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi * rel_num);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobGreaterInterFilter(a[i]));
        }
        job_scheduler.Barrier();
    }
    else {

        struct noninter_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobGreaterNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
            flush(cerr);
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range ; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobGreaterNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();
    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}

void Joiner::SelectLess(table_t *table, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    const unsigned table_index = table->column_j->table_index;
    const unsigned rel_num = table->rels_num;
    const unsigned size = table->tups_num;

    unsigned * old_row_ids = table->row_ids;
    unsigned * new_row_ids = NULL;

    /* Update the row ids of the table */
    bool inter_res = table->intermediate_res;
    unsigned new_tbi = 0;
    size_t range = RANGE;
    if (inter_res) {

        struct inter_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].old_rids = old_row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobLessInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi * rel_num);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobLessInterFilter(a[i]));
        }
        job_scheduler.Barrier();
    }
    else {

        struct noninter_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].filter = filter;
            a[i].prefix = 0;
            a[i].size = 0;
            job_scheduler.Schedule(new JobLessNonInterFindSize(a[i]));
        }
        job_scheduler.Barrier();

        /* Calculate the prefix sums */
        new_tbi += a[0].size;
        unsigned temp = a[0].size;
        for (size_t i = 1; i < range; i++) {
            a[i].prefix = temp;
            temp += a[i].size;
        }
        new_tbi = temp;

        // malloc new values
        new_row_ids = (unsigned *) malloc(sizeof(unsigned) * new_tbi);
        for (size_t i = 0; i < range; i++) {
            if (a[i].size == 0) continue;
            a[i].new_array = new_row_ids;
            job_scheduler.Schedule(new JobLessNonInterFilter(a[i]));
        }
        job_scheduler.Barrier();
    }

    /* Swap the old vector with the new one */
    (inter_res) ? (free(old_row_ids)) : ((void)0);
    table->row_ids = new_row_ids;
    table->tups_num = new_tbi;
    table->intermediate_res = true;
}
