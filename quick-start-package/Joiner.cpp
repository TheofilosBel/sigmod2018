#include <cassert>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Parser.hpp"
#include "QueryPlan.hpp"
#include "header.hpp"
#include "RadixJoin.hpp"
#include "Joiner.hpp"
using namespace std;

/* Timing variables */
double timeSelfJoin = 0;
double timeSelectFilter = 0;
double timeCreateTable = 0;
double timeTreegen = 0;
double timeCheckSum = 0;
double timeRadixJoin = 0;
double timePreparation = 0;
double timeLowJoin = 0;

extern double timeBuildPhase;
extern double timeProbePhase;
extern double timePartition;



#define time


column_t *Joiner::CreateColumn(SelectInfo& sel_info) {

    /* Get relation */
    Relation& rel = getRelation(sel_info.relId);

    /* Create and initialize the column */
    column_t * column = (column_t *) malloc(sizeof(column_t));
    column->size      = rel.size;
    column->values    = rel.columns[sel_info.colId];
    column->binding   = sel_info.binding;

    return column;  //TODO delete it when done
}


/* +---------------------+
   |The joiner functions |
   +---------------------+ */


/* Its better not to use it TODO change it */
void Joiner::Select(FilterInfo &fil_info, table_t* table) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Construct table  - Initialize variable */
    SelectInfo &sel_info = fil_info.filterColumn;
    column_t * column    = CreateColumn(sel_info);
    uint64_t filter      = fil_info.constant;

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, column, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, column, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, column, filter);
    }

    free(column);

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelectFilter += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif
}

void Joiner::SelectEqual(table_t *table, column_t *column, int filter) {

    /* Initialize helping variables */
    uint64_t *const values  = column->values;
    int table_index         = column->binding;
    const uint64_t size_rids  = table->size_of_row_ids;

    uint64_t **old_row_ids = table->row_ids;
    uint64_t **new_row_ids = (uint64_t**)malloc(sizeof(uint64_t*)*size_rids);
    std::vector<unsigned> &rbs = table->relations_bindings;

    const uint64_t size_rels = table->num_of_relations;

    /* index to noew the place of the row ids column */
    for(size_t index = 0; index < size_rels; index++) {
        if (rbs[index] == table_index) {
            table_index = index;
            break;
        }
    }
    size_t new_tb_index = 0;
    /* Update the row ids of the table */
    for (size_t rid = 0; rid < size_rids; rid++) {
        /* go to the old row ids and take the row id of the rid val */
        if (values[old_row_ids[rid][table_index]] == filter) {
            /* Allocate space for the relations storing */
            new_row_ids[new_tb_index] = old_row_ids[rid];
            new_tb_index++;
        }
    }

    /* Swap the old vector with the new one */
    delete table->row_ids;
    table->row_ids = new_row_ids;
    table->intermediate_res = true;
    table->allocated_size   = size_rids;
    table->size_of_row_ids  = new_tb_index;
}

void Joiner::SelectGreater(table_t *table, column_t *column, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = column->values;
    int table_index         = column->binding;
    const uint64_t size_rids  = table->size_of_row_ids;

    uint64_t **old_row_ids = table->row_ids;
    uint64_t **new_row_ids = (uint64_t**)malloc(sizeof(uint64_t*)*size_rids);
    std::vector<unsigned> &rbs = table->relations_bindings;

    const uint64_t size_rels = table->num_of_relations;

    /* index to noew the place of the row ids column */
    for(size_t index = 0; index < size_rels; index++) {
        if (rbs[index] == table_index) {
            table_index = index;
            break;
        }
    }

    size_t new_tb_index = 0;
    /* Update the row ids of the table */
    for (size_t rid = 0; rid < size_rids; rid++) {
        /* go to the old row ids and take the row id of the rid val */
        if (values[old_row_ids[rid][table_index]] > filter) {
            /* Allocate space for the relations storing */
            new_row_ids[new_tb_index] = old_row_ids[rid];
            new_tb_index++;
        }
    }

    /* Swap the old vector with the new one */
    delete table->row_ids;
    table->row_ids = new_row_ids;
    table->intermediate_res = true;
    table->allocated_size   = size_rids;
    table->size_of_row_ids  = new_tb_index;
}


void Joiner::SelectLess(table_t *table, column_t *column, int filter){

    /* Initialize helping variables */
    uint64_t *const values  = column->values;
    int table_index         = column->binding;
    const uint64_t size_rids  = table->size_of_row_ids;

    uint64_t **old_row_ids = table->row_ids;
    uint64_t **new_row_ids = (uint64_t**)malloc(sizeof(uint64_t*)*size_rids);
    std::vector<unsigned> &rbs = table->relations_bindings;

    const uint64_t size_rels = table->num_of_relations;

    /* index to noew the place of the row ids column */
    for(size_t index = 0; index < size_rels; index++) {
        if (rbs[index] == table_index) {
            table_index = index;
            break;
        }
    }
    size_t new_tb_index = 0;
    /* Update the row ids of the table */
    for (size_t rid = 0; rid < size_rids; rid++) {
        /* go to the old row ids and take the row id of the rid val */
        if (values[old_row_ids[rid][table_index]] < filter) {
            /* Allocate space for the relations storing */
            new_row_ids[new_tb_index] = old_row_ids[rid];
            new_tb_index++;
        }
    }

    /* Swap the old vector with the new one */
    delete table->row_ids;
    table->row_ids = new_row_ids;
    table->intermediate_res = true;
    table->allocated_size   = size_rids;
    table->size_of_row_ids  = new_tb_index;
}


// Creates a table_t object that represents a new relation
table_t* Joiner::CreateTableTFromId(unsigned rel_id, unsigned rel_binding) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Get the relation */
    Relation &rel  = getRelation(rel_id);

    /* Crate - Initialize a table_t */
    table_t *const table_t_ptr = new table_t;
    table_t_ptr->intermediate_res = false;
    table_t_ptr->size_of_row_ids  = rel.size;
    table_t_ptr->allocated_size   = rel.size;
    table_t_ptr->num_of_relations = 1;
    table_t_ptr->row_ids       = (uint64_t **) malloc(sizeof(uint64_t*) * rel.size);

    uint64_t** row_ids = table_t_ptr->row_ids;
    for (int i = 0; i < rel.size; ++i) {
        row_ids[i] = (uint64_t *) malloc(sizeof(uint64_t));  // Malloc one uint64 per row
        row_ids[i][0] = (uint64_t) i;
    }

    /* Keep the relation's id and binding */
    table_t_ptr->relation_ids.push_back(rel_id);
    table_t_ptr->relations_bindings.push_back(rel_binding);

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCreateTable += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return table_t_ptr;
}


/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
 * 4)Made the code repeatable to put some & in the arrays of row ids
*/
table_t* Joiner::low_join(table_t *table_r, column_t *column_r,  table_t *table_s, column_t *column_s) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* create hash_table for the hash_join phase */
    std::unordered_multimap<uint64_t, hash_entry> hash_c;

    /* the new table_t to continue the joins */
    table_t *updated_table_t = new table_t;


    /* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
    uint64_t hash_size,iter_size;
    column_t *hash_col;
    column_t *iter_col;

    int index_r                  = -1;
    int index_s                  = -1;
    for (ssize_t index = 0; index < table_r->num_of_relations ; index++) {

        if (column_r->binding == table_r->relations_bindings[index]) {
            index_r = index;
        }
    }
    for (ssize_t index = 0; index < table_s->num_of_relations ; index++) {

        if (column_s->binding == table_s->relations_bindings[index]) {
            index_s = index;
        }
    }

    /* check on wich table will create the hash_table */
    if (table_r->size_of_row_ids <= table_s->size_of_row_ids) {
        hash_size = table_r->size_of_row_ids;
        hash_col  = column_r;
        uint64_t ** h_rows = table_r->row_ids;
        int rel_num_left = table_r->num_of_relations;

        iter_size = table_s->size_of_row_ids;
        iter_col  = column_s;
        int rel_num_right  = table_s->num_of_relations;
        uint64_t ** i_rows = table_s->row_ids;

#ifdef time
        struct timeval start_build;
        gettimeofday(&start_build, NULL);
#endif

        /* now put the values of the column_r in the hash_table(construction phase) */
        for (uint64_t i = 0; i < hash_size; i++) {
            /* store hash[value of the column] = {rowid, index} */
            hash_entry hs;
            hs.row_id = h_rows[i][index_r];
            hs.index = i;
            hash_c.insert({hash_col->values[h_rows[i][index_r]], hs});
        }


#ifdef time
        struct timeval end_build;
        gettimeofday(&end_build, NULL);
        timeBuildPhase += (end_build.tv_sec - start_build.tv_sec) + (end_build.tv_usec - start_build.tv_usec) / 1000000.0;

        struct timeval start_probe;
        gettimeofday(&start_probe, NULL);
#endif
        /* create the updated relations_row_ids, merge the sizes*/
        uint32_t allocated_size = hash_size;
        updated_table_t->row_ids   = (uint64_t **) malloc(sizeof(uint64_t **) * allocated_size);
        updated_table_t->num_of_relations = table_s->num_of_relations + table_r->num_of_relations;
        updated_table_t->allocated_size = allocated_size;
        uint64_t ** update_row_ids = updated_table_t->row_ids;

        /* now the phase of hashing */
        uint32_t table_index = 0;
        uint32_t vector_size = updated_table_t->num_of_relations;
        for (uint64_t i = 0; i < iter_size; i++) {
            /* remember we may have multi vals in 1 key,if it isnt a primary key */
            /* vals->first = key ,vals->second = value */
            auto range_vals = hash_c.equal_range(iter_col->values[i_rows[i][index_s]]);
            for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
                /* store all the result then push it int the new row ids */
                /* its faster than to push back 1 every time */
                /* get the first values from the r's rows ids */
                /* Reallocate in case of emergency */
                if (table_index == allocated_size) {
                    //std::cerr << "In realloc" << '\n';

                    allocated_size = allocated_size * 2;
                    uint64_t** new_pointer = (uint64_t **) realloc(update_row_ids, sizeof(uint64_t*) * allocated_size);

                    if (new_pointer == NULL) std::cerr << "Error in realloc " << '\n';
                    update_row_ids = new_pointer;

                    updated_table_t->allocated_size = allocated_size;
                }

                update_row_ids[table_index] = (uint64_t *) malloc(sizeof(uint64_t *) * vector_size);

                /* Copy the vectors to the new ones */
                memcpy(&update_row_ids[table_index][0], &h_rows[vals->second.index][0], sizeof(uint64_t) * rel_num_left);
                memcpy(&update_row_ids[table_index][rel_num_left], &i_rows[i][0], sizeof(uint64_t) * rel_num_right);

                table_index++;
            }
        }
        updated_table_t->size_of_row_ids = table_index;
        updated_table_t->row_ids = update_row_ids;

        updated_table_t->relation_ids.reserve(table_r->relation_ids.size()+table_s->relation_ids.size());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_r->relation_ids.begin(), table_r->relation_ids.end());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_s->relation_ids.begin(), table_s->relation_ids.end());

        updated_table_t->relations_bindings.reserve(table_r->relations_bindings.size()+table_s->relations_bindings.size());
        updated_table_t->relations_bindings.insert(updated_table_t->relations_bindings.end() ,table_r->relations_bindings.begin(), table_r->relations_bindings.end());
        updated_table_t->relations_bindings.insert(updated_table_t->relations_bindings.end() ,table_s->relations_bindings.begin(), table_s->relations_bindings.end());
#ifdef time
        struct timeval end_probe;
        gettimeofday(&end_probe, NULL);
        timeProbePhase += (end_probe.tv_sec - start_probe.tv_sec) + (end_probe.tv_usec - start_probe.tv_usec) / 1000000.0;
#endif
    }
    /* table_r->column_j->size > table_s->column_j->size */
    else {
#ifdef time
        struct timeval start_build;
        gettimeofday(&start_build, NULL);
#endif
        hash_size = table_s->size_of_row_ids;
        hash_col = column_s;
        uint64_t ** h_rows = table_s->row_ids;
        int rel_num_left  = table_s->num_of_relations;

        iter_size = table_r->size_of_row_ids;
        iter_col  = column_r;
        uint64_t ** i_rows = table_r->row_ids;
        int rel_num_right  = table_r->num_of_relations;

        /* now put the values of the column_r in the hash_table(construction phase) */
        for (uint64_t i = 0; i < hash_size; i++) {
            /* store hash[value of the column] = {rowid, index} */
            hash_entry hs;
            hs.row_id = h_rows[i][index_s];
            hs.index = i;
            hash_c.insert({hash_col->values[h_rows[i][index_s]], hs});
        }

#ifdef time
        struct timeval end_build;
        gettimeofday(&end_build, NULL);
        timeBuildPhase += (end_build.tv_sec - start_build.tv_sec) + (end_build.tv_usec - start_build.tv_usec) / 1000000.0;

        struct timeval start_probe;
        gettimeofday(&start_probe, NULL);
#endif
        /* create the updated relations_row_ids, merge the sizes*/
        uint32_t allocated_size = hash_size;
        updated_table_t->row_ids   = (uint64_t **) malloc(sizeof(uint64_t **) * allocated_size);
        updated_table_t->num_of_relations = table_s->num_of_relations + table_r->num_of_relations;
        updated_table_t->allocated_size = allocated_size;
        uint64_t ** update_row_ids = updated_table_t->row_ids;

        /* now the phase of hashing */
        uint32_t table_index = 0;
        uint32_t vector_size = updated_table_t->num_of_relations;
        for (uint64_t i = 0; i < iter_size; i++) {
            /* remember we may have multi vals in 1 key,if it isnt a primary key */
            /* vals->first = key ,vals->second = value */
            auto range_vals = hash_c.equal_range(iter_col->values[i_rows[i][index_r]]);
            for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
                /* store all the result then push it int the new row ids */
                /* its faster than to push back 1 every time */
                /* get the first values from the r's rows ids */
                /* Reallocate in case of emergency */
                if (table_index == allocated_size) {
                    allocated_size = allocated_size * 2;
                    uint64_t** new_pointer = (uint64_t **) realloc(update_row_ids, sizeof(uint64_t*) * allocated_size);

                    if (new_pointer == NULL) std::cerr << "Error in realloc " << '\n';
                    update_row_ids = new_pointer;

                    updated_table_t->allocated_size = allocated_size;
                }

                update_row_ids[table_index] = (uint64_t *) malloc(sizeof(uint64_t *) * vector_size);

                /* Copy the vectors to the new ones */
                memcpy(&update_row_ids[table_index][0], &h_rows[vals->second.index][0], sizeof(uint64_t) * rel_num_left);
                memcpy(&update_row_ids[table_index][rel_num_left], &i_rows[i][0], sizeof(uint64_t) * rel_num_right);

                table_index++;
            }
        }
        updated_table_t->size_of_row_ids = table_index;
        updated_table_t->row_ids = update_row_ids;


        updated_table_t->relation_ids.reserve(table_s->relation_ids.size()+table_r->relation_ids.size());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_s->relation_ids.begin(), table_s->relation_ids.end());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_r->relation_ids.begin(), table_r->relation_ids.end());

        updated_table_t->relations_bindings.reserve(table_s->relations_bindings.size()+table_r->relations_bindings.size());
        updated_table_t->relations_bindings.insert(updated_table_t->relations_bindings.end() ,table_s->relations_bindings.begin(), table_s->relations_bindings.end());
        updated_table_t->relations_bindings.insert(updated_table_t->relations_bindings.end() ,table_r->relations_bindings.begin(), table_r->relations_bindings.end());

#ifdef time
        struct timeval end_probe;
        gettimeofday(&end_probe, NULL);
        timeProbePhase += (end_probe.tv_sec - start_probe.tv_sec) + (end_probe.tv_usec - start_probe.tv_usec) / 1000000.0;
#endif

    }
    /* concatenate the relaitons ids for the merge */
    updated_table_t->intermediate_res = true;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeLowJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return updated_table_t;
}

void Joiner::construct(table_t *table) {
#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Innitilize helping variables */
    column_t &column = *table->column_j;
    const uint64_t *column_values = column.values;
    const int       table_index   = column.table_index;
    const uint64_t  column_size   = table->relations_row_ids->operator[](table_index).size();
    std::vector<std::vector<int>> &row_ids = *table->relations_row_ids;

    /* Create a new value's array  */
    uint64_t *const new_values  = new uint64_t[column_size];

    /* Pass the values of the old column to the new one, based on the row ids of the joiner */
    for (int i = 0; i < column_size; i++) {
    	new_values[i] = column_values[row_ids[table_index][i]];
    }

    /* Update the column of the table */
    column.values = new_values;
    column.size   = column_size;

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeConstruct += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif
}


/* The self Join Function */
table_t * Joiner::SelfJoin(table_t *table, PredicateInfo *predicate_ptr) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Create - Initialize a new table */
    table_t *new_table            = new table_t;
    new_table->relation_ids       = std::vector<unsigned>(table->relation_ids);
    new_table->relations_bindings = std::vector<unsigned>(table->relations_bindings);
    new_table->row_ids  = (uint64_t **) malloc(sizeof(uint64_t**) * table->size_of_row_ids);

    /* Get the 2 rows ids arrays */
    uint64_t **row_ids     = table->row_ids;
    uint64_t **new_row_ids = new_table->row_ids;

    /* Get the 2 relations */
    Relation & relation_l        = getRelation(predicate_ptr->left.relId);
    Relation & relation_r        = getRelation(predicate_ptr->right.relId);

    /* Get their columns */
    uint64_t *column_values_l    = relation_l.columns[predicate_ptr->left.colId];
    uint64_t *column_values_r    = relation_r.columns[predicate_ptr->right.colId];

    /* Get their column's sizes */
    int row_ids_size             = table->size_of_row_ids;

    /* Find the indexes of the raltions in the table's */
    int index_l                  = -1;
    int index_r                  = -1;
    int relations_num            = table->num_of_relations;

    for (ssize_t index = 0; index < relations_num ; index++) {

        if (predicate_ptr->left.binding == table->relations_bindings[index]) {
            index_l = index;
        }
        if (predicate_ptr->right.binding == table->relations_bindings[index]){
            index_r = index;
        }
    }

#ifndef com
    if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';
    //std::cerr << "Index left " << index_l << " index right " << index_r << '\n';
    flush(cerr);
#endif

    /* Loop all the row_ids and keep the one's matching the predicate */
    unsigned rows_number     = table->size_of_row_ids;
    unsigned new_table_index = 0;
    for (ssize_t i = 0; i < rows_number; i++) {

        /* Apply the predicate: In case of success add to new table */
        if (column_values_l[row_ids[i][index_l]] == column_values_r[row_ids[i][index_r]]) {

            /* Add this row_id to all the relations */
            new_row_ids[new_table_index] = row_ids[i];
            new_table_index++;
        }
    }

    /* Update pointers */
    new_table->size_of_row_ids  = new_table_index;
    new_table->allocated_size   = table->size_of_row_ids;
    new_table->num_of_relations = table->num_of_relations;
    new_table->intermediate_res = true;


#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelfJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    /*Delete old table_t */
    free(table->row_ids);
    delete table;

    return new_table;
}

//CHECK SUM FUNCTION
uint64_t Joiner::check_sum(SelectInfo &sel_info, table_t *table) {

    /* to create the final cehcksum column */
    column_t * column = CreateColumn(sel_info);
    int   table_index = -1;

    /* Get the index from the row_ids 2d array */
    for (ssize_t index = 0; index < table->num_of_relations; index++) {

        if (column->binding == table->relations_bindings[index]) {
            table_index = index;
            break;
        }
    }

    if (table_index == -1) std::cerr << "Error in CheckSum: No mapping found for predicates" << '\n';

    uint64_t *  values  = column->values;
    uint64_t ** row_ids = table->row_ids;
    uint64_t sum = 0;
    int     size = table->size_of_row_ids;
    for (uint64_t i = 0 ; i < size; i++)
        sum += values[row_ids[i][table_index]];

    return sum;
}

// Loads a relation from disk
void Joiner::addRelation(const char* fileName) {
    relations.emplace_back(fileName);
}

// Loads a relation from disk
Relation& Joiner::getRelation(unsigned relationId) {
    if (relationId >= relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}

// The join function
table_t* Joiner::join(table_t *table_left, table_t *table_right, PredicateInfo * pred) {



    column_t * column_left = CreateColumn(pred->left);
    column_t * column_right = CreateColumn(pred->right);

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Do the radix join */
    //table_t * res = radix_join(table_left, column_left, table_right, column_right);
    table_t * res = low_join(table_left, column_left, table_right, column_right);

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeRadixJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    //free(table_left->row_ids);
    //free(table_right->row_ids);
    free(column_left);
    free(column_right);

    return res;
}

// Get the total number of relations
int Joiner::getRelationsCount() {
    return relations.size();
}

// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
void Joiner::join(QueryInfo& i) {}

/* +---------------------+
   |The Column functions |
   +---------------------+ */

void PrintColumn(column_t *column) {
    /* Print the column's table id */
    std::cerr << "Column of table " << column->binding << " and size " << column->size << '\n';

    /* Iterate over the column's values and print them */
    for (int i = 0; i < column->size; i++) {
        std::cerr << column->values[i] << '\n';
    }
}

int main(int argc, char* argv[]) {
    Joiner joiner;

    // Read join relations
    string line;
    while (getline(cin, line)) {
        if (line == "Done") break;
        joiner.addRelation(line.c_str());
    }

    #ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
    #endif

    // Preparation phase (not timed)
    //QueryPlan queryPlan;

    // Get the needed info of every column
    //queryPlan.fillColumnInfo(joiner);

    #ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timePreparation += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    #endif

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    int q_counter = 0;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch
#define titina
#ifdef titina
        // Parse the query
        //std::cerr << q_counter  << ": " << line << '\n';
        i.parseQuery(line);
        q_counter++;

        #ifdef time
        gettimeofday(&start, NULL);
        #endif

        JTree *jTreePtr = treegen(&i);
        // Create the optimal join tree
        //JoinTree* optimalJoinTree = queryPlan.joinTreePtr->build(i.relationIds, i.predicates);

        #ifdef time
        gettimeofday(&end, NULL);
        timeTreegen += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

        int *plan = NULL, plan_size = 0;
        //print_rec(jTreePtr, 0);
        table_t *result = jTreeMakePlan(jTreePtr, joiner, plan);

        // join
        //joiner.join(i);

        #ifdef time
        gettimeofday(&start, NULL);
        #endif

        string result_str;
        uint64_t checksum = 0;
        std::vector<SelectInfo> &selections = i.selections;
        for (size_t i = 0; i < selections.size(); i++) {
            checksum = joiner.check_sum(selections[i], result);

            if (checksum == 0) {
                result_str += "NULL";
            } else {
                result_str += std::to_string(checksum);
            }

            if (i != selections.size() - 1) {
                result_str +=  " ";
            }
        }

        #ifdef time
        gettimeofday(&end, NULL);
        timeCheckSum += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
        #endif

        /* Print the result */
        std::cout << result_str << endl;
#else
        std::cout << "Implelemt JOIN " << '\n';
#endif
    }

    #ifdef time
    std::cerr << "timeSelectFilter: " << (long)(timeSelectFilter * 1000) << endl;
    std::cerr << "timeSelfJoin: " << (long)(timeSelfJoin * 1000) << endl;
    std::cerr << "timeRadixJoin: " << (long)(timeRadixJoin * 1000) << endl;
    std::cerr << "    timePartition: " << (long)(timePartition * 1000) << endl;
    std::cerr << "    timeBuildPhase: " << (long)(timeBuildPhase * 1000) << endl;
    std::cerr << "    timeProbePhase: " << (long)(timeProbePhase * 1000) << endl;
    std::cerr << "timeCreateTable: " << (long)(timeCreateTable * 1000) << endl;
    std::cerr << "timeTreegen: " << (long)(timeTreegen * 1000) << endl;
    std::cerr << "timeCheckSum: " << (long)(timeCheckSum * 1000) << endl;
    std::cerr << "timePreparation: " << (long)(timePreparation * 1000) << endl;
    flush(std::cerr);
    #endif

    return 0;
}
