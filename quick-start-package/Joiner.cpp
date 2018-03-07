#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include "string"
#include <vector>
#include "Parser.hpp"
#include "QueryGraph.hpp"
#include "./include/header.hpp"

using namespace std;


/* +---------------------+
   |The joiner functions |
   +---------------------+ */

/* Its better not to use it TODO change it */
void Joiner::Select(FilterInfo &fil_info, table_t* table) {

    /* Construct table  - Initialize variable */
    (table->intermediate_res)? (construct(table)) : ((void)0);
    SelectInfo &sel_info = fil_info.filterColumn;
    uint64_t filter = fil_info.constant;

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, filter);
    }
}

void Joiner::SelectEqual(table_t *table, int filter) {
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    std::vector<std::vector<int>> &old_row_ids = *table->relations_row_ids;
    std::vector<std::vector<int>> *new_row_ids = new std::vector<std::vector<int>>(rel_num, std::vector<int>());

    const uint64_t size     = old_row_ids[table_index].size();

    /* Update the row ids of the table */
    //std::cerr << "Table index is " << table_index << '\n';
    //std::cerr << "Relation id in index " << table->relation_ids[table_index] << '\n';

    for (size_t index = 0; index < size; index++) {
        if (values[index] == filter) {
            for (size_t rel_index = 0; rel_index < rel_num; rel_index++) {
                new_row_ids->operator[](rel_index).push_back(old_row_ids[rel_index][index]);
            }
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}

void Joiner::SelectGreater(table_t *table, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    std::vector<std::vector<int>> &old_row_ids = *table->relations_row_ids;
    std::vector<std::vector<int>> *new_row_ids = new std::vector<std::vector<int>>(rel_num, std::vector<int>());

    const uint64_t size     = old_row_ids[table_index].size();

    /* Update the row ids of the table */
    //std::cerr << "Table index is " << table_index << '\n';
    //std::cerr << "Relation id in index " << table->relation_ids[table_index] << '\n';

    for (size_t index = 0; index < size; index++) {
        if (values[index] > filter) {
            for (size_t rel_index = 0; rel_index < rel_num; rel_index++) {
                new_row_ids->operator[](rel_index).push_back(old_row_ids[rel_index][index]);
            }
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}

void Joiner::SelectLess(table_t *table, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    std::vector<std::vector<int>> &old_row_ids = *table->relations_row_ids;
    std::vector<std::vector<int>> *new_row_ids = new std::vector<std::vector<int>>(rel_num, std::vector<int>());

    const uint64_t size     = old_row_ids[table_index].size();

    /* Update the row ids of the table */
    //std::cerr << "Table index is " << table_index << '\n';
    //std::cerr << "Relation id in index " << table->relation_ids[table_index] << '\n';

    for (size_t index = 0; index < size; index++) {
        if (values[index] < filter) {
            for (size_t rel_index = 0; rel_index < rel_num; rel_index++) {
                new_row_ids->operator[](rel_index).push_back(old_row_ids[rel_index][index]);
            }
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}

void Joiner::AddColumnToTableT(SelectInfo &sel_info, table_t *table) {

    /* Create a new column_t for table */
    column_t &column = *table->column_j;
    std::vector<int> &relation_ids = table->relation_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    column.size   = rel.size;
    column.values = rel.columns.at(sel_info.colId);
    column.id     = sel_info.colId;
    column.table_index = -1;
    RelationId relation_id = sel_info.relId;

    /* Get the right index from the relation id table */
    for (size_t index = 0; index < relation_ids.size(); index++) {
        if (relation_ids[index] == relation_id){
            column.table_index = index;
            break;
        }
    }

    /* Error msg for debuging */
    if (column.table_index == -1) {
        std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors" << '\n';
    }
}

void Joiner::AddColumnToIntermediatResult(SelectInfo &sel_info, table_t *table) {

    /* Only for intermediate */
    //if (!table->intermediate_res) return;

    /* Create a new column_t for table */
    column_t &column = *table->column_j;
    std::vector<int> &relation_ids = table->relation_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    column.size   = rel.size;
    column.values = rel.columns.at(sel_info.colId);
    column.id     = sel_info.colId;
    column.table_index = -1;
    RelationId relation_id = sel_info.relId;

    /* Get the right index from the relation id table */
    for (size_t index = 0; index < relation_ids.size(); index++) {
        if (relation_ids[index] == relation_id){
            column.table_index = index;
            break;
        }
    }

    /* Error msg for debuging */
    if (column.table_index == -1) {
        std::cerr << "At AddColumnToIntermediatResult, Id not matchin with intermediate result vectors" << '\n';
    }
}

table_t* Joiner::CreateTableTFromId(unsigned rel_id) {

    /* Crate - Initialize a table_t */
    table_t *const table_t_ptr = new table_t;
    table_t_ptr->column_j = new column_t;
    table_t_ptr->intermediate_res = false;
    table_t_ptr->relations_row_ids = new std::vector<std::vector<int>>;
    table_t_ptr->cartesian_product = NULL;

    std::vector<std::vector<int>> &rel_row_ids = *table_t_ptr->relations_row_ids;

    /* Get the relation */
    Relation &rel  = getRelation(rel_id);

    /* Create the relations_row_ids and relation_ids vectors */
    uint64_t rel_size  = rel.size;
    rel_row_ids.resize(1);
    for (size_t i = 0;  i < rel_size; i++) {
        rel_row_ids[0].push_back(i);
    }

    table_t_ptr->relation_ids.push_back(rel_id);

    return table_t_ptr;
}

table_t* Joiner::SelectInfoToTableT(SelectInfo &sel_info) {

    /* Crate - Initialize array that holds 2 table_t */
    table_t *const table_t_ptr = new table_t;

    table_t_ptr->intermediate_res = false;
    table_t_ptr->column_j = new column_t;
    table_t_ptr->relations_row_ids = new std::vector<std::vector<int>>;
    table_t_ptr->cartesian_product = NULL;

    column_t &column = *table_t_ptr->column_j;
    std::vector<std::vector<int>> &rel_row_ids = *table_t_ptr->relations_row_ids;

    /* Get the relation */
    Relation &rel  = getRelation(sel_info.relId);

    /* Initialize the column_t */
    column.values = rel.columns.at(sel_info.colId);
    column.size   = rel.size;
    column.id     = sel_info.colId;
    column.table_index  = 0;

    /* Create the relations_row_ids and relation_ids vectors */
    uint64_t rel_size  = column.size;
    rel_row_ids.resize(1);
    for (size_t i = 0;  i < rel_size; i++) {
        rel_row_ids[0].push_back(i);
    }

    table_t_ptr->relation_ids.push_back(sel_info.relId);
    table_t_ptr->relation_ids.push_back();

    return table_t_ptr;
}

table_t* Joiner::join(table_t *table_r, table_t *table_s) {
    /* Construct the tables in case of intermediate results */
    (table_r->intermediate_res)? (construct(table_r)) : ((void)0);
    (table_s->intermediate_res)? (construct(table_s)) : ((void)0);
    /* Join the columns */
    return low_join(table_r, table_s);
}

table_t* Joiner::cartesian_join(table_t *table_l, table_t *table_s) {

    /* Add to table_l all the elements of table_s*/
    std::vector<std::vector<int>> &left_row_ids = *(table_l->relations_row_ids);
    std::vector<std::vector<int>> &right_row_ids = *(table_s->relations_row_ids);

    std::vector<int> &left_rel_map = table_l->relation_ids;
    std::vector<int> &right_rel_map = table_s->relation_ids;

    int right_size = right_row_ids[0].size();
    int right_rel_size = right_rel_map.size();
    int left_size = left_row_ids[0].size();
    int left_rel_size = left_rel_map.size();

    /* Update the relation ids map */
    int size = right_rel_map.size();
    for (int i = 0; i < size; ++i) {
        left_rel_map.push_back(right_rel_map[i]);
    }

    /* Now add right_rel_size to the left vector */
    for (int i = 0; i < right_rel_size; ++i) {
        left_row_ids.push_back(std::vector<int>());
    }

    /* Simply add the new rows ids to the rows ids table */
    for (int relation = 0; relation < right_rel_size; ++relation) {
        for (int m = 0; m < right_size; ++m) {
            left_row_ids[left_rel_size - 1 + relation].push_back(right_row_ids[relation][m]);
        }
    }

    std::cerr << "Cartesian for " << left_rel_map[0] << "X" << right_rel_map[0] << '\n';
    std::cerr << "Sizes " << left_size << " " <<  right_size << '\n';

    /* TODO cartesian product of 2 cartesians */
    /* Fill the cartesian prodcut stuct */
    cartesian_product_t *c_product = new cartesian_product_t;
    c_product->size_of_product = left_size * right_size;
    c_product->size_of_left_relations = left_size;
    c_product->size_of_right_relations = right_size;
    c_product->left_relations = std::vector<int>(left_rel_map);
    c_product->right_relations = std::vector<int>(right_rel_map);
    table_l->cartesian_product = c_product;

#ifdef cp
    if (left_size == 0 || right_size == 0) {
        left_row_ids.resize(right_rel_map.size() + left_rel_map.size(), std::vector<int>(0));
        return table_l;
    }

    /* Reserve the left row id table */
    std::cerr << "Cartesian for " << left_size << " " <<  right_size << '\n';

    /* Add m lines for each line to the left row ids map */
    for (int relation = 0; relation < left_rel_size; ++relation) {
        for (int m = 1; m < right_size; ++m) {  // Not for m = 0 it already exists
            for (int n = 0; n < left_size; ++n) {  //[m*left_size + n]
                left_row_ids[relation].push_back(left_row_ids[relation][n]);
            }
        }
    }

    /* Now add right_rel_size to the left vector */
    for (int i = 0; i < right_rel_size; ++i) {
        left_row_ids.push_back(std::vector<int>());
    }

    std::cerr << "HERE" << '\n';

    /* For each relation of the  right vector add the m elements to each bach of m elements of m*/
    for (int relation = 0; relation < right_rel_size; ++relation) {
        for (int m = 0; m < right_size; ++m) {
            for (int n = 0; n < left_size; ++n) {  //[m*left_size + n]
                left_row_ids[left_rel_size - 1 + relation].push_back(right_row_ids[relation][m]);
            }
        }
    }

#endif

    return table_l;
}

/* The self Join Function */
table_t * Joiner::SelfJoin(table_t *table, PredicateInfo *predicate_ptr) {

    /* Create - Initialize a new table */
    table_t *new_table           = new table_t;
    new_table->relation_ids      = std::vector<int>(table->relation_ids);
    new_table->relations_row_ids = new std::vector<std::vector<int>>;
    new_table->cartesian_product = NULL;
    new_table->column_j          = new column_t;

    /* Get the 2 relation rows ids vectors in referances */
    matrix &row_ids_matrix       = *(table->relations_row_ids);
    matrix &new_row_ids_matrix   = *(new_table->relations_row_ids);

    /* Get the 2 relations */
    Relation & relation_l        = getRelation(predicate_ptr->left.relId);
    Relation & relation_r        = getRelation(predicate_ptr->right.relId);

    /* Get their columns */
    uint64_t *column_values_l    = relation_l.columns[predicate_ptr->left.colId];
    uint64_t *column_values_r    = relation_r.columns[predicate_ptr->right.colId];

    /* Get their column's sizes */
    int column_size_l            = relation_l.size;
    int column_size_r            = relation_r.size;

    /* Fint the indexes of the raltions in the table's */
    int index_l                  = -1;
    int index_r                  = -1;
    int relations_num            = table->relation_ids.size();
    std::cerr << "Relations";
    for (ssize_t index = 0; index < relations_num ; index++) {

        if (predicate_ptr->left.relId == table->relation_ids[index]) {
            index_l = index;
        }
        if (predicate_ptr->right.relId == table->relation_ids[index]){
            index_r = index;
        }

        /* Initialize the new matrix */
        new_row_ids_matrix.push_back(std::vector<int>());
        std::cerr << " " << new_table->relation_ids[index] ;
    }
    std::cerr << '\n' << index_l << " " << index_r << endl;

#ifndef com
    if (index_l == -1 || index_r == -1) std::cerr << "Error in SelfJoin: No mapping found for predicates" << '\n';
#endif

    /* Loop all the row_ids and keep the one's matching the predicate */
    int rows_number = table->relations_row_ids->operator[](0).size();
    for (ssize_t i = 0; i < rows_number; i++) {

        /* Apply the predicate: In case of success add to new table */
        if (column_values_l[row_ids_matrix[index_l][i]] == column_values_r[row_ids_matrix[index_r][i]]) {

            /* Add this row_id to all the relations */
            for (ssize_t relation = 0; relation < relations_num; relation++) {
                new_row_ids_matrix[relation].push_back(row_ids_matrix[relation][i]);
            }
        }
    }

    return new_table;
}


/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
 * 4)Made the code repeatable to put some & in the arrays of row ids
*/
table_t* Joiner::low_join(table_t *table_r, table_t *table_s) {
    /* create hash_table for the hash_join phase */
    std::unordered_multimap<uint64_t, hash_entry> hash_c;

    /* the new table_t to continue the joins */
    table_t *updated_table_t = new table_t;
    updated_table_t->cartesian_product = NULL;

    /* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
    uint64_t hash_size,iter_size;
    column_t *hash_col;
    column_t *iter_col;


    /* check on wich table will create the hash_table */
    if (table_r->column_j->size <= table_s->column_j->size) {
        hash_size = table_r->column_j->size;
        hash_col = table_r->column_j;
        std::vector<std::vector<int>> &h_rows = *table_r->relations_row_ids;

        iter_size = table_s->column_j->size;
        iter_col = table_s->column_j;
        std::vector<std::vector<int>> &i_rows = *table_s->relations_row_ids;

        /* now put the values of the column_r in the hash_table(construction phase) */
        for (uint64_t i = 0; i < hash_size; i++) {
            /* store hash[value of the column] = {rowid, index} */
            hash_entry hs;
            hs.row_id = h_rows[hash_col->table_index][i];
            hs.index = i;
            hash_c.insert({hash_col->values[i], hs});
        }
        /* create the updated relations_row_ids, merge the sizes*/
        updated_table_t->relations_row_ids = new std::vector<std::vector<int>>;
        updated_table_t->relations_row_ids->resize(h_rows.size()+i_rows.size(), std::vector<int>());
        std::vector<std::vector<int>> &update_row_ids = *updated_table_t->relations_row_ids;

        /* now the phase of hashing */
        for (uint64_t i = 0; i < iter_size; i++) {
            /* remember we may have multi vals in 1 key,if it isnt a primary key */
            /* vals->first = key ,vals->second = value */
            auto range_vals = hash_c.equal_range(iter_col->values[i]);
            for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
                /* store all the result then push it int the new row ids */
                /* its faster than to push back 1 every time */
                /* get the first values from the r's rows ids */
                for (uint64_t j = 0 ; j < h_rows.size(); j++)
                    update_row_ids[j].push_back(h_rows[j][vals->second.index]);

                /* then go to the s's row ids to get the values */
                for (uint64_t j = 0; j < i_rows.size(); j++)
                    update_row_ids[j + h_rows.size()].push_back(i_rows[j][i]);
            }
        }

        updated_table_t->relation_ids.reserve(table_r->relation_ids.size()+table_s->relation_ids.size());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_r->relation_ids.begin(), table_r->relation_ids.end());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_s->relation_ids.begin(), table_s->relation_ids.end());
    }
    /* table_r->column_j->size > table_s->column_j->size */
    else {
        hash_size = table_s->column_j->size;
        hash_col = table_s->column_j;
        std::vector<std::vector<int>> &h_rows = *table_s->relations_row_ids;

        iter_size = table_r->column_j->size;
        iter_col = table_r->column_j;
        std::vector<std::vector<int>> &i_rows = *table_r->relations_row_ids;

        /* now put the values of the column_r in the hash_table(construction phase) */
        for (uint64_t i = 0; i < hash_size; i++) {
            /* store hash[value of the column] = {rowid, index} */
            hash_entry hs;
            hs.row_id = h_rows[hash_col->table_index][i];
            hs.index = i;
            hash_c.insert({hash_col->values[i], hs});
        }

        /* create the updated relations_row_ids, merge the sizes*/
        updated_table_t->relations_row_ids = new std::vector<std::vector<int>>;
        updated_table_t->relations_row_ids->resize(h_rows.size()+i_rows.size(), std::vector<int>());
        std::vector<std::vector<int>> &update_row_ids = *updated_table_t->relations_row_ids;

        /* now the phase of hashing */
        for (uint64_t i = 0; i < iter_size; i++) {
            /* remember we may have multi vals in 1 key,if it isnt a primary key */
            /* vals->first = key ,vals->second = value */
            auto range_vals = hash_c.equal_range(iter_col->values[i]);
            for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
                /* store all the result then push it int the new row ids */
                /* its faster than to push back 1 every time */

                for (uint64_t j = 0 ; j < h_rows.size(); j++)
                    update_row_ids[j].push_back(h_rows[j][vals->second.index]);

                /* then go to the s's row ids to get the values */
                for (uint64_t j = 0; j < i_rows.size(); j++)
                    update_row_ids[j + h_rows.size()].push_back(i_rows[j][i]);
            }
        }
        updated_table_t->relation_ids.reserve(table_s->relation_ids.size()+table_r->relation_ids.size());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_s->relation_ids.begin(), table_s->relation_ids.end());
        updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_r->relation_ids.begin(), table_r->relation_ids.end());
    }
    /* concatenate the relaitons ids for the merge */
    updated_table_t->intermediate_res = true;
    updated_table_t->column_j = new column_t;

    /* do the cleaning */
    /* delete table_r->relations_row_ids;
    delete table_r;
    delete table_s->relations_row_ids;
    delete table_s; */
    return updated_table_t;
}

void Joiner::construct(table_t *table) {

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
}

//CHECK SUM FUNCTION
uint64_t Joiner::check_sum(SelectInfo &sel_info, table_t *table) {

    /* to create the final cehcksum column */
    AddColumnToTableT(sel_info, table);
    construct(table);

    const uint64_t* col = table->column_j->values;
    const uint64_t size = table->column_j->size;
    std::cerr << "Size in check sum " << table->relations_row_ids->operator[](0).size() << '\n';
    uint64_t sum = 0;

    for (uint64_t i = 0 ; i < size; i++)
        sum += col[i];

    uint64_t m = 1;
    if (table->cartesian_product != NULL) {
        std::cerr << "Its a cartesian result table" << '\n';
        for (int id: table->cartesian_product->left_relations) {
            if (id == sel_info.relId){
                m = table->cartesian_product->size_of_right_relations;
                break;
            }
        }
        if (m != 1)
            for (int id: table->cartesian_product->right_relations) {
                if (id == sel_info.relId){
                    m = table->cartesian_product->size_of_left_relations;
                    break;
                }
            }
    }

    return sum*m;
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

// Get the total number of relations
int Joiner::getRelationsCount() {
    return relations.size();
}

// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
void Joiner::join(QueryInfo& i) {

    std::vector<FilterInfo>    &filters    = i.filters;
    std::vector<PredicateInfo> &predicates = i.predicates;
    std::vector<SelectInfo>    &selections = i.selections;
    std::vector<table_t*> intermediate_results;
    table_t *table_r;
    table_t *table_s;
    table_t *table;
    table_t *result;
    int id;

    /* For all the predicate infos */
    for (PredicateInfo &predicate: predicates) {

        //std::cerr << "Predicates: " <<  '\n';
        //std::cerr << "Left: "  << predicate.left.relId << "." << predicate.left.colId << '\n';
        //std::cerr << "Right: " << predicate.right.relId << "." << predicate.right.colId << '\n';
        flush(cerr);

        int index_left  = -1;
        int index_right = -1;

        for (int j = 0; j < intermediate_results.size(); j++) {

            table = intermediate_results[j];
            //std::cerr << "Intermediate :" << j <<  " relation ids ( " << table->relation_ids.size() << ") : " << '\n';

            for (size_t jj = 0; jj < table->relation_ids.size(); jj++) {
                id = table->relation_ids[jj];
                std::cerr << '\t' << id << '\n';
                if (id == predicate.left.relId) {
                    index_left  = j;
                } else if (id == predicate.right.relId) {
                    index_right = j;
                }
            }
        }

        //std::cerr << "Intermediate result left index :" << index_left  << '\n';
        //std::cerr << "Intermediate result right index :" << index_right << '\n';
        flush(cerr);

        /* For the left column */
        if (index_left != -1) {
            AddColumnToIntermediatResult(predicate.left, intermediate_results[index_left]);
            table_r = intermediate_results[index_left];
        } else {
            //table_r = SelectInfoToTableT(predicate.left);
            table_r = CreateTableTFromId(predicate.left.relId);
            AddColumnToTableT(predicate.left, table_r);
        }

        /* For the right column */
        if (index_right != -1) {
            AddColumnToIntermediatResult(predicate.right, intermediate_results[index_left]);
            table_s = intermediate_results[index_left];
        } else {
            table_s = SelectInfoToTableT(predicate.right);
        }

        /* Erase the intermediate results from vector */
        if (index_left != -1 && index_left != index_right) {
            intermediate_results.erase(intermediate_results.begin() + index_left);
        } else if (index_right != -1 && index_left != index_right) {
            intermediate_results.erase(intermediate_results.begin() + index_right);
        } else if (index_left != -1 && index_left == index_right) {
            intermediate_results.erase(intermediate_results.begin() + index_left);
        }

        /* Join the tables and push back the new result */
        result = join(table_r, table_s);//join(table_r, table_s);
        //std::cerr << "Intermediate table rows: " << result->relations_row_ids->operator[](0).size() << '\n';
        intermediate_results.push_back(result);

        /* Run a checksum on first selection */
        //std::cerr << "Intermediate checksum on" << selections[0].relId << "." << selections[0].colId;
        //std::cerr << ("%d", check_sum(selections[0], result)) << '\n';
    }

    /* Cartesia join all the intermediate_results */
    result = intermediate_results[0];
    int count = intermediate_results.size();
    for (size_t index = 1; index < count; index++) {
        result = cartesian_join(result, intermediate_results[index]);
    }

    /* Loop all the filter s */
    for (FilterInfo &filter: filters) {
        //std::cerr << "Filter at "<< filter.filterColumn.relId << "." << filter.filterColumn.colId << " ";
        //std::cerr <<  ("%c",filter.comparison) << " " << filter.constant << '\n';
        AddColumnToIntermediatResult(filter.filterColumn, result);
        Select(filter, result);
    }
    //std::cerr << "Resulting table rows: " << result->relations_row_ids->operator[](0).size() << '\n';
    //std::cerr << "---------------------------" << '\n';

    string result_str;
    uint64_t checksum = 0;
    for (size_t i = 0; i < selections.size(); i++) {

        checksum = check_sum(selections[i], result);
        if (checksum == 0) {
            result_str += "NULL";
        } else {
            result_str += std::to_string(checksum);
        }

        if (i != selections.size() - 1) {
            result_str +=  " ";
        }
    }

    /* Print the result */
    cout << result_str << endl;
}

/* +---------------------+
   |The Column functions |
   +---------------------+ */

void PrintColumn(column_t *column) {
    /* Print the column's table id */
    std::cerr << "Column of table " << column->table_index << " and size " << column->size << '\n';

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

    // Preparation phase (not timed)
    // Build histograms, indices,...

    // Create a persistent query graph
    QueryGraph queryGraph(joiner.getRelationsCount());

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    int q_counter = 0;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch

        // Parse the query
        std::cerr << "Q " << q_counter  << ":" << line << '\n';
        i.parseQuery(line);
        q_counter++;

        JTree *jTreePtr = treegen(&i);
        int *plan = NULL, plan_size = 0;
        //print_rec(jTreePtr, 0);
        table_t *result = jTreeMakePlan(jTreePtr, joiner, plan);

        // join
        //joiner.join(i);

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

        /* Print the result */
        std::cout << result_str << endl;
        //std::cout << "Implelemt JOIN " << '\n';
    }

    return 0;
}
