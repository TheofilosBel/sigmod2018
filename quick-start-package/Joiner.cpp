#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
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

    /* Create table */
    SelectInfo &sel_info = fil_info.filterColumn;
    uint64_t filter = fil_info.constant;

    if (fil_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, filter);
    } else if (fil_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, filter);
    }

    return table;
}

void Joiner::SelectEqual(table_t *table, int filter) {
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    std::vector<std::vector<int>> &old_row_ids = *table->relations_row_ids;
    std::vector<std::vector<int>> *new_row_ids = new std::vector<std::vector<int>>;

    const uint64_t size     = old_row_ids[table_index].size();

    /* Update the row ids of the table */
    for (size_t index = 0; index < size; index++) {
        if (values[old_row_ids[table_index][index]] == filter) {
            new_row_ids->push_back(std::vector<int>());
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
    std::vector<std::vector<int>> *new_row_ids = new std::vector<std::vector<int>>;

    const uint64_t size     = old_row_ids[table_index].size();

    /* Update the row ids of the table */
    for (size_t index = 0; index < size; index++) {
        if (values[old_row_ids[table_index][index]] > filter) {
            new_row_ids->push_back(std::vector<int>());
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
    std::vector<std::vector<int>> *new_row_ids = new std::vector<std::vector<int>>;

    const uint64_t size     = old_row_ids[table_index].size();

    /* Update the row ids of the table */
    for (size_t index = 0; index < size; index++) {
        if (values[old_row_ids[table_index][index]] < filter) {
            new_row_ids->push_back(std::vector<int>());
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

void Joiner::AddColumnToIntermediatResult(SelectInfo &sel_info, table_t *table) {

    /* Only for intermediate */
    if (!table->intermediate_res) return;

    /* Create a new column_t for table */
    if (table->column_j == NULL)
        table->column_j = new column_t;
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
        }
    }

    /* Error msg for debuging */
    if (column.table_index == -1) {
        std::cerr << "At AddColumnToIntermediatResult, Id not matchin with intermediate result vectors" << '\n';
    }
}

table_t* Joiner::SelectInfoToTableT(SelectInfo &sel_info) {

    /* Crate - Initialize array that holds 2 table_t */
    table_t *const table_t_ptr = new table_t;

    table_t_ptr->intermediate_res = false;
    table_t_ptr->column_j = new column_t;
    table_t_ptr->relations_row_ids = new std::vector<std::vector<int>>;

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

    return table_t_ptr;
}

table_t* Joiner::join(table_t *table_r, table_t *table_s) {
    /* Construct the tables in case of intermediate results */
    (table_r->intermediate_res)? (construct(table_r)) : ((void)0);
    (table_s->intermediate_res)? (construct(table_s)) : ((void)0);
    /* Join the columns */
    return low_join(table_r, table_s);
}

//#ifdef def
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
                updated_table_t->relations_row_ids->push_back(std::vector<int>());

                /* get the first values from the r's rows ids */
                for (uint64_t j = 0 ; j < h_rows.size(); j++)
                    update_row_ids[j].push_back(h_rows[j][vals->second.index]);

                /* then go to the s's row ids to get the values */
                for (uint64_t j = 0; j < i_rows.size(); j++)
                    update_row_ids[j + h_rows.size()].push_back(i_rows[j][i]);
            }
        }
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
    }
    /* concatenate the relaitons ids for the merge */
    updated_table_t->relation_ids.reserve(table_r->relation_ids.size()+table_s->relation_ids.size());
    updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_r->relation_ids.begin(), table_r->relation_ids.end());
    updated_table_t->relation_ids.insert(updated_table_t->relation_ids.end() ,table_s->relation_ids.begin(), table_s->relation_ids.end());

    /* do the cleaning */
    /* delete table_r->relations_row_ids;
    delete table_r;
    delete table_s->relations_row_ids;
    delete table_s; */
    return updated_table_t;
}
//#endif


void Joiner::construct(table_t *table) {

    /* Innitilize helping variables */
    column_t &column = *table->column_j;
    const uint64_t *column_values = column.values;
    const int       table_index   = column.table_index;
    const uint64_t  column_size   = table->relations_row_ids[table_index].size();
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

    // print result to std::cout
    cout << "Implement join..." << endl;

    /* Call a construct function */
    table_t *table_r = SelectInfoToTableT(i.predicates[0].left);
    table_t *table_s = SelectInfoToTableT(i.predicates[0].right);

    std::cerr << "Left Table " << table_r->relation_ids[0]  << "." << table_r->column_j->id;
    std::cerr << " rows " << table_r->column_j->size << '\n';
    std::cerr << "Right Table " << table_s->relation_ids[0]  << "." << table_s->column_j->id;
    std::cerr << " rows " << table_s->column_j->size << '\n';

    table_t *result = join(table_r, table_s);

    /* if there exists a filter fo it */
    if (!i.filters.empty()) {
        FilterInfo &filter = i.filters[0];

        /* Take the filter and apply it to the table to the */
        filt

    }

    std::cerr << "Intermediate Table " << result->relation_ids[0] << "&" << result->relation_ids[1];
    std::cerr << " rows " << result->relations_row_ids->operator[](0).size() << '\n';
    std::cerr << "-----------------------------------------------------" << '\n';

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
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch

        // Parse the query
        i.parseQuery(line);

        JTree *jTreePtr = treegen(&i);

        int *plan = NULL, plan_size = 0;
        plan = jTreeMakePlan(jTreePtr, &plan_size, joiner);
        cout << "Implement join..." << endl;
    }

    return 0;
}
