#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Parser.hpp"
#include "QueryPlan.hpp"
#include "header.hpp"
#include "RadixJoin.hpp"

// #define time

using namespace std;

/* Timing variables */
double timeSelfJoin = 0;
double timeSelectFilter = 0;
double timeLowJoin = 0;
double timeCreateTable = 0;
double timeAddColumn = 0;
double timeTreegen = 0;
double timeCheckSum = 0;
double timeConstruct = 0;

extern double timePartition;
extern double timeBuildPhase;
extern double timeProbePhase;

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

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelectFilter += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

}

void Joiner::SelectEqual(table_t *table, int filter) {
    /* Initialize helping variables */
    uint64_t *const values  = table->column_j->values;
    int table_index         = table->column_j->table_index;
    const uint64_t rel_num  = table->relations_row_ids->size();

    matrix & old_row_ids = *table->relations_row_ids;
    const uint64_t size  = old_row_ids[table_index].size();
    matrix * new_row_ids = new matrix(rel_num);
    new_row_ids->at(0).reserve(size/2);

    /* Update the row ids of the table */
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

    matrix & old_row_ids = *table->relations_row_ids;
    const uint64_t size  = old_row_ids[table_index].size();
    matrix * new_row_ids = new matrix(rel_num);
    new_row_ids->at(0).reserve(size/2);

    /* Update the row ids of the table */

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

    matrix & old_row_ids = *table->relations_row_ids;
    const uint64_t size  = old_row_ids[table_index].size();
    matrix * new_row_ids = new matrix(rel_num);
    new_row_ids->at(0).reserve(size/2);

    /* Update the row ids of the table */
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

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Create a new column_t for table */
    column_t &column = *table->column_j;
    std::vector<unsigned> &relation_mapping = table->relations_bindings;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    column.size   = rel.size;
    column.values = rel.columns.at(sel_info.colId);
    column.id     = sel_info.colId;
    column.table_index = -1;
    unsigned relation_binding = sel_info.binding;


    /* Get the right index from the relation id table */
    for (size_t index = 0; index < relation_mapping.size(); index++) {
        if (relation_mapping[index] == relation_binding){
            column.table_index = index;
            break;
        }
    }


    /* Error msg for debuging */
    if (column.table_index == -1)
        std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors" << '\n';

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeAddColumn += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

}

table_t* Joiner::CreateTableTFromId(unsigned rel_id, unsigned rel_binding) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Get the relation */
    Relation &rel  = getRelation(rel_id);

    /* Crate - Initialize a table_t */
    table_t *const table_t_ptr = new table_t;
    table_t_ptr->column_j = new column_t;
    table_t_ptr->intermediate_res = false;
    table_t_ptr->relations_row_ids = new matrix(1, j_vector(rel.size));
    matrix & rel_row_ids = *table_t_ptr->relations_row_ids;

    /* Create the relations_row_ids and relation_ids vectors */
    uint64_t rel_size  = rel.size;
    for (size_t i = 0; i < rel_size; i++) {
        rel_row_ids[0][i] = i;
    }

    /* Keep a mapping with the rowids table and the relaito ids na bindings */
    table_t_ptr->relation_ids.push_back(rel_id);
    table_t_ptr->relations_bindings.push_back(rel_binding);

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeCreateTable += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    return table_t_ptr;
}

table_t* Joiner::join(table_t *table_r, table_t *table_s) {

    /* Construct the tables in case of intermediate results */
    //std::cerr << "HERE 1" << '\n';
    //flush(cerr);
    (table_r->intermediate_res)? (construct(table_r)) : ((void)0);
    (table_s->intermediate_res)? (construct(table_s)) : ((void)0);
    //std::cerr << "HERE 1" << '\n';
    //flush(cerr);

    /* Join the columns */
    //table_t * intermediate_result  = low_join(table_r, table_s);
    table_t * intermediate_result =  radix_join(table_r, table_s);

    //std::cerr << "HERE 2" << '\n';
    //flush(cerr);

    /* Free some results */
    (table_r->intermediate_res)? (delete table_r->column_j->values) : ((void)0);
    delete table_r->relations_row_ids;
    delete table_r;

    (table_s->intermediate_res)? (delete table_s->column_j->values) : ((void)0);
    delete table_s->relations_row_ids;
    delete table_s;

    //std::cerr << "HERE 2" << '\n';
    //flush(cerr);

    return intermediate_result;
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
    new_table->relations_row_ids  = new matrix;
    new_table->intermediate_res   = true;
    new_table->column_j           = new column_t;

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
    int relations_num            = table->relations_bindings.size();

    for (ssize_t index = 0; index < relations_num ; index++) {

        if (predicate_ptr->left.binding == table->relations_bindings[index]) {
            index_l = index;
        }
        if (predicate_ptr->right.binding == table->relations_bindings[index]){
            index_r = index;
        }

        /* Initialize the new matrix */
        new_row_ids_matrix.push_back(j_vector());
    }

#ifdef com
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

#ifdef time
    struct timeval end;
    gettimeofday(&end, NULL);
    timeSelfJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
#endif

    /*Delete old table_t */
    //delete table->relations_row_ids;

    return new_table;
}


/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
 * 4)Made the code repeatable to put some & in the arrays of row ids
*/
table_t* Joiner::low_join(table_t *table_r, table_t *table_s) {

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


    /* check on wich table will create the hash_table */
    if (table_r->column_j->size <= table_s->column_j->size) {
        hash_size = table_r->column_j->size;
        hash_col = table_r->column_j;
        matrix &h_rows = *table_r->relations_row_ids;

        iter_size = table_s->column_j->size;
        iter_col = table_s->column_j;
        matrix &i_rows = *table_s->relations_row_ids;

#ifdef time
        struct timeval start_build;
        gettimeofday(&start_build, NULL);
#endif

        /* now put the values of the column_r in the hash_table(construction phase) */
        for (uint64_t i = 0; i < hash_size; i++) {
            /* store hash[value of the column] = {rowid, index} */
            hash_entry hs;
            hs.row_id = h_rows[hash_col->table_index][i];
            hs.index = i;
            hash_c.insert({hash_col->values[i], hs});
        }


#ifdef time
        struct timeval end_build;
        gettimeofday(&end_build, NULL);
        timeBuildPhase += (end_build.tv_sec - start_build.tv_sec) + (end_build.tv_usec - start_build.tv_usec) / 1000000.0;

        struct timeval start_probe;
        gettimeofday(&start_probe, NULL);
#endif
        /* create the updated relations_row_ids, merge the sizes*/
        updated_table_t->relations_row_ids = new matrix(h_rows.size()+i_rows.size());
        uint64_t  allocated_size = (hash_size < iter_size ) ? (uint64_t)(hash_size) : (uint64_t)(iter_size);
        for (size_t relation = 0; relation < h_rows.size()+i_rows.size(); relation++) {
                updated_table_t->relations_row_ids->operator[](relation).reserve(allocated_size);
        }
        matrix &update_row_ids = *updated_table_t->relations_row_ids;

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
                    update_row_ids[j].emplace_back(h_rows[j][vals->second.index]);

                /* then go to the s's row ids to get the values */
                for (uint64_t j = 0; j < i_rows.size(); j++)
                    update_row_ids[j + h_rows.size()].emplace_back(i_rows[j][i]);
            }
        }

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
        hash_size = table_s->column_j->size;
        hash_col = table_s->column_j;
        matrix &h_rows = *table_s->relations_row_ids;

        iter_size = table_r->column_j->size;
        iter_col = table_r->column_j;
        matrix &i_rows = *table_r->relations_row_ids;

        /* now put the values of the column_r in the hash_table(construction phase) */
        for (uint64_t i = 0; i < hash_size; i++) {
            /* store hash[value of the column] = {rowid, index} */
            hash_entry hs;
            hs.row_id = h_rows[hash_col->table_index][i];
            hs.index = i;
            hash_c.insert({hash_col->values[i], hs});
        }
#ifdef time
        struct timeval end_build;
        gettimeofday(&end_build, NULL);
        timeBuildPhase += (end_build.tv_sec - start_build.tv_sec) + (end_build.tv_usec - start_build.tv_usec) / 1000000.0;

        struct timeval start_probe;
        gettimeofday(&start_probe, NULL);
#endif
        /* create the updated relations_row_ids, merge the sizes*/
        updated_table_t->relations_row_ids = new matrix(h_rows.size()+i_rows.size());
        uint64_t  allocated_size = (hash_size < iter_size ) ? (uint64_t)(hash_size) : (uint64_t)(iter_size);
        for (size_t relation = 0; relation < h_rows.size()+i_rows.size(); relation++) {
                updated_table_t->relations_row_ids->operator[](relation).reserve(allocated_size);
        }
        //updated_table_t->relations_row_ids->resize(h_rows.size()+i_rows.size(), std::vector<uint64_t>());
        matrix &update_row_ids = *updated_table_t->relations_row_ids;

        /* now the phase of hashing */
        for (uint64_t i = 0; i < iter_size; i++) {
            /* remember we may have multi vals in 1 key,if it isnt a primary key */
            /* vals->first = key ,vals->second = value */
            auto range_vals = hash_c.equal_range(iter_col->values[i]);
            for(auto &vals = range_vals.first; vals != range_vals.second; vals++) {
                /* store all the result then push it int the new row ids */
                /* its faster than to push back 1 every time */

                for (uint64_t j = 0 ; j < h_rows.size(); j++)
                    update_row_ids[j].emplace_back(h_rows[j][vals->second.index]);

                /* then go to the s's row ids to get the values */
                for (uint64_t j = 0; j < i_rows.size(); j++)
                    update_row_ids[j + h_rows.size()].emplace_back(i_rows[j][i]);
            }
        }
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
    updated_table_t->column_j = new column_t;

    /* do the cleaning */
    delete table_r->relations_row_ids;
    delete table_s->relations_row_ids;

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
    matrix &row_ids = *table->relations_row_ids;

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

//CHECK SUM FUNCTION
uint64_t Joiner::check_sum(SelectInfo &sel_info, table_t *table) {

    /* to create the final cehcksum column */
    AddColumnToTableT(sel_info, table);
    construct(table);

    const uint64_t* col = table->column_j->values;
    const uint64_t size = table->column_j->size;
    uint64_t sum = 0;

    for (uint64_t i = 0 ; i < size; i++)
        sum += col[i];

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
    QueryPlan queryPlan;

    /*
      somehow collect relationships stats
    */

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    int q_counter = 0;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch

        // Parse the query
        std::cerr << q_counter  << ": " << line << '\n';
        i.parseQuery(line);
        q_counter++;

        #ifdef time
        struct timeval start;
        gettimeofday(&start, NULL);
        #endif

        JTree *jTreePtr = treegen(&i);

        #ifdef time
        struct timeval end;
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
        //std::cout << "Implelemt JOIN " << '\n';
    }

    #ifdef time
    std::cerr << "timeSelectFilter: " << (long)(timeSelectFilter * 1000) << endl;
    std::cerr << "timeSelfJoin: " << (long)(timeSelfJoin * 1000) << endl;
    std::cerr << "timeLowJoin: " << (long)(timeLowJoin * 1000) << endl;
    std::cerr << "    timeConstruct: " << (long)(timeConstruct * 1000) << endl;
    std::cerr << "    timeBuildPhase: " << (long)(timeBuildPhase * 1000) << endl;
    std::cerr << "    timeProbePhase: " << (long)(timeProbePhase * 1000) << endl;
    std::cerr << "timeAddColumn: " << (long)(timeAddColumn * 1000) << endl;
    std::cerr << "timeCreateTable: " << (long)(timeCreateTable * 1000) << endl;
    std::cerr << "timeTreegen: " << (long)(timeTreegen * 1000) << endl;
    std::cerr << "timeCheckSum: " << (long)(timeCheckSum * 1000) << endl;
    std::cerr << "timeConstruct: " << (long)(timeConstruct * 1000) << endl;
    flush(std::cerr);
    #endif

    return 0;
}
