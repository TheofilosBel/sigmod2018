#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <stdlib>
#include "Parser.hpp"
#include "QueryPlan.hpp"
#include "header.hpp"

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
double timeBuildPhase = 0;
double timeProbePhase = 0;

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
    column_t * column    = CreateColumn(sel_info, this); 
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

    uint64_t **old_row_ids = *table->relations_row_ids;
    uint64_t **new_row_ids = (uint64_t**)malloc(sizeof(uint64_t*)*size_rids);
    std::vector<unsigned> &rbs = relations_bindings;
    
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
    for (size_t rid = 0; rd < size_rids; rid++) {
        /* go to the old row ids and take the row id of the rid val */
        if (values[old_row_rids[rid][table_index]] == filter) {
            /* Allocate space for the relations storing */
            new_row_ids[new_tb_index] = old_row_ids[rid][table_index];
            new_tb_index++;
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}

void Joiner::SelectGreater(table_t *table, column_t *column, int filter){
    /* Initialize helping variables */
    uint64_t *const values  = column->values;
    int table_index         = column->binding;
    const uint64_t size_rids  = table->size_of_row_ids;

    uint64_t **old_row_ids = *table->relations_row_ids;
    uint64_t **new_row_ids = (uint64_t**)malloc(sizeof(uint64_t*)*size_rids);
    std::vector<unsigned> &rbs = relations_bindings;
    
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
    for (size_t rid = 0; rd < size_rids; rid++) {
        /* go to the old row ids and take the row id of the rid val */
        if (values[old_row_rids[rid][table_index]] > filter) {
            /* Allocate space for the relations storing */
            new_row_ids[new_tb_index] = old_row_ids[rid][table_index];
            new_tb_index++;
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
}


void Joiner::SelectLess(table_t *table, column_t *column, int filter){
 
    /* Initialize helping variables */
    uint64_t *const values  = column->values;
    int table_index         = column->binding;
    const uint64_t size_rids  = table->size_of_row_ids;

    uint64_t **old_row_ids = *table->relations_row_ids;
    uint64_t **new_row_ids = (uint64_t**)malloc(sizeof(uint64_t*)*size_rids);
    std::vector<unsigned> &rbs = relations_bindings;
    
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
    for (size_t rid = 0; rd < size_rids; rid++) {
        /* go to the old row ids and take the row id of the rid val */
        if (values[old_row_rids[rid][table_index]] < filter) {
            /* Allocate space for the relations storing */
            new_row_ids[new_tb_index] = old_row_ids[rid][table_index];
            new_tb_index++;
        }
    }

    /* Swap the old vector with the new one */
    delete table->relations_row_ids;
    table->relations_row_ids = new_row_ids;
    table->intermediate_res = true;
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
    table_t *const table_t_ptr = (table_t *) malloc(sizeof(table_t));
    table_t_ptr->intermediate_res = false;
    table_t_ptr->row_ids       = (uint64_t **) malloc(sizeof(uint64_t*) * rel.size);

    uint64_t** row_ids = table_t_ptr->row_ids;
    for (int i = 0; i < rel.size; ++i) {
        row_ids[i] = (uint64_t *) malloc(sizeof(uint64_t));  // Malloc one uint64 per row 
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

#ifdef nono
/* The self Join Function */
table_t * Joiner::SelfJoin(table_t *table, PredicateInfo *predicate_ptr) {

#ifdef time
    struct timeval start;
    gettimeofday(&start, NULL);
#endif

    /* Create - Initialize a new table */
    table_t *new_table            = new table_t;
    new_table->relation_ids       = std::vector<int>(table->relation_ids);
    new_table->relations_bindings = std::vector<unsigned>(table->relations_bindings);
    new_table->relations_row_ids  = new std::vector<std::vector<int>>;
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
        new_row_ids_matrix.push_back(std::vector<int>());
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
#endif

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

    // Get the needed info of every column
    queryPlan.fillColumnInfo(joiner);

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    int q_counter = 0;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch

        // Parse the query
        //std::cerr << q_counter  << ": " << line << '\n';
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
