#include "Joiner.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Parser.hpp"

using namespace std;


/* +---------------------+
   |The joiner functions |
   +---------------------+ */

table_t& Joiner::Select(SelectInfo &sel_info) {

    /* Create table */
    table_t &table;
    uint64_t filter = sel_info.constant;

    if (sel_info.comparison == FilterInfo::Comparison::Less) {
        SelectLess(table, filter);
    } else if (sel_info.comparison == FilterInfo::Comparison::Greater) {
        SelectGreater(table, filter);
    } else if (sel_info.comparison == FilterInfo::Comparison::Equal) {
        SelectEqual(table, filter);
    }

}

table_t& Joiner::SelectEqual(table_t &table, int filter) {

    /* Initialize helping variables */
    uint64_t *const values = table.column_j->values;
    int table_index = table.column_j->table_index;

    const uint64_t rel_num = table.relations_row_ids.size();
    std::vector<std::vector<int>> &old_row_ids = table.relations_row_ids;
    const uint64_t size = table.relations_row_ids[table.column_j->table_index].size();
    std::vector<std::vector<int>> new_row_ids = new std::vector<std::vector<int>>(rel_num);

    /* Loop to cut down the values */
    for (size_t index = 0; index < size; index++) {

        if (values[old_row_ids[table_index][index]] == filter) {
            std::vector<int> row(rel_num);
            for (size_t i = 0; i < rel_num; i++) {
                row[i] = old_row_ids[i][index];
            }
            new_row_ids.push_back(row);
        }
    }

    /* Swap the old vectors with the new ones */


}

table_t& Joiner::SelectGreater(table_t &table, int filter){

}

table_t& Joiner::SelectLess(table_t &table, int filter){

}

void Joiner::AddColumnToIntermediatResult(SelectInfo &sel_info, table_t &table) {

    /* Only for intermediate */
    if (!table.intermediate_res) return;

    /* Create a new column_t for table */
    if (table.column_j == NULL)
        table.column_j = new column_t;
    column_t &column = *table.column_j;
    std::vector<int> &relation_ids = table.relation_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    column.size   = rel.size;
    column.values = rel.columns.at(sel_info.colId);
    column.table_index = -1;
    RelationId relation_id = sel_info.colId;

    /* Get the right index from the relation id table */
    for (size_t index = 0; index < relation_ids.size(); index++) {
        if (relation_ids[index] = relation_id){
            column.table_index = index;
        }
    }

    /* Error msg for debuging */
    if (column.table_index == -1) {
        std::cerr << "At AddColumnToIntermediatResult, Id not matchin with intermediate result vectors" << '\n';
    }
}

table_t* Joiner::PredicateToTableT(PredicateInfo &pred_info) {

    /* Crate - Initialize array that holds 2 table_t */
    table_t *const table_t_ptr = new table_t[2];

    table_t_ptr[0].intermediate_res = false;
    table_t_ptr[0].column_j = new column_t;
    column_t &left_column = *table_t_ptr[0].column_j;
    std::vector<std::vector<int>> &left_rel_row_ids = table_t_ptr[0].relations_row_ids;

    table_t_ptr[1].intermediate_res = false;
    table_t_ptr[1].column_j = new column_t;
    column_t &right_column = *table_t_ptr[1].column_j;
    std::vector<std::vector<int>> &right_rel_row_ids = table_t_ptr[1].relations_row_ids;

    /* Get the needed data from the predicate */
    Relation &left_rel  = getRelation(pred_info.left.relId);
    Relation &right_rel = getRelation(pred_info.right.relId);

    left_column.values  = left_rel.columns.at(pred_info.left.colId);
    left_column.size  = left_rel.size;
    left_column.table_index  = 0;

    right_column.values = right_rel.columns.at(pred_info.right.colId);
    right_column.size = right_rel.size;
    right_column.table_index = 1;

    /* Create the relations_row_ids and relation_ids vectors */
    uint64_t left_rel_size  = left_column.size;
    uint64_t right_rel_size = right_column.size;

    left_rel_row_ids.resize(1);
    right_rel_row_ids.resize(1);
    for (size_t i = 0;  i < left_rel_size || i < right_rel_size; i++) {
        if (i < right_rel_size && i < left_rel_size) {
            left_rel_row_ids[0].push_back(i);
            right_rel_row_ids[0].push_back(i);
        } else if (i < right_rel_size) {
            right_rel_row_ids[0].push_back(i);
        } else {
            left_rel_row_ids[0].push_back(i);
        }
    }

    table_t_ptr[0].relation_ids.push_back(pred_info.left.relId);
    table_t_ptr[1].relation_ids.push_back(pred_info.right.relId);

    return table_t_ptr;
}

void  Joiner::join(table_t &table_r, table_t &table_s) {

    /* Remember that here we need the columns to be present */

    /* Construct the tables in case of intermediate results */
    (table_r.intermediate_res)? (construct(table_r)) : ((void)0);
    (table_s.intermediate_res)? (construct(table_s)) : ((void)0);


    /* Join the columns */
    //low_join();

}


#ifdef def

/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
*/
void Joiner::low_join(table_t &table_r, table_t &table_s) {
    /* create hash_table for the hash_join phase */
	std::unordered_map<uint64_t, uint64_t> hash_c;
	/* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
	uint64_t hash_size,iter_size;
    /* first ptr points to values that will use to create the hash_table */
    /* second ptr points to values that will be hashed for join */
    column_t *hash_col;
    column_t *iter_col;

	/* check for size to decide wich hash_table to create for the hash join */
	if (left_column.size <= right_column.size) {
		hash_size = left_column.size;
        hash_col = &left_column;
        iter_size = right_column.size;
        iter_col = &right_column;
	}
	else {
		hash_size = right_column.size;
        hash_col = &right_column;
		iter_size = left_column.size;
        iter_col = &left_column;
	}

	/* wipe out the vectors first */
	/* to store the new ids */
    std::vector<std::vector<int>>  rowIds = *row_ids;
	rowIds[hash_col->].resize(0);
	rowIds[iter_col->table_index].resize(0);

	/* now put the values of the column_r in the hash_table */
	for (uint64_t i = 0; i < hash_size; i++)
		hash_c.insert({hash_col->values[i], i});

	/* now the phase of hashing */
	for (uint64_t i = 0; i < iter_size; i++) {
        auto search = hash_c.find(iter_col->values[i]);
		/* if we found it */
		if (search != hash_c.end()) {
		/* update both of rowIds vectors */
			//std::cerr << "search result key->" << search->first << ",val->" << search->second << "\n";
			rowIds[hash_col->table_index].push_back(search->second);
			rowIds[iter_col->table_index].push_back(i);

            /* Use a vector row for one line push_back */
		}
	}
	return ;
}

#endif

void Joiner::construct(table_t &table) {

    /* Innitilize helping variables */
    column_t &column = *table.column_j;
    const uint64_t *column_values = column.values;
    const int       table_index   = column.table_index;
    const uint64_t  column_size   = table.relations_row_ids[table_index].size();
    std::vector<std::vector<int>> row_ids = table.relations_row_ids;

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

// Hashes a value and returns a check-sum
// The check should be NULL if there is no qualifying tuple
void Joiner::join(QueryInfo& i) {
    //TODO implement

    /* Call a construct function
    table_t *tables = PredicateToTableT(i.predicates[0]);
    table_t &table_ref = tables[0];
    construct(table_ref);
    */

    cout << "Implement join..." << endl;
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

    // The test harness will send the first query after 1 second.
    QueryInfo i;
    while (getline(cin, line)) {
        if (line == "F") continue; // End of a batch
        i.parseQuery(line);
        joiner.join(i);
    }

    return 0;
}
