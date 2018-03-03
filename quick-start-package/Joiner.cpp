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

void Joiner::RowIdArrayInit(QueryInfo &query_info) {

    /* Initilize helping variables */

    /* Initilize the row ids vector */
    row_ids = new std::vector<int>*[query_info.relationIds.size()];
    int rel_allias = 0;
    for (RelationId relation_Id : query_info.relationIds) {

        /* Each relation at the begging of the query has all its rows */
        int total_row_num = getRelation(relation_Id).size;
        row_ids[rel_allias] = new std::vector<int>;
        row_ids[rel_allias]->reserve(total_row_num);  // Rerve the space , instead of pushing back

        for (int row_n = 0; row_n < total_row_num; row_n++) {
            row_ids[rel_allias]->push_back(row_n + 1);
        }

        /* Go to the next relation */
        rel_allias++;
    }

}

void  Joiner::join(PredicateInfo &pred_info) {

    /* Initialize helping variables */


    /* Get the Columns from the predicate */
    Relation &left_rel  = getRelation(pred_info.left.relId);
    Relation &right_rel = getRelation(pred_info.right.relId);
    left_column.values  = left_rel.columns.at(pred_info.left.colId);
    right_column.values = right_rel.columns.at(pred_info.right.colId);
    left_column.size  = left_rel.size;
    right_column.size = right_rel.size;
    left_column.table_id  = pred_info.left.binding;
    right_column.table_id = pred_info.right.binding;

    /* Construct the columns if needed */
    
    /* Join the columns */
    low_join();

}

/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
*/
void Joiner::low_join() {
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
	row_ids[hash_col->table_id]->resize(0);
	row_ids[iter_col->table_id]->resize(0);

	/* now put the values of the column_r in the hash_table */
	for (uint64_t i = 0; i < hash_size; i++)
		hash_c.insert({hash_col->values[i], i});
	/* now the phase of hashing */
	for (uint64_t i = 0; i < iter_size; i++) {
        auto search = hash_c.find(iter_col->values[i]);
		/* if we found it */
		if (search != hash_c.end()) {
		/* update both of row_ids vectors */
			//std::cerr << "search result key->" << search->first << ",val->" << search->second << "\n";
			row_ids[hash_col->table_id]->push_back(search->second);
			row_ids[iter_col->table_id]->push_back(i);
		}
	}
	return ;
}

column_t* Joiner::construct(const column_t *column) {

    /* Innitilize helping variables */
    const uint64_t  col_size = this->sizes[column->table_id];
    const int       col_id   = column->table_id;
    const uint64_t *col_values  = column->values;
    std::vector<int> **const row_ids = this->row_ids;

    /* Create - Initilize  a new column */
    column_t * const new_column = new column_t;
    new_column->table_id   = column->table_id;
    new_column->size       = col_size;
    uint64_t *const new_values  = new uint64_t[col_size];

    /* Pass the values of the old column to the new one, based on the row ids of the joiner */
    for (int i = 0; i < col_size; i++) {
        //std::cout << "Row id " << joiner->row_ids[column->table_id][i] << '\n';
    	new_values[i] = col_values[row_ids[col_id]->at(i)];
    }

    /* Add the new values to the new column */
    new_column->values = new_values;   /* --- Rember to delete[]  ---- */

    /* Return the new column */
    return new_column;
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
    // print result to std::cout

    /* Initilize the row_ids of the joiner */
    RowIdArrayInit(i);

    /* Take the first predicate and put it through our function */
    join(i.predicates[0]);
    cout << "Implement join..." << endl;
}


/* +---------------------+
   |The Column functions |
   +---------------------+ */

void PrintColumn(column_t *column) {
    /* Print the column's table id */
    std::cout << "Column of table " << column->table_id << " and size " << column->size << '\n';

    /* Iterate over the column's values and print them */
    for (int i = 0; i < column->size; i++) {
        std::cout << column->values[i] << '\n';
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
