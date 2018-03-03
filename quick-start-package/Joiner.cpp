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

#ifdef def
void Joiner::RowIdArrayInit(QueryInfo &query_info) {

    /* Initilize helping variables */

    /* Initilize the row ids vector */
    row_ids = new std::vector<std::vector<int>> (query_info.relationIds.size());
    //row_ids.resize(query_info.relationIds.size(), std::vector<int>(0));
    std::vector<std::vector<int>>  rowIds = *row_ids;
    int rel_allias = 0;
    for (RelationId relation_Id : query_info.relationIds) {

        /* Each relation at the begging of the query has all its rows */
        int total_row_num = getRelation(relation_Id).size;
        for (int row_n = 0; row_n < total_row_num; row_n++) {
            rowIds[rel_allias].push_back(row_n + 1);
        }

        /* Go to the next relation */
        rel_allias++;
    }

}


void  Joiner::join(PredicateInfo &pred_info) {

    /* Initialize helping variables */


    /* Get the Columns from the predicate
    Relation &left_rel  = getRelation(pred_info.left.relId);
    Relation &right_rel = getRelation(pred_info.right.relId);
    left_column.values  = left_rel.columns.at(pred_info.left.colId);
    right_column.values = right_rel.columns.at(pred_info.right.colId);
    left_column.size  = left_rel.size;
    right_column.size = right_rel.size;
    left_column.table_index  = pred_info.left.binding;
    right_column.table_index = pred_info.right.binding;*/

    /* Construct the columns if needed */
    /* Join the columns */
    low_join();

}

/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
*/
table_t& Joiner::low_join(table_t &table_r, table_t &table_s) {
    /* create hash_table for the hash_join phase */
	std::unordered_multimap<uint64_t, hash_entry> hash_c;
    
    /* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
	uint64_t hash_size,iter_size;
    
    /* first ptr points to values that will use to create the hash_table */
    /* second ptr points to values that will be hashed for join */
    column_t *hash_col;
    column_t *iter_col;
    std::vector<std::vector<int>> &h_rows;
    std::vector<std::vector<int>> &i_rows;
    
    /* the new table_t to continue the joins */
    table_t updated_table_t = new table_t;

    /* check for size to decide wich hash_table to create for the hash join */
	if (table_r.column_j->size <= table_s.column_j->size) {
		hash_size = table_r.column_j->size;
        hash_col = table_r.column_j;
        h_rows = table_r.relations_row_ids;
        
        iter_size = table_s.column_j->size;
        iter_col = table_s.column_j;
        i_rows = table_s.relations_row_ids;
	}
	else {
		hash_size = table_s.column_j->size;
        hash_col = table_s.column_j;
        h_rows = table_s.relations_row_ids;
		
        iter_size = table_r.column_j->size;
        iter_col = table_r.column_j;
        i_rows = table_r.relations_row_ids;
	}

	/* now put the values of the column_r in the hash_table(construction phase) */
	for (uint64_t i = 0; i < hash_size; i++) {
        /* store hash[value of the column] = {rowid, index} */
        hash_entry hs;
        hs.row_id = h_rows[hash_col->table_index][i];
		hs.index = i;
        hash_c.insert({hash_col->values[i], hs});
    }
    /* create the updated relations_row_ids, merge the sizes*/
    updated_table_t.relations_row_ids.resize(h_rows.size()+i_rows.size());

	/* now the phase of hashing */
	for (uint64_t i = 0; i < iter_size; i++) {
        /* remember we may have multi vals in 1 key,if it isnt a primary key */
        /* vals->first = key ,vals->second = value */ 
        auto range_vals = hash_c.equal_range(iter_col->values[i]);
        for(auto vals = range_vals.first; i != range_vals.second; vals++) {
            /* store all the result then push it int the new row ids */
            /* its faster than to push back 1 every time */
            std::vector<int> temp_row_ids;
            /* get the first values from the r's rows ids */
            for (uint64_t j = 0 ; j < h_rows.size(); j++)
                temp_row_ids.push_back(h_rows[j][vals.hs.index]);
            /* then go to the s's row ids to get the values */
            for (uint64_t j = 0 ; j < i_rows.size(); j++)
                temp_row_ids.push_back(i_rows[j][i])
            updated_table_t.relations_row_ids.push_back(temp_row_ids);
        }
	}
    /* concatenate the relaitons ids for the merge */
    updated_table_t.relation_ids.resize(table_r.relation_ids.size()+table_s.relation_ids.size());
	updated_table_t.relation_ids.insert(updated_table_t.relation_ids.end() ,table_r.relation_ids.begin(), table_r.relation_ids.end());
    updated_table_t.relation_ids.insert(updated_table_t.relation_ids.end() ,table_s.relation_ids.begin(), table_s.relation_ids.end());
    return updated_table_t;
}

column_t* Joiner::construct(const column_t *column) {

    /* Innitilize helping variables */
    const uint64_t  col_size = this->sizes[column->table_index];
    const int       col_id   = column->table_index;
    const uint64_t *col_values  = column->values;
    std::vector<std::vector<int>>  rowIds = *row_ids;

    /* Create - Initilize  a new column */
    column_t * const new_column = new column_t;
    new_column->table_index   = column->table_index;
    new_column->size       = col_size;
    uint64_t *const new_values  = new uint64_t[col_size];

    /* Pass the values of the old column to the new one, based on the row ids of the joiner */
    for (int i = 0; i < col_size; i++) {
        //std::cout << "Row id " << joiner->row_ids[column->table_index][i] << '\n';
    	new_values[i] = col_values[rowIds[col_id][i]];
    }

    /* Add the new values to the new column */
    new_column->values = new_values;   /* --- Rember to delete[]  ---- */

    /* Return the new column */
    return new_column;
}

#endif

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
