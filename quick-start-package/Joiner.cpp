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

void  Joiner::join(PredicateInfo pinfo) {



    /* Get the relations from the predicate */
    






}

/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
*/
void Joiner::low_join(column_t *column_r, column_t *column_s) {
	/* create hash_table for the hash_join phase */
	std::unordered_map<int, int> hash_c;

	/* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
	int hash_size,iter_size;

	/* check for size to decide wich hash_table to create for the hash join */
	if (column_r->size <= column_s->size) {
		hash_size = column_r->size;
		iter_size = column_s->size;
	}
	else {
		hash_size = column_s->size;
		iter_size = column_r->size;
	}

	/* wipe out the vectors first */
	/* to store the new ids */
	this->row_ids[column_r->table_id]->resize(0);
	this->row_ids[column_s->table_id]->resize(0);

	/* now put the values of the column_r in the hash_table */
	for (int i = 0; i < hash_size; i++)
		hash_c.insert({column_r->values[i], i});

	/* now the phase of hashing */
	for (int i = 0; i < iter_size; i++) {
		auto search = hash_c.find(column_s->values[i]);
		/* if we found it */
		if (search != hash_c.end()) {
		/* update both of row_ids vectors */
			std::cout << "search result key->" << search->first << ",val->" << search->second << "\n";
			this->row_ids[column_r->table_id]->push_back(search->second);
			this->row_ids[column_s->table_id]->push_back(i);
		}
	}
	return ;
}

column_t* Joiner::construct(const column_t *column) {

    /* Innitilize helping variables */
    const uint64_t col_size = this->sizes[column->table_id];
    const int      col_id   = column->table_id;
    const int     *col_values  = column->values;
    std::vector<int> **const row_ids = this->row_ids;

    /* Create - Initilize  a new column */
    column_t * const new_column = new column_t;
    new_column->table_id   = column->table_id;
    new_column->size       = col_size;
    int *const new_values  = new int[new_column->size];


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
