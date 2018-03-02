#include "joiner.hpp"
/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
*/
void join(const column_t *column_r, const column_t *column_s, joiner_t *joiner) {
	/* create hash_table for the hash_join phase */
	std::unordered_map<int, int> hash_c;
	std::vector<int> ** const row_ids  = joiner->row_ids;

	/* hash_size->size of the hashtable,iter_size->size to iterate over to find same vals */
	uint64_t hash_size,iter_size;

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
	row_ids[column_r->table_id]->resize(0);
	row_ids[column_s->table_id]->resize(0);

	/* now put the values of the column_r in the hash_table */
	for (uint64_t i = 0; i < hash_size; i++)
		hash_c.insert({column_r->values[i], i});

	/* now the phase of hashing */
	for (uint64_t i = 0; i < iter_size; i++) {
		auto search = hash_c.find(column_s->values[i]);
		/* if we found it */
		if (search != hash_c.end()) {
		/* update both of row_ids vectors */
			std::cout << "search result key->" << search->first << ",val->" << search->second << "\n";
			row_ids[column_r->table_id]->push_back(search->second);
			row_ids[column_s->table_id]->push_back(i);
		}
	}
}

column_t* construct(const column_t *column, const joiner_t *joiner) {

    /* Innitilize helping variables */
    const uint64_t col_size = joiner->sizes[column->table_id];
    const int      col_id   = column->table_id;
    const int * col_values  = column->values;
    std::vector<int> ** const row_ids  = joiner->row_ids;

    /* Create - Initilize  a new column */
    column_t * const new_column = new column_t;
    new_column->table_id    = column->table_id;
    new_column->size        = col_size;
    int * const new_values  = new int[new_column->size];


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


/* +---------------------+
   |The joiner functions |
   +---------------------+ */

void JoinerDestroy(joiner_t *joiner) {
    /* TODO Fill in bases on joiner */
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
