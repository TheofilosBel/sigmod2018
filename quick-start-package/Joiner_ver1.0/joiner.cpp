#include "joiner.hpp"
/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
 * 3)Ids E [0,...,size-1]
*/ 
joiner_t* join(column_t *column_r, column_t *column_s, joiner_t *joiner) {
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
	joiner->row_ids[column_r->table_id]->resize(0);
	joiner->row_ids[column_s->table_id]->resize(0);

	/* now put the values of the column_r in the hash_table */
	for (int i = 0; i < hash_size; i++)
		hash_c.insert({column_r->values[i], i});
	
	/* now the phase of hashing */
	for (int i = 0; i < iter_size; i++) {
		auto search = hash_c.find(column_s->values[i]);
		/* if we found it */
		if (search != hash_c.end()) {
		/* update both of row_ids vectors */
			std::cout<<"search result key->"<<search->first<<",val->"<<search->second<<std::endl;
			joiner->row_ids[column_r->table_id]->push_back(search->second);
			joiner->row_ids[column_s->table_id]->push_back(i);
		}
	}
	return joiner;
}



column_t* construct(column_t *column, joiner_t *joiner) {

	/* Create - Initilize  a new column */
	
	/*column_t *new_column = new column_t;
	new_column->table_id = column->table_id;
	new_column->size     = joiner->sizes[column->table_id];
	int *new_values      = new int[new_column-> size];
	*/

	/* Pass the values of the old column to the new one, based on the row ids of the joiner */
	
	/*int *values_array = column->values;
	for (int element_counter = 0; element_counter < new_column->size; element_counter++) {
		std::cout << "Row id " << joiner->row_ids[column->table_id][element_counter] << '\n';
		new_values[element_counter] = values_array[joiner->row_ids[column->table_id][element_counter]];
	}
	*/
	
	/* Add the new values to the new column */
	//new_column->values = new_values;   /* --- Rember to delete[]  ---- */

	/* Return the new column */
	//return new_column;
}



void PrintColumn(column_t *column) {
	/* Print the column's table id */
	std::cout << "Column of table " << column->table_id << " and size " << column->size << '\n';

	/* Iterate over the column's values and print them */
	for (int i = 0; i < column->size; i++) {
		std::cout << column->values[i] << '\n';
  	}
}
