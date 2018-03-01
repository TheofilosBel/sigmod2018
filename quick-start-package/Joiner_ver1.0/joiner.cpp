#include "joiner.hpp"

/*
 * 1)Classic hash_join implementation with unorderd_map(stl)
 * 2)Create hashtable from the row_table with the lowest size
*/ 
joiner_t* join(column_t *column_r, column_t *column_s, joiner_t *joiner) {
	/* create hash_table for the hash_join phase */
	td::unordered_map<int, int> hash_c;
	
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
	joiner->row_ids[column_r->table_id]->resize();
	joiner->row_ids[column_s->table_id]->resize();

	/* now put the values of the column_r in the hash_table */
	for (int i = 0; i < hash_size; i++)
		hash_c.insert({column_r->values[i], i});
	
	/* now the phase of hashing */
	for (int i = 0; i < iter_size; i++) {
		auto search = hash_c.find(column_s[i]);
		/* if we found it */
		if (search != hash_c.end()) {
			/* update both of hte arrays */
			joiner.row_ids[column_r->table_id]->push_back(search->second);
			joiner.row_ids[column_s->table_id]->push_back(i);
		}
	}
	return joiner;
}

/* the impl of construct function */
column_t* construct(column_t *column, joiner_t *joiner) {
  /* TODO Fill in*/
}
