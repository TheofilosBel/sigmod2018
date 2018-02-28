#include "joiner.hpp"

/* The impl of join function */
joiner_t* join(int *column_r, int size_r, int *column_s, int size_s, joiner_t *joiner)
{
   /* TODO Fill in */
}



column_t* construct(column_t *column, joiner_t *joiner) {

  /* Create - Initilize  a new column */
  column_t *new_column = new column_t;
	new_column->table_id = column->table_id;
	new_column->size     = joiner->sizes[column->table_id];
  int *new_values      = new int[new_column-> size];

  /* Pass the values of the old column to the new one, based on the row ids of the joiner */
  int *values_array = column->values;
  for (int element_counter = 0; element_counter < new_column->size; element_counter++) {
		std::cout << "Row id " << joiner->row_ids[column->table_id][element_counter] << '\n';
  	new_values[element_counter] = values_array[joiner->row_ids[column->table_id][element_counter]];
  }

	/* Add the new values to the new column */
	new_column->values = new_values;   /* --- Rember to delete[]  ---- */

	/* Return the new column */
	return new_column;
}



void PrintColumn(column_t *column) {
	/* Print the column's table id */
  std::cout << "Column of table " << column->table_id << " and size " << column->size << '\n';

	/* Iterate over the column's values and print them */
  for (int i = 0; i < column->size; i++) {
    std::cout << column->values[i] << '\n';
  }
}
