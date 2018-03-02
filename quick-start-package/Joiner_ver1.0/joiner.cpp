#include "joiner.hpp"

/* The impl of join function */
joiner_t* join(int *column_r, int size_r, int *column_s, int size_s, joiner_t *joiner)
{
   /* TODO Fill in */
}

column_t* construct(column_t *column, joiner_t *joiner) {

    /* Innitilize helping variables */
    const uint64_t col_size = joiner->sizes[column->table_id];
    const int      col_id   = column->table_id;
    const int * col_values  = column->values;
    int ** const   row_ids  = joiner->row_ids;

    /* Create - Initilize  a new column */
    column_t * const new_column = new column_t;
    new_column->table_id       = column->table_id;
    new_column->size           = col_size;
    int * const new_values  = new int[new_column->size];


    /* Pass the values of the old column to the new one, based on the row ids of the joiner */
    for (int i = 0; i < col_size; i++) {
        //std::cout << "Row id " << joiner->row_ids[column->table_id][i] << '\n';
    	new_values[i] = col_values[row_ids[col_id][i]];
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
