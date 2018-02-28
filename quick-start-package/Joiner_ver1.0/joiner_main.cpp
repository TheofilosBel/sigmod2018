#include "iostream"
#include "joiner.hpp"


int main(int argc, char const *argv[]) {

  /* Create a joiner and a column*/
  joiner_t *joiner = new joiner_t;
  column_t *column = new column_t;

  /* Initilize the joiner / column */
  joiner->row_ids     = new int* [2];
  joiner->row_ids[0]     = new int[3]; joiner->row_ids[1]     = new int[3];
  joiner->row_ids[0][0] = 1 ; joiner->row_ids[0][1] = 2; joiner->row_ids[0][2] = 3;
  joiner->row_ids[1][0] = 3 ; joiner->row_ids[1][1] = 4; joiner->row_ids[1][2] = 5;
  joiner->sizes       = new int[2];
  joiner->sizes[0] = 3; joiner->sizes[1] = 3;
  column->values      = new int[10];
  for (int i = 0; i < 10; i++) {
    column->values[i] = i;
  }
  column->table_id    = 1;
  column->size        = 10;

  PrintColumn(column);

  /* Call the construct function */
  column_t *constructed_columnt = construct(column, joiner);

  /* Print the column */
  PrintColumn(constructed_columnt);

  return 0;
}
