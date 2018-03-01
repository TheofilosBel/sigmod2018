#ifndef  __JOINER_H__
#define  __JOINER_H__

#include <iostream>
#include <unordered_map>

/*  The row id Table
+--------------------------------------+
|  rows_S*  |  rows_R*  |   ...  | ... |
+--------------------------------------+
    |
    +
    -
    -
    -
    -
*/

typedef struct {
  std::vector<int> **row_ids;       /* It keeps the row Ids of the reults of all the joined tables,put vector to do*/
  int *sizes;                       /* It keeps the sizes of all the tables of the above array of tables */
} joiner_t;

typedef struct {
  int table_id;
  int *values;
  int size;
} column_t;


/* The join function
 *
 * Joins two columns and returns the result(row_ids).
 *
 * Returns:   A joiner_t pointer with the row_ids of the results updated.
 * Arguments: @column_r is an array with the values of the r relation , and @size_r it's size
 *            @column_s is an array with the values of the s relation , and @size_s it's size
 */
joiner_t* join(column_t *column_r, column_t *column_s, joiner_t *joiner);


/* The construct function
 *
 * Joins two columns and returns the result(row_ids).
 *
 * Returns:   A joiner_t pointer with the row_ids of the results updated.
 * Arguments: @column_r is an array with the values of the r relation , and @size_r it's size
 *            @column_s is an array with the values of the s relation , and @size_s it's size
 */
 column_t* construct(column_t *column, joiner_t *joiner);





#endif
