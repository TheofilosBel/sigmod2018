#ifndef  __JOINER_H__
#define  __JOINER_H__

#include <iostream>
#include <vector>
#include <unordered_map>

/*--------- JOINER STUCT & FUNCTIONS---------*/

/*  The row id Table
+--------------------------------------+
|  rows_S*  |  rows_R*  |   ...  | ... |
+--------------------------------------+
    |
 +--+---+
 |rowS.0|
 |rowS.1|
 |rowS.2|
 | ...  |
 +------+

*/

typedef struct {
  std::vector<int> **row_ids;       /* It keeps the row Ids of the reults of all the joined tables,update:vector instead of int */
  int *sizes;                       /* It keeps the sizes of all the tables of the above array of tables */
} joiner_t;

/*
 * Construct the joiner object
 *
 * Returns   : Returns the joiner object initilized
 * Arguments : Columns
 */
joiner_t * JoinerCreate();


/*
 * Destroys the joiner object
 *
 * Returns   : Void
 * Arguments : The @joiner to be destroyed
 */
 void JoinerDestroy(joiner_t *joiner);


/*------COLUMN STURCT & FUNCTIONS----------*/

typedef struct {
    int       table_id;
    int      *values;
    uint64_t  size;
} column_t;

/* Column Print Function
 *
 * Prints a column
 *
 * Arguments : A @column of column_t type
 */
void PrintColumn(column_t *column);


/*------------ JOIN FUNCTIONALITY --------------*/

/* The join function
 *
 * Joins two columns and returns the result(row_ids).
 *
 * Returns:   A joiner_t pointer with the row_ids of the results updated.
 * Arguments: @column_r is an array with the values of the r relation , and @size_r it's size
 *            @column_s is an array with the values of the s relation , and @size_s it's size
 */
void join(const column_t *column_r, const column_t *column_s, joiner_t *joiner);


/* The construct function
 *
 * Constructs a column bases on the row_ids of the joiner struct (which holds the result lines after join)
 *
 * Returns:   A column_t pointer with column's values updated.
 * Arguments: @column is an array with the values of the r relation
 *            @joiner is the object that holds the row_ids of the reults after the joins
 */
 column_t* construct(const column_t *column, const joiner_t *joiner);

#endif
