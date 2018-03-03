#pragma once
#include <vector>
#include <cstdint>
#include "Relation.hpp"
#include "Parser.hpp"


/*------COLUMN STURCT & FUNCTIONS----------*/
typedef struct {
    uint64_t *values;
    uint64_t  size;
    int       table_id;
} column_t;

/* Column Print Function
 *
 * Prints a column
 *
 * Arguments : A @column of column_t type
 */
void PrintColumn(column_t *column);


/*--------- JOINER STUCT & FUNCTIONS---------*/
class Joiner {

    std::vector<Relation> relations;  // The relations that might be joined
    std::vector<int> **row_ids;       /* It keeps the row Ids of the reults of all the joined tables,update:vector instead of int */
    int *sizes;                       /* It keeps the sizes of all the tables of the above array of tables */

    /* Each Join has two columns */
    column_t left_column;             /**/
    column_t right_column;            /**/

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
    public:

    /* Initialize the row_id Array */
    void RowIdArrayInit(QueryInfo &query_info);

    // Add relation
    void addRelation(const char* fileName);

    // Get relation
    Relation& getRelation(unsigned id);

    // Joins a given set of relations
    void join(QueryInfo& i);
    void join(PredicateInfo &pred_info);

    /* The join function
     *
     * Joins two columns and returns the result(row_ids).
     *
     * Returns:   A joiner_t pointer with the row_ids of the results updated.
     * Arguments: @column_r is an array with the values of the r relation , and @size_r it's size
     *            @column_s is an array with the values of the s relation , and @size_s it's size
     */
    void low_join(column_t *column_r, column_t *column_s);

    /* The construct function
     *
     * Constructs a column bases on the row_ids of the joiner struct (which holds the result lines after join)
     *
     * Returns:   A column_t pointer with column's values updated.
     * Arguments: @column is an array with the values of the r relation
     *            @joiner is the object that holds the row_ids of the reults after the joins
     */
     column_t* construct(const column_t *column);
};
