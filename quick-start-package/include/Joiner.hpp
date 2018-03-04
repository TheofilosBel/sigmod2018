#pragma once
#include <vector>
#include <cstdint>
#include "Relation.hpp"
#include "Parser.hpp"
#include "table_t.hpp"


/*
 * Prints a column
 * Arguments : A @column of column_t type
 */
void PrintColumn(column_t *column);


class Joiner {

    std::vector<Relation> relations;  // The relations that might be joined

    public:

    /* Initialize the row_id Array */
    void RowIdArrayInit(QueryInfo &query_info);

    // Add relation
    void addRelation(const char* fileName);

    // Get relation
    Relation& getRelation(unsigned id);

    // Get the total number of relations
    int getRelationsCount();

    table_t* PredicateToTableT(PredicateInfo &pred_info);

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
    table_t& low_join(table_t &table_r, table_t &table_s);

    /* The construct function
     *
     * Constructs a column bases on the row_ids of the joiner struct (which holds the result lines after join)
     *
     * Returns:   A column_t pointer with column's values updated.
     * Arguments: @column is an array with the values of the r relation
     *            @joiner is the object that holds the row_ids of the reults after the joins
     */
     void construct(table_t &table);
};
