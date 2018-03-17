#pragma once
#include <vector>
#include <cstdint>
#include <sys/time.h>
#include "Relation.hpp"
#include "Parser.hpp"
#include "table_t.hpp"
#include "parallel_radix_join.h"

/* Timing variables */
extern double timeSelfJoin;
extern double timeSelectFilter;
extern double timeLowJoin;
extern double timeCreateTable;
extern double timeAddColumn;
extern double timeTreegen;
extern double timeCheckSum;

/*
 * Prints a column
 * Arguments : A @column of column_t type
 */
void PrintColumn(column_t *column);


class Joiner {

    std::vector<Relation> relations;  // The relations that might be joined

    public:
    /* do the checksum */
    uint64_t check_sum(SelectInfo &sel_info, table_t *table);

    /* Initialize the row_id Array */
    void RowIdArrayInit(QueryInfo &query_info);

    // Add relation
    void addRelation(const char* fileName);

    // Get relation
    Relation& getRelation(unsigned id);

    // Get the total number of relations
    int getRelationsCount();

    table_t* CreateTableTFromId(unsigned rel_id, unsigned rel_binding);
    relation_t * CreateRelationT(table_t * table, SelectInfo &sel_info);
    table_t * CreateTableT(result_t * result, table_t * table_r, table_t * table_s);
    void AddColumnToTableT(SelectInfo &sel_info, table_t *table);

    // The select functions
    void Select(FilterInfo &sel_info, table_t *table);
    void SelectEqual(table_t *table, int filter);
    void SelectGreater(table_t *table, int filter);
    void SelectLess(table_t *table, int filter);

    // Joins a given set of relations
    void join(QueryInfo& i);
    table_t* join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info);
    table_t* SelfJoin(table_t *table, PredicateInfo *pred_info);


    /* The join function
     *
     * Joins two columns and returns the result(row_ids).
     *
     * Returns:   A joiner_t pointer with the row_ids of the results updated.
     * Arguments: @column_r is an array with the values of the r relation , and @size_r it's size
     *            @column_s is an array with the values of the s relation , and @size_s it's size
     */
    table_t* low_join(table_t *table_r, table_t *table_s);

    /* The construct function
     *
     * Constructs a column bases on the row_ids of the joiner struct (which holds the result lines after join)
     *
     * Returns:   A column_t pointer with column's values updated.
     * Arguments: @column is an array with the values of the r relation
     *            @joiner is the object that holds the row_ids of the reults after the joins
     */
     void construct(table_t *table);
};
