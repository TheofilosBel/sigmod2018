#pragma once
#include <unordered_map>
#include <sys/time.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <cstdio>
#include <iostream>
#include <thread>
#include <vector>
#include <cstdint>
#include <string>
#include <map>
#include <limits>   // for numeric_limits

#include "Relation.hpp"
#include "Parser.hpp"
#include "table_t.hpp"
#include "parallel_radix_join.h"
#include "create_job.h"
#include "checksum_job.h"

/* THread pool Includes */
/*----------------*/

//#define time
//#define prints
#define MASTER_THREADS  2
#define THREAD_NUM_1CPU 10
#define THREAD_NUM_2CPU 10
#define NUMA_REG1 0
#define NUMA_REG2 1

using namespace std;


class JTree;
struct ColumnInfo;
// Use it in filter cpp for the result of the map
typedef std::map<Selection, cached_t*>::iterator cache_res;
typedef std::map<SelectInfo, ColumnInfo> columnInfoMap;

//caching info
extern std::map<Selection, cached_t*> idxcache;
extern pthread_mutex_t cache_mtx;
/*
 * Prints a column
 * Arguments : A @column of column_t type
 */
void PrintColumn(column_t *column);


class Joiner {
    std::vector<Relation> relations; // The relations that might be joined

    public:

    // Job scheduler
    int mst;
    JobScheduler job_scheduler;


    /* Initialize the row_id Array */
    void RowIdArrayInit(QueryInfo &query_info);

    // Add relation
    void addRelation(const char* fileName);

    // Get relation
    Relation& getRelation(unsigned id);

    // Get the total number of relations
    int getRelationsCount();

    //
    table_t* join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> & selections, int leafs, string & result_str);
    table_t* join_t64(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> & selections, int leafs, string & result_str);
    table_t* join_t32_t64(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> & selections, int leafs, string & result_str);
    table_t* join_t64_t32(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> & selections, int leafs, string & result_str);

    relation_t* CreateRelationT(table_t * table, SelectInfo &sel_info);
    table_t*    CreateTableT(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap);
    void        CheckSumOnTheFly(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap, std::vector<SelectInfo> selections, string &);

    relation64_t* CreateRelationT_t64(table_t * table, SelectInfo &sel_info);
    table_t*      CreateTableT_t64(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap);
    void          CheckSumOnTheFly_t64(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap, std::vector<SelectInfo> selections, string &);


    table_t*    CreateTableTFromId(unsigned rel_id, unsigned rel_binding);
    void        AddColumnToTableT(SelectInfo &sel_info, table_t *table);

    // The select functions
    void SelectAll(std::vector<FilterInfo*> & filterPtrs, table_t* table);
    void Select(FilterInfo &sel_info, table_t *table, ColumnInfo* columnInfo);
    void SelectEqual(table_t *table, int filter);
    void SelectGreater(table_t *table, int filter);
    void SelectLess(table_t *table, int filter);
    cached_t* Cached_SelectEqual(uint64_t fil, cache_res& cache_info, table_t* table);

    // Joins a given set of relations
    table_t* SelfJoin(table_t *table, PredicateInfo *pred_info, columnInfoMap & cmap);
    table_t* SelfJoinCheckSumOnTheFly(table_t *table, PredicateInfo *predicate_ptr, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str);
};

#include "QueryPlan.hpp"

int cleanQuery(QueryInfo &);
