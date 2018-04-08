#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <array>
#include <utility>
#include <vector>
#include <map>
#include <set>
#include <pthread.h>

#include "Joiner.hpp"
#include "js_master.h"
#include "main_job.h"
#include "queryFill_job.h"

using namespace std;

//caching info
std::map<Selection, cached_t*> idxcache;
pthread_mutex_t cache_mtx;

int cleanQuery(QueryInfo &info) {

    /* Remove weak filters */
    int changed = 0;

    map<SelectInfo, FilterInfo> filter_mapG;
    map<SelectInfo, FilterInfo> filter_mapL;
    set<FilterInfo> filters;

    for (auto filter: info.filters) {
        if (filter.comparison == '<') {
            if ((filter_mapL.find(filter.filterColumn) == filter_mapL.end())
                    || (filter_mapL[filter.filterColumn].constant > filter.constant)) {
                filter_mapL[filter.filterColumn] = filter;
            }

        }
        else if (filter.comparison == '>'){
            if ((filter_mapG.find(filter.filterColumn) == filter_mapG.end())
                    || (filter_mapG[filter.filterColumn].constant < filter.constant)) {
                filter_mapG[filter.filterColumn] = filter;
            }
        }
        else {
                if ((filter_mapL.find(filter.filterColumn) != filter_mapL.end())) {
                    if (filter_mapL[filter.filterColumn].constant < filter.constant)
                        return -1;
                    else {
                        filter_mapL.erase(filter.filterColumn);

                    }
                }


                if ((filter_mapG.find(filter.filterColumn) != filter_mapG.end())) {
                    if (filter_mapG[filter.filterColumn].constant > filter.constant)
                        return -1;
                    else {
                        filter_mapG.erase(filter.filterColumn);

                    }

                }
                filters.insert(filter);
        }
    }

    info.filters.clear();
    vector<FilterInfo> newfilters;
    for (auto filter: filters) {
        info.filters.push_back(filter);
    }

    for (std::map<SelectInfo,FilterInfo>::iterator it=filter_mapG.begin(); it!=filter_mapG.end(); ++it) {
        info.filters.push_back(it->second);
    }

    for (std::map<SelectInfo,FilterInfo>::iterator it=filter_mapL.begin(); it!=filter_mapL.end(); ++it) {
        info.filters.push_back(it->second);
    }

    /* Remove duplicate predicates */
    changed = 0;
    set <PredicateInfo> pred_set;
    for (auto pred: info.predicates) {
        if (!(pred.left < pred.right)) {
            SelectInfo tmp = pred.left;
            pred.left = pred.right;
            pred.right = tmp;
        //    cerr << "swapped" << endl;
        }

        if (pred_set.find(pred) != pred_set.end()) {
            changed = 1;
            continue;
        }
        pred_set.insert(pred);
    }

    if (changed == 0) {
        return 0;
    }


    info.predicates.clear();
    for (auto pred: pred_set) {
        info.predicates.push_back(pred);
    }

    return 0;
}
/* +---------------------+
   |The joiner functions |
   +---------------------+ */

void Joiner::AddColumnToTableT(SelectInfo &sel_info, table_t *table) {

    /* Create a new column_t for table */
    column_t &column = *table->column_j;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    column.size   = rel.size;
    column.values = rel.columns.at(sel_info.colId);
    column.id     = sel_info.colId;
    column.table_index = -1;
    unsigned relation_binding = sel_info.binding;

    /* Get the right index from the relation id table */
    unordered_map<unsigned, unsigned>::iterator itr = table->relations_bindings.find(relation_binding);
    if (itr != table->relations_bindings.end())
       column.table_index = itr->second;
    else
       std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors for " << relation_binding <<'\n';

}

table_t* Joiner::CreateTableTFromId(unsigned rel_id, unsigned rel_binding) {

    /* Get realtion */
    Relation & rel = getRelation(rel_id);

    /* Crate - Initialize a table_t */
    table_t *const table_t_ptr = new table_t;
    table_t_ptr->column_j = new column_t;
    table_t_ptr->intermediate_res = false;
    table_t_ptr->tups_num = rel.size;
    table_t_ptr->rels_num = 1;
    table_t_ptr->row_ids = NULL;

    /* Keep a mapping with the rowids table and the relaito ids na bindings */
    table_t_ptr->relations_bindings.insert(make_pair(rel_binding, 0));

    return table_t_ptr;
}


table_t* Joiner::join(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> & selections, int leafs, string & result_str) {
    relation_t * r1;
    relation_t * r2;

    //HERE WE CHECK FOR CACHED PARTITIONS
    Cacheinf c;
    size_t threads = THREAD_NUM_1CPU;


    Selection left(pred_info.left);
    Selection right(pred_info.right);
    int ch_flag;

    if (leafs&1) {
        pthread_mutex_lock(&cache_mtx);
        ch_flag = (idxcache.find(left) != idxcache.end());
        pthread_mutex_unlock(&cache_mtx);
        if (ch_flag == 1) {
            r1 = (relation_t *)malloc(sizeof(relation_t));
            r1->num_tuples = table_r->tups_num;
            pthread_mutex_lock(&cache_mtx);
            c.R = idxcache[left];
            pthread_mutex_unlock(&cache_mtx);
        }
        else {

            r1 = CreateRelationT(table_r, pred_info.left);
            c.R = (cached_t *) calloc(threads, sizeof(cached_t));
        }
    } else {

        r1 = CreateRelationT(table_r, pred_info.left);
        c.R = NULL;
    }

    // Check for cached filter
    if (leafs&2) {
        pthread_mutex_lock(&cache_mtx);
        ch_flag = (idxcache.find(right) != idxcache.end());
        pthread_mutex_unlock(&cache_mtx);
        if (ch_flag == 1) {
            r2 = (relation_t *)malloc(sizeof(relation_t));
            r2->num_tuples = table_s->tups_num;
            pthread_mutex_lock(&cache_mtx);
            c.S = idxcache[right];
            pthread_mutex_unlock(&cache_mtx);
        }
        else {
            r2 = CreateRelationT(table_s, pred_info.right);
            c.S = (cached_t *) calloc(threads, sizeof(cached_t));;
        }
    } else {
        r2 = CreateRelationT(table_s, pred_info.right);
        c.S = NULL;
    }

    result_t * res  = PRO(r1, r2, threads, c, job_scheduler);

    if (leafs&1) {
        free(r1);
        pthread_mutex_lock(&cache_mtx);
        idxcache[left] = c.R;
        pthread_mutex_unlock(&cache_mtx);
    }

    if (leafs&2) {
        free(r2);
        pthread_mutex_lock(&cache_mtx);
        idxcache[right] = c.S;
        pthread_mutex_unlock(&cache_mtx);
    }

    // On root dont create a resilting table just get the checksums
    table_t *temp = NULL;
    if (isRoot) {
        CheckSumOnTheFly(res, table_r, table_s, cmap, selections, result_str);
    } else {
        temp = CreateTableT(res, table_r, table_s, cmap);
    }


    /* Free the tables and the result of radix */
    free(table_r->row_ids);
    delete table_r->column_j;
    delete table_r;
    free(table_s->row_ids);
    delete table_s->column_j;
    delete table_s;
    free(res->resultlist);
    free(res);

    return temp;
}

// 64 bit version of joiner
table_t* Joiner::join_t64(table_t *table_r, table_t *table_s, PredicateInfo &pred_info, columnInfoMap & cmap, bool isRoot, std::vector<SelectInfo> & selections, int leafs, string & result_str) {

    relation64_t * r1;
    relation64_t * r2;

    //HERE WE CHECK FOR CACHED PARTITIONS
    Cacheinf c;
    size_t threads = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    Selection left(pred_info.left);
    Selection right(pred_info.right);

    /* Debug orest */
    if (leafs&1) {
        if (idxcache.find(left) != idxcache.end()) {
            r1 = (relation64_t *)malloc(sizeof(relation64_t));
            r1->num_tuples = table_r->tups_num;
            c.R = idxcache[left];
        }
        else {

            r1 = CreateRelationT_t64(table_r, pred_info.left);
            c.R = (cached_t *) calloc(threads, sizeof(cached_t));
        }
    } else {

        r1 = CreateRelationT_t64(table_r, pred_info.left);
        c.R = NULL;
    }

    if (leafs&2) {
        if (idxcache.find(right) != idxcache.end()) {
            r2 = (relation64_t *)malloc(sizeof(relation64_t));
            r2->num_tuples = table_s->tups_num;
            c.S = idxcache[right];
        }
        else {
            r2 = CreateRelationT_t64(table_s, pred_info.right);
            c.S = (cached_t *) calloc(threads, sizeof(cached_t));;
        }
    } else {
        r2 = CreateRelationT_t64(table_s, pred_info.right);
        c.S = NULL;
    }

    result_t * res  = PRO_t64(r1, r2, threads, c, job_scheduler);

    if (leafs&1) {
        free(r1);
        idxcache[left] = c.R;
    }
    if (leafs&2) {
        free(r2);
        idxcache[right] = c.S;
    }


    #ifdef time
    gettimeofday(&end, NULL);
    timeRadixJoin += (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec) / 1000000.0;
    #endif

    table_t *temp = NULL;
    // On root dont create a resilting table just get the checksums
    if (isRoot) {
        CheckSumOnTheFly_t64(res, table_r, table_s, cmap, selections, result_str);
    } else {
        temp = CreateTableT_t64(res, table_r, table_s, cmap);
    }


    /* Free the tables and the result of radix */
    free(table_r->row_ids);
    delete table_r->column_j;
    delete table_r;
    free(table_s->row_ids);
    delete table_s->column_j;
    delete table_s;
    free(res->resultlist);
    free(res);

    return temp;
}

// Loads a relation from disk
void Joiner::addRelation(const char* fileName) {
    relations.emplace_back(fileName);
}

// Loads a relation from disk
Relation& Joiner::getRelation(unsigned relationId) {
    if (relationId >= relations.size()) {
        cerr << "Relation with id: " << relationId << " does not exist" << endl;
        throw;
    }
    return relations[relationId];
}

// Get the total number of relations
int Joiner::getRelationsCount() {
    return relations.size();
}


// Print a column_t
void PrintColumn(column_t *column) {
    /* Print the column's table id */
    std::cerr << "Column of table " << column->id << " and size " << column->size << '\n';

    /* Iterate over the column's values and print them */
    for (int i = 0; i < column->size; i++) {
        std::cerr << column->values[i] << '\n';
    }
}


// Sort the cost array
bool sortinrev(const pair<int,int> &a,
               const pair<int,int> &b)
{
       return (a.first > b.first);
}

/* --------------------------------MAIN-------------------------------*/
int main(int argc, char* argv[]) {
    Joiner joiner;

    /* Initialize the mutex */
    pthread_mutex_init(&cache_mtx, 0);

    //Joiner joiner;
    JobSchedulerMaster main_js;

    // Read join relations
    string line;
    std::vector<string> file_names;
    while (getline(cin, line)) {
        if (line == "Done") break;
        file_names.push_back(line);
        joiner.addRelation(line.c_str());
    }

    // Time statistics
    struct timeval startS;
    gettimeofday(&startS, NULL);

    /* Initialize master thread scheduler */
    main_js.Init(file_names, MASTER_THREADS);

    // Create 2 js's for the stats
    JobScheduler js1,js2;
    js1.Init(THREAD_NUM_1CPU, NUMA_REG1);
    js2.Init(THREAD_NUM_2CPU, NUMA_REG2);

    // Preparation phase (not timed)
    QueryPlan queryPlan;
    bool switch_64t = false;

    // Get the statistics for the planner
    main_js.Schedule(new JobFillQueryPlan(queryPlan, js1, js2, switch_64t));

    // Wait for jobs to end
    main_js.Barrier();

    if (!switch_64t)
        queryPlan.Pre_Caching(joiner, js1, js2, startS);

    // Desoty the Js's
    js1.Stop(false);   js1.Destroy();
    js2.Stop(false);   js2.Destroy();

    // The test harness will send the first query after 1 second.
    int rv;
    int query_no    = 0;
    bool unsatisfied_filters = false;
    std::vector<JobMain*> jobs;
    std::vector<pair<int, int>> costVector;
    while (getline(cin, line)) {
        // If bacth ended
        if (line == "F") {

            // Sort Jobs by cost and schedule
            sort(costVector.begin(), costVector.end(), sortinrev);
            for (auto & x: costVector) {
                main_js.Schedule(jobs[x.second]);
            }

            // Wait for jobs to end
            main_js.Barrier();

            // Print the result with the orded the querys came
            for (size_t i = 0; i < jobs.size(); i++) {
                std::cout << jobs[i]->result_ << '\n';
                delete jobs[i];
            }

            // Clean The Vectors
            jobs.clear();
            costVector.clear();

            continue;
        }

        //Parse the query
        QueryInfo * i = new QueryInfo;
        i->parseQuery(line);

        // Clean query from weak filters
        rv = cleanQuery(*i);
        if (rv == -1)
            unsatisfied_filters = true;
        else
            unsatisfied_filters = false;

        // Create the tree
        JoinTree * optimalJT = queryPlan.joinTreePtr->build(*i, queryPlan.columnInfos);

        //Create job on runtime
        JobMain * job = new JobMain(i, line, optimalJT, query_no, switch_64t, unsatisfied_filters);

        // Keep jobs in a vector
        costVector.push_back( make_pair(optimalJT->root->treeCost, jobs.size()) );
        jobs.push_back(job);
        query_no++;
    }

    /* Destroy it */
    pthread_mutex_destroy(&cache_mtx);

    return 0;
}
