#include <cassert>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <vector>
#include <pthread.h>

#include "job_scheduler.h"
#include "Joiner.hpp"         // Implemets Joiner Functions
#include "generator.h"        // For gern_rel(_t64)

#include "tuple_buffer.h"     // Buffers 32 bit ver
#include "create_job.h"       // Check sums 32 bit ver

#include "tuple_buffer_64.h"  // Buffers 64 bit ver
#include "create_job_64.h"    // Check sums 64 bit ver

using namespace std;


extern std::map<Selection, cached_t*> idxcache;
extern pthread_mutex_t cache_mtx;

// 32 Bit functions

/* ================================ */
/* Table_t <=> Relation_t fuctnions */
/* ================================ */
relation_t * Joiner::CreateRelationT(table_t * table, SelectInfo &sel_info) {

    /* Create a new column_t for table */
    unsigned * row_ids = table->row_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    uint64_t * values = rel.columns.at(sel_info.colId);
    uint64_t s = rel.size;

    /* Create the relation_t */
    relation_t * new_relation = gen_rel(table->tups_num);

    // Get the range for the threds chinking
    size_t range = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    if (table->intermediate_res) {

        unsigned table_index = -1;
        unsigned relation_binding = sel_info.binding;

        /* Get the right index from the relation id table */
        unordered_map<unsigned, unsigned>::iterator itr = table->relations_bindings.find(relation_binding);
        if (itr != table->relations_bindings.end())
            table_index = itr->second;
        else
            std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors for " << relation_binding <<'\n';

        /* Initialize relation */
        uint32_t size    = table->tups_num;
        uint32_t rel_num = table->rels_num;
        struct interRel_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].tups = new_relation->tuples;
            a[i].rids = row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            job_scheduler.Schedule(new JobCreateInterRel(a[i]));
        }
        job_scheduler.Barrier();
    }
    else {
        /* Initialize relation */
        uint32_t size = table->tups_num;
        struct noninterRel_arg a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].tups = new_relation->tuples;
            job_scheduler.Schedule(new JobCreateNonInterRel(a[i]));
        }
        job_scheduler.Barrier();
    }

    return new_relation;
}

table_t * Joiner::CreateTableT(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap) {

    /* Only one relation can be victimized */
    unsigned rel_num_r = table_r->relations_bindings.size();
    unsigned rel_num_s = table_s->relations_bindings.size();
    unordered_map<unsigned, unsigned>::iterator itr;
    bool victimize = true;
    int  index     = -1;
    char where     =  0;  // 1 is table R and 2 is table S
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        victimize = true;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            if (it->first.binding == itr->first) {
                victimize = false;
                break;
            }
        }
        if (victimize) {
            index = itr->second;
            where = 1;
        }
    }

    // If it was not found sarch the other relation
    if (index != -1)
        for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
            victimize = true;
            for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
                if (it->first.binding == itr->first) {
                    victimize = false;
                    break;
                }
            }
            if (victimize) {
                index = itr->second;
                where = 2;
            }
        }

    /* Create - Initialize the new table_t */
    unsigned num_relations = (index == -1) ? rel_num_r + rel_num_s : rel_num_r + rel_num_s - 1;
    table_t * new_table = new table_t;
    new_table->intermediate_res = true;
    new_table->column_j = new column_t;
    new_table->tups_num = result->totalresults;
    new_table->rels_num = num_relations;
    new_table->row_ids = (unsigned *) malloc(sizeof(unsigned) * num_relations * result->totalresults);

    /* Create the new maping vector */
    int rem = 0;
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        if (where == 1 && index == itr->second){
            rem = 1;
            continue;
        }else if (where == 1 && index < itr->second)
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second - 1));
        else
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second));
    }

    for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
        if (where == 2 && index == itr->second)
            continue;
        else if (where == 2 && index < itr->second)
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second + rel_num_r - 1));
        else
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second + rel_num_r - rem));
    }

    /* Get the 3 row_ids matrixes in referances */
    unsigned * rids_res = new_table->row_ids;
    unsigned * rids_r   = table_r->row_ids;
    unsigned * rids_s   = table_s->row_ids;

    uint32_t idx = 0;  // POints to the right index on the res
    uint32_t tup_i;
    size_t range = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    /* Find jobs number */
    int jobs_num = 0;
    void * job_args = NULL;
    for (size_t th = 0; th < range; th++) {
        chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
        jobs_num += cb->numbufs;
    }

    if (table_r->intermediate_res && table_s->intermediate_res) {
        //struct interInterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;
        struct interInterTable_arg * ja = (struct interInterTable_arg *) malloc(sizeof(struct interInterTable_arg ) * jobs_num);
        job_args = (void *) ja;
        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct interInterTable_arg * a = ja++;//new interInterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_r = rel_num_r;
                a->rel_num_s = rel_num_s;
                a->rids_res  = rids_res;
                a->rids_r = rids_r;
                a->rids_s = rids_s;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateInterInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }
    else if (table_r->intermediate_res) {
        //struct interNoninterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;
        struct interNoninterTable_arg * ja = (struct interNoninterTable_arg *) malloc(sizeof(struct interNoninterTable_arg ) * jobs_num);
        job_args = (void *) ja;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct interNoninterTable_arg * a = ja++;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_r = rel_num_r;
                a->rids_res  = rids_res;
                a->rids_r = rids_r;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateInterNonInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }
    else if (table_s->intermediate_res) {
        //struct noninterInterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;
        struct noninterInterTable_arg * ja = (struct noninterInterTable_arg *) malloc(sizeof(struct noninterInterTable_arg ) * jobs_num);
        job_args = (void *) ja;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct noninterInterTable_arg * a = ja++;//new noninterInterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_s = rel_num_s;
                a->rids_res  = rids_res;
                a->rids_s = rids_s;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateNonInterInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }
    else {
        //struct noninterNoninterTable_arg a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer_t * tb;
        struct noninterNoninterTable_arg * ja = (struct noninterNoninterTable_arg *) malloc(sizeof(struct noninterNoninterTable_arg ) * jobs_num);
        job_args = (void *) ja;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct noninterNoninterTable_arg * a = ja++;//new noninterNoninterTable_arg;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rids_res  = rids_res;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateNonInterNonInterTable(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }

    /* Barrier here */
    job_scheduler.Barrier();

    for (size_t i = 0; i < range; i++) {
        chainedtuplebuffer_free((chainedtuplebuffer_t *) result->resultlist[i].results);
    }

    /* free */
    free(job_args);
    return new_table;
}

// 64 bit version

/* ================================ */
/* Table_t <=> relation64_t fuctnions */
/* ================================ */
relation64_t * Joiner::CreateRelationT_t64(table_t * table, SelectInfo &sel_info) {

    /* Create a new column_t for table */
    unsigned * row_ids = table->row_ids;

    /* Get the relation from joiner */
    Relation &rel = getRelation(sel_info.relId);
    uint64_t * values = rel.columns.at(sel_info.colId);
    uint64_t s = rel.size;

    /* Create the relation64_t */
    relation64_t * new_relation = gen_rel_t64(table->tups_num);

    // Get the range for the threds chinking
    size_t range = THREAD_NUM_1CPU;

    if (table->intermediate_res) {

        unsigned table_index = -1;
        unsigned relation_binding = sel_info.binding;

        /* Get the right index from the relation id table */
        unordered_map<unsigned, unsigned>::iterator itr = table->relations_bindings.find(relation_binding);
        if (itr != table->relations_bindings.end())
            table_index = itr->second;
        else
            std::cerr << "At AddColumnToTableT, Id not matchin with intermediate result vectors for " << relation_binding <<'\n';

        /* Initialize relation */
        uint32_t size    = table->tups_num;
        uint32_t rel_num = table->rels_num;
        struct interRel_arg64_t a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].tups = new_relation->tuples;
            a[i].rids = row_ids;
            a[i].rel_num = rel_num;
            a[i].table_index = table_index;
            job_scheduler.Schedule(new JobCreateInterRel_t64(a[i]));
        }
        job_scheduler.Barrier();
    }
    else {

        /* Initialize relation */
        uint32_t size = table->tups_num;
        struct noninterRel_arg64_t a[range];
        for (size_t i = 0; i < range; i++) {
            a[i].low   = (i < size % range) ? i * (size / range) + i : i * (size / range) + size % range;
            a[i].high  = (i < size % range) ? a[i].low + size / range + 1 :  a[i].low + size / range;
            a[i].values = values;
            a[i].tups = new_relation->tuples;
            job_scheduler.Schedule(new JobCreateNonInterRel_t64(a[i]));
        }
        job_scheduler.Barrier();
    }

    return new_relation;
}

table_t * Joiner::CreateTableT_t64(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap) {

    /* Only one relation can be victimized */
    unsigned rel_num_r = table_r->relations_bindings.size();
    unsigned rel_num_s = table_s->relations_bindings.size();
    unordered_map<unsigned, unsigned>::iterator itr;
    bool victimize = true;
    int  index     = -1;
    char where     =  0;  // 1 is table R and 2 is table S
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        victimize = true;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            if (it->first.binding == itr->first) {
                victimize = false;
                break;
            }
        }
        if (victimize) {
            index = itr->second;
            where = 1;
        }
    }

    // If it was not found sarch the other relation
    if (index != -1)
        for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
            victimize = true;
            for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
                if (it->first.binding == itr->first) {
                    victimize = false;
                    break;
                }
            }
            if (victimize) {
                index = itr->second;
                where = 2;
            }
        }

    /* Create - Initialize the new table_t */
    unsigned num_relations = (index == -1) ? rel_num_r + rel_num_s : rel_num_r + rel_num_s - 1;
    table_t * new_table = new table_t;
    new_table->intermediate_res = true;
    new_table->column_j = new column_t;
    new_table->tups_num = result->totalresults;
    new_table->rels_num = num_relations;
    new_table->row_ids = (unsigned *) malloc(sizeof(unsigned) * num_relations * result->totalresults);

    /* Create the new maping vector */
    int rem = 0;
    for (itr = table_r->relations_bindings.begin(); itr != table_r->relations_bindings.end(); itr++) {
        if (where == 1 && index == itr->second){
            rem = 1;
            continue;
        }else if (where == 1 && index < itr->second)
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second - 1));
        else
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second));
    }

    for (itr = table_s->relations_bindings.begin(); itr != table_s->relations_bindings.end(); itr++) {
        if (where == 2 && index == itr->second)
            continue;
        else if (where == 2 && index < itr->second)
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second + rel_num_r - 1));
        else
            new_table->relations_bindings.insert(make_pair(itr->first, itr->second + rel_num_r - rem));
    }

    /* Get the 3 row_ids matrixes in referances */
    unsigned * rids_res = new_table->row_ids;
    unsigned * rids_r   = table_r->row_ids;
    unsigned * rids_s   = table_s->row_ids;

    uint32_t idx = 0;  // POints to the right index on the res
    uint32_t tup_i;
    size_t range = THREAD_NUM_1CPU; // + THREAD_NUM_2CPU;

    /* Find jobs number */
    int jobs_num = 0;
    void * job_args = NULL;
    for (size_t th = 0; th < range; th++) {
        chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
        jobs_num += cb->numbufs;
    }

    if (table_r->intermediate_res && table_s->intermediate_res) {
        //struct interInterTable_arg64_t a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer64_t * tb;
        struct interInterTable_arg64_t * ja = (struct interInterTable_arg64_t *) malloc(sizeof(struct interInterTable_arg64_t ) * jobs_num);
        job_args = (void *) ja;
        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct interInterTable_arg64_t * a = ja++;//new interInterTable_arg64_t;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_r = rel_num_r;
                a->rel_num_s = rel_num_s;
                a->rids_res  = rids_res;
                a->rids_r = rids_r;
                a->rids_s = rids_s;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateInterInterTable_t64(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }
    else if (table_r->intermediate_res) {
        //struct interNoninterTable_arg64_t a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer64_t * tb;
        struct interNoninterTable_arg64_t * ja = (struct interNoninterTable_arg64_t *) malloc(sizeof(struct interNoninterTable_arg64_t ) * jobs_num);
        job_args = (void *) ja;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct interNoninterTable_arg64_t * a = ja++;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_r = rel_num_r;
                a->rids_res  = rids_res;
                a->rids_r = rids_r;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateInterNonInterTable_t64(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }
    else if (table_s->intermediate_res) {
        //struct noninterInterTable_arg64_t a[jobs_num];
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer64_t * tb;
        struct noninterInterTable_arg64_t * ja = (struct noninterInterTable_arg64_t *) malloc(sizeof(struct noninterInterTable_arg64_t ) * jobs_num);
        job_args = (void *) ja;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct noninterInterTable_arg64_t * a = ja++;//new noninterInterTable_arg64_t;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rel_num_s = rel_num_s;
                a->rids_res  = rids_res;
                a->rids_s = rids_s;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateNonInterInterTable_t64(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }
    else {
        unsigned prefix = 0, inner_prefix = 0, buffs_prefix = 0;
        tuplebuffer64_t * tb;
        struct noninterNoninterTable_arg64_t * ja = (struct noninterNoninterTable_arg64_t *) malloc(sizeof(struct noninterNoninterTable_arg64_t ) * jobs_num);
        job_args = (void *) ja;

        /* Loop all the buffers */
        for (int th = 0; th < range ; th++) {
            chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
            inner_prefix = 0;
            tb = cb->buf;
            for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                struct noninterNoninterTable_arg64_t * a = ja++;//new noninterNoninterTable_arg64_t;
                a->start_index = inner_prefix + prefix;
                a->rel_num_all = num_relations;
                a->rids_res  = rids_res;
                a->index  = index;
                a->where  = where;
                a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                a->tb = tb;
                job_scheduler.Schedule(new JobCreateNonInterNonInterTable_t64(*a));
                inner_prefix += (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                tb = tb->next;
            }
            buffs_prefix += cb->numbufs;
            prefix += result->resultlist[th].nresults;
        }
    }

    /* Barrier here */
    job_scheduler.Barrier();

    /* free */
    for (size_t i = 0; i < range; i++) {
        chainedtuplebuffer_free_t64((chainedtuplebuffer64_t *) result->resultlist[i].results);
    }
    free(job_args);
    return new_table;
}
