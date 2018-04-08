#include <cassert>
#include <iostream>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <utility>
#include <vector>
#include <pthread.h>

#include "job_scheduler.h"      // The job scheduler Class
#include "Joiner.hpp"           // Implements Joiner Functions
#include "checksum_job.h"       // Parallel Checksums 32 bit ver
#include "checksum_job_64.h"    // Parallel Checksums 64 bit ver

using namespace std;

#define CHECK_SUM_RANGE 20

// The self Join CheckSum on the Fly method
table_t * Joiner::SelfJoinCheckSumOnTheFly(table_t *table, PredicateInfo *predicate_ptr, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str) {

    /* Get the 2 relation rows ids vectors in referances */
    unsigned * row_ids_matrix       = table->row_ids;

    /* Get the 2 relations */
    Relation & relation_l        = getRelation(predicate_ptr->left.relId);
    Relation & relation_r        = getRelation(predicate_ptr->right.relId);

    /* Get their columns */
    uint64_t *column_values_l    = relation_l.columns[predicate_ptr->left.colId];
    uint64_t *column_values_r    = relation_r.columns[predicate_ptr->right.colId];

    /* Fint the indexes of the raltions in the table's */
    int index_l                  = -1;
    int index_r                  = -1;

    index_l = table->relations_bindings.find(predicate_ptr->left.binding)->second;
    index_r = table->relations_bindings.find(predicate_ptr->right.binding)->second;

    unsigned rows_number = table->tups_num;
    unsigned rels_number = table->rels_num;

    /* Crete a vector for the pairs Column, Index in relationR/S */
    vector<struct checksumST> distinctPairs;
    struct checksumST st;

    /* take the distinct columns in a vector */
    unordered_map<unsigned, unsigned>::iterator itr;
    unsigned index = 0;
    for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
        index = -1;
        itr = table->relations_bindings.find(it->first.binding);
        if (itr != table->relations_bindings.end()) {
            st.colId = it->first.colId;
            st.binding = it->first.binding;
            st.index = itr->second;
            st.values = getRelation(it->first.relId).columns[st.colId];
            distinctPairs.push_back(st);
        }
    }

    // Range for chunking
    size_t range = CHECK_SUM_RANGE;

    /* Calculate check sums on the fly , if its the last query */
    vector<uint64_t> sum(distinctPairs.size(), 0);
    vector<uint64_t*> sums(range);
    for (size_t i = 0; i < sums.size(); i++) {
        sums[i] = (uint64_t *) calloc (sum.size(), sizeof(uint64_t));
    }

    struct selfJoinSum_arg a[range];
    for (size_t i = 0; i < range; i++) {
        a[i].low   = (i < rows_number % range) ? i * (rows_number / range) + i : i * (rows_number / range) + rows_number % range;
        a[i].high  = (i < rows_number % range) ? a[i].low + rows_number / range + 1 :  a[i].low + rows_number / range;
        a[i].column_values_l = column_values_l;
        a[i].column_values_r = column_values_r;
        a[i].row_ids_matrix = row_ids_matrix;
        a[i].index_l = index_l;
        a[i].index_r = index_r;
        a[i].relations_num = rels_number;
        a[i].priv_checsums = sums[i];
        a[i].distinctPairs = &distinctPairs;
        job_scheduler.Schedule(new JobCheckSumSelfJoin(a[i]));
    }
    job_scheduler.Barrier();


    /* Create the checksum */
    for (size_t j = 0; j < sum.size(); j++) {
        for (size_t i = 0; i < sums.size(); i++) {
            sum[j] += sums[i][j];
        }
    }

    /* Construct the checksums in the right way */
    bool found = false;
    for (size_t i = 0; i < selections.size(); i++) {

        // Look up the check sum int the array
        for (size_t j = 0; j < distinctPairs.size(); j++) {
            if (selections[i].colId == distinctPairs[j].colId
                && selections[i].binding == distinctPairs[j].binding)
            {
                (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j]);
                found = true;
                break;
            }
        }

        // Create the write check sum
        if (i != selections.size() - 1) {
            result_str +=  " ";
        }
    }

    return NULL;
}

// 32 bit version
void Joiner::CheckSumOnTheFly(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str) {

        /* Crete a vector for the pairs Column, Index in relationR/S */
        vector<struct checksumST> distinctPairs_in_R;
        vector<struct checksumST> distinctPairs_in_S;
        struct checksumST st;

        /* take the distinct columns in a vector */
        unordered_map<unsigned, unsigned>::iterator itr;
        unsigned index = 0;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            index = -1;
            itr = table_r->relations_bindings.find(it->first.binding);
            if (itr != table_r->relations_bindings.end() ) {
                st.colId = it->first.colId;
                st.binding = it->first.binding;
                st.index = itr->second;
                st.values = getRelation(it->first.relId).columns[st.colId];
                distinctPairs_in_R.push_back(st);
            }
            else {
                itr = table_s->relations_bindings.find(it->first.binding);
                (itr != table_s->relations_bindings.end()) ? (index = itr->second) : (index = -1);
                st.colId = it->first.colId;
                st.binding = it->first.binding;
                st.index = itr->second;
                st.values = getRelation(it->first.relId).columns[st.colId];
                distinctPairs_in_S.push_back(st);
            }

        }

        size_t range = THREAD_NUM_1CPU;  // always eqyal with threads of radix
        int jobs_num = 0;
        void * job_args  = NULL;
        for (size_t th = 0; th < range; th++) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
            jobs_num += cb->numbufs;
        }

        vector<uint64_t> sum(distinctPairs_in_R.size() + distinctPairs_in_S.size(), 0);
        vector<uint64_t*> sums(jobs_num);
        for (size_t i = 0; i < sums.size(); i++) {
            sums[i] = (uint64_t *) calloc (sum.size(), sizeof(uint64_t));
        }

        if (table_r->intermediate_res && table_s->intermediate_res) {

            //struct interInterTable_arg a[jobs_num];
            tuplebuffer_t * tb;
            struct interInterSum_arg * ja = (struct interInterSum_arg *) malloc(sizeof(struct interInterSum_arg) * jobs_num);

            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct interInterSum_arg * a = ja + idx;//new interInterSum_arg;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->rel_num_r = table_r->rels_num;
                    a->rel_num_s = table_s->rels_num;
                    a->rids_r = table_r->row_ids;
                    a->rids_s = table_s->row_ids;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumInterInter(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }
        else if (table_r->intermediate_res) {
            tuplebuffer_t * tb;
            struct interNoninterSum_arg * ja = (struct interNoninterSum_arg *) malloc(sizeof(struct interNoninterSum_arg) * jobs_num);
            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct interNoninterSum_arg * a = ja + idx;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->rel_num_r = table_r->rels_num;
                    a->rids_r = table_r->row_ids;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumInterNonInter(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }
        else if (table_s->intermediate_res) {
            tuplebuffer_t * tb;
            struct noninterInterSum_arg * ja = (struct noninterInterSum_arg *) malloc(sizeof(struct noninterInterSum_arg) * jobs_num);
            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct noninterInterSum_arg * a = ja + idx;//new noninterInterSum_arg;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->rel_num_s = table_s->rels_num;
                    a->rids_s = table_s->row_ids;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumNonInterInter(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }
        else {
            tuplebuffer_t * tb;
            struct noninterNoninterSum_arg * ja = (struct noninterNoninterSum_arg *) malloc(sizeof(struct noninterNoninterSum_arg) * jobs_num);
            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct noninterNoninterSum_arg * a = ja + idx;//new noninterNoninterSum_arg;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumNonInterNonInter(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }


        /* Barrier here */
        job_scheduler.Barrier();
        /* Create the checksum */
        for (size_t j = 0; j < sum.size(); j++) {
            for (size_t i = 0; i < sums.size(); i++) {
                sum[j] += sums[i][j];
            }
        }

        //delete_

        /* Free args */
        free(job_args);


        /* Construct the checksums in the right way */
        bool found = false;
        for (size_t i = 0; i < selections.size(); i++) {

            // Check if checksum is cached
            for (size_t j = 0; j < distinctPairs_in_R.size(); j++) {
                if (selections[i].colId == distinctPairs_in_R[j].colId
                    && selections[i].binding == distinctPairs_in_R[j].binding)
                {
                    (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j]);
                    found = true;
                    break;
                }
            }

            /* Search in the other */
            for (size_t j = 0; j < distinctPairs_in_S.size() && found != true; j++) {
                if (selections[i].colId == distinctPairs_in_S[j].colId
                    && selections[i].binding == distinctPairs_in_S[j].binding)
                {
                    (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j +  distinctPairs_in_R.size()]);
                    found = true;
                    break;
                }
            }

            /* Flag for the next loop */
            found = false;

            // Create the write check sum
            if (i != selections.size() - 1) {
                result_str +=  " ";
            }
        }

        // Frees
        for (size_t i = 0; i < range; i++) {
            chainedtuplebuffer_free((chainedtuplebuffer_t *) result->resultlist[i].results);
        }
}

//64 Bit version

void Joiner::CheckSumOnTheFly_t64(result_t * result, table_t * table_r, table_t * table_s, columnInfoMap & cmap, std::vector<SelectInfo> selections, string & result_str) {

        /* Crete a vector for the pairs Column, Index in relationR/S */
        vector<struct checksumST> distinctPairs_in_R;
        vector<struct checksumST> distinctPairs_in_S;
        struct checksumST st;

        /* take the distinct columns in a vector */
        unordered_map<unsigned, unsigned>::iterator itr;
        unsigned index = 0;
        for (columnInfoMap::iterator it=cmap.begin(); it != cmap.end(); it++) {
            index = -1;
            itr = table_r->relations_bindings.find(it->first.binding);
            if (itr != table_r->relations_bindings.end() ) {
                st.colId = it->first.colId;
                st.binding = it->first.binding;
                st.index = itr->second;
                st.values = getRelation(it->first.relId).columns[st.colId];
                distinctPairs_in_R.push_back(st);
            }
            else {
                itr = table_s->relations_bindings.find(it->first.binding);
                (itr != table_s->relations_bindings.end()) ? (index = itr->second) : (index = -1);
                st.colId = it->first.colId;
                st.binding = it->first.binding;
                st.index = itr->second;
                st.values = getRelation(it->first.relId).columns[st.colId];
                distinctPairs_in_S.push_back(st);
            }

        }

        size_t range = THREAD_NUM_1CPU;  // always eqyal with threads of radix
        int jobs_num = 0;
        void * job_args  = NULL;
        for (size_t th = 0; th < range; th++) {
            chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
            jobs_num += cb->numbufs;
        }

        vector<uint64_t> sum(distinctPairs_in_R.size() + distinctPairs_in_S.size(), 0);
        vector<uint64_t*> sums(jobs_num);
        for (size_t i = 0; i < sums.size(); i++) {
            sums[i] = (uint64_t *) calloc (sum.size(), sizeof(uint64_t));
        }

        if (table_r->intermediate_res && table_s->intermediate_res) {

            //struct interInterTable_arg64_t a[jobs_num];
            tuplebuffer64_t * tb;
            struct interInterSum_arg64_t * ja = (struct interInterSum_arg64_t *) malloc(sizeof(struct interInterSum_arg64_t) * jobs_num);

            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct interInterSum_arg64_t * a = ja + idx;//new interInterSum_arg;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->rel_num_r = table_r->rels_num;
                    a->rel_num_s = table_s->rels_num;
                    a->rids_r = table_r->row_ids;
                    a->rids_s = table_s->row_ids;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumInterInter_t64(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }
        else if (table_r->intermediate_res) {
            tuplebuffer64_t * tb;
            struct interNoninterSum_arg64_t * ja = (struct interNoninterSum_arg64_t *) malloc(sizeof(struct interNoninterSum_arg64_t) * jobs_num);
            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct interNoninterSum_arg64_t * a = ja + idx;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->rel_num_r = table_r->rels_num;
                    a->rids_r = table_r->row_ids;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumInterNonInter_t64(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }
        else if (table_s->intermediate_res) {
            tuplebuffer64_t * tb;
            struct noninterInterSum_arg64_t * ja = (struct noninterInterSum_arg64_t *) malloc(sizeof(struct noninterInterSum_arg64_t) * jobs_num);
            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct noninterInterSum_arg64_t * a = ja + idx;//new noninterInterSum_arg64_t;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->rel_num_s = table_s->rels_num;
                    a->rids_s = table_s->row_ids;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumNonInterInter_t64(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }
        else {
            tuplebuffer64_t * tb;
            struct noninterNoninterSum_arg64_t * ja = (struct noninterNoninterSum_arg64_t *) malloc(sizeof(struct noninterNoninterSum_arg64_t) * jobs_num);
            /* Loop all the buffers */
            int idx = 0;
            for (int th = 0; th < range ; th++) {
                chainedtuplebuffer64_t * cb = (chainedtuplebuffer64_t *) result->resultlist[th].results;
                tb = cb->buf;
                for (int buff = 0; buff < cb->numbufs && result->resultlist[th].nresults != 0; buff++) {
                    struct noninterNoninterSum_arg64_t * a = ja + idx;//new noninterNoninterSum_arg64_t;
                    a->priv_checsums   = sums[idx++];
                    a->distinctPairs_r = &distinctPairs_in_R;
                    a->distinctPairs_s = &distinctPairs_in_S;
                    a->size   = (buff == 0) ? cb->writepos : CHAINEDBUFF_NUMTUPLESPERBUF;
                    a->tb = tb;
                    job_scheduler.Schedule(new JobCheckSumNonInterNonInter_t64(*a));
                    tb = tb->next;
                }
            }
            job_args = (void *) ja;
        }

        /* free cb maybe */


        /* Barrier here */
        job_scheduler.Barrier();
        /* Create the checksum */
        for (size_t j = 0; j < sum.size(); j++) {
            for (size_t i = 0; i < sums.size(); i++) {
                sum[j] += sums[i][j];
            }
        }

        /* Free args */
        free(job_args);


        /* Construct the checksums in the right way */
        bool found = false;
        for (size_t i = 0; i < selections.size(); i++) {

            // Check if checksum is cached
            for (size_t j = 0; j < distinctPairs_in_R.size(); j++) {
                if (selections[i].colId == distinctPairs_in_R[j].colId
                    && selections[i].binding == distinctPairs_in_R[j].binding)
                {
                    (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j]);
                    found = true;
                    break;
                }
            }

            /* Search in the other */
            for (size_t j = 0; j < distinctPairs_in_S.size() && found != true; j++) {
                if (selections[i].colId == distinctPairs_in_S[j].colId
                    && selections[i].binding == distinctPairs_in_S[j].binding)
                {
                    (sum[j] == 0) ? result_str += "NULL" : result_str += to_string(sum[j +  distinctPairs_in_R.size()]);
                    found = true;
                    break;
                }
            }

            /* Flag for the next loop */
            found = false;

            // Create the write check sum
            if (i != selections.size() - 1) {
                result_str +=  " ";
            }
        }

        // Frees
        for (size_t i = 0; i < range; i++) {
            chainedtuplebuffer_free_t64((chainedtuplebuffer64_t *) result->resultlist[i].results);
        }
}
