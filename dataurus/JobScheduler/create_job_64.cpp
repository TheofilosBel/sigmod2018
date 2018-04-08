#include "create_job_64.h"


/*+++++++++++++++++++++++++*/
/* CREATE TABLES FUNCTIONS */
/*+++++++++++++++++++++++++*/

// Create table with inter R , inter S
int JobCreateInterInterTable_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb     = args_.tb;
    unsigned   rel_num_all = args_.rel_num_all;
    unsigned   start_idx   = args_.start_index;
    unsigned   rel_num_r   = args_.rel_num_r;
    unsigned   rel_num_s   = args_.rel_num_s;
    unsigned * rids_res    = args_.rids_res;
    unsigned   vict_idx    = args_.index;
    unsigned * rids_r      = args_.rids_r;
    unsigned * rids_s      = args_.rids_s;
    unsigned   size        = args_.size;
    char       vict_where  = args_.where;

    /* Create new table */
    unsigned  row_i;
    tuple64_t * tups = tb->tuples;

    int rem;
    for (size_t i = 0; i < size; i++) {
        rem = 0;
        row_i = tups[i].key;
        for (size_t j = 0; j < rel_num_r; j++) {
            if (vict_where == 1 && vict_idx == j) {rem = 1; continue;}
            rids_res[(start_idx + i)*rel_num_all + j - rem] =  rids_r[row_i*rel_num_r + j];
        }

        //std::cerr << "rem " << rem << '\n';

        row_i = tups[i].payload;
        for (size_t j = 0; j < rel_num_s; j++) {
            if (vict_where == 2 && vict_idx == j) {rem = 1; continue;}
            rids_res[(start_idx + i)*rel_num_all + j + rel_num_r - rem] = rids_s[row_i*rel_num_s + j];
        }
    }
}

// Create table with inter R , non inter S
int JobCreateInterNonInterTable_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb     = args_.tb;
    unsigned   rel_num_all = args_.rel_num_all;
    unsigned   start_idx   = args_.start_index;
    unsigned   rel_num_r   = args_.rel_num_r;
    unsigned * rids_res    = args_.rids_res;
    unsigned   vict_idx    = args_.index;
    unsigned * rids_r      = args_.rids_r;
    unsigned   size        = args_.size;
    char       vict_where  = args_.where;

    /* Create new table */
    unsigned  row_i;
    tuple64_t * tups = tb->tuples;

    int rem;
    for (size_t i = 0; i < size; i++) {
        row_i = tups[i].key;
        rem = 0;
        for (size_t j = 0; j < rel_num_r; j++) {
            if (vict_where == 1 && vict_idx == j) {rem = 1; continue;}
            rids_res[(start_idx + i)*rel_num_all + j - rem] =  rids_r[row_i*rel_num_r + j];
        }

        if (vict_where == 2) {continue;}
        row_i = tups[i].payload;
        rids_res[(start_idx + i)*rel_num_all + rel_num_r - rem] = row_i;
    }
}

// Create table with non inter R , inter S
int JobCreateNonInterInterTable_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb     = args_.tb;
    unsigned   rel_num_all = args_.rel_num_all;
    unsigned   start_idx   = args_.start_index;
    unsigned   rel_num_s   = args_.rel_num_s;
    unsigned * rids_res    = args_.rids_res;
    unsigned   vict_idx    = args_.index;
    unsigned * rids_s      = args_.rids_s;
    unsigned   size        = args_.size;
    char       vict_where  = args_.where;

    /* Create new table */
    unsigned  row_i;
    tuple64_t * tups = tb->tuples;

    int rem;
    for (size_t i = 0; i < size; i++) {
        rem = 0;

        // Loop R relation
        if (vict_where != 1) {
            row_i = tups[i].key;
            rids_res[(start_idx + i)*rel_num_all] = row_i;
        }
        else {
            rem = 1;
        }

        // Loop S relation
        row_i = tups[i].payload;
        for (size_t j = 0; j < rel_num_s; j++) {
            if (vict_where == 2 && vict_idx == j) {rem = 1; continue;}
            rids_res[(start_idx + i)*rel_num_all + j + 1 - rem] = rids_s[row_i*rel_num_s + j];
        }
    }
}

// Create table with non inter R , non inter S
int JobCreateNonInterNonInterTable_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb     = args_.tb;
    unsigned   rel_num_all = args_.rel_num_all;
    unsigned   start_idx   = args_.start_index;
    unsigned * rids_res    = args_.rids_res;
    unsigned   vict_idx    = args_.index;
    unsigned   size        = args_.size;
    char       vict_where  = args_.where;

    /* Create new table */
    unsigned  row_i;
    tuple64_t * tups = tb->tuples;

    int rem;
    for (size_t i = 0; i < size; i++) {

        if (vict_where == 1) {

            // Get S relation
            row_i = tups[i].payload;
            rids_res[(start_idx + i)*rel_num_all] = row_i;
        }
        else if (vict_where == 2) {

            // Get R relation
            row_i = tups[i].key;
            rids_res[(start_idx + i)*rel_num_all] = row_i;
        }
        else {
            row_i = tups[i].key;
            rids_res[(start_idx + i)*rel_num_all] = row_i;

            row_i = tups[i].payload;
            rids_res[(start_idx + i)*rel_num_all + 1] = row_i;
        }
    }
}


/*+++++++++++++++++++++++++++*/
/* CREATE RELATION FUNCTIONS */
/*+++++++++++++++++++++++++++*/

int JobCreateNonInterRel_t64::Run() {
    /* Initialize the tuple array */
    for (size_t i = args_.low; i < args_.high; i++) {
        args_.tups[i].key     = args_.values[i];
        args_.tups[i].payload = i;
    }
}

int JobCreateInterRel_t64::Run() {
    /* Initialize the tuple array */
    for (size_t i = args_.low; i < args_.high; i++) {
        args_.tups[i].key     = args_.values[args_.rids[i*(args_.rel_num) + args_.table_index]];
        args_.tups[i].payload = i;
    }
}
