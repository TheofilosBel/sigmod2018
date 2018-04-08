#include "checksum_job.h"


/*+++++++++++++++++++++++++++++++++++++++*/
/* NON INTERMEDIATE CHECKSUMS FUNCTIONS  */
/*+++++++++++++++++++++++++++++++++++++++*/

int JobCheckSumSelfJoin::Run() {

    unsigned   relnum = args_.relations_num;
    unsigned * rids = args_.row_ids_matrix;
    uint64_t * column_l = args_.column_values_l;
    uint64_t * column_r = args_.column_values_r;
    uint64_t * csums   = args_.priv_checsums;

    /* Get the checksums for table R */
    int index = 0;
    for (struct checksumST & p: *(args_.distinctPairs)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = args_.low; i < args_.high; i++) {
            if (column_l[rids[i*relnum + args_.index_l]] == column_r[rids[i*relnum + args_.index_r]])
                csums[index] += values[rids[i*relnum + idx]];
        }
        index++;
    }

    // free(&args_);
}




int JobCheckSumInterInter::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_r;
    unsigned   size    = args_.size;
    unsigned   relnum  = args_.rel_num_r;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[rids[(tups[i].key)*relnum + idx]];
        }
        index++;
    }

    /* Get the checksums for table S */
    rids   = args_.rids_s;
    relnum = args_.rel_num_s;
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[rids[(tups[i].payload)*relnum + idx]];
        }
        index++;
    }
}


int JobCheckSumInterNonInter::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_r;
    unsigned   size    = args_.size;
    unsigned   relnum  = args_.rel_num_r;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[rids[(tups[i].key)*relnum + idx]];
        }
        index++;
    }

    /* Get the checksums for table S */
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[tups[i].payload];
        }
        index++;
    }
}

// R no inter  S inter
int JobCheckSumNonInterInter::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_s;
    unsigned   size    = args_.size;
    unsigned   relnum  = args_.rel_num_s;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[tups[i].key];
        }
        index++;
    }

    /* Get the checksums for table S */
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[rids[(tups[i].payload)*relnum + idx]];
        }
        index++;
    }
}

// R no inter  S non inter
int JobCheckSumNonInterNonInter::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned   size    = args_.size;

    /* Get the checksums for table R */
    int index = 0;
    tuple_t * tups = tb->tuples;
    for (struct checksumST & p: *(args_.distinctPairs_r)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[tups[i].key];
        }
        index++;
    }

    /* Get the checksums for table S */
    for (struct checksumST & p: *(args_.distinctPairs_s)) {
        unsigned   idx    = p.index;
        uint64_t * values = p.values;

        /* Loop the 2d array to find checksums */
        for (size_t i = 0; i < size; i++) {
            csums[index] += values[tups[i].payload];
        }
        index++;
    }
}
