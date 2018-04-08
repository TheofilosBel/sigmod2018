#include "checksum_job_64.h"


/*+++++++++++++++++++++++++++++++++++++++*/
/* NON INTERMEDIATE CHECKSUMS FUNCTIONS  */
/*+++++++++++++++++++++++++++++++++++++++*/

int JobCheckSumInterInter_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_r;
    unsigned   size    = args_.size;
    unsigned   relnum  = args_.rel_num_r;

    /* Get the checksums for table R */
    int index = 0;
    tuple64_t * tups = tb->tuples;
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


int JobCheckSumInterNonInter_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_r;
    unsigned   size    = args_.size;
    unsigned   relnum  = args_.rel_num_r;

    /* Get the checksums for table R */
    int index = 0;
    tuple64_t * tups = tb->tuples;
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
int JobCheckSumNonInterInter_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned * rids    = args_.rids_s;
    unsigned   size    = args_.size;
    unsigned   relnum  = args_.rel_num_s;

    /* Get the checksums for table R */
    int index = 0;
    tuple64_t * tups = tb->tuples;
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
int JobCheckSumNonInterNonInter_t64::Run () {

    /* Get some variables form the chained buffer */
    tuplebuffer64_t * tb = args_.tb;
    uint64_t * csums   = args_.priv_checsums;
    unsigned   size    = args_.size;

    /* Get the checksums for table R */
    int index = 0;
    tuple64_t * tups = tb->tuples;
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
