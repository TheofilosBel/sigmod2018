#pragma once

#include <vector>
#include "job.h"
#include "tuple_buffer_64.h"
#include "types.h"

using namespace std;


// Args for intermediate functions
struct interInterSum_arg64_t {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer64_t * tb;
    uint64_t * priv_checsums;
    unsigned   rel_num_r;
    unsigned   rel_num_s;
    unsigned * rids_r;
    unsigned * rids_s;
    unsigned   size;
};

// R inter S non Inter
struct interNoninterSum_arg64_t {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer64_t * tb;
    uint64_t * priv_checsums;
    unsigned   rel_num_r;
    unsigned * rids_r;
    unsigned   size;
};

// R non inter S inter
struct noninterInterSum_arg64_t {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer64_t * tb;
    uint64_t * priv_checsums;
    unsigned   rel_num_s;
    unsigned * rids_s;
    unsigned   size;
};

// R non inter S inter
struct noninterNoninterSum_arg64_t {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer64_t * tb;
    uint64_t * priv_checsums;
    unsigned   size;
};

class JobCheckSumInterInter_t64 : public JobChechkSum {
public:
    struct interInterSum_arg64_t & args_;

    JobCheckSumInterInter_t64(struct interInterSum_arg64_t & args)
    :args_(args)
    {}

    ~JobCheckSumInterInter_t64() {};

    int Run();
};

class JobCheckSumInterNonInter_t64 : public JobChechkSum {
public:
    struct interNoninterSum_arg64_t & args_;

    JobCheckSumInterNonInter_t64(struct interNoninterSum_arg64_t & args)
    :args_(args)
    {}

    ~JobCheckSumInterNonInter_t64() {};

    int Run();
};

// R Non intermediate S intermediate
class JobCheckSumNonInterInter_t64 : public JobChechkSum {
public:
    struct noninterInterSum_arg64_t & args_;

    JobCheckSumNonInterInter_t64(struct noninterInterSum_arg64_t & args)
    :args_(args)
    {}

    ~JobCheckSumNonInterInter_t64() {};

    int Run();
};

// R Non intermediate S Non intermediate
class JobCheckSumNonInterNonInter_t64 : public JobChechkSum {
public:
    struct noninterNoninterSum_arg64_t & args_;

    JobCheckSumNonInterNonInter_t64(struct noninterNoninterSum_arg64_t & args)
    :args_(args)
    {}

    ~JobCheckSumNonInterNonInter_t64() {};

    int Run();
};
