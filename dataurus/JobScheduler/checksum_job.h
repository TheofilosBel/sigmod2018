#pragma once
#include <vector>

#include "job.h"
#include "tuple_buffer.h"
#include "types.h"

using namespace std;

struct selfJoinSum_arg {
    vector<struct checksumST> * distinctPairs;
    uint64_t * column_values_l;
    uint64_t * column_values_r;
    uint64_t * priv_checsums;
    unsigned * row_ids_matrix;
    unsigned low;
    unsigned high;
    int relations_num;
    int index_l;
    int index_r;
};

// Args for intermediate functions
struct interInterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer_t * tb;
    uint64_t * priv_checsums;
    unsigned   rel_num_r;
    unsigned   rel_num_s;
    unsigned * rids_r;
    unsigned * rids_s;
    unsigned   size;
};

// R inter S non Inter
struct interNoninterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer_t * tb;
    uint64_t * priv_checsums;
    unsigned   rel_num_r;
    unsigned * rids_r;
    unsigned   size;
};

// R non inter S inter
struct noninterInterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer_t * tb;
    uint64_t * priv_checsums;
    unsigned   rel_num_s;
    unsigned * rids_s;
    unsigned   size;
};

// R non inter S inter
struct noninterNoninterSum_arg {
    vector<struct checksumST> * distinctPairs_r;
    vector<struct checksumST> * distinctPairs_s;
    tuplebuffer_t * tb;
    uint64_t * priv_checsums;
    unsigned   size;
};


class JobCheckSumSelfJoin : public JobChechkSum {
public:
    struct selfJoinSum_arg & args_;

    JobCheckSumSelfJoin(struct selfJoinSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumSelfJoin() {};

    int Run();
};

class JobCheckSumInterInter : public JobChechkSum {
public:
    struct interInterSum_arg & args_;

    JobCheckSumInterInter(struct interInterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumInterInter() {};

    int Run();
};

class JobCheckSumInterNonInter : public JobChechkSum {
public:
    struct interNoninterSum_arg & args_;

    JobCheckSumInterNonInter(struct interNoninterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumInterNonInter() {};

    int Run();
};

// R Non intermediate S intermediate
class JobCheckSumNonInterInter : public JobChechkSum {
public:
    struct noninterInterSum_arg & args_;

    JobCheckSumNonInterInter(struct noninterInterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumNonInterInter() {};

    int Run();
};

// R Non intermediate S Non intermediate
class JobCheckSumNonInterNonInter : public JobChechkSum {
public:
    struct noninterNoninterSum_arg & args_;

    JobCheckSumNonInterNonInter(struct noninterNoninterSum_arg & args)
    :args_(args)
    {}

    ~JobCheckSumNonInterNonInter() {};

    int Run();
};
