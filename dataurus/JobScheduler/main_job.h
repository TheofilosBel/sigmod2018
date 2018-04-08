#pragma once
#include "job_master.h"
#include "Joiner.hpp"

using namespace std;


class JobMain: public JobMaster {
public:
    QueryInfo * i_;
    JoinTree  * joinTreePtr_;
    string result_;
    string line_;
    int query_no_;
    bool switch_64_;
    bool unsatisfied_filters_;

    JobMain(QueryInfo * i, string line, JoinTree * joinTreePtr, int query_no, bool switch_64, bool unsat_filters)
    : i_(i), joinTreePtr_(joinTreePtr), query_no_(query_no),
      line_(line), switch_64_(switch_64), unsatisfied_filters_(unsat_filters) {}
    ~JobMain() {}
    int Run(Joiner & joiner);
};
