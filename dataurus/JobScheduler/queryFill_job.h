#pragma once
#include "job_master.h"
#include "QueryPlan.hpp"

using namespace std;

class JobFillQueryPlan: public JobMaster {
public:
    QueryPlan& queryPlan_;
    JobScheduler & js1_;
    JobScheduler & js2_;
    bool & switch_64_;

    JobFillQueryPlan(QueryPlan & qp, JobScheduler & js1, JobScheduler & js2, bool & switch_64) 
    : queryPlan_(qp), js1_(js1), js2_(js2), switch_64_(switch_64) {}

    ~JobFillQueryPlan() {}
    int Run(Joiner & joiner);
};
