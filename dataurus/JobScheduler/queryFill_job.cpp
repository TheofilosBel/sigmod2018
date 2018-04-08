#include "queryFill_job.h"

int JobFillQueryPlan::Run(Joiner & joiner) {
    queryPlan_.fillColumnInfo(joiner, js1_, js2_, switch_64_);
}
