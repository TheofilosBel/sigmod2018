#include "main_job.h"
#include "time.h"

static double timeExecute = 0;
static double timeTreegen = 0;


int JobMain::Run(Joiner & joiner) {

    // Count Query Time
    double timeMain;
    struct timeval start;
    gettimeofday(&start, NULL);


    // If we have unsatisfied filters print NULL
    // if (unsat_filters) {
    //     result_ = "";
    //     for (size_t i = 0; i < queryInfo.selections.size(); i++) {
    //         result_ += "NULL";
    //
    //         // Create the write check sum
    //         if (i != queryInfo.selections.size() - 1) {
    //             result_str +=  " ";
    //         }
    //     }
    // }


    // run the right execute
    bool stop = false;
    table_t* result;
    if (switch_64_)
        result = joinTreePtr_->root->execute_t64(joinTreePtr_->root, joiner, *i_, result_, &stop);
    else
        result = joinTreePtr_->root->execute(joinTreePtr_->root, joiner, *i_, result_, &stop);

    // Free unused vars
    joinTreePtr_->destroy();
    delete i_;
    delete joinTreePtr_;
}
