#pragma once
#include "defn.h"
#include "Joiner.hpp"

// Class JobMaster - Abstract
class JobMaster {
public:
    JobMaster() = default;
    virtual ~JobMaster() {}

    // This method should be implemented by subclasses.
    virtual int Run(Joiner &) = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(JobMaster);
};
