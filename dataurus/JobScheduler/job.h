#pragma once
#include "defn.h"
#include <iostream>


struct checksumST {
    unsigned colId;
    unsigned index;
    unsigned binding;
    uint64_t * values;
};

// Class Job - Abstract
class Job {
 public:
  Job() = default;
  virtual ~Job() {}

  // This method should be implemented by subclasses.
  virtual int Run() = 0;

 private:
  DISALLOW_COPY_AND_ASSIGN(Job);
};

class CreateJob : public Job {
public:
    CreateJob() {}

    ~CreateJob() {};

    virtual int Run()=0;
};


class JobChechkSum : public Job {
public:
    JobChechkSum() {}

    ~JobChechkSum() {};

    virtual int Run()=0;
};
