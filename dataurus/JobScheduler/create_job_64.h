#pragma once

#include "job.h"
#include "tuple_buffer_64.h"
#include "types.h"


// Args for Non intermediate functions
struct noninterRel_arg64_t {
    unsigned low;
    unsigned high;
    tuple64_t  * tups;
    uint64_t * values;
};

// Args for intermediate functions
struct interRel_arg64_t {
    unsigned low;
    unsigned high;
    uint64_t * values;
    tuple64_t  * tups;
    unsigned * rids;
    unsigned rel_num;
    unsigned table_index;
};

// Args for inter R inter S Table create
struct interInterTable_arg64_t {
    tuplebuffer64_t * tb;
    unsigned   start_index;
    unsigned   rel_num_all;
    unsigned   rel_num_r;
    unsigned   rel_num_s;
    unsigned * rids_res;
    unsigned * rids_r;
    unsigned * rids_s;
    unsigned   index;
    unsigned   size;
    char       where;  // 1 is on R 2 is on S (victimized rel)
};

// Args for inter R non inter S Table create
struct interNoninterTable_arg64_t {
    tuplebuffer64_t * tb;
    unsigned   start_index;
    unsigned   rel_num_all;
    unsigned   rel_num_r;
    unsigned * rids_res;
    unsigned * rids_r;
    unsigned   index;
    unsigned   size;
    char       where;  // 1 is on R 2 is on S (victimized rel)
};

// Args for inter R non inter S Table create
struct noninterInterTable_arg64_t {
    tuplebuffer64_t * tb;
    unsigned   start_index;
    unsigned   rel_num_all;
    unsigned   rel_num_s;
    unsigned * rids_res;
    unsigned * rids_s;
    unsigned   index;
    unsigned   size;
    char       where;  // 1 is on R 2 is on S (victimized rel)
};

// Args for inter R non inter S Table create
struct noninterNoninterTable_arg64_t {
    tuplebuffer64_t * tb;
    unsigned   start_index;
    unsigned   rel_num_all;
    unsigned * rids_res;
    unsigned   index;
    unsigned   size;
    char       where;  // 1 is on R 2 is on S (victimized rel)
};

// Create Table T R is inter S is inter
class JobCreateInterInterTable_t64 : public CreateJob {
public:
    struct interInterTable_arg64_t & args_;

    JobCreateInterInterTable_t64(struct interInterTable_arg64_t & args)
    :args_(args)
    {}

    ~JobCreateInterInterTable_t64() {};

    int Run();
};

// Create Table T R is inter S is non inter
class JobCreateInterNonInterTable_t64 : public CreateJob {
public:
    struct interNoninterTable_arg64_t & args_;

    JobCreateInterNonInterTable_t64(struct interNoninterTable_arg64_t & args)
    :args_(args)
    {}

    ~JobCreateInterNonInterTable_t64() {};

    int Run();
};

// Create Table T R is non inter S is inter
class JobCreateNonInterInterTable_t64 : public CreateJob {
public:
    struct noninterInterTable_arg64_t & args_;

    JobCreateNonInterInterTable_t64(struct noninterInterTable_arg64_t & args)
    :args_(args)
    {}

    ~JobCreateNonInterInterTable_t64() {};

    int Run();
};

// Create Table T R is non inter S is non inter
class JobCreateNonInterNonInterTable_t64 : public CreateJob {
public:
    struct noninterNoninterTable_arg64_t & args_;

    JobCreateNonInterNonInterTable_t64(struct noninterNoninterTable_arg64_t & args)
    :args_(args)
    {}

    ~JobCreateNonInterNonInterTable_t64() {};

    int Run();
};

/*+++++++++++++++++++++++++++*/
/*  RELATIONS CREATION JOBS  */
/*+++++++++++++++++++++++++++*/

// Create Non intermediate realtion
class JobCreateNonInterRel_t64 : public CreateJob {
public:
    struct noninterRel_arg64_t & args_;

    JobCreateNonInterRel_t64(struct noninterRel_arg64_t & args)
    :args_(args)
    {}

    ~JobCreateNonInterRel_t64() {};

    int Run();
};

// Create intermediate realtion
class JobCreateInterRel_t64 : public CreateJob {
public:
    struct interRel_arg64_t & args_;

    JobCreateInterRel_t64(struct interRel_arg64_t & args)
    :args_(args)
    {}

    ~JobCreateInterRel_t64() {};

    int Run();
};
