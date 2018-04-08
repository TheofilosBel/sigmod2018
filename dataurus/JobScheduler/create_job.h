#pragma once
#include "job.h"
#include "tuple_buffer.h"
#include "types.h"


// Args for Non intermediate functions
struct noninterRel_arg {
    unsigned low;
    unsigned high;
    tuple_t  * tups;
    uint64_t * values;
};

// Args for intermediate functions
struct interRel_arg {
    unsigned low;
    unsigned high;
    uint64_t * values;
    tuple_t  * tups;
    unsigned * rids;
    unsigned rel_num;
    unsigned table_index;
};

// Args for inter R inter S Table create
struct interInterTable_arg {
    tuplebuffer_t * tb;
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
struct interNoninterTable_arg {
    tuplebuffer_t * tb;
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
struct noninterInterTable_arg {
    tuplebuffer_t * tb;
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
struct noninterNoninterTable_arg {
    tuplebuffer_t * tb;
    unsigned   start_index;
    unsigned   rel_num_all;
    unsigned * rids_res;
    unsigned   index;
    unsigned   size;
    char       where;  // 1 is on R 2 is on S (victimized rel)
};

// Create Table T R is inter S is inter
class JobCreateInterInterTable : public CreateJob {
public:
    struct interInterTable_arg & args_;

    JobCreateInterInterTable(struct interInterTable_arg & args)
    :args_(args)
    {}

    ~JobCreateInterInterTable() {};

    int Run();
};

// Create Table T R is inter S is non inter
class JobCreateInterNonInterTable : public CreateJob {
public:
    struct interNoninterTable_arg & args_;

    JobCreateInterNonInterTable(struct interNoninterTable_arg & args)
    :args_(args)
    {}

    ~JobCreateInterNonInterTable() {};

    int Run();
};

// Create Table T R is non inter S is inter
class JobCreateNonInterInterTable : public CreateJob {
public:
    struct noninterInterTable_arg & args_;

    JobCreateNonInterInterTable(struct noninterInterTable_arg & args)
    :args_(args)
    {}

    ~JobCreateNonInterInterTable() {};

    int Run();
};

// Create Table T R is non inter S is non inter
class JobCreateNonInterNonInterTable : public CreateJob {
public:
    struct noninterNoninterTable_arg & args_;

    JobCreateNonInterNonInterTable(struct noninterNoninterTable_arg & args)
    :args_(args)
    {}

    ~JobCreateNonInterNonInterTable() {};

    int Run();
};

/*+++++++++++++++++++++++++++*/
/*  RELATIONS CREATION JOBS  */
/*+++++++++++++++++++++++++++*/

// Create Non intermediate realtion
class JobCreateNonInterRel : public CreateJob {
public:
    struct noninterRel_arg & args_;

    JobCreateNonInterRel(struct noninterRel_arg & args)
    :args_(args)
    {}

    ~JobCreateNonInterRel() {};

    int Run();
};

// Create intermediate realtion
class JobCreateInterRel : public CreateJob {
public:
    struct interRel_arg & args_;

    JobCreateInterRel(struct interRel_arg & args)
    :args_(args)
    {}

    ~JobCreateInterRel() {};

    int Run();
};
