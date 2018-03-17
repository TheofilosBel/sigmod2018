/**
 * @file    tuple_buffer.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Thu Nov  8 22:08:54 2012
 * @version $Id: tuple_buffer.h 3203 2013-01-21 22:10:35Z bcagri $ 
 * 
 * @brief   Implements a chained-buffer storage model for tuples.
 * 
 * 
 */
#ifndef TUPLE_BUFFER_H
#define TUPLE_BUFFER_H

#include <stdlib.h>
#include <stdio.h>

#include "types.h"


#define CHAINEDBUFF_NUMTUPLESPERBUF (1024*1024)

/** If rid-pairs are coming from a sort-merge join then 1, otherwise for hash
    joins it is always 0 since output is not sorted. */
#define SORTED_MATERIALIZE_TO_FILE 0

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif


typedef struct chainedtuplebuffer_t chainedtuplebuffer_t;
typedef struct tuplebuffer_t tuplebuffer_t;

struct tuplebuffer_t {
    tuple_t * tuples;
    tuplebuffer_t * next;
};

struct chainedtuplebuffer_t {
    tuplebuffer_t * buf;
    tuplebuffer_t * readcursor;
    tuplebuffer_t * writecursor;
    uint32_t writepos;
    uint32_t readpos;
    uint32_t readlen;
    uint32_t numbufs;
};

static inline void 
cb_begin(chainedtuplebuffer_t * cb)
{
    cb->readpos = 0;
    cb->readlen = cb->writepos;
    cb->readcursor = cb->buf;
}

static inline void 
cb_begin_backwards(chainedtuplebuffer_t * cb)
{
    cb->readpos = cb->writepos - 1;
    cb->readcursor = cb->buf;
}

static inline tuple_t *
cb_read_next(chainedtuplebuffer_t * cb)
{
    if(cb->readpos == cb->readlen) {
        cb->readpos = 0;
        cb->readlen = CHAINEDBUFF_NUMTUPLESPERBUF;
        cb->readcursor = cb->readcursor->next;
    }

    return (cb->readcursor->tuples + cb->readpos ++);
}

static inline tuple_t *
cb_read_backwards(chainedtuplebuffer_t * cb)
{
    tuple_t * res = (cb->readcursor->tuples + cb->readpos);

    if(cb->readpos == 0) {
        cb->readpos = CHAINEDBUFF_NUMTUPLESPERBUF - 1;
        cb->readcursor = cb->readcursor->next;
    }
    else {
        cb->readpos --;
    }

    return res;
}

static inline tuple_t *
cb_next_writepos(chainedtuplebuffer_t * cb)
{
    if(cb->writepos == CHAINEDBUFF_NUMTUPLESPERBUF) {
        tuplebuffer_t * newbuf = (tuplebuffer_t*) malloc(sizeof(tuplebuffer_t));
        posix_memalign ((void **)&newbuf->tuples, 
                            CACHE_LINE_SIZE, sizeof(tuple_t)
                            * CHAINEDBUFF_NUMTUPLESPERBUF);

        newbuf->next = cb->buf;
        cb->buf = newbuf;
        cb->writepos = 0;
        cb->numbufs ++;
    }

    return (cb->buf->tuples + cb->writepos ++);
}

static chainedtuplebuffer_t *
chainedtuplebuffer_init(void)
{
    chainedtuplebuffer_t * newcb = (chainedtuplebuffer_t *) 
                                   malloc(sizeof(chainedtuplebuffer_t));
    tuplebuffer_t * newbuf = (tuplebuffer_t *) malloc(sizeof(tuplebuffer_t));

    newbuf->tuples = (tuple_t *) malloc(sizeof(tuple_t)
                                        * CHAINEDBUFF_NUMTUPLESPERBUF);
    newbuf->next   = NULL;
    
    newcb->buf      = newcb->readcursor = newcb->writecursor = newbuf;
    newcb->writepos = newcb->readpos = 0;
    newcb->numbufs  = 1;

    return newcb;
}

static void
chainedtuplebuffer_free(chainedtuplebuffer_t * cb)
{
    tuplebuffer_t * tmp = cb->buf;

    while(tmp) {
        tuplebuffer_t * tmp2 = tmp->next;
        free(tmp->tuples);
        free(tmp);
        tmp = tmp2;
    }

    free(cb);
}

/** Descending order comparison */
static inline int __attribute__((always_inline))
thrkeycmp(const void * k1, const void * k2)
{
    int val = ((tuple_t *)k2)->key - ((tuple_t *)k1)->key;
    return val;
}

/**
 * Works only when result_t->threadresult_t->results is of type
 * chainedtuplebuffer_t
 */
static void
write_result_relation(result_t * res, char * filename)
{
#if SORTED_MATERIALIZE_TO_FILE
    FILE * fp = fopen(filename, "w");
    int i;
    int64_t j;

    //fprintf(fp, "#KEY, VAL\n");

    tuple_t threadorder[res->nthreads];

    for(i = 0; i < res->nthreads; i++) {
        int64_t nresults = res->resultlist[i].nresults;

        if(nresults > 0) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *)
                                        res->resultlist[i].results;

            cb_begin_backwards(cb);
            tuple_t * tup = cb_read_backwards(cb);
            threadorder[i].key = tup->key;
            threadorder[i].payload = i; /* thread index */
        }
        else {
            threadorder[i].key = 0;
            threadorder[i].payload = -1;
        }
    }

    /* just to output thread results sorted */
    qsort(threadorder, res->nthreads, sizeof(tuple_t), thrkeycmp);

    for(i = 0; i < res->nthreads; i++) {
        int tid = threadorder[i].payload;

        if(tid != -1) {
            chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *)
                                        res->resultlist[tid].results;
            int64_t nresults = res->resultlist[tid].nresults;
            cb_begin_backwards(cb);

            for (j = 0; j < nresults; j++) {
                tuple_t * tup = cb_read_backwards(cb);
                fprintf(fp, "%d %d\n", tup->key, tup->payload);
            }
        }
    }

    fclose(fp);

#else

    FILE * fp = fopen(filename, "w");
    int i;
    int64_t j;

    for(i = 0; i < res->nthreads; i++) {

        // char fname[256];
        // sprintf(fname, "Thread-%d.tbl", i);
        // fp = fopen(fname, "w");

        chainedtuplebuffer_t * cb = (chainedtuplebuffer_t *)
                                    res->resultlist[i].results;
        int64_t nresults = res->resultlist[i].nresults;

        cb_begin_backwards(cb);

        for (j = 0; j < nresults; j++) {
            tuple_t * tup = cb_read_backwards(cb);
            fprintf(fp, "%d %d\n", tup->key, tup->payload);
        }

        // fclose(fp);
    }

    fclose(fp);

#endif

}

#endif /* TUPLE_BUFFER_H */
