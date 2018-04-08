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
#ifndef TUPLE_BUFFER_64_H
#define TUPLE_BUFFER_64_H

#include <stdlib.h>
#include <stdio.h>

#include "types.h"

#ifndef CHAINEDBUFF_NUMTUPLESPERBUF
#define CHAINEDBUFF_NUMTUPLESPERBUF (1024*1024)  // maybe * 2
#endif

/** If rid-pairs are coming from a sort-merge join then 1, otherwise for hash
    joins it is always 0 since output is not sorted. */
#define SORTED_MATERIALIZE_TO_FILE 0

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif


typedef struct chainedtuplebuffer64_t chainedtuplebuffer64_t;
typedef struct tuplebuffer64_t tuplebuffer64_t;

struct tuplebuffer64_t {
    tuple64_t * tuples;
    tuplebuffer64_t * next;
};

struct chainedtuplebuffer64_t {
    tuplebuffer64_t * buf;
    tuplebuffer64_t * readcursor;
    tuplebuffer64_t * writecursor;
    uint32_t writepos;
    uint32_t readpos;
    uint32_t readlen;
    uint32_t numbufs;
};

static inline void
cb_begin_t64(chainedtuplebuffer64_t * cb)
{
    cb->readpos = 0;
    cb->readlen = cb->writepos;
    cb->readcursor = cb->buf;
}

static inline void
cb_begin_backwards_t64(chainedtuplebuffer64_t * cb)
{
    cb->readpos = cb->writepos - 1;
    cb->readcursor = cb->buf;
}

static inline tuple64_t *
cb_read_next_t64(chainedtuplebuffer64_t * cb)
{
    if(cb->readpos == cb->readlen) {
        cb->readpos = 0;
        cb->readlen = CHAINEDBUFF_NUMTUPLESPERBUF;
        cb->readcursor = cb->readcursor->next;
    }

    return (cb->readcursor->tuples + cb->readpos ++);
}

static inline tuple64_t *
cb_read_backwards_t64(chainedtuplebuffer64_t * cb)
{
    tuple64_t * res = (cb->readcursor->tuples + cb->readpos);

    if(cb->readpos == 0) {
        cb->readpos = CHAINEDBUFF_NUMTUPLESPERBUF - 1;
        cb->readcursor = cb->readcursor->next;
    }
    else {
        cb->readpos --;
    }

    return res;
}

static inline tuple64_t *
cb_next_writepos_t64(chainedtuplebuffer64_t * cb)
{
    int w;
    if(cb->writepos == CHAINEDBUFF_NUMTUPLESPERBUF) {
        tuplebuffer64_t * newbuf = (tuplebuffer64_t*) malloc(sizeof(tuplebuffer64_t));
        w = posix_memalign ((void **)&newbuf->tuples,
                            CACHE_LINE_SIZE, sizeof(tuple64_t)
                            * CHAINEDBUFF_NUMTUPLESPERBUF);

        newbuf->next = cb->buf;
        cb->buf = newbuf;
        cb->writepos = 0;
        cb->numbufs ++;
    }

    return (cb->buf->tuples + cb->writepos ++);
}

static chainedtuplebuffer64_t *
chainedtuplebuffer_init_t64(void)
{
    chainedtuplebuffer64_t * newcb = (chainedtuplebuffer64_t *)
                                   malloc(sizeof(chainedtuplebuffer64_t));
    tuplebuffer64_t * newbuf = (tuplebuffer64_t *) malloc(sizeof(tuplebuffer64_t));

    newbuf->tuples = (tuple64_t *) malloc(sizeof(tuple64_t)
                                        * CHAINEDBUFF_NUMTUPLESPERBUF);
    newbuf->next   = NULL;

    newcb->buf      = newcb->readcursor = newcb->writecursor = newbuf;
    newcb->writepos = newcb->readpos = 0;
    newcb->numbufs  = 1;

    return newcb;
}

static void
chainedtuplebuffer_free_t64(chainedtuplebuffer64_t * cb)
{
    tuplebuffer64_t * tmp = cb->buf;

    while(tmp) {
        tuplebuffer64_t * tmp2 = tmp->next;
        free(tmp->tuples);
        free(tmp);
        tmp = tmp2;
    }

    free(cb);
}

/** Descending order comparison */
// static inline int __attribute__((always_inline))
// thrkeycmp_t64(const void * k1, const void * k2)
// {
//     int val = ((tuple64_t *)k2)->key - ((tuple64_t *)k1)->key;
//     return val;
// }

#endif /* TUPLE_BUFFER_H */
