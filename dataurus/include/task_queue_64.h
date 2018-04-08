/**
 * @file    task_queue.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Sat Feb  4 20:00:58 2012
 * @version $Id: task_queue.h 3017 2012-12-07 10:56:20Z bcagri $
 *
 * @brief  Implements task queue facility for the join processing.
 *
 */
#ifndef TASK_QUEUE_64_H
#define TASK_QUEUE_64_H

#include <pthread.h>
#include <stdlib.h>

#include "types.h" /* relation64_t, int32_t */

/**
 * @defgroup TaskQueue Task Queue Implementation
 * @{
 */

typedef struct task64_t task64_t;
typedef struct task_list64_t task_list64_t;
typedef struct task_queue64_t task_queue64_t;

struct task64_t {
    relation64_t relR;
    relation64_t tmpR;
    relation64_t relS;
    relation64_t tmpS;
    task64_t *   next;
};

struct task_list64_t {
    task64_t *    tasks;
    task_list64_t * next;
    int           curr;
};

struct task_queue64_t {
    pthread_mutex_t lock;
    pthread_mutex_t alloc_lock;
    task64_t *        head;
    task_list64_t *   free_list;
    int32_t         count;
    int32_t         alloc_size;
};

inline
task64_t *
get_next_task_t64(task_queue64_t * tq) __attribute__((always_inline));

inline
void
add_tasks_t64(task_queue64_t * tq, task64_t * t) __attribute__((always_inline));

inline
task64_t *
get_next_task_t64(task_queue64_t * tq)
{
    pthread_mutex_lock(&tq->lock);
    task64_t * ret = 0;
    if(tq->count > 0){
        ret = tq->head;
        tq->head = ret->next;
        tq->count --;
    }
    pthread_mutex_unlock(&tq->lock);

    return ret;
}

inline
void
add_tasks_t64(task_queue64_t * tq, task64_t * t)
{
    pthread_mutex_lock(&tq->lock);
    t->next = tq->head;
    tq->head = t;
    tq->count ++;
    pthread_mutex_unlock(&tq->lock);
}

/* atomically get the next available task */
inline
task64_t *
task_queue_get_atomic_t64(task_queue64_t * tq) __attribute__((always_inline));

/* atomically add a task */
inline
void
task_queue_add_atomic_t64(task_queue64_t * tq, task64_t * t)
    __attribute__((always_inline));

inline
void
task_queue_add_t64(task_queue64_t * tq, task64_t * t) __attribute__((always_inline));

inline
void
task_queue_copy_atomic_t64(task_queue64_t * tq, task64_t * t)
    __attribute__((always_inline));

/* get a free slot of task64_t */
inline
task64_t *
task_queue_get_slot_atomic_t64(task_queue64_t * tq) __attribute__((always_inline));

inline
task64_t *
task_queue_get_slot_t64(task_queue64_t * tq) __attribute__((always_inline));

/* initialize a task queue with given allocation block size */
task_queue64_t *
task_queue_init_t64(int alloc_size);

void
task_queue_free_t64(task_queue64_t * tq);

/**************** DEFINITIONS ********************************************/

inline
task64_t *
task_queue_get_atomic_t64(task_queue64_t * tq)
{
    pthread_mutex_lock(&tq->lock);
    task64_t * ret = 0;
    if(tq->count > 0){
        ret      = tq->head;
        tq->head = ret->next;
        tq->count --;
    }
    pthread_mutex_unlock(&tq->lock);

    return ret;
}

inline
void
task_queue_add_atomic_t64(task_queue64_t * tq, task64_t * t)
{
    pthread_mutex_lock(&tq->lock);
    t->next  = tq->head;
    tq->head = t;
    tq->count ++;
    pthread_mutex_unlock(&tq->lock);

}

inline
void
task_queue_add_t64(task_queue64_t * tq, task64_t * t)
{
    t->next  = tq->head;
    tq->head = t;
    tq->count ++;
}

inline
void
task_queue_copy_atomic_t64(task_queue64_t * tq, task64_t * t)
{
    pthread_mutex_lock(&tq->lock);
    task64_t * slot = task_queue_get_slot_t64(tq);
    *slot = *t; /* copy */
    task_queue_add_t64(tq, slot);
    pthread_mutex_unlock(&tq->lock);
}

inline
task64_t *
task_queue_get_slot_t64(task_queue64_t * tq)
{
    task_list64_t * l = tq->free_list;
    task64_t * ret;
    if(l->curr < tq->alloc_size) {
        ret = &(l->tasks[l->curr]);
        l->curr++;
    }
    else {
        task_list64_t * nl = (task_list64_t*) malloc(sizeof(task_list64_t));
        nl->tasks = (task64_t*) malloc(tq->alloc_size * sizeof(task64_t));
        nl->curr = 1;
        nl->next = tq->free_list;
        tq->free_list = nl;
        ret = &(nl->tasks[0]);
    }

    return ret;
}

/* get a free slot of task64_t */
inline
task64_t *
task_queue_get_slot_atomic_t64(task_queue64_t * tq)
{
    pthread_mutex_lock(&tq->alloc_lock);
    task64_t * ret = task_queue_get_slot_t64(tq);
    pthread_mutex_unlock(&tq->alloc_lock);

    return ret;
}

/* initialize a task queue with given allocation block size */
task_queue64_t *
task_queue_init_t64(int alloc_size)
{
    task_queue64_t * ret = (task_queue64_t*) malloc(sizeof(task_queue64_t));
    ret->free_list = (task_list64_t*) malloc(sizeof(task_list64_t));
    ret->free_list->tasks = (task64_t*) malloc(alloc_size * sizeof(task64_t));
    ret->free_list->curr = 0;
    ret->free_list->next = NULL;
    ret->count      = 0;
    ret->alloc_size = alloc_size;
    ret->head       = NULL;
    pthread_mutex_init(&ret->lock, NULL);
    pthread_mutex_init(&ret->alloc_lock, NULL);

    return ret;
}

void
task_queue_free_t64(task_queue64_t * tq)
{
    task_list64_t * tmp = tq->free_list;
    while(tmp) {
        free(tmp->tasks);
        task_list64_t * tmp2 = tmp->next;
        free(tmp);
        tmp = tmp2;
    }
    free(tq);
}

/** @} */

#endif /* TASK_QUEUE_64_H */
