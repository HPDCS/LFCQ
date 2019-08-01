// micheal-scott locking fifo queue
/* For now used this for fast implementation, then try to move to LCRQ */
//@TODO move to LCRQ
#define MSQ_H
#ifndef MSQ_H
#define MSQ_H

#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#include "../gc/gc.h"

#define GC_NODES 0

extern int gc_aid[];
extern int gc_hid[];
extern __thread ptst_t *ptst;

typedef struct __node_t node_t;
struct __node_t
{
    void* value;
    node_t *next;
};

typedef struct  
{
    node_t *head;
    node_t *tail;
    pthread_spinlock_t h_lock;
    pthread_spinlock_t t_lock;
}
queue_t;

void _init_gc_queue();
void init_queue(queue_t *queue, unsigned int numa_node);
void msq_enqueue(queue_t *queue, void* payload, unsigned int numa_node);
bool msq_dequeue(queue_t *queue, void* *result);

#endif /* !MSQ_H */
