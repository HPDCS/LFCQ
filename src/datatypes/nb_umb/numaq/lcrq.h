#ifndef LCRQ_H
#define LCRQ_H

#include <stdint.h>
#include <stdbool.h>

#include "../gc/gc.h"

// Definition: RING_POW
// --------------------
// The LCRQ's ring size will be 2^{RING_POW}.
#ifndef RING_POW
#define RING_POW        (4) //find a way to use a size of at least 6
#endif
#define RING_SIZE       (1ull << RING_POW)

extern int gc_aid[];
extern int gc_hid[];
extern __thread ptst_t *ptst;

#define GC_RING_QUEUE   0

typedef struct RingNode {
    volatile uint64_t val;
    volatile uint64_t idx;
    uint64_t pad[14];
} RingNode __attribute__ ((aligned (128)));

typedef struct RingQueue {
    volatile int64_t head __attribute__ ((aligned (128)));
    volatile int64_t tail __attribute__ ((aligned (128)));
    struct RingQueue *next __attribute__ ((aligned (128)));
    RingNode array[RING_SIZE];
} RingQueue __attribute__ ((aligned (128)));

typedef struct LCRQ {
    struct RingQueue *head __attribute__ ((aligned (128)));
    struct RingQueue *tail __attribute__ ((aligned (128)));
} LCRQ __attribute__ ((aligned (128)));

// Function used by the PQ
void _init_gc_lcrq();
void init_lcrq(LCRQ *queue, unsigned int numa_node);
void lcrq_enqueue(LCRQ *queue, void* payload, unsigned int numa_node);
bool lcrq_dequeue(LCRQ *queue, void* *result);

#endif /* !LCRQ_H */
