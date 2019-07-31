#ifndef LCRQ_H
#define LCRQ_H

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#include "../gc/gc.h"
#include "../../../utils/hpdcs_utils.h"
// Definition: RING_POW
// --------------------
// The LCRQ's ring size will be 2^{RING_POW}.
#ifndef RING_POW
#define RING_POW        (4)//(17)   //otherwise the allocator cannot handle it
#endif
#define RING_SIZE       (1ull << RING_POW)

#define __CAS2(ptr, o1, o2, n1, n2)                             \
({                                                              \
    char __ret;                                                 \
    __typeof__(o2) __junk;                                      \
    __typeof__(*(ptr)) __old1 = (o1);                           \
    __typeof__(o2) __old2 = (o2);                               \
    __typeof__(*(ptr)) __new1 = (n1);                           \
    __typeof__(o2) __new2 = (n2);                               \
    asm volatile("lock cmpxchg16b %2;setz %1"                   \
                   : "=d"(__junk), "=a"(__ret), "+m" (*ptr)     \
                   : "b"(__new1), "c"(__new2),                  \
                     "a"(__old1), "d"(__old2));                 \
    __ret; })

#define CAS2(ptr, o1, o2, n1, n2)    __CAS2(ptr, o1, o2, n1, n2)

#define BIT_TEST_AND_SET(ptr, b)                                \
({                                                              \
    char __ret;                                                 \
    asm volatile("lock btsq $63, %0; setnc %1" : "+m"(*ptr), "=a"(__ret) : : "cc"); \
    __ret;                                                      \
})

#define __FAA64(A, B)   __sync_fetch_and_add(A, B)
#define FAA64(A, B) __FAA64((volatile int64_t *)A, (int64_t)B);

#define __CAS64(A, B, C)    __sync_bool_compare_and_swap(A, B, C)
#define CAS64(A, B, C) __CAS64((uint64_t *)A, (uint64_t)B, (uint64_t)C)

#define __CASPTR(A, B, C)   __sync_bool_compare_and_swap((long *)A, (long)B, (long)C)
#define CASPTR(A, B, C) __CASPTR((void *)A, (void *)B, (void *)C)

#define ReadPrefetch(A)            __builtin_prefetch((const void *)A, 0, 3);
#define StorePrefetch(A)           __builtin_prefetch((const void *)A, 1, 3);


extern int gc_aid[];
extern int gc_hid[];
extern __thread ptst_t *ptst;

#define GC_RING_NODE    0
#define GC_RING_QUEUE   1

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
void _init_gc_queue_lrcq();
void init_lrcq_queue(LCRQ *queue, unsigned int numa_node);
void lcrq_enqueue(LCRQ *queue, void* payload, unsigned int numa_node);
bool lcrq_dequeue(LCRQ *queue, void* *result);

#endif /* !LCRQ_H */