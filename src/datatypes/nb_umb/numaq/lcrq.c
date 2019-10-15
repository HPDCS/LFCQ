// Copyright (c) 2013, Adam Morrison and Yehuda Afek.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
//  * Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
//  * Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in
//    the documentation and/or other materials provided with the
//    distribution.
//  * Neither the name of the Tel Aviv University nor the names of the
//    author of this software may be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <numa.h>
#include <numaif.h>

#include "primitives.h"
#include "lcrq.h"

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

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

#ifdef DEBUG
#define CAS2(ptr, o1, o2, n1, n2)                               \
({                                                              \
    int res;                                                    \
    res = __CAS2(ptr, o1, o2, n1, n2);                          \
    __executed_cas[__stats_thread_id].v++;                      \
    __failed_cas[__stats_thread_id].v += 1 - res;               \
    res;                                                        \
})
#else
#define CAS2(ptr, o1, o2, n1, n2)    __CAS2(ptr, o1, o2, n1, n2)
#endif


#define BIT_TEST_AND_SET(ptr, b)                                \
({                                                              \
    char __ret;                                                 \
    asm volatile("lock btsq $63, %0; setnc %1" : "+m"(*ptr), "=a"(__ret) : : "cc"); \
    __ret;                                                      \
})

static inline int is_empty(uint64_t v) __attribute__ ((pure));
static inline uint64_t node_index(uint64_t i) __attribute__ ((pure));
static inline uint64_t set_unsafe(uint64_t i) __attribute__ ((pure));
static inline uint64_t node_unsafe(uint64_t i) __attribute__ ((pure));
static inline uint64_t tail_index(uint64_t t) __attribute__ ((pure));
static inline int crq_is_closed(uint64_t t) __attribute__ ((pure));

static inline int is_empty(uint64_t v)  {
    return (v == (uint64_t)-1);
}


static inline uint64_t node_index(uint64_t i) {
    return (i & ~(1ull << 63));
}


static inline uint64_t set_unsafe(uint64_t i) {
    return (i | (1ull << 63));
}


static inline uint64_t node_unsafe(uint64_t i) {
    return (i & (1ull << 63));
}


static inline uint64_t tail_index(uint64_t t) {
    return (t & ~(1ull << 63));
}


static inline int crq_is_closed(uint64_t t) {
    return (t & (1ull << 63)) != 0;
}


void _lcrq_free_hook(ptst_t *p, void *ptr) 
{ 
    /*
    int node = -1, ret;
    ret = get_mempolicy(&node, NULL, 0, (void*)ptr, MPOL_F_NODE | MPOL_F_ADDR);
    if (ret != 0)
        abort();
    numa_free(ptr, node);
    */
   free(ptr); 
}

void _init_gc_lcrq() 
{
    printf("\n########\nRing Node size Bytes: %ld, Ring Queue Size Bytes: %ld\n#########\n", sizeof(RingNode), sizeof(RingQueue));
    //gc_aid[GC_RING_QUEUE] = gc_add_allocator(sizeof(RingQueue));
    gc_hid[3] = gc_add_hook(_lcrq_free_hook);
}


static inline void init_ring(RingQueue *r) {
    int i;

    for (i = 0; i < RING_SIZE; i++) {
        r->array[i].val = -1;
        r->array[i].idx = i;
    }

    r->head = r->tail = 0;
    r->next = null;
}

static inline void fixState(RingQueue *rq) {

    while (1) {
        uint64_t t = FAA64(&rq->tail, 0);
        uint64_t h = FAA64(&rq->head, 0);

        if (unlikely(rq->tail != t))
            continue;

        if (h > t) {
            if (CAS64(&rq->tail, t, h)) break;
            continue;
        }
        break;
    }
}

// SHARED_OBJECT_INIT
void lcrq_init(LCRQ *queue, unsigned int numa_node) {
    RingQueue *rq = malloc(sizeof(RingQueue));//numa_alloc_onnode(sizeof(RingQueue),numa_node);//gc_alloc_node(ptst, gc_aid[GC_RING_QUEUE], numa_node);
    init_ring(rq);
    queue->head = queue->tail = rq;
}

#ifdef RING_STATS
__thread uint64_t mycloses;
__thread uint64_t myunsafes;

uint64_t closes;
uint64_t unsafes;

inline void count_closed_crq(void) {
    mycloses++;
}


inline void count_unsafe_node(void) {
    myunsafes++;
}
#else
static inline void count_closed_crq(void) { }
static inline void count_unsafe_node(void) { }
#endif


static inline int close_crq(RingQueue *rq, const uint64_t t, const int tries) {
    if (tries < 10)
        return CAS64(&rq->tail, t + 1, (t + 1)|(1ull<<63));
    else
        return BIT_TEST_AND_SET(&rq->tail, 63);
}

bool _lcrq_enqueue(LCRQ *queue, uint64_t arg, unsigned int numa_node) {

    RingQueue *nrq = null;
    int try_close = 0;

    while (1) {
        RingQueue *rq = queue->tail;

#ifdef HAVE_HPTRS
        SWAP(&hazardptr, rq);
        if (unlikely(tail != rq))
            continue;
#endif

        RingQueue *next = rq->next;
 
        if (unlikely(next != null)) {
            CASPTR(&queue->tail, rq, next);
            continue;
        }

        uint64_t t = FAA64(&rq->tail, 1);

        if (crq_is_closed(t)) {
alloc:
            if (nrq == null) {
                nrq = (RingQueue*) malloc(sizeof(RingQueue));//numa_alloc_onnode(sizeof(RingQueue), numa_node);//gc_alloc_node(ptst, gc_aid[GC_RING_QUEUE], numa_node);
                init_ring(nrq);
            }

            // Solo enqueue
            nrq->tail = 1, nrq->array[0].val = arg, nrq->array[0].idx = 0;

            if (CASPTR(&rq->next, null, nrq)) {
                CASPTR(&queue->tail, rq, nrq);
                nrq = null;
                return true;
            }
            continue;
        }
           
        RingNode* cell = &rq->array[t & (RING_SIZE-1)];
        StorePrefetch(cell);

        uint64_t idx = cell->idx;
        uint64_t val = cell->val;

        if (likely(is_empty(val))) {
            if (likely(node_index(idx) <= t)) {
                if ((likely(!node_unsafe(idx)) || rq->head < t) && CAS2((uint64_t*)cell, -1, idx, arg, t)) {
                    if (nrq != null) {
                        //gc_free(ptst, nrq, gc_aid[GC_RING_QUEUE]); // to avoid use per thread variable
                        gc_add_ptr_to_hook_list(ptst, nrq, gc_hid[3]);
                    }
                    return true;
                }
            }
        } 

        uint64_t h = rq->head;

        if (unlikely((int64_t)(t - h) >= (int64_t)RING_SIZE) && close_crq(rq, t, ++try_close)) {
            count_closed_crq();
            goto alloc;
        }
    }
}

//inline Object dequeue(int pid) {
bool _lcrq_dequeue(LCRQ *queue, int64_t* item) {

    while (1) {
        RingQueue *rq = queue->head;
        RingQueue *next;

#ifdef HAVE_HPTRS
        SWAP(&hazardptr, rq);
        if (unlikely(head != rq))
            continue;
#endif

        uint64_t h = FAA64(&rq->head, 1);


        RingNode* cell = &rq->array[h & (RING_SIZE-1)];
        StorePrefetch(cell);

        uint64_t tt = 0;
        int r = 0;

        while (1) {

            uint64_t cell_idx = cell->idx;
            uint64_t unsafe = node_unsafe(cell_idx);
            uint64_t idx = node_index(cell_idx);
            uint64_t val = cell->val;

            if (unlikely(idx > h)) break;

            if (likely(!is_empty(val))) {
                if (likely(idx == h)) {
                    if (CAS2((uint64_t*)cell, val, cell_idx, -1, unsafe | (h + RING_SIZE))) {
                        *item = val;
                        return true;
                    }
                } else {
                    if (CAS2((uint64_t*)cell, val, cell_idx, val, set_unsafe(idx))) {
                        count_unsafe_node();
                        break;
                    }
                }
            } else {
                if ((r & ((1ull << 10) - 1)) == 0)
                    tt = rq->tail;

                // Optimization: try to bail quickly if queue is closed.
                int crq_closed = crq_is_closed(tt);
                uint64_t t = tail_index(tt);

                if (unlikely(unsafe)) { // Nothing to do, move along
                    if (CAS2((uint64_t*)cell, val, cell_idx, val, unsafe | (h + RING_SIZE)))
                        break;
                } else if (t < h + 1 || r > 200000 || crq_closed) {
                    if (CAS2((uint64_t*)cell, val, idx, val, (h + RING_SIZE))) {
                        if (r > 200000 && tt > RING_SIZE)
                            BIT_TEST_AND_SET(&rq->tail, 63);
                        break;
                    }
                } else {
                    ++r;
                }
            }
        }

        if (tail_index(rq->tail) <= h + 1) {
            fixState(rq);
            // try to return empty
            next = rq->next;
            if (next == null)
                return false;
            if (tail_index(rq->tail) <= h + 1)
                if (CASPTR(&queue->head, rq, next))
                { 
                    //gc_free(ptst, rq, gc_aid[GC_RING_QUEUE]);
                    gc_add_ptr_to_hook_list(ptst, rq, gc_hid[3]);
                }
        }
    }
}
  
void init_lcrq(LCRQ *queue, unsigned int numa_node) {
    lcrq_init(queue, numa_node);
}

void lcrq_enqueue(LCRQ *queue, void* payload, unsigned int numa_node) {
    _lcrq_enqueue(queue, payload, numa_node);
}

bool lcrq_dequeue(LCRQ *queue, void* *result) {
    return _lcrq_dequeue(queue, result);
}