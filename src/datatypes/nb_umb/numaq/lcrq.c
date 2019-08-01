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

#include <stdio.h>

#include "lcrq.h"

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

static inline void init_ring(RingQueue *r) {
    int i;

    for (i = 0; i < RING_SIZE; i++) {
        r->array[i].val = -1;
        r->array[i].idx = i;
    }

    r->head = r->tail = 0;
    r->next = NULL;
}

static inline void fixState(RingQueue *rq) {

    uint64_t t, h, n;

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

static inline int close_crq(RingQueue *rq, const uint64_t t, const int tries) {
    if (tries < 10)
        return CAS64(&rq->tail, t + 1, (t + 1)|(1ull<<63));
    else
        return BIT_TEST_AND_SET(&rq->tail, 63);
}

void _init_gc_queue() {

    printf("Ring Node size: %ld, Ring Queue size: %ld\n", sizeof(RingNode), sizeof(RingQueue));

    gc_aid[GC_RING_NODE] = gc_add_allocator(sizeof(RingNode));
    gc_aid[GC_RING_QUEUE] = gc_add_allocator(sizeof(RingQueue));
}

// SHARED_OBJECT_INIT
void init_lcrq(LCRQ *queue, unsigned int numa_node) {
    RingQueue *rq = gc_alloc_node(ptst, gc_aid[GC_RING_QUEUE], numa_node);
    init_ring(rq);
    queue->head = queue->tail = rq;
}

void lcrq_enqueue(LCRQ *queue, void* payload, unsigned int numa_node) {
    
    int try_close = 0;
    RingQueue * nrq = NULL;

    while (1) {

        RingQueue *rq = queue->tail;
        RingQueue *next = rq->next;

        if (unlikely(next != NULL)) {
            CASPTR(&queue->tail, rq, next);
            continue;
        }
        
        uint64_t t = FAA64(&rq->tail, 1);

        if (crq_is_closed(t)) {
alloc:
            if (nrq == NULL) {
                nrq = gc_alloc_node(ptst, gc_aid[GC_RING_QUEUE], numa_node);
                init_ring(nrq);
            }
            // Solo enqueue
            nrq->tail = 1, nrq->array[0].val = payload, nrq->array[0].idx = 0;

            if (CASPTR(&rq->next, NULL, nrq)) {
                CASPTR(&queue->tail, rq, nrq);
                return;
            }
            //we didn't enqueue nrq
            continue;
        }

        RingNode* cell = &rq->array[t & (RING_SIZE-1)];
        StorePrefetch(cell);

        uint64_t idx = cell->idx;
        uint64_t val = cell->val;

        if (likely(is_empty(val))) {
            if (likely(node_index(idx) <= t)) {
                if ((likely(!node_unsafe(idx)) || rq->head < t) && CAS2((uint64_t*)cell, -1, idx, payload, t)) {
                    
                    if (nrq != NULL) {
                        gc_free(ptst, nrq, gc_aid[GC_RING_QUEUE]); // to avoid use per thread variable
                    }
                    return;
                }
            }
        } 

        uint64_t h = rq->head;

        if (unlikely((int64_t)(t - h) >= (int64_t)RING_SIZE) && close_crq(rq, t, ++try_close)) {
            goto alloc;
        }
    }   
}

bool lcrq_dequeue(LCRQ *queue, void* *result) {

    while (1) {
        RingQueue *rq = queue->head;
        RingQueue *next;

        uint64_t h = FAA64(&rq->head, 1);

        RingNode* cell = &rq->array[h & (RING_SIZE-1)];
        StorePrefetch(cell);

        uint64_t tt;
        int r = 0;

        while (1) {

            uint64_t cell_idx = cell->idx;
            uint64_t unsafe = node_unsafe(cell_idx);
            uint64_t idx = node_index(cell_idx);
            uint64_t val = cell->val;

            if (unlikely(idx > h)) break;

            if (likely(!is_empty(val))) {
                if (likely(idx == h)) {
                    if (CAS2((uint64_t*)cell, val, cell_idx, -1, unsafe | h + RING_SIZE))
                        *result = val;
                        return true;
                } else {
                    if (CAS2((uint64_t*)cell, val, cell_idx, val, set_unsafe(idx))) {
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
                    if (CAS2((uint64_t*)cell, val, cell_idx, val, unsafe | h + RING_SIZE))
                        break;
                } else if (t < h + 1 || r > 200000 || crq_closed) {
                    if (CAS2((uint64_t*)cell, val, idx, val, h + RING_SIZE)) {
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
            if (next == NULL) {
                *result = NULL;
                return false;  // EMPTY
            }
            if (tail_index(rq->tail) <= h + 1)
                CASPTR(&queue->head, rq, next);
        }
    }
}