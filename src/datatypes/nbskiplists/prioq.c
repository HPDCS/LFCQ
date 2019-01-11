/*************************************************************************
 * prioq.c
 * 
 * Lock-free concurrent priority queue.
 *
 * Copyright (c) 2012-2014, Jonatan Linden
 * 
 * Adapted from Keir Fraser's skiplist, 
 * Copyright (c) 2001-2003, Keir Fraser
 * 
 * Keir Fraser's skiplist is available at
 * http://www.cl.cam.ac.uk/research/srg/netos/lock-free/.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 
 *  * Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials provided
 *    with the distribution.
 * 
 *  * The name of the author may not be used to endorse or promote
 *    products derived from this software without specific prior
 *    written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


#include <assert.h>
#include <stdlib.h>


/* interface, constant defines, and typedefs */
#include "prioq.h"

#include <stddef.h>



/* deletemin
 *
 * Delete element with smallest key in queue.
 * Try to update the head node's pointers, if offset > max_offset.
 *
 * Traverse level 0 next pointers until one is found that does
 * not have the delete bit set. 
 */
pkey_t
pq_dequeue(void *p, void **result)
{    
    pq_t *pq = (pq_t*)p;
    pval_t   v = NULL;
    node_t *x, *nxt, *obs_head = NULL, *newhead, *cur;
    int offset;
    
    newhead = NULL;
    offset = 0;
	critical_enter();
    x = pq->head;
    obs_head = x->next[0];

    do {
        offset++;

        /* expensive, high probability that this cache line has
         * been modified */
        nxt = x->next[0];

        // tail cannot be deleted
        if (get_unmarked_ref(nxt) == pq->tail) {
            *result = NULL;
            return INFTY;
        }

        /* Do not allow head to point past a node currently being
         * inserted. This makes the lock-freedom quite a theoretic
         * matter. */
        if (newhead == NULL && x->inserting) newhead = x;
 
        /* optimization */
        if (is_marked_ref(nxt)) continue;
        /* the marker is on the preceding pointer */
        /* linearisation point deletemin */
        nxt = __sync_fetch_and_or(&x->next[0], 1);
    }
    while ( (x = get_unmarked_ref(nxt)) && is_marked_ref(nxt) );

    assert(!is_marked_ref(x));

    v = x->v;
    
    /* If no inserting node was traversed, then use the latest 
     * deleted node as the new lowest-level head pointed node
     * candidate. */
    if (newhead == NULL) newhead = x;

    /* if the offset is big enough, try to update the head node and
     * perform memory reclamation */
    if (offset <= pq->max_offset) goto out;

    /* Optimization. Marginally faster */
    if (pq->head->next[0] != obs_head) goto out;
    
    /* try to swing the lowest level head pointer to point to newhead,
     * which is deleted */
    if (__sync_bool_compare_and_swap(&pq->head->next[0], obs_head, get_marked_ref(newhead)))
    {
        /* Update higher level pointers. */
        restructure(pq);

        /* We successfully swung the upper head pointer. The nodes
         * between the observed head (obs_head) and the new bottom
         * level head pointed node (newhead) are guaranteed to be
         * non-live. Mark them for recycling. */

        cur = get_unmarked_ref(obs_head);
        while (cur != get_unmarked_ref(newhead)) {
            nxt = get_unmarked_ref(cur->next[0]);
            assert(is_marked_ref(cur->next[0]));
            free_node(cur);
            cur = nxt;
        }
    }
 out:
	critical_exit();
    *result = v;
    return x->k;
}
