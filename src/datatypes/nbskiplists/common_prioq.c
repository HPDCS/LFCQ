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




/* thread state. */
__thread unsigned long long s_compact = 0;
__thread unsigned long long s_tried = 0;
__thread unsigned long long s_changed = 0;

int gc_id[NUM_LEVELS];

#define MASTER_THREAD 42

/* initialize new node */
static node_t *
alloc_node()
{
	node_t *n;
    int level = 1;
    /* crappy rng */
    unsigned int r = ptst->rand;
    ptst->rand = r * 1103515245 + 12345;
    r &= (1u << (NUM_LEVELS - 1)) - 1;
    /* uniformly distributed bits => geom. dist. level, p = 0.5 */
    while ((r >>= 1) & 1)
	++level;
    assert(1 <= level && level <= 32);

    n = gc_alloc(ptst, gc_id[level - 1]);
    n->level = level;
    n->inserting = 1;
    memset(n->next, 0, level * sizeof(node_t *));

    return n;
}


/* Mark node as ready for reclamation to the garbage collector. */
void 
free_node(node_t *n)
{
    gc_free(ptst, (void *)n, gc_id[(n->level) - 1]);
}


/***** locate_preds *****
 * Record predecessors and non-deleted successors of key k.  If k is
 * encountered during traversal of list, the node will be in succs[0].
 *
 * To detect skew in insert operation, return a pointer to the only
 * deleted node not having it's delete flag set.
 *
 * Skew example illustration, when locating 3. Level 1 is shifted in
 * relation to level 0, due to not noticing that s[1] is deleted until
 * level 0 is reached. (pointers in illustration are implicit, e.g.,
 * 0 --> 7 at level 2.)
 *
 *                   del
 *                   p[0]
 * p[2]  p[1]        s[1]  s[0]  s[2]
 *  |     |           |     |     |
 *  v     |           |     |     v
 *  _     v           v     |     _
 * | |    _           _	    v    | |
 * | |   | |    _    | |    _    | |
 * | |   | |   | |   | |   | |   | |
 *  0     1     2     4     6     7
 *  d     d     d
 *
 */
static node_t *
locate_preds(pq_t * restrict pq, pkey_t k, node_t ** restrict preds, node_t ** restrict succs)
{
    node_t *x, *x_next, *del = NULL;
    int d = 0, i;

    x = pq->head;
    i = NUM_LEVELS - 1;
    while (i >= 0)
    {
       x_next = x->next[i];
       d = is_marked_ref(x_next);
       x_next = get_unmarked_ref(x_next);
       assert(x_next != NULL);
	
       while (x_next->k < k || is_marked_ref(x_next->next[0])
              || ((i == 0) && d)) {
           /* Record bottom level deleted node not having delete flag
            * set, if traversed. */
           if (i == 0 && d)
               del = x_next;
           x = x_next;
           x_next = x->next[i];
           d = is_marked_ref(x_next);
           x_next = get_unmarked_ref(x_next);
           assert(x_next != NULL);
       }
       preds[i] = x;
       succs[i] = x_next;
       i--;
    }
    return del;
}

/***** insert *****
 * Insert a new node n with key k and value v.
 * The node will not be inserted if another node with key k is already
 * present in the list.
 *
 * The predecessors, preds, and successors, succs, at all levels are
 * recorded, after which the node n is inserted from bottom to
 * top. Conditioned on that succs[i] is still the successor of
 * preds[i], n will be spliced in on level i.
 */
int 
pq_enqueue(void *p, pkey_t k, pval_t v)
{
    pq_t *pq = (pq_t*)p;
    node_t *preds[NUM_LEVELS], *succs[NUM_LEVELS];
    node_t *new = NULL, *del = NULL;
    int res=0;
    
    critical_enter();

    assert(SENTINEL_KEYMIN < k && k < SENTINEL_KEYMAX);

    /* Initialise a new node for insertion. */
    new    = alloc_node();
    new->k = k;
    new->v = v;

    /* lowest level insertion retry loop */
 retry:
    del = locate_preds(pq, k, preds, succs);

    /* return if key already exists, i.e., is present in a non-deleted
     * node */
    if (succs[0]->k == k && !is_marked_ref(preds[0]->next[0]) && preds[0]->next[0] == succs[0]) {
        new->inserting = 0;
        free_node(new);
        res=1;
        goto out;
    }
    new->next[0] = succs[0];

    /* The node is logically inserted once it is present at the bottom
     * level. */
    if (!__sync_bool_compare_and_swap(&preds[0]->next[0], succs[0], new)) {
        /* either succ has been deleted (modifying preds[0]),
         * or another insert has succeeded or preds[0] is head,
         * and a restructure operation has updated it */
        goto retry;
    }

    /* Insert at each of the other levels in turn. */
    int i = 1;
    while ( i < new->level)
    {
        /* If successor of new is deleted, we're done. (We're done if
         * only new is deleted as well, but this we can't tell) If a
         * candidate successor at any level is deleted, we consider
         * the operation completed. */
        if (is_marked_ref(new->next[0]) ||
            is_marked_ref(succs[i]->next[0]) ||
            del == succs[i])
            goto success;

        /* prepare next pointer of new node */
        new->next[i] = succs[i];
        if (!__sync_bool_compare_and_swap(&preds[i]->next[i], succs[i], new))
        {
            /* failed due to competing insert or restructure */
            del = locate_preds(pq, k, preds, succs);

            /* if new has been deleted, we're done */
            if (succs[0] != new) goto success;
	    
        } else {
            /* Succeeded at this level. */
            i++;
        }
    }
 success:
    if (new) {
        /* this flag must be reset *after* all CAS have completed */
        new->inserting = 0;
    }
    
out:    
      critical_exit();
      return res;
}


/***** restructure *****
 *
 * Update the head node's pointers from level 1 and up. Will locate
 * the last node at each level that has the delete flag set, and set
 * the head to point to the successor of that node. After completion,
 * if operating in isolation, for each level i, it holds that
 * head->next[i-1] is before or equal to head->next[i]. 
 *
 * Illustration valid state after completion:
 *
 *             h[0]  h[1]  h[2]
 *              |     |     |
 *              |     |     v
 *  _           |     v     _ 
 * | |    _     v     _	   | |
 * | |   | |    _    | |   | |
 * | |   | |   | |   | |   | |
 *  d     d
 * 
 */
void
restructure(pq_t *pq)
{
    node_t *pred, *cur, *h;
    int i = NUM_LEVELS - 1;

    pred = pq->head;
    while (i > 0) {
        /* the order of these reads must be maintained */
        h = pq->head->next[i]; /* record observed head */
        CMB();
        cur = pred->next[i]; /* take one step forward from pred */
        if (!is_marked_ref(h->next[0])) {
            i--;
            continue;
        }
        /* traverse level until non-marked node is found
         * pred will always have its delete flag set
         */
        while(is_marked_ref(cur->next[0])) {
            pred = cur;
            cur = pred->next[i];
        }
        assert(is_marked_ref(pred->next[0]));
	
        /* swing head pointer */
        if (__sync_bool_compare_and_swap(&pq->head->next[i],h,cur))
            i--;
    }
}


/*
 * Init structure, setup sentinel head and tail nodes.
 */
void*
pq_init(unsigned int threads, double none, unsigned int max_offset)
{
    pq_t *pq;
    node_t *t, *h;
    int i;

	
    _init_gc_subsystem();
    /* head and tail nodes */
    t = calloc(1, sizeof *t + (NUM_LEVELS-1)*sizeof(node_t *));
    h = calloc(1, sizeof *h + (NUM_LEVELS-1)*sizeof(node_t *));
    
    t->inserting = 0;
    h->inserting = 0;

    t->k = SENTINEL_KEYMAX;
    h->k = SENTINEL_KEYMIN;
    h->level = NUM_LEVELS;
    t->level = NUM_LEVELS;
    
    for ( i = 0; i < NUM_LEVELS; i++ ){
        h->next[i] = t;
		gc_id[i] = gc_add_allocator(i*sizeof(node_t*)+sizeof(node_t));
    }
    pq = malloc(sizeof *pq);
    pq->head = h;
    pq->tail = t;
    pq->max_offset = max_offset;

    return pq;
}

/* Cleanup, mark all the nodes for recycling. */
void
pq_destroy(pq_t *pq)
{
    node_t *cur, *pred;
    cur = pq->head;
    while (cur != pq->tail) {
	pred = cur;
	cur = get_unmarked_ref(pred->next[0]);
	free_node(pred);
    }
    free(pq->tail);
    free(pq->head);
    free(pq);
}

void pq_report(int tid){
	printf("\n%d- NEAR %llu %llu %llu %d", TID, s_compact, s_tried, s_changed, 0);
}

void pq_reset_statistics(){ }

unsigned int pq_num_malloc(){  return 0;}