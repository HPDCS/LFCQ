/******************************************************************************
 * gc.c
 * 
 * A fully recycling epoch-based garbage collector. Works by counting
 * threads in and out of critical regions, to work out when
 * garbage queues can be fully deleted.
 * 
 * Copyright (c) 2001-2003, K A Fraser
 * 
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * * Redistributions of source code must retain the above copyright 
 * notice, this list of conditions and the following disclaimer.
 * 
 * * Redistributions in binary form must reproduce the above copyright 
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * 
 * * The name of the author may not be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR 
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE 
 * DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES 
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#include <sys/syscall.h>
#include <numa.h>
#include <numaif.h>


#include "portable_defns.h"
#include "gc.h"

#define _PAGE_SIZE 4096
#define _PAGE_NODE_BITMAKS ~0xFFFUL

#define MAX_NODES 16 /* UP to one thread per node <- _NUMA_NODES MACRO in makefile does the job*/

/*#define MINIMAL_GC*/
/*#define YIELD_TO_HELP_PROGRESS*/
//#define PROFILE_GC

/* Recycled nodes are filled with this value if WEAK_MEM_ORDER. */
#define INVALID_BYTE 0
#define INITIALISE_NODES(_p,_c) memset((_p), INVALID_BYTE, (_c));

/* Number of unique block sizes we can deal with. */
#define MAX_SIZES 32 //cannot handle sizes that goes over  (page size - 8) Bytes

#define MAX_HOOKS 4

/*
 * The initial number of allocation chunks for each per-blocksize list.
 * Popular allocation lists will steadily increase the allocation unit
 * in line with demand.
 */
#define ALLOC_CHUNKS_PER_LIST 10

/*
 * How many times should a thread call gc_enter(), seeing the same epoch
 * each time, before it makes a reclaim attempt?
 */
#define ENTRIES_PER_RECLAIM_ATTEMPT 100

/*
 *  0: current epoch -- threads are moving to this;
 * -1: some threads may still throw garbage into this epoch;
 * -2: no threads can see this epoch => we can zero garbage lists;
 * -3: all threads see zeros in these garbage lists => move to alloc lists.
 */
#ifdef WEAK_MEM_ORDER
#define NR_EPOCHS 4
#else
#define NR_EPOCHS 3
#endif

/*
 * A chunk amortises the cost of allocation from shared lists. It also
 * helps when zeroing nodes, as it increases per-cacheline pointer density
 * and means that node locations don't need to be brought into the cache 
 * (most architectures have a non-temporal store instruction).
 */
#define BLKS_PER_CHUNK 71//100
typedef struct chunk_st chunk_t;
struct chunk_st
{
    chunk_t *next;             /* chunk chaining                 */
    unsigned int i;            /* the next entry in blk[] to use */
    void *blk[BLKS_PER_CHUNK];
};

static struct gc_global_st
{
    CACHE_PAD(0);

    /* The current epoch. */
    VOLATILE unsigned int current;
    CACHE_PAD(1);

    /* Exclusive access to gc_reclaim(). */
    VOLATILE unsigned int inreclaim;
    CACHE_PAD(2);

    /*
     * RUN-TIME CONSTANTS (to first approximation)
     */

    /* Memory page size, in bytes. */
    unsigned int page_size;

    /* Configured Numa Nodes */
    unsigned int numa_nodes;

    /* Node sizes (run-time constants). */
    int nr_sizes;
    int blk_sizes[MAX_SIZES];

    /* Registered epoch hooks. */
    int nr_hooks;
    hook_fn_t hook_fns[MAX_HOOKS];
    CACHE_PAD(3);

    /*
     * DATA WE MAY HIT HARD
     */

    /* Chain of free, empty chunks. */
    chunk_t * VOLATILE free_chunks[_NUMA_NODES]; /* One cache per node - trying to reduce cross accesses at least in allocation */

    /* Main allocation lists. */
    chunk_t * VOLATILE alloc[_NUMA_NODES][MAX_SIZES];
    VOLATILE unsigned int alloc_size[_NUMA_NODES][MAX_SIZES];
#ifdef PROFILE_GC
    VOLATILE unsigned int total_size;
    VOLATILE unsigned int allocations;
#endif
} gc_global;


/* Per-thread state. */
struct gc_st
{
    /* Epoch that this thread sees. */
    unsigned int epoch;

    /* Number of calls to gc_entry() since last gc_reclaim() attempt. */
    unsigned int entries_since_reclaim;

#ifdef YIELD_TO_HELP_PROGRESS
    /* Number of calls to gc_reclaim() since we last yielded. */
    unsigned int reclaim_attempts_since_yield;
#endif

    /* Used by gc_async_barrier(). */
    void *async_page;
    int   async_page_state;

    /* Garbage lists. */
    chunk_t *garbage[_NUMA_NODES][NR_EPOCHS][MAX_SIZES];
    chunk_t *garbage_tail[_NUMA_NODES][NR_EPOCHS][MAX_SIZES];
    chunk_t *chunk_cache[_NUMA_NODES];

    /* Local allocation lists. */
    chunk_t *alloc[_NUMA_NODES][MAX_SIZES];
    unsigned int alloc_chunks[_NUMA_NODES][MAX_SIZES];

    /* Hook pointer lists. */
    chunk_t *hook[NR_EPOCHS][MAX_HOOKS];
};


#define MEM_FAIL(_s)                                                         \
do {                                                                         \
    fprintf(stderr, "OUT OF MEMORY: %d bytes at line %d\n", (int) (_s), __LINE__); \
    exit(1);                                                                 \
} while ( 0 )


/* Allocate more empty chunks from the heap. */
//#define CHUNKS_PER_ALLOC 1000
static chunk_t* node_alloc_more_chunks(unsigned int node)
{
    //int i;
    unsigned int page_size = gc_global.page_size;
    char *mem_area, *end, *next_page;

    chunk_t *h, *p;
    size_t alloc_size = page_size * 150; //1050 chnunks

    /* I care which is the numa node of this area of memory because of hooks*/
    mem_area = numa_alloc_onnode(alloc_size, node);
    if ( mem_area == NULL ) MEM_FAIL(alloc_size);
    
    end = mem_area + alloc_size;
    next_page = mem_area + page_size;

    mem_area += sizeof(unsigned long); // @TODO change the skip, the empty goes to the beginning, in this way the addresses power of 2 will be aligned (More performances)

    h = p = mem_area;
    mem_area += sizeof(chunk_t);

    while (next_page <= end) {
        // mark the page with node number.
        *((unsigned long *) ((unsigned long) mem_area & _PAGE_NODE_BITMAKS)) = node;

        while (mem_area + sizeof(chunk_t) <= next_page)
        {
            p->next = (chunk_t*) mem_area;
            p = p->next;
            mem_area += sizeof(chunk_t);
        }

        mem_area = next_page;
        next_page = mem_area + page_size;
        mem_area = mem_area + sizeof(unsigned long);

    }

    p->next = h;
    return (h);
}

/* Put a chain of chunks onto a list. */ 
static void add_chunks_to_list(chunk_t *ch, chunk_t *head)
{
    chunk_t *h_next, *new_h_next, *ch_next;
    ch_next    = ch->next;
    new_h_next = head->next;
    do { ch->next = h_next = new_h_next; WMB_NEAR_CAS(); }
    while ( (new_h_next = CASPO(&head->next, h_next, ch_next)) != h_next );
}

/* Allocate a chain of @n empty chunks. Pointers may be garbage. */
static chunk_t *node_get_empty_chunks(int n, unsigned int node)
{
    int i;
    chunk_t *new_rh, *rh, *rt, *head;

 retry:
    head = gc_global.free_chunks[node];
    new_rh = head->next;
    do {
        rh = new_rh;
        rt = head;
        WEAK_DEP_ORDER_RMB();
        for ( i = 0; i < n; i++ )
        {
            if ( (rt = rt->next) == head )
            {
                /* Allocate some more chunks. */
                add_chunks_to_list(node_alloc_more_chunks(node), head);
                goto retry;
            }
        }
    }
    while ( (new_rh = CASPO(&head->next, rh, rt->next)) != rh );

    rt->next = rh;
    return(rh);
}

/* Get @n filled chunks, pointing at blocks of @sz bytes each. 
 * Get them from requested node // @TODO Fix this bad boy
 * */
static chunk_t *node_get_filled_chunks(int n, int sz, unsigned int numa_node)
{
    chunk_t *h, *p;
    char *node, *start, *end, *check;
    int i;
    
    unsigned int page_size = gc_global.page_size;

    int alloc_size, num_pages, sizes_per_page, page_available;

#ifdef PROFILE_GC
    ADD_TO(gc_global.total_size, n * BLKS_PER_CHUNK * sz);
    ADD_TO(gc_global.allocations, 1);
#endif
 
    sizes_per_page = (page_size - sizeof(unsigned long))/sz;
    page_available = sizes_per_page * sz;

    alloc_size = n * BLKS_PER_CHUNK * sz; // original size
    num_pages = (alloc_size / page_available) + 1;
    alloc_size = num_pages * page_size;

    node = start = numa_alloc_onnode(alloc_size, numa_node);
    if ( node == NULL ) MEM_FAIL(alloc_size);

    end = node + alloc_size;

#ifdef WEAK_MEM_ORDER
    INITIALISE_NODES(node, n * BLKS_PER_CHUNK * sz);
#endif

    h = p = node_get_empty_chunks(n, numa_node);

    check = node+page_size;
    //node += sizeof(unsigned long);

    /*
    do {
        p->i = BLKS_PER_CHUNK;
        i = 0;
        do {
            p->blk[i++] = node;
            node = node + sz;
            if (node + sz >= check) {
                node = check;
                check = node+page_size;
                node += sizeof(unsigned long);
            }
        } while(i < BLKS_PER_CHUNK);
    } while((p = p->next) != h);
    */
    
    p->i = BLKS_PER_CHUNK;
    i = 0;

    while (node + page_size <= end) 
    {
        check = node + page_size;

        // skip numa node idx
        node += sizeof(unsigned long); // @TODO change the skip, the empty goes to the beginning, in this way the addresses will be aligned (More performances)

        while (node + sz <= check) //we have enough memory to handle request?
        {
            // assign node to blk index
            p->blk[i++] = node;
            
            // take next sz
            node += sz;

            if (i == BLKS_PER_CHUNK) {
                if ( (p = p->next) == h ) goto out;
                p->i = BLKS_PER_CHUNK;
                i = 0;
            }
        }
        node = check;
    }
    out:
    return(h);
}

/*
 * gc_async_barrier: Cause an asynchronous barrier in all other threads. We do 
 * this by causing a TLB shootdown to be propagated to all other processors. 
 * Each time such an action is required, this function calls:
 *   mprotect(async_page, <page size>, <new flags>)
 * Each thread's state contains a memory page dedicated for this purpose.
 */
#ifdef WEAK_MEM_ORDER
static void gc_async_barrier(gc_t *gc)
{
    mprotect(gc->async_page, gc_global.page_size,
             gc->async_page_state ? PROT_READ : PROT_NONE);
    gc->async_page_state = !gc->async_page_state;
}
#else
#define gc_async_barrier(_g) ((void)0)
#endif


/* Grab a level @i allocation chunk from main chain. */
static chunk_t *node_get_alloc_chunk(gc_t *gc, int i, unsigned int numa_node)
{
    chunk_t *alloc, *p, *new_p, *nh;
    unsigned int sz;

    alloc = gc_global.alloc[numa_node][i];
    new_p = alloc->next;

    do {
        p = new_p;
        while ( p == alloc )
        {
            sz = gc_global.alloc_size[numa_node][i];
            nh = node_get_filled_chunks(sz, gc_global.blk_sizes[i], numa_node);
            ADD_TO(gc_global.alloc_size[numa_node][i], sz >> 3);
            gc_async_barrier(gc);
            add_chunks_to_list(nh, alloc);
            p = alloc->next;
        }
        WEAK_DEP_ORDER_RMB();
    }
    while ( (new_p = CASPO(&alloc->next, p, p->next)) != p );

    p->next = p;
    assert(p->i == BLKS_PER_CHUNK);
    return(p);
}

#ifndef MINIMAL_GC
/*
 * gc_reclaim: Scans the list of struct gc_perthread looking for the lowest
 * maximum epoch number seen by a thread that's in the list code. If it's the
 * current epoch, the "nearly-free" lists from the previous epoch are 
 * reclaimed, and the epoch is incremented.
 */
static void gc_reclaim(ptst_t * our_ptst)
{
    ptst_t       *ptst, *first_ptst; //, *our_ptst = NULL;
    gc_t         *gc = NULL;
    unsigned long curr_epoch;
    chunk_t      *ch, *t, *next_t;
    //int           two_ago, three_ago, i, j;
    int           three_ago, i, j, k;
    
    /* Barrier to entering the reclaim critical section. */
    if ( gc_global.inreclaim || CASIO(&gc_global.inreclaim, 0, 1) ) return;

    /*
     * Grab first ptst structure *before* barrier -- prevent bugs
     * on weak-ordered architectures.
     */
    first_ptst = ptst_first();
    MB();
    curr_epoch = gc_global.current;

    /* Have all threads seen the current epoch, or not in mutator code? */
    for ( ptst = first_ptst; ptst != NULL; ptst = ptst_next(ptst) )
    {
        if ( (ptst->count > 1) && (ptst->gc->epoch != curr_epoch) ) goto out;
    }

    /*
     * Three-epoch-old garbage lists move to allocation lists.
     * Two-epoch-old garbage lists are cleaned out.
     */
    //two_ago   = (curr_epoch+2) % NR_EPOCHS;
    three_ago = (curr_epoch+1) % NR_EPOCHS;
    //if ( gc_global.nr_hooks != 0 )
        //our_ptst = (ptst_t *)pthread_getspecific(ptst_key);
    for ( ptst = first_ptst; ptst != NULL; ptst = ptst_next(ptst) )
    {
        gc = ptst->gc;

        for ( k=0; k < gc_global.numa_nodes; k++)
        {   
            for ( i = 0; i < gc_global.nr_sizes; i++ )
            {
                /* NB. Leave one chunk behind, as it is probably not yet full. */
                t = gc->garbage[k][three_ago][i];
                if ( (t == NULL) || ((ch = t->next) == t) ) continue;
                gc->garbage_tail[k][three_ago][i]->next = ch;
                gc->garbage_tail[k][three_ago][i] = t;
                t->next = t;

                add_chunks_to_list(ch, gc_global.alloc[k][i]);
            }
        
        }

        for ( i = 0; i < gc_global.nr_hooks; i++ )
        {
            hook_fn_t fn = gc_global.hook_fns[i];
            ch = gc->hook[three_ago][i];
            if ( ch == NULL ) continue;
            gc->hook[three_ago][i] = NULL;

            t = next_t = ch;
            do { 
                for ( j = 0; j < t->i; j++ ) 
                    fn(our_ptst, t->blk[j]);

                k = *((unsigned long*) (((unsigned long) t) & _PAGE_NODE_BITMAKS));
                
                next_t = t->next;
                
                // @TODO try to improve this, maybe we can reduce the number of accesses to gc_global
                add_chunks_to_list(t, gc_global.free_chunks[k]); //this is heavy since we are adding one chunk at a time
                    
            } while ( next_t != ch );

        }

    }

    /* Update current epoch. */
    WMB();
    gc_global.current = (curr_epoch+1) % NR_EPOCHS;

 out:
    gc_global.inreclaim = 0;
}

#endif /* MINIMAL_GC */
void *gc_alloc_node(ptst_t *ptst, int alloc_id, unsigned int node)
{

    gc_t *gc = ptst->gc;
    chunk_t *ch;
    void *ret;
    unsigned long* tmp;
    unsigned long x;

    ch = gc->alloc[node][alloc_id];
    if ( ch->i == 0 )
    {
        if ( gc->alloc_chunks[node][alloc_id]++ == 100 )
        {
            gc->alloc_chunks[node][alloc_id] = 0;
            add_chunks_to_list(ch, gc_global.free_chunks[node]);
            gc->alloc[node][alloc_id] = ch = node_get_alloc_chunk(gc, alloc_id, node);
        }
        else
        {
            chunk_t *och = ch;
            ch = node_get_alloc_chunk(gc, alloc_id, node);
            ch->next  = och->next;
            och->next = ch;
            gc->alloc[node][alloc_id] = ch;        
        }
    }
    
    ret = ch->blk[--ch->i];
    tmp = (unsigned long*) (((unsigned long) ret) & _PAGE_NODE_BITMAKS);

    x = __sync_val_compare_and_swap(tmp, 0, node); //mark the page with zone id   
    assert((x == 0) || (x == node)); //either is first allocation, or the page was already marked
    
    return ret;
}

static chunk_t *node_chunk_from_cache(gc_t *gc, unsigned int node)
{
    chunk_t *ch = gc->chunk_cache[node], *p = ch->next;

    if ( ch == p )
    {
        gc->chunk_cache[node] = node_get_empty_chunks(100, node);
    }
    else
    {
        ch->next = p->next;
        p->next  = p;
    }

    p->i = 0;
    return(p);
}

void gc_free(ptst_t *ptst, void *p, int alloc_id) 
{
    /*
    int node = -1, ret;
    ret = get_mempolicy(&node, NULL, 0, (void*)p, MPOL_F_NODE | MPOL_F_ADDR);
    assert(ret == 0);
    */
#ifndef MINIMAL_GC

    unsigned int node = *((unsigned long*) (((unsigned long) p) & _PAGE_NODE_BITMAKS));

    gc_t *gc = ptst->gc;
    chunk_t *prev, *new, *ch = gc->garbage[node][gc->epoch][alloc_id];

    if ( ch == NULL )
    {
        gc->garbage[node][gc->epoch][alloc_id] = ch = node_chunk_from_cache(gc, node);
        gc->garbage_tail[node][gc->epoch][alloc_id] = ch;
    }
    else if ( ch->i == BLKS_PER_CHUNK )
    {
        prev = gc->garbage_tail[node][gc->epoch][alloc_id];
        new  = node_chunk_from_cache(gc, node);
        gc->garbage[node][gc->epoch][alloc_id] = new;
        new->next  = ch;
        prev->next = new;
        ch = new;
    }

    ch->blk[ch->i++] = p;
#endif
}

// What if ptr was not allocated by gcalloc?
void gc_add_ptr_to_hook_list(ptst_t *ptst, void *ptr, int hook_id)
{

    gc_t *gc = ptst->gc;
    chunk_t *och, *ch;

    int node = -1, ret;
    ret = get_mempolicy(&node, NULL, 0, (void*)ptr, MPOL_F_NODE | MPOL_F_ADDR);
    assert(ret == 0);

    ch = gc->hook[gc->epoch][hook_id];

    if ( ch == NULL )
    {
        //which node should serve request
        gc->hook[gc->epoch][hook_id] = ch = node_chunk_from_cache(gc, node); //explode_here
    }
    else
    {
        ch = ch->next;
        if ( ch->i == BLKS_PER_CHUNK )
        {
            och       = gc->hook[gc->epoch][hook_id];
            ch        = node_chunk_from_cache(gc, node);
            ch->next  = och->next;
            och->next = ch;
        }
    }

    ch->blk[ch->i++] = ptr;
}

/* 
 * The node parameter is used to determine which cache in case of failure
 * */
void node_gc_add_ptr_to_hook_list(ptst_t *ptst, void *ptr, int hook_id, unsigned int node)
{
    gc_t *gc = ptst->gc;
    chunk_t *och, *ch;

    ch = gc->hook[gc->epoch][hook_id];

    if ( ch == NULL )
    {
        //which node should serve request - local node
        gc->hook[gc->epoch][hook_id] = ch = node_chunk_from_cache(gc, node);
    }
    else
    {
        ch = ch->next;
        if ( ch->i == BLKS_PER_CHUNK )
        {
            och       = gc->hook[gc->epoch][hook_id];
            ch        = node_chunk_from_cache(gc, node);
            ch->next  = och->next;
            och->next = ch;
        }
    }

    ch->blk[ch->i++] = ptr;
}

void gc_unsafe_free(ptst_t *ptst, void *p, int alloc_id)
{
    gc_t *gc = ptst->gc;
    chunk_t *ch;

    unsigned long node = * ((unsigned long*) (((unsigned long) p) & _PAGE_NODE_BITMAKS));
    
    ch = gc->alloc[node][alloc_id];
    if ( ch->i < BLKS_PER_CHUNK )
    {
         ch->blk[ch->i++] = p;
    }
    else
    {
        gc_free(ptst, p, alloc_id);
    }
}


void gc_enter(ptst_t *ptst)
{
#ifdef MINIMAL_GC
    ptst->count++;
    MB();
#else
    gc_t *gc = ptst->gc;
    int new_epoch, cnt;
 
 retry:
    cnt = ptst->count++;
    MB();
    if ( cnt == 1 )
    {
        new_epoch = gc_global.current;
        if ( gc->epoch != new_epoch )
        {
            gc->epoch = new_epoch;
            gc->entries_since_reclaim        = 0;
#ifdef YIELD_TO_HELP_PROGRESS
            gc->reclaim_attempts_since_yield = 0;
#endif
        }
        else if ( gc->entries_since_reclaim++ == 100 )
        {
            ptst->count--;
#ifdef YIELD_TO_HELP_PROGRESS
            if ( gc->reclaim_attempts_since_yield++ == 10000 )
            {
                gc->reclaim_attempts_since_yield = 0;
                sched_yield();
            }
#endif
            gc->entries_since_reclaim = 0;
            gc_reclaim(ptst);
            goto retry;    
        }
    }
#endif
}


void gc_exit(ptst_t *ptst)
{
    MB();
    ptst->count--;
}

gc_t *gc_init(void)
{
    gc_t *gc;
    int   i, k;

    gc = ALIGNED_ALLOC(sizeof(*gc));
    if ( gc == NULL ) MEM_FAIL(sizeof(*gc));
    memset(gc, 0, sizeof(*gc));

#ifdef WEAK_MEM_ORDER
    /* Initialise shootdown state. */
    gc->async_page = mmap(NULL, gc_global.page_size, PROT_NONE, 
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if ( gc->async_page == (void *)MAP_FAILED ) MEM_FAIL(gc_global.page_size);
    gc->async_page_state = 1;
#endif

    for ( k = 0; k < gc_global.numa_nodes; k++)
    {
        gc->chunk_cache[k] = node_get_empty_chunks(100, k);

        /* Get ourselves a set of allocation chunks. */
        for ( i = 0; i < gc_global.nr_sizes; i++ )
        {
            gc->alloc[k][i] = node_get_alloc_chunk(gc, i, k);
        }
        for ( ; i < MAX_SIZES; i++ )
        {
            gc->alloc[k][i] = node_chunk_from_cache(gc, k);
        }
    }

    return(gc);
}

int gc_add_allocator(int alloc_size)
{
    int ni, i = gc_global.nr_sizes, k;
    while ( (ni = CASIO(&gc_global.nr_sizes, i, i+1)) != i ) i = ni;
    gc_global.blk_sizes[i]  = alloc_size;
    for (k = 0; k < gc_global.numa_nodes; k++) {
        gc_global.alloc_size[k][i] = ALLOC_CHUNKS_PER_LIST;
        gc_global.alloc[k][i] = node_get_filled_chunks(ALLOC_CHUNKS_PER_LIST, alloc_size, k);
    }
    
    return i;
}

void gc_remove_allocator(int alloc_id)
{
    /* This is a no-op for now. */
}


int gc_add_hook(hook_fn_t fn)
{
    int ni, i = gc_global.nr_hooks;
    while ( (ni = CASIO(&gc_global.nr_hooks, i, i+1)) != i ) i = ni;
    gc_global.hook_fns[i] = fn;
    return i;    
}


void gc_remove_hook(int hook_id)
{
    /* This is a no-op for now. */
}


void _destroy_gc_subsystem(void)
{
#ifdef PROFILE_GC
    printf("Total heap: %u bytes (%.2fMB) in %u allocations\n",
           gc_global.total_size, (double)gc_global.total_size / 1000000,
           gc_global.allocations);
#endif
}


void _init_gc_subsystem(void)
{
    int i;
    memset(&gc_global, 0, sizeof(gc_global));

    gc_global.page_size   = (unsigned int)sysconf(_SC_PAGESIZE); //maybe move it to a macro
    gc_global.numa_nodes = (unsigned int) numa_num_configured_nodes(); // count number of numa nodes

    for (i=0; i<gc_global.numa_nodes; i++) {
        gc_global.free_chunks[i] = node_alloc_more_chunks(i);
    }
    
    //gc_global.free_chunks = alloc_more_chunks();

    gc_global.nr_hooks = 0;
    gc_global.nr_sizes = 0;
}
