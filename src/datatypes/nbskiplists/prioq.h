#ifndef PRIOQ_H
#define PRIOQ_H


#include <float.h>
#define INFTY DBL_MAX

#include "../../utils/common.h"
/* keir fraser's garbage collection */
#include "../../gc/ptst.h"

typedef double pkey_t;
typedef void         *pval_t;

#define TID tid


#define KEY_NULL 0
#define NUM_LEVELS 32
/* Internal key values with special meanings. */
#define SENTINEL_KEYMIN ( 0UL) /* Key value of first dummy node. */
#define SENTINEL_KEYMAX (~1UL) /* Key value of last dummy node.  */

#define FRASER_ALLOCATOR 1
#define MULTIPLE_NODE_SIZES 1

typedef struct node_s
{
    pkey_t    k;
    int       level;
    int       inserting; //char pad2[4];
    pval_t    v;
    #if MULTIPLE_NODE_SIZES == 1
    struct node_s *next[1];
    #else
    struct node_s *next[NUM_LEVELS];
    char pad[24];
    #endif
} node_t;

typedef struct
{
    unsigned long long    max_offset;
    int    max_level;
    int    nthreads;
    node_t *head;
    node_t *tail;
    char   pad[128];
} pq_t;

#define get_marked_ref(_p)      ((void *)(((uintptr_t)(_p)) | 1))
#define get_unmarked_ref(_p)    ((void *)(((uintptr_t)(_p)) & ~1))
#define is_marked_ref(_p)       (((uintptr_t)(_p)) & 1)

extern __thread unsigned int TID;
extern __thread ptst_t *ptst;
extern int gc_id[];


/* Interface */

extern pq_t *pq_init(unsigned int threads, double none, unsigned long long max_offset);

extern void pq_destroy(pq_t *pq);

extern void insert(pq_t *pq, pkey_t k, pval_t v);

extern pval_t deletemin(pq_t *pq);

extern void sequential_length(pq_t *pq);

extern void print_stats();
extern void pq_prune();

extern void free_node(node_t *n);

extern void restructure(pq_t *pq);

#endif // PRIOQ_H
