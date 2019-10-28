#include "common_nb_calqueue.h"
#include "../gc/set.h"

/*
 * Queue wide cache - Speedup of enqueues
 * 
 * The keys are the VB. In the item we keep the last reference to the 
 * 
 * */

// @TODO merge with deq sw_cache

typedef struct item_s item_t;
struct item_s
{
    table* h;                       // reference to the table -> If changes the cache is stale
    nbc_bucket_node* last_node;     // reference to last node -> Last node of the VB predecessor of the looked one
    unsigned long node_counter;     // last value of the ref counter of the node
};

void _init_gc_cache(); // initialize GC for skiplist anc cache subsystem
nbc_bucket_node* get_last_node(setkey_t vb_index, table* h);
void update_last_node(setkey_t vb_index, table*h, nbc_bucket_node* node);