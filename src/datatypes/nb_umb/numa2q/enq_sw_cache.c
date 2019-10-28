#include "enq_sw_cache.h"

static int gc_id[1];

set_t* cache_lists[_NUMA_NODES];

void _init_gc_cache() 
{
    #ifdef _CACHE_ON
    int i;
    _init_set_subsystem();
    gc_id[0] = gc_add_allocator(sizeof(item_t));

    for (i=0; i < _NUMA_NODES; ++i)
        cache_lists[i] = set_alloc();
    #else
    return;
    #endif
}


nbc_bucket_node* get_last_node(setkey_t vb_index, table* h) 
{
    #ifdef _CACHE_ON
    item_t* value;
    set_t* set = cache_lists[NID];

    value = set_lookup(set, vb_index);
    if (value == NULL)
        return NULL;
    
    if (h != value->h || value->last_node->nid > value->node_counter || is_marked(value->last_node->next))
    {
        // table is changed, this is no longer valid
        set_remove(set, vb_index);
        return NULL;
    }

    return value->last_node;
    #else
    return NULL;
    #endif
}

void update_last_node(setkey_t vb_index, table*h, nbc_bucket_node* node)
{
    #ifdef _CACHE_ON
    set_t* set = cache_lists[NID];
    item_t *old_node, *new_item = NULL;
    if (node != NULL)
    {
        new_item = gc_alloc_node(ptst, gc_id[0], NID);
        new_item->h = h;
        new_item->last_node = node;
    }
    
    old_node = set_update(set, vb_index, new_item, 1, NID);
    if (old_node != NULL)
        gc_free(ptst, old_node, gc_id[0]);
    #else
    return;
    #endif
}