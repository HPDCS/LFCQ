#include <numa.h>
#include <numaif.h>

#include "mapping.h"

#ifndef _NM_USE_SPINLOCK
#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0
#endif

struct __op_slot
{
	#ifdef _NM_USE_SPINLOCK
	spinlock_t spin;
	#else
	volatile int busy;
	#endif
	volatile int response; 		// 0 posted/wait to be executed; >=1 read/executed;
	
	char pad0[56];

    operation_t* volatile content;

	char pad1[56];
};

op_slot *res_mapping[_NUMA_NODES];
op_slot *req_mapping[_NUMA_NODES];

void init_mapping() 
{
    unsigned int max_threads = _NUMA_NODES * num_cpus_per_node;
    int i, j;

    for (i = 0; i < _NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_slot)*max_threads, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_slot)*max_threads, i);
        
        for (j = 0; j < max_threads; ++j) 
        {
            #ifdef _NM_USE_SPINLOCK
            spinlock_init(&(req_mapping[i][j].spin));
            spinlock_init(&(res_mapping[i][j].spin));
            #else
            req_mapping[i][j].busy = L_FREE;
            res_mapping[i][j].busy = L_FREE;
            #endif

            res_mapping[i][j].response = 1;
            req_mapping[i][j].response = 1;
        }
    }
    LOG("init_mapping() done! %s\n","(mapping)");
}

op_slot* get_req_slot_from_node(unsigned int numa_node)
{
    return &req_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_slot* get_req_slot_to_node(unsigned int numa_node)
{
    return &req_mapping[numa_node][TID];
}

op_slot* get_res_slot_from_node(unsigned int numa_node)
{
    // if (unlikely(res_in_slots==NULL))
    //     init_local_mapping();

    // return res_in_slots[numa_node];
    return &res_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_slot* get_res_slot_to_node(unsigned int numa_node)
{
    return &res_mapping[numa_node][TID];
}

bool read_slot(op_slot* slot, operation_t** operation) 
{
    
    int val;

    #ifdef _NM_USE_SPINLOCK
    if (!spin_trylock_x86(&(slot->spin)))
        return false;
    #else
    /*val = __sync_val_compare_and_swap(&(slot->busy), L_FREE, R_BUSY);
    if (val == W_BUSY)
        return false;
    */
    if (!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, R_BUSY))
        return false;
    #endif

    val = __sync_fetch_and_add(&(slot->response), 1);
    if (val != 0)
    {
        slot->busy = L_FREE;
        return false;
    }

    *operation = slot->content;

    #ifdef _NM_USE_SPINLOCK
    spin_unlock_x86(&(slot->spin));
    #else
    //__sync_bool_compare_and_swap(&(slot->busy), R_BUSY, L_FREE);
    slot->busy = L_FREE;
    #endif

    return true;
}

bool write_slot(op_slot* slot, 
    operation_t* operation)
{

    int val;

    #ifdef _NM_USE_SPINLOCK
    spin_lock_x86(&(slot->spin));
    #else
    while(!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, W_BUSY));
    #endif

    operation_t* old_op = slot->content;
    if (!BOOL_CAS(&slot->content, old_op, operation))
        return false;

    val = __sync_fetch_and_and(&(slot->response), 0);

    #ifdef _NM_USE_SPINLOCK
    spin_unlock_x86(&(slot->spin));
    #else
    slot->busy = L_FREE;
    #endif
    if (val != 0)
        return true;
    else 
        return false;
}