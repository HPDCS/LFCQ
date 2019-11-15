#include <numa.h>
#include <numaif.h>

#include "mapping.h"

#ifndef _NM_USE_SPINLOCK
#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0
#endif

struct __op_load
{
	#ifdef _NM_USE_SPINLOCK
	spinlock_t spin;
	#else
	volatile int busy;
	#endif
	volatile int response; 		// 0 posted/wait to be executed; >=1 read/executed;
	
	char pad0[56];

    unsigned int dest_node;
	unsigned int type;			// ENQ | DEQ
	int ret_value;
	pkey_t timestamp;			// ts of node to enqueue
 	void *payload;				// paylod to enqueue | dequeued payload
	//24
	char pad[48 - sizeof(pkey_t) ];

};

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

void init_mapping() 
{
    unsigned int max_threads = _NUMA_NODES * num_cpus_per_node;
    int i, j;
    for (i = 0; i < _NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, i);
        
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

inline op_node* get_req_slot_from_node(unsigned int numa_node)
{
    return &req_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

inline op_node* get_req_slot_to_node(unsigned int numa_node)
{
    return &req_mapping[numa_node][TID];
}

inline op_node* get_req_slot_from_to_node(unsigned int src_node, unsigned int dst_node)
{
    return &req_mapping[dst_node][(src_node * num_cpus_per_node) + LTID];
}

inline op_node* get_res_slot_from_node(unsigned int numa_node)
{
    return &res_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

inline op_node* get_res_slot_to_node(unsigned int numa_node)
{
    return &res_mapping[numa_node][TID];
}

bool read_slot(op_node* slot, 
    unsigned int* type, 
    int *ret_value, 
    pkey_t *timestamp, 
    void** payload,
    unsigned int *node) 
{
    
    int val;

    #ifdef _NM_USE_SPINLOCK
    if (!spin_trylock_x86(&(slot->spin)))
        return false;
    #else
    if (!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, R_BUSY))
        return false;
    #endif

    val = __sync_fetch_and_add(&(slot->response), 1);
    if (val != 0)
    {
        slot->busy = L_FREE;
        return false;
    }

    *node = slot->dest_node;
    *type = slot->type;
    *ret_value = slot->ret_value;
    *timestamp = slot->timestamp; 
    *payload = slot->payload;
    
    #ifdef _NM_USE_SPINLOCK
    spin_unlock_x86(&(slot->spin));
    #else
    slot->busy = L_FREE;
    #endif

    return true;
}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload,
    unsigned int node)
{

    int val;

    #ifdef _NM_USE_SPINLOCK
    spin_lock_x86(&(slot->spin));
    #else
    while(!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, W_BUSY));
    #endif

    slot->dest_node = node;
    slot->type = type;
    slot->ret_value = ret_value;
    slot->timestamp = timestamp; 
    slot->payload = payload;

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

