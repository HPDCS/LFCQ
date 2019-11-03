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

	unsigned int type;			// ENQ | DEQ
	int ret_value;
	pkey_t timestamp;			// ts of node to enqueue
 	void *payload;				// paylod to enqueue | dequeued payload
	//24
	char pad[48 - sizeof(pkey_t) ];
};

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

//__thread op_node** req_out_slots  = NULL; // slot per "postare" la richiesta su altri nodi
//__thread op_node** req_in_slots   = NULL; // slot per "leggere" la richiesta da altri nodi

//__thread op_node** res_out_slots  = NULL; // slot per "postare" la risposta su nodi 
//__thread op_node** res_in_slots   = NULL; // slot per "leggere" la risposta da nodi 

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

/*
static inline void init_local_mapping() 
{
    req_in_slots  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);
    req_out_slots = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);

    res_slots  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi

    //res_in_slots  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi
    //res_out_slots = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi

    int i, j = LTID;
    
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        req_in_slots[i]    = &req_mapping[NID][j];

        req_out_slots[i]   = &req_mapping[i][TID];
        
        res_slots[i]       = &req_mapping[i][j];

        //res_in_slots[i]    = &res_mapping[NID][j];
       
        //res_out_slots[i]   = &res_mapping[i][TID];


        j += num_cpus_per_node;
    }
}
*/

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    /*
    if (unlikely(req_in_slots==NULL))
        init_local_mapping();
    
    return req_in_slots[numa_node];
    */
    return &req_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    /*
    if (unlikely(req_out_slots==NULL))
        init_local_mapping();

    return req_out_slots[numa_node];
    */
    return &req_mapping[numa_node][TID];
}

/*
op_node* get_res_slot(unsigned int numa_node)
{
    
    // if (unlikely(res_slots==NULL))
    //     init_local_mapping();

    // return res_slots[numa_node];
    
    return &req_mapping[numa_node][(numa_node * num_cpus_per_node) + LTID];
}
*/


op_node* get_res_slot_from_node(unsigned int numa_node)
{
    // if (unlikely(res_in_slots==NULL))
    //     init_local_mapping();

    // return res_in_slots[numa_node];
    return &res_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_node* get_res_slot_to_node(unsigned int numa_node)
{
    // if (unlikely(res_out_slots==NULL))
    //     init_local_mapping();

    // return res_out_slots[numa_node];
    return &res_mapping[numa_node][TID];
}

bool read_slot(op_node* slot, 
    unsigned int* type, 
    int *ret_value, 
    pkey_t *timestamp, 
    void** payload) 
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

    *type = slot->type;
    *ret_value = slot->ret_value;
    *timestamp = slot->timestamp; 
    *payload = slot->payload;

    
    
    #ifdef _NM_USE_SPINLOCK
    spin_unlock_x86(&(slot->spin));
    #else
    //__sync_bool_compare_and_swap(&(slot->busy), R_BUSY, L_FREE);
    slot->busy = L_FREE;
    #endif

    return true;
}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload)
{

    int val;

    #ifdef _NM_USE_SPINLOCK
    spin_lock_x86(&(slot->spin));
    #else
    while(!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, W_BUSY));
    #endif

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

