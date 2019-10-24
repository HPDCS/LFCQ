#include <numa.h>
#include <numaif.h>

#include "mapping.h"

#ifndef _NM_USE_SPINLOCK
#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0
#endif

op_node *res_mapping[NUM_SOCKETS];
op_node *req_mapping[NUM_SOCKETS];

//__thread op_node** req_out_slots  = NULL; // slot per "postare" la richiesta su altri nodi
//__thread op_node** req_in_slots   = NULL; // slot per "leggere" la richiesta da altri nodi

//__thread op_node** res_out_slots  = NULL; // slot per "postare" la risposta su nodi 
//__thread op_node** res_in_slots   = NULL; // slot per "leggere" la risposta da nodi 

void init_mapping() 
{
    int i, j;
    unsigned int node;
    for (i = 0; i < NUM_SOCKETS; ++i) 
    {
        node = i << 1;
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, node);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, node);
        
        for (j = 0; j < THREADS; ++j) 
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
    return &req_mapping[SID][((numa_node>>1) * CPU_PER_SOCKET) + LSID];
}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    /*
    if (unlikely(req_out_slots==NULL))
        init_local_mapping();

    return req_out_slots[numa_node];
    */
    return &req_mapping[(numa_node>>1)][TID];
}

op_node* get_res_slot_from_node(unsigned int numa_node)
{
    // if (unlikely(res_in_slots==NULL))
    //     init_local_mapping();

    // return res_in_slots[numa_node];
    return &res_mapping[SID][((numa_node>>1) * CPU_PER_SOCKET) + LSID];
}

op_node* get_res_slot_to_node(unsigned int numa_node)
{
    // if (unlikely(res_out_slots==NULL))
    //     init_local_mapping();

    // return res_out_slots[numa_node];
    return &res_mapping[(numa_node>>1)][TID];
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

