#include <numa.h>
#include <numaif.h>

#include "mapping.h"

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

__thread op_node** req_out_slots  = NULL; // slot per "postare" la richiesta su altri nodi
__thread op_node** req_in_slots   = NULL; // slot per "leggere" la richiesta da altri nodi

__thread op_node** res_out_slots  = NULL; // slot per "postare" la risposta su nodi 
__thread op_node** res_in_slots   = NULL; // slot per "leggere" la risposta da nodi 

void init_mapping() 
{
    int i, j;
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        
        for (j = 0; j < THREADS; ++j) 
        {
            spinlock_init(&(req_mapping[i][j].spin));
            req_mapping[i][j].response = 1;

            spinlock_init(&(res_mapping[i][j].spin));
            res_mapping[i][j].response = 1;
        }
    }
}

static inline void init_local_mapping() 
{
    req_in_slots  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);
    req_out_slots = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);

    res_in_slots  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi
    res_out_slots = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi

    int i, j = LTID;
    
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        req_in_slots[i]    = &req_mapping[NID][j];

        req_out_slots[i]   = &req_mapping[i][TID];

        res_in_slots[i]    = &res_mapping[NID][j];
       
        res_out_slots[i]   = &res_mapping[i][TID];


        j += num_cpus_per_node;
    }
}

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    if (unlikely(req_in_slots==NULL))
        init_local_mapping();

    return req_in_slots[numa_node];

}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    if (unlikely(req_out_slots==NULL))
        init_local_mapping();

    return req_out_slots[numa_node];
}

op_node* get_res_slot_from_node(unsigned int numa_node)
{
    if (unlikely(res_in_slots==NULL))
        init_local_mapping();

    return res_in_slots[numa_node];
}

op_node* get_res_slot_to_node(unsigned int numa_node)
{
    if (unlikely(res_out_slots==NULL))
        init_local_mapping();

    return res_out_slots[numa_node];
}

bool read_slot(op_node* slot, 
    unsigned int* type, 
    int *ret_value, 
    pkey_t *timestamp, 
    void** payload) 
{
    
    int val;

    if (!spin_trylock_x86(&(slot->spin)))
        return false;
    
    *type = slot->type;
    *ret_value = slot->ret_value;
    *timestamp = slot->timestamp; 
    *payload = slot->payload;

    val = slot->response++;
    
    spin_unlock_x86(&(slot->spin));
    
    if (val == 0)
        return true;
    else 
        return false;
    

}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload)
{

    spin_lock_x86(&(slot->spin));

    slot->type = type;
    slot->ret_value = ret_value;
    slot->timestamp = timestamp; 
    slot->payload = payload;

    slot->response = 0;

    spin_unlock_x86(&(slot->spin));
    
    return true;
}

