#include <numa.h>
#include <numaif.h>

#include "mapping.h"

op_node *mapping[_NUMA_NODES];

//#define MAP_DEBUG
__thread op_node** res_mapping      = NULL; // slot per "postare" la risposta su nodi diversi [NID] è la risposta che sto aspettando
__thread op_node** req_out_mapping  = NULL; // slot per "postare" la richiesta su altri nodi
__thread op_node** req_in_mapping   = NULL; // slot per "leggere" la richiesta da altri nodi

void init_mapping() 
{
    int i, j;
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        for (j = 0; j < THREADS; ++j) 
        {
            spinlock_init(&(mapping[i][j].spin));
            mapping[i][j].response = 1;
        }
    }

    //printf("TID %d with LTID %d\n", TID, LTID);
    #ifdef MAP_DEBUG
    
    int j;
    for (i = 0; i < THREADS; ++i)
    {
        printf("THREAD %d \t", i);
        for (j = 0; j < ACTIVE_NUMA_NODES; ++j)
        {
            printf("%p\t", &mapping[j][i]);
        } 
        printf("\n");
    }
    printf("#########\n");  
    #endif
}

static inline void init_local_mapping() 
{
    res_mapping     = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi
    req_out_mapping = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);
    req_in_mapping  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);

    int i, j = LTID;
    
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i)
    {
        res_mapping[i]      = &mapping[i][j];

        req_out_mapping[i]  = &mapping[i][TID];

        req_in_mapping[i]   = &mapping[NID][j];

        j += num_cpus_per_node;
    }
    #ifdef MAP_DEBUG
    printf("TID %d with LTID %d\n", TID, LTID);
    for (i = 0; i<ACTIVE_NUMA_NODES; ++i) 
    {
        printf("TID %d - LID %d - NODE %d - res %p - out %p - in %p\n", TID, LTID, i, res_mapping[i], req_out_mapping[i], req_in_mapping[i]);
    } 
    #endif
}

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    if (unlikely(req_in_mapping==NULL))
        init_local_mapping();

    return req_in_mapping[numa_node];

}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    if (unlikely(req_out_mapping==NULL))
        init_local_mapping();

    return req_out_mapping[numa_node];
}

op_node* get_res_slot(unsigned int numa_node)
{
    if (unlikely(res_mapping==NULL))
        init_local_mapping();

    return res_mapping[numa_node];
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

