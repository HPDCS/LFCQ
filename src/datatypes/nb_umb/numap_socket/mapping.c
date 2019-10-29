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

void init_mapping() 
{
    int i, j;
    unsigned int node;
    unsigned int max_threads = NUM_SOCKETS*CPU_PER_SOCKET; 
    for (i = 0; i < NUM_SOCKETS; ++i) 
    {

        node = i << 1;
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, node);  // even if CPU are not active we can use the channel-> operation will be always handled remotely
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, node); 
        
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

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    return &req_mapping[SID][((numa_node>>1) * CPU_PER_SOCKET) + LSID];
}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    return &req_mapping[(numa_node>>1)][TID];
}

op_node* get_res_slot_from_node(unsigned int numa_node)
{
    return &res_mapping[SID][((numa_node>>1) * CPU_PER_SOCKET) + LSID];
}

op_node* get_res_slot_to_node(unsigned int numa_node)
{
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

