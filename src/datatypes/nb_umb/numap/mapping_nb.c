#include <numa.h>
#include <numaif.h>

#include "mapping_nb.h"

#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

void init_mapping() 
{
    int i, j, k;
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        
        for (j = 0; j < THREADS; ++j) 
        {
            req_mapping[i][j].current = 0;
            res_mapping[i][j].current = 0;
            
            for (k = 0; k < SLOT_NUMBER; ++k)
            {
            
                res_mapping[i][j].slots[k].counter = 1;
                req_mapping[i][j].slots[k].counter = 1;
        
            }
        }
    }
}

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    return &req_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    return &req_mapping[numa_node][TID];
}

op_node* get_res_slot_from_node(unsigned int numa_node)
{
    return &res_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_node* get_res_slot_to_node(unsigned int numa_node)
{
    return &res_mapping[numa_node][TID];
}

bool read_slot(op_node* slot, 
    unsigned int* type, 
    int *ret_value, 
    pkey_t *timestamp, 
    void** payload) 
{
    unsigned long current = slot->current;
    unsigned long val, new_current;
    op_payload *slot_arr = slot->slots;

    val = __sync_fetch_and_add(&(slot_arr[current].counter), 1);
    if (val == 0)
    {
        *type = slot_arr[current].type;
        *ret_value = slot_arr[current].ret_value;
        *timestamp = slot_arr[current].timestamp; 
        *payload = slot_arr[current].payload;

        return true;
    }

    new_current = (current + 1) % SLOT_NUMBER;
    if (!BOOL_CAS(&slot->current, current, new_current))
    {    
        printf("CANNOT READ - unset current");
        return false;
    }

    val = __sync_fetch_and_add(&(slot_arr[current].counter), 1);
    if (val == 0)
    {
        *type = slot_arr[current].type;
        *ret_value = slot_arr[current].ret_value;
        *timestamp = slot_arr[current].timestamp; 
        *payload = slot_arr[current].payload;

        return true;
    }

    return false;

}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload)
{
    
    unsigned long i, index, current = slot->current;

    op_payload *slot_arr = slot->slots;

    // get free slot
    for (i = 0; i < SLOT_NUMBER; i++) 
    {
        index = (index + i) % SLOT_NUMBER;
        if (index != current && slot_arr[index].counter != 0)
            break;
    }
    if (index == current)
    {
        printf("NO FREE SLOTS\n");
        return false; // no free slots
    }

    slot_arr[index].type = type;
    slot_arr[index].ret_value = ret_value;
    slot_arr[index].timestamp = timestamp; 
    slot_arr[index].payload = payload;

    __sync_fetch_and_and(&(slot_arr[index].counter), 0);
    
    if (slot_arr[index].counter == 0)
    {
        printf("WRITING ON GOOD SLOT\n");
        return false;
    }

    // nobody read the current slot
    if (slot_arr[current].counter == 0)
        return true;

    if (BOOL_CAS(&slot->current, current, index))
        return true;
    else
    {
        printf("CURRENT NOT SET\n"); // we have only one writer this cannot happen
        return false;
    }
    
}

