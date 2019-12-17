#include <numa.h>
#include <numaif.h>

#include "mapping.h"

struct op_payload_s
{
	long s_counter; // read count
    long e_counter;
    operation_t* operation;
};

struct op_load_s
{
	volatile unsigned long current;
	char lpad[56];
    op_payload slots[SLOT_NUMBER];
};

__thread unsigned long last_update;

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

void init_mapping() 
{
    unsigned int max_threads = _NUMA_NODES * num_cpus_per_node;
    int i, j, k;
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, i);
        
        for (j = 0; j < max_threads; ++j) 
        {
            req_mapping[i][j].current = 0x1ull;	    
	        res_mapping[i][j].current = 0x1ull;
            
            for (k = 0; k < SLOT_NUMBER; ++k)
            {
            
                res_mapping[i][j].slots[k].s_counter = 1;
                res_mapping[i][j].slots[k].s_counter = 1;
                res_mapping[i][j].slots[k].operation = NULL;
                
                req_mapping[i][j].slots[k].e_counter = 1;
                req_mapping[i][j].slots[k].e_counter = 1;
                req_mapping[i][j].slots[k].operation = NULL;
            }
        }
    }
    LOG("init_mapping(22) done! %s\n","(mapping_nb)");
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
    operation_t **operation) 
{
    unsigned long current = slot->current;
    op_payload* registers = slot->slots;

    unsigned int index = current >> 32;
    unsigned int op_start = current << 32;

    if (last_update == index)
        if (op_start == 0)
        {
            *operation = registers[last_update].operation;
            return true;
        }
        else
        {
            return false;
        }
    
    __sync_fetch_and_add(&registers[last_update].e_counter, 1);
    unsigned long tmp_curr = __sync_add_and_fetch(&slot->current, 1);                
    last_update = tmp_curr >> 32;
    if (tmp_curr << 32 == 1)
    {
        *operation = registers[tmp_curr>>32].operation;
        return true;
    }
    else
    {
        return false;
    }
    

}

bool write_slot(op_node* slot, 
    operation_t *operation)
{
    
    int i;
    op_payload* registers = slot->slots;

    for (i = 0; i < SLOT_NUMBER; ++i)
    {
        if (i != last_update && registers[i].e_counter > 0 && registers[i].e_counter == registers[i].s_counter)
            break;
    }
    if (i == SLOT_NUMBER)
        return false;
    
    registers[i].operation = operation;
    registers[i].s_counter = 0;
    registers[i].e_counter = 0;

    unsigned long old_curr;
    do
    {
        old_curr = slot->current;
    } while (!__sync_bool_compare_and_swap(&slot->current, old_curr, (unsigned long) i<<32));
    
    unsigned int old_slot = old_curr>>32;
    registers[old_slot].s_counter = old_curr & 0xFFFFFFFF;
    return true;
}

