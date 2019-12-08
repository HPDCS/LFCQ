#include <numa.h>
#include <numaif.h>

#include "mapping.h"

struct op_payload_s
{
	volatile long counter; // read count
    operation_t* operation;
};

struct op_load_s
{
	volatile unsigned long current;
	char lpad[56];
    volatile unsigned long last_updated;
	char pad1[56];
	op_payload slots[SLOT_NUMBER];
};

op_node *res_mapping[NUM_SOCKETS];
op_node *req_mapping[NUM_SOCKETS];

void init_mapping() 
{
    unsigned int node;
    unsigned int max_threads = NUM_SOCKETS*CPU_PER_SOCKET; 
    int i, j, k;
    for (i = 0; i < ACTIVE_SOCKETS; ++i) 
    {
        node = i << 1;
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, node);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, node);
        
        for (j = 0; j < max_threads; ++j) 
        {
            req_mapping[i][j].current = 0;
            req_mapping[i][j].last_updated = 0;
	    
	        res_mapping[i][j].current = 0;
            res_mapping[i][j].last_updated = 0;
            
            for (k = 0; k < SLOT_NUMBER; ++k)
            {
            
                res_mapping[i][j].slots[k].counter = 1;
                req_mapping[i][j].slots[k].counter = 1;
        
            }
        }
    }
    LOG("init_mapping() done! %s\n","(mapping_nb)");
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
    operation_t **operation) 
{
    unsigned long current = slot->current;
    unsigned long last_update = slot->last_updated;

    unsigned long val, new_current;
    op_payload *slot_arr = slot->slots;

    // first attempt - Try to read current slot
    val = __sync_fetch_and_add(&(slot_arr[current].counter), 1);
    if (val == 0)
    {
        *operation = slot_arr[current].operation;
        return true;
    }

    if (last_update == current) // no slot available
	    return false;

    // current slot has been already read - increase the current
    new_current = (current + 1) % SLOT_NUMBER;
    
    if (slot_arr[new_current].counter != 0)
	    return false;                       // change this in find next valid slot
    

    if (!BOOL_CAS(&slot->current, current, new_current))
    {    
        //printf("CANNOT READ - unset current");
        return false;
    }

    current = new_current;

    // second attempt - Read new slot
    val = __sync_fetch_and_add(&(slot_arr[current].counter), 1);
    if (val == 0)
    {
        *operation = slot_arr[current].operation;
        return true;
    }

    // try to read from all slots? 
    return false;

}

bool write_slot(op_node* slot, 
    operation_t *operation)
{
    unsigned long i, index, new_index, val, current = slot->current;
    unsigned long last_written = slot->last_updated;

    op_payload *slot_arr = slot->slots;

    new_index = current;

    // get free slot
    for (i = 0; i < SLOT_NUMBER; i++) 
    {
        index = (current + i) % SLOT_NUMBER;
        if (index != current)
	{
		if(slot_arr[index].counter != 0)
        		break;
		else
			new_index = index;

    	}
    }
    if (index == current)
    {
        printf("NO FREE SLOTS\n");
        return false; // no free slots
    }
    if (new_index == current)
	    new_index = index;

    // write on slot
    slot_arr[index].operation       = operation;

    // set the slot as new
    val = __sync_fetch_and_and(&(slot_arr[index].counter), 0);
    // the new value is now visible    

    // we have written on a good slot - this is not possible
    if (val == 0)
    {
        printf("WRITING ON GOOD SLOT\n");
        return false;
    }

    // nobody has read the current slot - cannot move to next current
    if (slot_arr[current].counter == 0)
    {
        BOOL_CAS(&slot->last_updated, last_written, index);
	    return true;
    }	
    val = VAL_CAS(&slot->current, current, new_index);
    return true; // if this fails maybe some read already updated the currentz
    // the current slot is stale - update the current
    /*if (val == current || val == new_index)
        return true;
    else
    {
       // printf("CURRENT NOT SET - old_value %lu, new_value %lu, current %lu\n", val, new_index, current ); // we have only one writer this cannot happen
	    return false;
    }*/
    
}

