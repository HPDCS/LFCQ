#include <numa.h>
#include <numaif.h>

#include "mapping.h"

#ifndef _NM_USE_SPINLOCK
#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0
#endif

#define SLOT_NUMBER 4 

typedef struct __op_payload op_payload;
struct __op_payload
{
	volatile long counter;
	operation_t* volatile content;
    char pad1[56];
};

struct __op_slot
{
	#ifdef _NM_USE_SPINLOCK
	spinlock_t spin;
	#else
	volatile int busy;
	#endif
	volatile int current; 		// 0 posted/wait to be executed; >=1 read/executed;
	
	char pad0[56];

    op_payload slots[SLOT_NUMBER];

	char pad1[56];
};

op_slot *res_mapping[_NUMA_NODES];
op_slot *req_mapping[_NUMA_NODES];

void init_mapping() 
{
    unsigned int max_threads = _NUMA_NODES * num_cpus_per_node;
    int i, j,k;

    for (i = 0; i < _NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_slot)*max_threads, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_slot)*max_threads, i);
        
        for (j = 0; j < max_threads; ++j) 
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
    
    unsigned long current = slot->current;
    unsigned long val, new_current;
    op_payload *slot_arr = slot->slots;

     // first attempt - Try to read current slot
    val = __sync_fetch_and_add(&(slot_arr[current].counter), 1);
    if (val == 0)
    {
        *operation = slot_arr[current].content;
        return true;
    }

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
        *operation = slot_arr[current].content;
        return true;
    }

    // try to read from all slots? 
    return false;
}

bool write_slot(op_slot* slot, 
    operation_t* operation)
{

    unsigned long i, index, new_index, val, current = slot->current;
    new_index = current;

    op_payload *slot_arr = slot->slots;

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

    operation_t* old_op = slot_arr[index].content;
    if (!BOOL_CAS(&slot_arr[index].content, old_op, operation))
        return false;


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
        return true;
	
    val = VAL_CAS(&slot->current, current, new_index);

    // the current slot is stale - update the current
    if (val == current || val == new_index)
        return true;
    else
    {
        printf("CURRENT NOT SET - old_value %lu, new_value %lu, current %lu\n", val, new_index, current ); // we have only one writer this cannot happen
	    return false;
    }
}