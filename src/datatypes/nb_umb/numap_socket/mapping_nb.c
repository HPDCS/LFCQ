#include <numa.h>
#include <numaif.h>

#include "mapping.h"

#define SLOT_NUMBER 4 

typedef struct __op_payload op_payload;
struct __op_payload
{
	volatile long counter;
	unsigned int type;			// ENQ | DEQ
	int ret_value;
	pkey_t timestamp;			// ts of node to enqueue
 	char ppad0[8 - sizeof(pkey_t) ];
	void *payload;				// paylod to enqueue | dequeued payload
	char ppad1[32];
};

struct __op_load
{
	volatile unsigned long current;
	char lpad[56];
	op_payload slots[SLOT_NUMBER];
};

op_node *res_mapping[NUM_SOCKETS];
op_node *req_mapping[NUM_SOCKETS];

void init_mapping() 
{
    int i, j, k;
    unsigned int node,
        max_threads = NUM_SOCKETS*CPU_PER_SOCKET; 
    for (i = 0; i < NUM_SOCKETS; ++i) 
    {
        node = i << 1;
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, node);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, node);
        
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
    LOG("init_mapping() done! %s\n","(mapping_nb)");
}

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    return &req_mapping[SID][((numa_node>>1) * CPU_PER_SOCKET) + LSID];
}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    return &req_mapping[(numa_node>>1)][TID];;
}

op_node* get_res_slot_from_node(unsigned int numa_node)
{
    return &res_mapping[SID][((numa_node>>1) * CPU_PER_SOCKET) + LSID];
}

op_node* get_res_slot_to_node(unsigned int numa_node)
{
    return &res_mapping[(numa_node>>1)][TID];;
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

    // first attempt - Try to read current slot
    val = __sync_fetch_and_add(&(slot_arr[current].counter), 1);
    if (val == 0)
    {
        *type = slot_arr[current].type;
        *ret_value = slot_arr[current].ret_value;
        *timestamp = slot_arr[current].timestamp; 
        *payload = slot_arr[current].payload;

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
        *type = slot_arr[current].type;
        *ret_value = slot_arr[current].ret_value;
        *timestamp = slot_arr[current].timestamp; 
        *payload = slot_arr[current].payload;

        return true;
    }

    // try to read from all slots? 
    return false;

}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload)
{
    
    unsigned long i, index, new_index, val, current = slot->current;

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
    slot_arr[index].type = type;
    slot_arr[index].ret_value = ret_value;
    slot_arr[index].timestamp = timestamp; 
    slot_arr[index].payload = payload;

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

