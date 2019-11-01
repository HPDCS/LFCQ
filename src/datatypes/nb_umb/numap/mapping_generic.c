#include <numa.h>
#include <numaif.h>

#include "mapping_generic.h"

#ifndef _NM_USE_SPINLOCK
#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0
#endif

static int gc_id[1];

struct _op_payload
{
	unsigned int type;			// ENQ | DEQ
	int ret_value;
	pkey_t timestamp;			// ts of node to enqueue
 	void *payload;				// paylod to enqueue | dequeued payload	
};

struct __op_load
{
	volatile int busy;
	char pad0[60];
	op_payload* volatile content;
	char pad1[56];
};

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

void init_mapping() 
{

    gc_id[0] = gc_add_allocator(sizeof(op_payload));

    int i, j;
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);
        
        for (j = 0; j < THREADS; ++j) 
        {
            #ifdef _NM_USE_SPINLOCK
            spinlock_init(&(req_mapping[i][j].spin));
            spinlock_init(&(res_mapping[i][j].spin));
            #else
            req_mapping[i][j].busy = L_FREE;
            res_mapping[i][j].busy = L_FREE;
            #endif

            res_mapping[i][j].content = (void*) 0x1ull;
            req_mapping[i][j].content = (void*) 0x1ull;
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

inline int atomic_test_x64(unsigned long long *b) 
{
    int result = 0;

	__asm__  __volatile__ (
		LOCK "bt $0, %1;\n\t"
		"adc %0, %0"
		: "=r" (result)
		: "m" (*b), "0" (result)
		: "memory"
	);

	return !result;
}

bool read_slot(op_node* slot, 
    unsigned int* type, 
    int *ret_value, 
    pkey_t *timestamp, 
    void** payload) 
{
    
    int val;
    op_payload *content;

    if (!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, R_BUSY))
        return false;

    content = FETCH_AND_OR(&(slot->content), (unsigned long) 0x1ull);
    if (is_marked(content, DEL))
    {
        slot->busy = L_FREE;
        return false;
    }
    
    *type = content->type;
    *ret_value = content->ret_value;
    *timestamp = content->timestamp; 
    *payload = content->payload;
    
    slot->busy = L_FREE;
    
    return true;
}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload,
    unsigned int dest_node)
{

    int val;
    op_payload* load = gc_alloc_node(ptst, gc_id[0], dest_node); // to do take the slot numa node

    op_payload *old, *new;

    #ifdef _NM_USE_SPINLOCK
    spin_lock_x86(&(slot->spin));
    #else
    while(!__sync_bool_compare_and_swap(&(slot->busy), L_FREE, W_BUSY));
    #endif

    load->type = type;
    load->ret_value = ret_value;
    load->timestamp = timestamp; 
    load->payload = payload;

    do
    {
        old = get_marked(slot->content, DEL);

        new = __sync_val_compare_and_swap(&slot->content, old, load);

        if (new != old && !is_marked(new))
        {
            gc_free(ptst, load, gc_id[0]);
            slot->busy = L_FREE;
            return false;
            // writing on a good slot->someone else wrote on the slot
        }
        else if (new == old)
        {  
            if (get_unmarked(new) != NULL)
                gc_free(ptst, get_unmarked(new), gc_id[0]);
            break;
        }
    } while(1);

    slot->busy = L_FREE;
    return true;
    
}

