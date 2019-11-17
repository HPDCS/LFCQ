#include <numa.h>
#include <numaif.h>

#include "mapping.h"
#include "../op_queue/task_queue.h"
#include "../gc/ptst.h"

#ifndef _NM_USE_SPINLOCK
#define R_BUSY 0x2
#define W_BUSY 0x1
#define L_FREE 0x0
#endif

static int gc_id[1];

typedef struct _operation_s
{
    unsigned int dest_node;
	unsigned int type;			// ENQ | DEQ
	int ret_value;
	pkey_t timestamp;			// ts of node to enqueue
 	void *payload;				// paylod to enqueue | dequeued payload

} operation_t;

struct __op_load
{
	task_queue queue;
};

op_node *res_mapping[_NUMA_NODES];
op_node *req_mapping[_NUMA_NODES];

void init_mapping() 
{

    gc_id[0] = gc_add_allocator(sizeof(operation_t));
    critical_enter();
    unsigned int max_threads = _NUMA_NODES * num_cpus_per_node;
    int i, j;
    for (i = 0; i < _NUMA_NODES; ++i) 
    {
        req_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, i);
        res_mapping[i] = numa_alloc_onnode(sizeof(op_node)*max_threads, i);
        
        for (j = 0; j < max_threads; ++j) 
        {
            init_tq(&req_mapping[i][j].queue, i);
            init_tq(&res_mapping[i][j].queue, i);
        }
    }
    critical_exit();
    LOG("init_mapping() done! %s\n","(slots are LCRQ)");
}

op_node* get_req_slot_from_node(unsigned int numa_node)
{
    return &req_mapping[NID][(numa_node * num_cpus_per_node) + LTID];
}

op_node* get_req_slot_to_node(unsigned int numa_node)
{
    return &req_mapping[numa_node][TID];
}

op_node* get_req_slot_from_to_node(unsigned int src_node, unsigned int dst_node)
{
    return &req_mapping[dst_node][(src_node * num_cpus_per_node) + LTID];
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
    void** payload,
    unsigned int *node) 
{
    operation_t* extracted;
    critical_enter();
    if (!tq_dequeue(&slot->queue, extracted))
    {    
        critical_exit();
        return false;
    }

    *type = extracted->type;
    *ret_value = extracted->ret_value;
    *timestamp = extracted->timestamp;
    *payload = extracted->payload;
    *node = extracted->dest_node;

    gc_free(ptst, extracted, gc_id[0]);
    critical_exit();
    return true;
}

bool write_slot(op_node* slot, 
    unsigned int type, 
    int ret_value, 
    pkey_t timestamp, 
    void* payload,
    unsigned int node)
{
    operation_t* insert;
    
    critical_enter();
    insert = gc_alloc_node(ptst, gc_id[0], node);

    insert->type = type;
    insert->ret_value = ret_value;
    insert->timestamp = timestamp;
    insert->payload = payload;
    insert->dest_node = node;

    tq_enqueue(&slot->queue, insert, node);

    critical_exit();

    return true;

}

