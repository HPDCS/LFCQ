#ifndef _NUMA_MAPPING
#define _NUMA_MAPPING

#include "common_nb_calqueue.h"

#include "../gc/ptst.h"

#define OP_PQ_ENQ 0x0
#define OP_PQ_DEQ 0x1

typedef struct _operation operation_t;
struct _operation
{
	unsigned int type;			// ENQ | DEQ
	int ret_value;
	pkey_t timestamp;			// ts of node to enqueue
    char pad[8-sizeof(pkey_t)];
 	void *payload;				// paylod to enqueue | dequeued payload
};

typedef struct __op_slot op_slot;

void init_mapping();


/* Get a pointer to the operation posted by twin thread on numa node
 * The memory area is local to numa node 
 * */
op_slot* get_req_slot_from_node(unsigned int numa_node);

/* Get a pointer to an operation that will be posted on twin thread 
 * The memory area is remote 
 * */
op_slot* get_req_slot_to_node(unsigned int numa_node);

/* Get a pointer to a slot where we can read/write the result of an operation
 * */
//op_node* get_res_slot(unsigned int numa_node);

/* Get a pointer to a slot where we can read the result of an operation
 * */
op_slot* get_res_slot_from_node(unsigned int numa_node);

/* Get a pointer to a slot where we can post the result of an operation
 * */
op_slot* get_res_slot_to_node(unsigned int numa_node);

bool read_slot(op_slot* slot, operation_t** operation);

bool write_slot(op_slot* slot, operation_t* operation);

#endif /* _NUMA_MAPPING */
