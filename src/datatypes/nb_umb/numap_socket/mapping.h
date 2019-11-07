#ifndef _NUMA_MAPPING
#define _NUMA_MAPPING

#include "common_nb_calqueue.h"

#define OP_PQ_ENQ 0x0
#define OP_PQ_DEQ 0x1

typedef struct __op_load op_node;

void init_mapping();

/* Get a pointer to the operation posted by twin thread on numa node
 * The memory area is local to numa node 
 * */
op_node* get_req_slot_from_node(unsigned int numa_node);

/* Get a pointer to an operation that will be posted on twin thread 
 * The memory area is remote 
 * */
op_node* get_req_slot_to_node(unsigned int numa_node);

/* Get a pointer to a slot where we can post the operation requested by someone else
 * */
op_node* get_req_slot_from_to_node(unsigned int src_node, unsigned int dst_node);

/* Get a pointer to a slot where we can read the result of an operation
 * */
op_node* get_res_slot_from_node(unsigned int numa_node);

/* Get a pointer to a slot where we can post the result of an operation
 * */
op_node* get_res_slot_to_node(unsigned int numa_node);

bool read_slot(op_node* slot, unsigned int* type, int *ret_value, pkey_t *timestamp, void** payload, unsigned int *node);

bool write_slot(op_node* slot, unsigned int type, int ret_value, pkey_t timestamp, void* payload, unsigned int node);
#endif /* _NUMA_MAPPING */
