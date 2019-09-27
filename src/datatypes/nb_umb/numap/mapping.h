#ifndef _NUMA_MAPPING
#define _NUMA_MAPPING

#include "common_nb_calqueue.h"

typedef struct __op_load op_node;

void init_mapping();

/* Get a pointer to the operation posted by twin thread on numa node
 * The memory area is local to numa node 
 * */
op_node* get_request_slot_from_node(unsigned int numa_node);

/* Get a pointer to an operation that will be posted on twin thread 
 * The memory area is remote 
 * */
op_node* get_request_slot_to_node(unsigned int numa_node);

/* Get a pointer to a slot where we can post the result of an operation
 * */
op_node* get_response_slot(unsigned int numa_node);


bool read_slot(op_node* slot, unsigned int* type, int *ret_value, pkey_t *timestamp, void** payload);

bool write_slot(op_node* slot, unsigned int type, int ret_value, pkey_t timestamp, void* payload);

#endif /* _NUMA_MAPPING */
