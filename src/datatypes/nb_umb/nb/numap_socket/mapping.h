#ifndef _NUMA_MAPPING
#define _NUMA_MAPPING

#define SLOT_NUMBER 4 

#include "common_nb_calqueue.h"

typedef struct op_load_s op_node;
typedef struct op_payload_s op_payload;

void init_mapping();

/* Get a pointer to the operation posted by twin thread on numa node
 * The memory area is local to numa node 
 * */
op_node* get_req_slot_from_node(unsigned int numa_node);

/* Get a pointer to an operation that will be posted on twin thread 
 * The memory area is remote 
 * */
op_node* get_req_slot_to_node(unsigned int numa_node);

/* Get a pointer to a slot where we can read/write the result of an operation
 * */
//op_node* get_res_slot(unsigned int numa_node);

/* Get a pointer to a slot where we can read the result of an operation
 * */
op_node* get_res_slot_from_node(unsigned int numa_node);

/* Get a pointer to a slot where we can post the result of an operation
 * */
op_node* get_res_slot_to_node(unsigned int numa_node);

bool read_slot(op_node* slot, operation_t **operation);

bool write_slot(op_node* slot, operation_t *operation);

#endif /* _NUMA_MAPPING */
