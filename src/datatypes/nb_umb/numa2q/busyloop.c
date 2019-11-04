/*****************************************************************************
*
*	This file is part of NBCQ, a lock-free O(1) priority queue.
*
*   Copyright (C) 2019, Romolo Marotta
*
*   This program is free software: you can redistribute it and/or modify
*   it under the terms of the GNU General Public License as published by
*   the Free Software Foundation, either version 3 of the License, or
*   (at your option) any later version.
*
*   This is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.
*
*   You should have received a copy of the GNU General Public License
*   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
******************************************************************************/
/*
 *  common_nb_calqueue.c
 *
 *  Author: Romolo Marotta
 */

#include <stdlib.h>
#include <limits.h>

#include "busyloop.h"

/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

int gc_aid[32];
int gc_hid[4];

unsigned long op_counter = 2;
task_queue enq_queue[_NUMA_NODES]; // (!new) per numa node queue
task_queue deq_queue[_NUMA_NODES];

/*************************************
 * THREAD LOCAL VARIABLES			 *
 ************************************/

__thread unsigned long long concurrent_enqueue;
__thread unsigned long long performed_enqueue;
__thread unsigned long long concurrent_dequeue;
__thread unsigned long long performed_dequeue;
__thread unsigned long long scan_list_length;
__thread unsigned long long scan_list_length_en;
__thread unsigned int read_table_count = UINT_MAX;
__thread unsigned long long num_cas = 0ULL;
__thread unsigned long long num_cas_useful = 0ULL;
__thread unsigned long long near = 0;
__thread unsigned int acc = 0;
__thread unsigned int acc_counter = 0;

void std_free_hook(ptst_t *p, void *ptr) { free(ptr); }

/**
 * This function create an instance of a NBCQ.
 *
 * @param threshold: ----------------
 * @param perc_used_bucket: set the percentage of occupied physical buckets 
 * @param elem_per_bucket: set the expected number of items for each virtual bucket
 * @return a pointer a new queue
 */
void *pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{

	ACTIVE_NUMA_NODES = (((THREADS * _NUMA_NODES)) / NUM_CPUS) + ((((THREADS * _NUMA_NODES)) % NUM_CPUS) != 0); // (!new) compute the number of active numa nodes 
	ACTIVE_NUMA_NODES = ACTIVE_NUMA_NODES < _NUMA_NODES? ACTIVE_NUMA_NODES:_NUMA_NODES;
	LOG("\n#######\nThreads %d, NUMA Nodes %d, CPUS %d, ACTIVE NUMA Nodes %d\n########\n", THREADS, _NUMA_NODES, NUM_CPUS, ACTIVE_NUMA_NODES);

	unsigned int i = 0;
	int res_mem_posix = 0;
	nb_calqueue *res = NULL;

	// init fraser garbage collector/allocator
	_init_gc_subsystem();
	_init_gc_tq();
	_init_gc_cache();
	// add allocator of nbc_bucket_node
	gc_aid[GC_BUCKETNODE] = gc_add_allocator(sizeof(nbc_bucket_node));
	gc_aid[GC_OPNODE] = gc_add_allocator(sizeof(op_node));
	// add callback for set tables and array of nodes whene a grace period has been identified
	gc_hid[0] = gc_add_hook(std_free_hook);
	critical_enter();
	critical_exit();

	// allocate memory
	res_mem_posix = posix_memalign((void **)(&res), CACHE_LINE_SIZE, sizeof(nb_calqueue));
	if (res_mem_posix != 0)
		error("No enough memory to allocate queue\n");
	res_mem_posix = posix_memalign((void **)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table));
	if (res_mem_posix != 0)
		error("No enough memory to allocate set table\n");
	res_mem_posix = posix_memalign((void **)(&res->hashtable->array), CACHE_LINE_SIZE, MINIMUM_SIZE * sizeof(nbc_bucket_node));
	if (res_mem_posix != 0)
		error("No enough memory to allocate array of heads\n");

	res->threshold = threshold;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->read_table_period = READTABLE_PERIOD;
	res->tail = numa_node_malloc(NULL, INFTY, 0, 0);
	res->tail->next = NULL;

	// (!new) initialize numa queues
	for (i = 0; i < _NUMA_NODES; i++)
	{
		init_tq(&enq_queue[i], i);
		init_tq(&deq_queue[i], i);
	}

	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;
	res->hashtable->current = 0;
	res->hashtable->last_resize_count = 0;
	res->hashtable->resize_count = 0;
	res->hashtable->e_counter.count = 0;
	res->hashtable->d_counter.count = 0;
	res->hashtable->read_table_period = res->read_table_period;

	for (i = 0; i < MINIMUM_SIZE; i++)
	{
		res->hashtable->array[i].next = res->tail;
		res->hashtable->array[i].tail = res->tail;
		res->hashtable->array[i].timestamp = (pkey_t)i;
		res->hashtable->array[i].counter = 0;
	}

	return res;
}

__thread pkey_t last_ts;
__thread unsigned int next_node_deq;

int __attribute__((optimize("O0"))) single_step_pq_enqueue(table *h, pkey_t timestamp, void *payload, nbc_bucket_node* volatile * candidate, op_node *operation)
{
    unsigned long i = LOOP_COUNT;
    last_ts = timestamp;

    while(i > 0)
		i--;

	performed_enqueue++;
	return 1;
}


int __attribute__((optimize("O0"))) single_step_pq_dequeue(table *h, nb_calqueue *queue, pkey_t* ret_ts, void **result, unsigned long op_id, nbc_bucket_node* volatile *candidate)
{
    unsigned long i = LOOP_COUNT;
    while(i > 0)
		i--;
        
    *result = (void *) 0x1ull;
	*ret_ts = last_ts;
	performed_dequeue++;
	return 1;
}

int pq_enqueue(void* q, pkey_t timestamp, void *payload) 
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");

	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *operation, *extracted_op,
		*requested_op, *handling_op;
	
	unsigned int dest_node;	 
	int ret;
	
	bool mine = false;

	critical_enter();

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	requested_op = NULL;
	operation = extracted_op = NULL;
	
	h = read_table(&queue->hashtable, th, epb, pub);

	dest_node = NODE_HASH((unsigned long) timestamp);

	requested_op = operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->op_id = 1;//__sync_fetch_and_add(&op_counter, 1);
	requested_op->type = OP_PQ_ENQ;
	requested_op->timestamp = timestamp;
	requested_op->payload = payload; //DEADBEEF
	requested_op->response = -1;
	requested_op->candidate = NULL;
	requested_op->requestor = &requested_op;

	do {
		// read table
		h = read_table(&queue->hashtable, th, epb, pub);

		if (operation != NULL  && !mine)
		{
			dest_node = NODE_HASH((unsigned long) operation->timestamp);	
			
			// need to move to another queue?
			if (dest_node != NID) 
			{
				/*
				// The node has been extracted from a non optimal queue
				new_operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
				new_operation->op_id = operation->op_id;
				new_operation->type = operation->type;
				new_operation->timestamp = operation->timestamp;
				new_operation->payload = operation->payload;
				new_operation->response = operation->response;
				new_operation->candidate = operation->candidate;
				new_operation->requestor = operation->requestor;
					
				do{
					tmp = *(new_operation->requestor);
				} while(!BOOL_CAS(new_operation->requestor, tmp, new_operation));
				*/
				// publish op on right queue
				tq_enqueue(&enq_queue[dest_node], (void *)operation, dest_node);
				
				//gc_free(ptst, operation, gc_aid[GC_OPNODE]);
				operation = NULL; // need to extract another op			
			}
			// here we keep the operation if it is not null
		}
		extracted_op = operation;

		if (extracted_op == NULL)
		{
			// check if my op was done
			if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) != -1)
			{
				gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
				critical_exit();
				requested_op = NULL;
				return ret;
			}

			if (!tq_dequeue(&enq_queue[NID], &extracted_op)) 
			{
				extracted_op = requested_op;
				mine = true;
			}
			else
				mine = false;
		}

		handling_op = extracted_op;
		if (handling_op->response != -1) {
			operation = NULL;
			continue;
		}

		ret = single_step_pq_enqueue(h, handling_op->timestamp, handling_op->payload, &handling_op->candidate, handling_op);
		if (ret != -1) //enqueue succesful
		{
			__sync_bool_compare_and_swap(&(handling_op->response), -1, ret); /* Is this an overkill? */
			operation = NULL;
			continue;
		}

		if (!mine) 
		{
			handling_op = NULL;
			operation = extracted_op;
		}
		mine = false;

	} while(1);
}

pkey_t pq_dequeue(void *q, void **result) 
{
	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *operation, *extracted_op = NULL,
		*requested_op, *handling_op;

	unsigned long long vb_index;
	unsigned int dest_node;	 
	int ret;
	pkey_t ret_ts;
	void* new_payload;

	bool mine = false;

	critical_enter();

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	requested_op = NULL;
	operation = extracted_op = NULL;
	
	h = read_table(&queue->hashtable, th, epb, pub);

	vb_index  = (h->current) >> 32;
	dest_node = NODE_HASH(next_node_deq++);

	requested_op = operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->op_id = __sync_fetch_and_add(&op_counter, 1);
	requested_op->type = OP_PQ_DEQ;
	requested_op->timestamp = vb_index * (h->bucket_width);
	requested_op->payload = NULL; //DEADBEEF
	requested_op->response = -1;
	requested_op->candidate = NULL;
	requested_op->requestor = &requested_op;

	do {

		// read table
		h = read_table(&queue->hashtable, th, epb, pub);

		if (operation != NULL)
		{
			// compute vb
			vb_index = (h->current) >> 32;
			dest_node = NODE_HASH(next_node_deq);
			
			// need to move to another queue?
			if (dest_node != NID && !mine) 
			{
				/*
				// The node has been extracted from a non optimal queue
				new_operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
				new_operation->op_id = operation->op_id;
				new_operation->type = operation->type;
				new_operation->timestamp = operation->timestamp;
				new_operation->payload = operation->payload;
				new_operation->response = operation->response;
				new_operation->candidate = operation->candidate;
				new_operation->requestor = operation->requestor;
					
				do{
					tmp = *(new_operation->requestor);
				} while(!BOOL_CAS(new_operation->requestor, tmp, new_operation));
				*/
				// publish op on right queue
				tq_enqueue(&deq_queue[dest_node], (void *) operation, dest_node);
				
				//gc_free(ptst, operation, gc_aid[GC_OPNODE]);
				operation = NULL; // need to extract another op
			}
			
			// keep the operation in case it's on the same node
			
		}

		extracted_op = operation;

		// dequeue one op
		if (extracted_op == NULL)
		{
			if (!tq_dequeue(&deq_queue[NID], &extracted_op)) 
			{
				if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) != -1)
				{
					*result=requested_op->payload;
					ret_ts = requested_op->timestamp;
					gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
					critical_exit();
					requested_op = NULL;
					// dovrebbe essere come se il thread fosse stato deschedulato prima della return
					return ret_ts; // someone did my op, we can return
				}
				extracted_op = requested_op;
				mine = true;
			}
			else 
				mine = false;
		}
			
		// execute op
		handling_op = extracted_op;
		if (handling_op->response != -1) {
			operation = NULL;
			continue;
		}

		ret = single_step_pq_dequeue(h, queue, &ret_ts, &new_payload, handling_op->op_id, &handling_op->candidate);
		if (ret != -1)
		{ //dequeue failed
			performed_dequeue++;
			handling_op->payload = new_payload;
			handling_op->timestamp = ret_ts;
			__sync_bool_compare_and_swap(&(handling_op->response), -1, 1); /* Is this an overkill? */
			operation = NULL;
			continue;
		}
		
		if (!mine) 
		{
			handling_op = NULL;
			operation = extracted_op;
		}
		mine = false;

	} while(1);
}

void pq_report(int TID)
{

	printf("%d- "
		   "Enqueue: %.10f LEN: %.10f ### "
		   "Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ### "
		   "NEAR: %llu "
		   "RTC:%d,M:%lld\n",
		   TID,
		   ((float)concurrent_enqueue) / ((float)performed_enqueue),
		   ((float)scan_list_length_en) / ((float)performed_enqueue),
		   ((float)concurrent_dequeue) / ((float)performed_dequeue),
		   ((float)scan_list_length) / ((float)performed_dequeue),
		   num_cas, num_cas_useful,
		   near,
		   read_table_count,
		   malloc_count);
}

void pq_reset_statistics()
{
	near = 0;
	num_cas = 0;
	num_cas_useful = 0;
}

unsigned int pq_num_malloc() { return (unsigned int)malloc_count; }
