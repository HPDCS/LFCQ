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

#include "fake.h"

/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

int gc_aid[32];
int gc_hid[4];

unsigned long op_counter = 2;
task_queue enq_queue[NUM_SOCKETS]; // (!new) per numa node queue
task_queue deq_queue[NUM_SOCKETS];

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

__thread unsigned long remote_deq = 0;
__thread unsigned long local_deq = 0;

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

	ACTIVE_SOCKETS = (((THREADS * NUM_SOCKETS)) / NUM_CPUS) + ((((THREADS * NUM_SOCKETS)) % NUM_CPUS) != 0);
	ACTIVE_SOCKETS = ACTIVE_SOCKETS < NUM_SOCKETS? ACTIVE_SOCKETS:NUM_SOCKETS;

	ACTIVE_NUMA_NODES = (((THREADS * _NUMA_NODES)) / NUM_CPUS) + ((((THREADS * _NUMA_NODES)) % NUM_CPUS) != 0); // (!new) compute the number of active numa nodes 
	ACTIVE_NUMA_NODES = ACTIVE_NUMA_NODES < _NUMA_NODES? ACTIVE_NUMA_NODES:_NUMA_NODES;
	LOG("\n#######\nThreads %d, NUMA Nodes %d, CPUS %d, ACTIVE NUMA Nodes %d\n########\n", THREADS, _NUMA_NODES, NUM_CPUS, ACTIVE_NUMA_NODES);

	unsigned int i = 0;
	int res_mem_posix = 0;
	nb_calqueue *res = NULL;

	// init fraser garbage collector/allocator
	_init_gc_subsystem();
	_init_gc_tq();
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
	for (i = 0; i < ACTIVE_SOCKETS; i++)
	{
		init_tq(&enq_queue[i], i<<1);
		init_tq(&deq_queue[i], i<<1);
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
	res->hashtable->pad = 3; // base epb
	
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

int do_pq_enqueue(void* q, pkey_t timestamp, void* payload)
{

	last_ts = timestamp;
	performed_enqueue++;
	return 1;
}


int do_pq_dequeue(void *q, pkey_t* timestamp, void** result)
{

	*result = (void *) 0x1ull;
	*timestamp = last_ts;
	performed_dequeue++;
	return 1;
}

int pq_enqueue(void* q, pkey_t timestamp, void *payload) 
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");

	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *operation, *requested_op;

	unsigned long long vb_index;
	unsigned int dest_node, dest_socket;
	int ret;
	
	bool mine = false;

	critical_enter();

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	operation = NULL;
	
	h = read_table(&queue->hashtable, th, epb, pub);

	vb_index  = hash(timestamp, h->bucket_width);
	dest_node = NODE_HASH((unsigned long) timestamp);

	requested_op = operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->type = OP_PQ_ENQ;
	requested_op->timestamp = timestamp;
	requested_op->payload = payload; //DEADBEEF
	requested_op->response = OP_CLEAN;
	
	operation = requested_op;

	// we should enqueue it, otherwise we cannot publish it!

	do {
		// read table
		h = read_table(&queue->hashtable, th, epb, pub);

		// STEP 1 - ENQUEUE/HOLD
		if (operation != NULL && !mine) //(If I'm handling my op I don't need to re-enqueue it - except for first iteration)
		{
			// compute vb

			vb_index  = hash(operation->timestamp, h->bucket_width);
			dest_node = NODE_HASH((unsigned long) operation->timestamp);		
			
			dest_socket = dest_node >> 1;

			// need to move to another queue? 
			if (dest_socket != SID) 
			{
				ret = VAL_CAS(&operation->response, OP_HANDLING, OP_CLEAN);
				if (ret != OP_CLEAN && ret != OP_HANDLING)
				{	
					LOG("%d ENQ - Cannot repost", TID);
					abort();
				}
				tq_enqueue(&enq_queue[dest_socket], (void*) operation, dest_node);
                operation = NULL;
			}
			// the operation is still for us, we keep it!
		}

		// STEP 2 - EXTRACTION/HOLDING
		if (operation == NULL)
		{	
			// check if my op was done - done here since we don't want to remove someone op from queues
			if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) == OP_DONE)
			{
				gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
				critical_exit();
				// dovrebbe essere come se il thread fosse stato deschedulato prima della return
				return ret; // someone did my op, we can return
			}

			if (!tq_dequeue(&enq_queue[SID], &operation)) 
			{
				operation = requested_op;
				mine = true;
			}
			else
				mine = false;
		}

		// STEP 3 EXECUTION	
		if (!BOOL_CAS(&operation->response, OP_CLEAN, OP_HANDLING))
		{
			// the op is in handling by someone else or already done
			if (mine)
			{
				mine = false;
			}
			operation = NULL;
			continue;
		}

		ret = do_pq_enqueue(q, operation->timestamp, operation->payload);
		if (ret != -1) //enqueue succesful
		{
			BOOL_CAS(&(operation->response), OP_HANDLING, OP_DONE); // Is this an overkill?
			operation = NULL;
			continue;
		}
		
		if (!BOOL_CAS(&operation->response, OP_HANDLING, OP_CLEAN))
		{
			abort();
		}
		
	} while(1);
}

pkey_t pq_dequeue(void *q, void **result) 
{

	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *operation, *requested_op;

	unsigned long long vb_index;
	unsigned int dest_node, dest_socket;	 
	int ret;
	pkey_t ret_ts;
	void* new_payload;

	bool mine = false;

	critical_enter();

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	operation = NULL;
	
	h = read_table(&queue->hashtable, th, epb, pub);

	vb_index  = (h->current) >> 32;
	dest_node = NODE_HASH(next_node_deq++);

	requested_op = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->type = OP_PQ_DEQ;
	requested_op->timestamp = vb_index * (h->bucket_width);
	requested_op->payload = NULL; //DEADBEEF
	requested_op->response = OP_CLEAN;

	operation = requested_op;

	do {

		// read table
		h = read_table(&queue->hashtable, th, epb, pub);

		if (operation != NULL && !mine)
		{

			vb_index = (h->current) >> 32;
			dest_node = NODE_HASH(next_node_deq);

			dest_socket = dest_node >> 1;

			// need to move to another queue?
			if (dest_socket != SID) 
			{
				ret = VAL_CAS(&operation->response, OP_HANDLING, OP_CLEAN);
				if (ret != OP_HANDLING && ret != OP_CLEAN)
				{	
					LOG("%d ENQ - Cannot repost", TID);
					abort();
				}
				// The node has been extracted from a non optimal queue
				tq_enqueue(&deq_queue[dest_socket], (void*) operation, dest_node);
				operation = NULL; // yeld the op since is no longer for us.
			}
			// keep the operation	
		}

		// dequeue one op
		if (operation == NULL)
		{
			
			// check if my op was done // we could lose op
			if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) == OP_DONE)
			{
				*result = requested_op->payload;
				ret_ts = requested_op->timestamp;
				gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
				critical_exit();
				// dovrebbe essere come se il thread fosse stato deschedulato prima della return
				return ret_ts; // someone did my op, we can return
			}

			if (!tq_dequeue(&deq_queue[SID], &operation)) {
				operation = requested_op;
				mine = true;
			}
			else 
				mine = false;
		}	
		
		// execute op
		if (!BOOL_CAS(&operation->response, OP_CLEAN, OP_HANDLING))
		{
			// the op is in handling by someone else or already done
			if (mine)
			{
				mine = false;
			}
			operation = NULL;
			continue;
		}

		ret = do_pq_dequeue(q, &ret_ts, &new_payload);
		if (ret != -1)
		{
			performed_dequeue++;
			operation->payload = new_payload;
			operation->timestamp = ret_ts;
			BOOL_CAS(&(operation->response), OP_HANDLING, OP_DONE); // Is this an overkill?
			operation = NULL;
			continue;
		}
		
		if (!BOOL_CAS(&operation->response, OP_HANDLING, OP_CLEAN))
		{
			abort();
		}

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
