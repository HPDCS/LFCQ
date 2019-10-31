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
task_queue enq_queue[_NUMA_NODES]; // per numa node queue
task_queue deq_queue[_NUMA_NODES]; 

volatile unsigned deq_lock = 0;

#ifndef ENQ_MAX_WAIT_ATTEMPTS
#define ENQ_MAX_WAIT_ATTEMPTS 100
#endif
 
#ifndef DEQ_MAX_WAIT_ATTEMPTS
#define DEQ_MAX_WAIT_ATTEMPTS 100
#endif

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

__thread unsigned long long local_enq = 0ULL;
__thread unsigned long long local_deq = 0ULL;
__thread unsigned long long remote_enq = 0ULL;
__thread unsigned long long remote_deq = 0ULL;

__thread unsigned long long enq_steal_done = 0ULL;
__thread unsigned long long enq_steal_attempt = 0ULL;
__thread unsigned long long deq_steal_done = 0ULL;
__thread unsigned long long deq_steal_attempt = 0ULL;
__thread unsigned long long repost_enq = 0ULL;
__thread unsigned long long repost_deq = 0ULL;

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
	for (i = 0; i < ACTIVE_NUMA_NODES; i++)
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

/**
 * This function implements the enqueue interface of the NBCQ.
 * Cost O(1) when succeeds
 *
 * @param q: pointer to the queueu
 * @param timestamp: the key associated with the value
 * @param payload: the value to be enqueued
 * @return true if the event is inserted in the set table else false
 */
int __attribute__((optimize("O0"))) do_pq_enqueue(void* q, pkey_t timestamp, void* payload)
{
	unsigned long i = LOOP_COUNT;
	last_ts = timestamp;

	while(i > 0)
		i--;

	performed_enqueue++;
	return 1;
}


/**
 * This function extract the minimum item from the NBCQ. 
 * The cost of this operation when succeeds should be O(1)
 *
 * @param  q: pointer to the interested queue
 * @param  result: location to save payload address
 * @return the highest priority 
 *
 */
pkey_t __attribute__((optimize("O0"))) do_pq_dequeue(void *q, void** result)
{

	unsigned long i = LOOP_COUNT;
	while(i > 0)
		i--;

	*result = (void *) 0x1ull;
	performed_dequeue++;
	return last_ts;

}

static inline int handle_enq(void* q) 
{
	op_node * operation;
	void *pld;
	pkey_t ts;
	int ret, count;

	count = 0;
	while(tq_dequeue(&enq_queue[NID], &operation))
	{
		// block the operation
		if (!BOOL_CAS(&(operation->response), OP_NEW, OP_IN_HANDLING))
			continue;
		ts = operation->timestamp;
		pld = operation->payload;
		ret = do_pq_enqueue(q, ts, pld);
		__sync_bool_compare_and_swap(&(operation->response), OP_IN_HANDLING, ret);
		count++;
	}

	return count;
}

static inline int handle_deq(void* q) 
{
	op_node * operation;
	void *pld;
	pkey_t ts;
	int count = 0;
	// get the lock of dequeue
	if (BOOL_CAS(&deq_lock, 0, 1))
	{
		while(tq_dequeue(&deq_queue[NID], &operation))
		{	
			// block the operation
			if (!BOOL_CAS(&(operation->response), OP_NEW, OP_IN_HANDLING))
				continue;
			ts = do_pq_dequeue(q, &pld);
			operation->timestamp = ts;
			operation->payload = pld;
			__sync_bool_compare_and_swap(&(operation->response), OP_IN_HANDLING, 1);
			count++;
		}
	
		// deq_lock
		deq_lock = 0;
	}
	return count;
}

__thread unsigned int next_node_deq;

int pq_enqueue(void* q, pkey_t timestamp, void *payload) 
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range "KEY_STRING"\n", timestamp);

	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *requested_op, *operation;

	unsigned long long vb_index;
	unsigned int dest_node, new_dest_node;
	unsigned int attempts, count;	 
	int ret;

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	requested_op = NULL;

	critical_enter();

	//handle_ops(q);

	h = read_table(&queue->hashtable, th, epb, pub);

	vb_index  = hash(timestamp, h->bucket_width);
	dest_node = NODE_HASH((unsigned long) timestamp);

	
	if (dest_node == NID)
	{
		ret = do_pq_enqueue(q, timestamp, payload);
		critical_exit();
		return ret;
	}
	

	// post the operation on one queue

	requested_op = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->type = OP_PQ_ENQ;
	requested_op->timestamp = timestamp;
	requested_op->payload = payload; //DEADBEEF
	requested_op->response = OP_NEW;

	tq_enqueue(&enq_queue[dest_node], (void *)requested_op, dest_node);

	//
	attempts = 0;
	do 
	{	
		if (attempts > ENQ_MAX_WAIT_ATTEMPTS)
		{
			// mark op as in handling
			enq_steal_attempt++;
			if (BOOL_CAS(&(requested_op->response), OP_NEW, OP_IN_HANDLING))
			{
				
				enq_steal_done++;

				h = read_table(&queue->hashtable, th, epb, pub);
				new_dest_node =   NODE_HASH((unsigned long) timestamp);
				if (new_dest_node == dest_node || new_dest_node == NID)
				{
					ret = do_pq_enqueue(q, timestamp, payload);
					break;
				}
				dest_node = new_dest_node;

				// repost operation
				operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
				operation->type = OP_PQ_ENQ;
				operation->timestamp = timestamp;
				operation->payload = payload; //DEADBEEF
				operation->response = OP_NEW;

				gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
				requested_op = operation;

				tq_enqueue(&enq_queue[dest_node], (void *)requested_op, dest_node);
				repost_enq++;
			}
			attempts = 0;
		}
		// steal/repost

		count = handle_enq(q);
		count += handle_deq(q);
		
		if (count > 0)
			attempts=0;
		else
			attempts++;

		if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) >= 0)
			break;
	} while(1);

	gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
	return ret;
}


pkey_t pq_dequeue(void *q, void **result) 
{
	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *requested_op, *operation;

	unsigned long long vb_index;
	unsigned int dest_node, new_dest_node;
	unsigned int count, attempts;
	int ret;
	pkey_t ret_ts;
	

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	critical_enter();

	requested_op = NULL;
	
	//handle_ops(q);

	h = read_table(&queue->hashtable, th, epb, pub);

	vb_index  = next_node_deq++;
	dest_node = NODE_HASH(vb_index);

	if (dest_node == NID) {
		ret_ts = do_pq_dequeue(q, result);
		critical_exit();
		return ret_ts;
	}

	requested_op = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->type = OP_PQ_DEQ;
	//requested_op->timestamp = vb_index * (h->bucket_width);
	requested_op->payload = NULL; //DEADBEEF
	requested_op->response = OP_NEW;

	tq_enqueue(&deq_queue[dest_node], (void *)requested_op, dest_node);
	
	attempts = 0;
	do 
	{
		if (attempts > DEQ_MAX_WAIT_ATTEMPTS)
		{
			deq_steal_attempt++;
			// mark op as in handling -> who extract it from queue will yeld it
			if (BOOL_CAS(&(requested_op->response), OP_NEW, OP_IN_HANDLING))
			{
				deq_steal_done++;
				h = read_table(&queue->hashtable, th, epb, pub);
				new_dest_node =  NODE_HASH(vb_index);
				if (new_dest_node == dest_node || new_dest_node == NID)
				{
					ret_ts = do_pq_dequeue(q, result);
					break; // steal succesful
				}
				dest_node = new_dest_node;

				operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
				operation->type = OP_PQ_DEQ;
				//operation->timestamp = vb_index * (h->bucket_width);
				operation->payload = NULL; //DEADBEEF
				operation->response = OP_NEW;

				gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
				requested_op = operation;

				tq_enqueue(&deq_queue[dest_node], (void *)requested_op, dest_node);
				repost_deq++;
			}
			attempts = 0;
		}
		
		count = handle_enq(q);
		count += handle_deq(q);
		
		if (count > 0)
			attempts = 0;
		else 
			attempts++;

		if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) >= 0)
		{
			result = requested_op->payload;
			ret_ts = requested_op->timestamp;
			break;
		}
	} while(1);

	gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
	critical_exit();
	return ret_ts;
}

void pq_report(int TID)
{

	printf("%d- "
	"Enqueue: %.10f LEN: %.10f ST: %llu : %llu : %llu ### "
	"Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ST: %llu : %llu : %llu ### "
	"NEAR: %llu "
	"RTC:%d, M:%lld, "
	"Local ENQ: %llu DEQ: %llu, Remote ENQ: %llu DEQ: %llu\n",
			TID,
			((float)concurrent_enqueue) /((float)performed_enqueue),
			((float)scan_list_length_en)/((float)performed_enqueue),
			enq_steal_attempt, enq_steal_done, repost_enq,
			((float)concurrent_dequeue) /((float)performed_dequeue),
			((float)scan_list_length)   /((float)performed_dequeue),
			num_cas, num_cas_useful,
			deq_steal_attempt, deq_steal_done, repost_deq,
			near,
			read_table_count	  ,
			malloc_count,
			local_enq, local_deq, remote_enq, remote_deq);
}

void pq_reset_statistics()
{
	near = 0;
	num_cas = 0;
	num_cas_useful = 0;
}

unsigned int pq_num_malloc() { return (unsigned int)malloc_count; }
