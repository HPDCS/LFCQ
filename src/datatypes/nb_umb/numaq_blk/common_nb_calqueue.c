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

#include "common_nb_calqueue.h"
#include "table_utils.h"

/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

int gc_aid[32];
int gc_hid[4];

unsigned long op_counter = 2;
task_queue op_queue[_NUMA_NODES]; // (!new) per numa node queue


#ifndef ENQ_MAX_WAIT_ATTEMPTS
#define ENQ_MAX_WAIT_ATTEMPTS 10000
#endif
 
#ifndef DEQ_MAX_WAIT_ATTEMPTS
#define DEQ_MAX_WAIT_ATTEMPTS 10000
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
		init_tq(&op_queue[i], i);
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

/**
 * This function implements the enqueue interface of the NBCQ.
 * Cost O(1) when succeeds
 *
 * @param q: pointer to the queueu
 * @param timestamp: the key associated with the value
 * @param payload: the value to be enqueued
 * @return true if the event is inserted in the set table else false
 */
static inline int do_pq_enqueue(void* q, pkey_t timestamp, void* payload)
{

	nb_calqueue* queue = (nb_calqueue*) q; 	

	nbc_bucket_node *bucket, *new_node = numa_node_malloc(payload, timestamp, 0, NID);
	table * h = NULL;		
	unsigned int index, size;
	unsigned long long newIndex = 0;
	
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	int dest_node;
	bool remote = false; // tells whether the enqueue touched a remote node

	int res, con_en = 0;
	

	//init the result
	res = MOV_FOUND;
	
	//repeat until a successful insert
	while(res != OK){
		
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){
			
			//free the old node
			node_free(new_node);
			
			// check for a resize
			h = read_table(&queue->hashtable, th, epb, pub);
			// get actual size
			size = h->size;
	        // read the actual epoch
        	//new_node->epoch = (h->current & MASK_EPOCH);
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			
			// compute the index of the physical bucket
			index = ((unsigned int) newIndex) % size;	

			dest_node = NODE_HASH(index);
			if (dest_node != NID)
				remote = true;

			// allocate a new node on numa node
			new_node = numa_node_malloc(payload, timestamp, 0, dest_node);
			new_node->epoch = (h->current & MASK_EPOCH);
	
			// get the bucket
			bucket = h->array + index;
			// read the number of executed enqueues for statistics purposes
			con_en = h->e_counter.count;
		}


		#if KEY_TYPE != DOUBLE
		if(res == PRESENT){
			res = 0;
			goto out;
		}
		#endif

		// search the two adjacent nodes that surround the new key and try to insert with a CAS 
	    res = search_and_insert(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node);
	}


	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
	flush_current(h, newIndex, new_node);
	performed_enqueue++;
	res=1;
	
	// updates for statistics
	
	concurrent_enqueue += (unsigned long long) (__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);
	
	#if COMPACT_RANDOM_ENQUEUE == 1
	// clean a random bucket
	unsigned long long oldCur = h->current;
	unsigned long long oldIndex = oldCur >> 32;
	unsigned long long dist = 1;
	double rand;
	nbc_bucket_node *left_node, *right_node;
	drand48_r(&seedT, &rand);
	search(h->array+((oldIndex + dist + (unsigned int)( ( (double)(size-dist) )*rand )) % size), -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
	#endif

  #if KEY_TYPE != DOUBLE
  out:
  #endif

	// check if local or not
	if (!remote)
		local_enq++;
	else
		remote_enq++;
	

	return res;
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
static inline pkey_t do_pq_dequeue(void *q, void** result)
{

	nb_calqueue *queue = (nb_calqueue*)q;
	nbc_bucket_node *min, *min_next, 
					*left_node, *left_node_next, 
					*tail, *array;
	table * h = NULL;
	
	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned int size, attempts = 0;
	unsigned int counter, dest_node;
	pkey_t left_ts;
	double bucket_width, left_limit, right_limit;

	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	unsigned int ep = 0;
	int con_de = 0;
	bool prob_overflow = false;
	tail = queue->tail;
	performed_dequeue++;
	
	bool remote = false;

begin:
	// Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub);

	// Get data from the table
	size = h->size;
	array = h->array;
	bucket_width = h->bucket_width;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	do
	{	
		// To many attempts: there is some problem? recheck the table
		if( h->read_table_period == attempts){
			goto begin;
		}
		attempts++;

		// get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;


		// get the physical bucket
		min = array + (index % (size));
		left_node = min_next = min->next;
		
		dest_node = NODE_HASH(index % (size));
		if (dest_node != NID)
			remote = true;

		// get the left limit
		left_limit = ((double)index)*bucket_width;

		index++;

		// get the right limit
		right_limit = ((double)index)*bucket_width;
		// check for a possible overflow
		prob_overflow = (index > MASK_EPOCH);
		
		// reset variables for a new scan
		counter = ep = 0;
		
		// a reshuffle has been detected => restart
		if(is_marked(min_next, MOV)) goto begin;
		
		do
		{
			
			// get data from the current node	
			left_node_next = left_node->next;
			left_ts = left_node->timestamp;

			// increase count of traversed deleted items
			counter++;

			// Skip marked nodes, invalid nodes and nodes with timestamp out of range
			if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV) || (left_ts < left_limit && left_node != tail)) continue;
			
			// Abort the operation since there is a resize or a possible insert in the past
			if(is_marked(left_node_next, MOV) || left_node->epoch > epoch) goto begin;
			
			// The virtual bucket is empty
			if(left_ts >= right_limit || left_node == tail) break;
			
			// the node is a good candidate for extraction! lets try for it
			int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));

			// the extraction is failed
			if(!res) left_node_next = left_node->next;

			//left_node_next = FETCH_AND_OR(&left_node->next, DEL);
			
			// the node cannot be extracted && is marked as MOV	=> restart
			if(is_marked(left_node_next, MOV))	goto begin;

			// the node cannot be extracted && is marked as DEL => skip
			if(is_marked(left_node_next, DEL))	continue;
			
			// the node has been extracted

			// use it for count the average number of traversed node per dequeue
			scan_list_length += counter;
			// use it for count the average of completed extractions
			concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

			*result = left_node->payload;
				
			// check if local or not
			if (!remote)
				local_deq++;
			else
				remote_deq++;

			return left_ts;
										
		}while( (left_node = get_unmarked(left_node_next)));
		

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		{
			*result = NULL;
			return INFTY;
		}
				
		new_current = h->current;
		if(new_current == current){

			if(prob_overflow && h->e_counter.count == 0) goto begin;
			


			assertf(prob_overflow, "\nOVERFLOW INDEX:%llu" "BW:%.10f"  "SIZE:%u TAIL:%p TABLE:%p\n", index, bucket_width, size, tail, h);
			//assertf((index - (last_curr >> 32) -1) <= dist, "%s\n", "PROVA");

			num_cas++;
			old_current = VAL_CAS( &(h->current), current, ((index << 32) | epoch) );
			if(old_current == current){
				current = ((index << 32) | epoch);
				num_cas_useful++;
			}
			else
				current = old_current;
		}
		else
			current = new_current;
		
	}while(1);
	
	return INFTY;
}

static inline int handle_ops(void* q) 
{
	
	op_node *operation;
	void *pld;

	pkey_t ts;

	unsigned int type;
	int ret, count;

	count = 0;
	while(tq_dequeue(&op_queue[NID], &operation))
	{
		// block the operation
		if (!BOOL_CAS(&(operation->response), OP_NEW, OP_IN_HANDLING))
			continue;

		// handle the operation
		type = operation->type;
		if (type == OP_PQ_ENQ)
		{
			ts = operation->timestamp;
			pld = operation->payload;
			ret = do_pq_enqueue(q, ts, pld);
			__sync_bool_compare_and_swap(&(operation->response), OP_IN_HANDLING, ret);
		}
		else 
		{
			ts = do_pq_dequeue(q, &pld);
			operation->timestamp = ts;
			operation->payload = pld;
			__sync_bool_compare_and_swap(&(operation->response), OP_IN_HANDLING, 1);
		}
		count++;
	}

	return count;	
}

// @TODO - enable blocking steal of operations
// @TODO - add steal/repost counters

int pq_enqueue(void* q, pkey_t timestamp, void *payload) 
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");

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
	dest_node = NODE_HASH(vb_index % h->size);

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

	tq_enqueue(&op_queue[dest_node], (void *)requested_op, dest_node);

	//
	attempts = 0;
	do 
	{	
		if (attempts > ENQ_MAX_WAIT_ATTEMPTS)
		{
			// mark op as in handling
			if (BOOL_CAS(&(requested_op->response), OP_NEW, OP_IN_HANDLING))
			{
				h = read_table(&queue->hashtable, th, epb, pub);
				new_dest_node =  NODE_HASH(hash(timestamp, h->bucket_width) % (h->size));
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

				tq_enqueue(&op_queue[dest_node], (void *)requested_op, dest_node);
			}
			attempts = 0;
		}
		// steal/repost

		count = handle_ops(q);

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

	vb_index  = (h->current) >> 32;
	dest_node = NODE_HASH(vb_index % h->size);

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

	tq_enqueue(&op_queue[dest_node], (void *)requested_op, dest_node);
	
	attempts = 0;
	do 
	{
		if (attempts > ENQ_MAX_WAIT_ATTEMPTS)
		{
			// mark op as in handling -> who extract it from queue will yeld it
			if (BOOL_CAS(&(requested_op->response), OP_NEW, OP_IN_HANDLING))
			{
				h = read_table(&queue->hashtable, th, epb, pub);
				new_dest_node =  NODE_HASH(((h->current) >> 32) % (h->size));
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

				tq_enqueue(&op_queue[dest_node], (void *)requested_op, dest_node);

			}
			attempts = 0;
		}
		

		count = handle_ops(q);
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
	"Enqueue: %.10f LEN: %.10f ### "
	"Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ### "
	"NEAR: %llu "
	"RTC:%d, M:%lld, "
	"Local ENQ: %llu DEQ: %llu, Remote ENQ: %llu DEQ: %llu\n",
			TID,
			((float)concurrent_enqueue) /((float)performed_enqueue),
			((float)scan_list_length_en)/((float)performed_enqueue),
			((float)concurrent_dequeue) /((float)performed_dequeue),
			((float)scan_list_length)   /((float)performed_dequeue),
			num_cas, num_cas_useful,
			near,
			read_table_count,
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
