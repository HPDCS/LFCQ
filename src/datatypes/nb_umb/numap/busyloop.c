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

int gc_aid[1];
int gc_hid[1];

unsigned int ACTIVE_NUMA_NODES;

#ifndef ENQ_MAX_WAIT_ATTEMPTS
#define ENQ_MAX_WAIT_ATTEMPTS 100
#endif
 
#ifndef DEQ_MAX_WAIT_ATTEMPTS
#define DEQ_MAX_WAIT_ATTEMPTS 100
#endif

#define abort_line() do{\
	printf("Aborting @Line %d\n", __LINE__);\
	abort();\
	}while (0)


/*************************************
 * THREAD LOCAL VARIABLES			 *
 ************************************/

__thread unsigned long long concurrent_enqueue;
__thread unsigned long long performed_enqueue ;
__thread unsigned long long concurrent_dequeue;
__thread unsigned long long performed_dequeue ;
__thread unsigned long long scan_list_length;
__thread unsigned long long scan_list_length_en ;
__thread unsigned int 		read_table_count	 = UINT_MAX;
__thread unsigned long long num_cas = 0ULL;
__thread unsigned long long num_cas_useful = 0ULL;
__thread unsigned long long near = 0;
__thread unsigned int 		acc = 0;
__thread unsigned int 		acc_counter = 0;

__thread unsigned long long enq_steal_done = 0ULL;
__thread unsigned long long enq_steal_attempt = 0ULL;
__thread unsigned long long deq_steal_done = 0ULL;
__thread unsigned long long deq_steal_attempt = 0ULL;

__thread unsigned long long local_enq = 0ULL;
__thread unsigned long long local_deq = 0ULL;
__thread unsigned long long remote_enq = 0ULL;
__thread unsigned long long remote_deq = 0ULL;

__thread unsigned long long repost_enq = 0ULL;
__thread unsigned long long repost_deq = 0ULL;

void std_free_hook(ptst_t *p, void *ptr){	free(ptr); }


/**
 * This function create an instance of a NBCQ.
 *
 * @param threshold: ----------------
 * @param perc_used_bucket: set the percentage of occupied physical buckets 
 * @param elem_per_bucket: set the expected number of items for each virtual bucket
 * @return a pointer a new queue
 */
void* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{

	ACTIVE_NUMA_NODES = (((THREADS * _NUMA_NODES)) / NUM_CPUS) + ((((THREADS * _NUMA_NODES)) % NUM_CPUS) != 0); // (!new) compute the number of active numa nodes 
	ACTIVE_NUMA_NODES = ACTIVE_NUMA_NODES < _NUMA_NODES? ACTIVE_NUMA_NODES:_NUMA_NODES;
	LOG("\n#######\nThreads %d, NUMA Nodes %d, CPUs %d, ACTIVE NUMA NODES%d\n########\n", THREADS, _NUMA_NODES, NUM_CPUS, ACTIVE_NUMA_NODES);

	unsigned int i = 0;
	int res_mem_posix = 0;
	nb_calqueue* res = NULL;

	// init fraser garbage collector/allocator //maybe using basic allocator will be better
	_init_gc_subsystem();
	init_mapping();
	// add allocator of nbc_bucket_node
	gc_aid[0] = gc_add_allocator(sizeof(nbc_bucket_node));
	// add callback for set tables and array of nodes whene a grace period has been identified
	gc_hid[0] = gc_add_hook(std_free_hook);
	critical_enter();
	critical_exit();

	// allocate memory
	res_mem_posix = posix_memalign((void**)(&res), CACHE_LINE_SIZE, sizeof(nb_calqueue));
	if(res_mem_posix != 0) 	error("No enough memory to allocate queue\n");
	res_mem_posix = posix_memalign((void**)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table));
	if(res_mem_posix != 0)	error("No enough memory to allocate set table\n");
	res_mem_posix = posix_memalign((void**)(&res->hashtable->array), CACHE_LINE_SIZE, MINIMUM_SIZE*sizeof(nbc_bucket_node));
	if(res_mem_posix != 0)	error("No enough memory to allocate array of heads\n");
	

	
	res->threshold = threshold;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->read_table_period = READTABLE_PERIOD;
	res->tail = numa_node_malloc(NULL, INFTY, 0, 0);
	res->tail->next = NULL;

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
 * @param q: pointer to the queue
 * @param timestamp: the key associated with the value
 * @param payload: the value to be enqueued
 * @return true if the event is inserted in the set table else false
 */
int __attribute__((optimize("O0"))) do_pq_enqueue(void* q, pkey_t timestamp, void* payload)
{

    unsigned long i = LOOP_COUNT;
	while(i > 0)
		i--;

	last_ts = timestamp;
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

	*result = (void*) 0x1;
	performed_dequeue++;
	return last_ts;
	
}

static inline int handle_ops(void* q) 
{
	int i, ret, count ;
	
	unsigned int type;
	unsigned int node;
	pkey_t ts;
	void *pld;

	op_node *to_me, *from_me;

	count = 0;
	for (i = NID+1; i % ACTIVE_NUMA_NODES != NID; i++)
	{
		i = i % ACTIVE_NUMA_NODES;
	
		to_me 	= get_req_slot_from_node(i);

		if (read_slot(to_me, &type, &ret, &ts, &pld, &node))
		{
			from_me = get_res_slot_to_node(i);

			if (type == OP_PQ_ENQ) 
			{
				local_enq++;
				ret = do_pq_enqueue(q, ts, pld);
			}
			else
			{ 
				local_deq++;
				ts = do_pq_dequeue(q, &pld);
			}
			if (!write_slot(from_me, type, ret, ts, pld, node))
			{
				abort_line();
			}
			count++;
		}
	}

	return count;	

}

int pq_enqueue(void* q, pkey_t timestamp, void* payload) {

	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");
	nb_calqueue* queue = (nb_calqueue*) q;

	op_node *resp,		// pointer to slot on which someone will post a response 
			*from_me;	// pointer to slot on which I will post my operation or my response

	int ret;
	unsigned int type;

	pkey_t	ts;
	
	void* pld;

	unsigned long attempts, count;

	critical_enter();

	table * h = NULL;
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	unsigned int dest_node, new_dest_node;

	count = handle_ops(q);	

	// read table
	h = read_table(&queue->hashtable, th, epb, pub);
	
	// check destination
	//dest_node = NODE_HASH(hash(timestamp, h->bucket_width) % h->size);
	dest_node = NODE_HASH((unsigned int) timestamp);

	// if NID execute
	if (dest_node == NID)
	{
		local_enq++;
		int ret = do_pq_enqueue(q, timestamp, payload);
		critical_exit();
		return ret;
	}

	// posting the operation
	from_me = get_req_slot_to_node(dest_node);
	resp = get_res_slot_from_node(dest_node);
	//read_slot(resp, &type, &ret, &ts, &pld);
	
	if (!write_slot(from_me, OP_PQ_ENQ, 0, timestamp, payload, dest_node))
	{
		abort_line();
	}

	attempts = 0;
	do {

		if (attempts > ENQ_MAX_WAIT_ATTEMPTS) 
		{
			enq_steal_attempt++;
			from_me = get_req_slot_to_node(dest_node);
			if (read_slot(from_me, &type, &ret, &ts, &pld, &new_dest_node))
			{
				enq_steal_done++;

				h = read_table(&queue->hashtable, th, epb, pub);
				new_dest_node =  NODE_HASH((unsigned int) timestamp);

				if (new_dest_node == dest_node || new_dest_node == NID)
				{
					if (new_dest_node == NID)
						local_enq++;
					else
						remote_enq++;

					ret = do_pq_enqueue(q, timestamp, payload);
					break;
				}
				dest_node = new_dest_node;

				// posting the operation
				from_me = get_req_slot_to_node(dest_node);
				resp = get_res_slot_from_node(dest_node);
				
				if (!write_slot(from_me, OP_PQ_ENQ, 0, timestamp, payload, dest_node))
				{
					abort_line();
				}

				repost_enq++;
			}
			attempts = 0;
		}
 
		// do all ops
		count = handle_ops(q);	
		
		if (count == 0)
			attempts++;
		else
			attempts=0;

		// check response
		if (read_slot(resp, &type, &ret, &ts, &pld, &new_dest_node))
			break;
	} while(1);

	critical_exit();
	return ret;	
}

__thread unsigned int next_node_deq;

pkey_t pq_dequeue(void *q, void** result)
{
	// read table
	nb_calqueue* queue = (nb_calqueue*) q;

	op_node *resp,		// pointer to slot on which someone will post a response 
			*from_me;	// pointer to slot on which I will post my operation or my response

	int ret;

	unsigned int	type;

	unsigned long attempts, count;

	pkey_t	ts;
	void* pld;

	critical_enter();
	table * h = NULL;
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	unsigned int dest_node, new_dest_node;
	
	count = handle_ops(q);
	
	// read table
	h = read_table(&queue->hashtable, th, epb, pub);
	// check destination
	dest_node = NODE_HASH(next_node_deq++);

	if (dest_node == NID) {
		local_deq++;
		pkey_t ret = do_pq_dequeue(q, result);
		critical_exit();
		return ret;
	}
	
	// posting the operation
	from_me = get_req_slot_to_node(dest_node);
	resp = get_res_slot_from_node(dest_node);
	//read_slot(resp, &type, &ret, &ts, &pld);

	if (!write_slot(from_me, OP_PQ_DEQ, 0, 0, 0, dest_node))
	{
		abort_line();
	}


	attempts = 0;
	do {
		
		if (attempts > DEQ_MAX_WAIT_ATTEMPTS) 
		{
			deq_steal_attempt++;
			from_me = get_req_slot_to_node(dest_node);
			if (read_slot(from_me, &type, &ret, &ts, &pld, &new_dest_node))
			{
				deq_steal_done++;

				h = read_table(&queue->hashtable, th, epb, pub);
				new_dest_node = NODE_HASH(((h->current)>>32)%(h->size));

				// if the dest node is mine or is unchanged
				if (new_dest_node == NID || new_dest_node == dest_node)
				{
					if (new_dest_node == NID)
						local_deq++;
					else
						remote_deq++;
					ts = do_pq_dequeue(q, &pld);
					break;
				}

				dest_node = new_dest_node;

				from_me = get_req_slot_to_node(dest_node);
				resp = get_res_slot_from_node(dest_node);

				if (!write_slot(from_me, OP_PQ_DEQ, 0, 0, 0, dest_node))
				{			
					abort_line();
				}	
				repost_deq++;
			}
			attempts = 0;
		}

		// do all ops
		count = handle_ops(q);	
		
		if (count == 0)
			attempts++;
		else
			attempts=0;

		// check response
		if (read_slot(resp, &type, &ret, &ts, &pld, &new_dest_node))
			break;
	} while(1);

	*result = pld;
	critical_exit();
	return ts;
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

void pq_reset_statistics(){
		near = 0;
		num_cas = 0;
		num_cas_useful = 0;	
}

unsigned int pq_num_malloc(){ return (unsigned int) malloc_count; }
