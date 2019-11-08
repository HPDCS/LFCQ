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

#include "base.h"

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
	res->hashtable->pad = 3;

	for (i = 0; i < MINIMUM_SIZE; i++)
	{
		res->hashtable->array[i].next = res->tail;
		res->hashtable->array[i].tail = res->tail;
		res->hashtable->array[i].timestamp = (pkey_t)i;
		res->hashtable->array[i].counter = 0;
	}

	return res;
}

int single_step_pq_enqueue(table *h, pkey_t timestamp, void *payload, nbc_bucket_node* volatile * candidate, op_node *operation)
{

	nbc_bucket_node *bucket, *new_node, *ins_node;

	unsigned int index, size;
	unsigned long long newIndex = 0;

	int res, con_en = 0;

	// get actual size
	size = h->size;
	// compute virtual bucket index
	newIndex = hash(timestamp, h->bucket_width);
	// compute the index of physical bucket
	index = ((unsigned int)newIndex) % size;

	// allocate node on right NUMA NODE
	new_node = numa_node_malloc(payload, timestamp, 0, NODE_HASH(index));
	new_node->op_id = 0x1ull; // (!new) now nodes cannot be dequeued until resize
	new_node->requestor = operation->requestor; // who requested the insertion?;
	// read actual epoch
	new_node->epoch = (h->current & MASK_EPOCH);

	// get the bucket
	bucket = h->array + index;

	//read the number of executed enqueues for statistical purposes
	con_en = h->e_counter.count;

	res = ABORT;

	do
	{
		res = search_and_insert(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node, h);
		/* Can return MOV_FOUND, OK, PRESENT, ABORT */
	} while (res == ABORT);

	if (res == MOV_FOUND)
	{
		// no allocation done
		node_free(new_node);
		return -1;
	}

	//nbc_bucket_node *tmp;
	wideptr curr_state;
	wideptr new_state;

#if KEY_TYPE != DOUBLE
	if (res == PRESENT)
	{
		nbc_bucket_node* tmp = __sync_val_compare_and_swap(candidate, NULL, 1);
		if (tmp == 1 || tmp == NULL)
			return 0;
		else
			return 1;
	}
#endif

	if (res == OK)
	{
		// the CAS succeeds, thus we want to ensure that the insertion becomes visible
		// il nodo è stato inserito, possibilmente più volte da operazioni parallele - non può essere ancora rimosso
		// provo a settare il mio nodo come candidato, se non c'è già qualcosa.
		
		ins_node = __sync_val_compare_and_swap(candidate, NULL, new_node);
		if (ins_node != NULL) {

			do {
				
				curr_state.next = new_node->next;
				curr_state.op_id = new_node->op_id;

				new_state.next = get_marked(new_node->next, DEL); //((unsigned long) new_node->next) | DEL;
				new_state.op_id = 0;

			} while(__sync_val_compare_and_swap(&new_node->widenext, curr_state.widenext, new_state.widenext) != curr_state.widenext);
		}
		// leggi il candidato 
		ins_node = *candidate;
		if (((unsigned long long) ins_node) == 0x1ull)
			return 0;
			
		// prova a valdiare il nodo (se non lo è già)
		if (!__sync_bool_compare_and_swap(&(ins_node->op_id), 1, 0))
			return 1; //-1

		// must be done once
		flush_current(h, newIndex, new_node);
		performed_enqueue++;

		// updates for statistics
		concurrent_enqueue += (unsigned long long)(__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);

		return 1;
	}

	return res;
}


int single_step_pq_dequeue(table *h, nb_calqueue *queue, pkey_t* ret_ts, void **result, unsigned long op_id, nbc_bucket_node* volatile *candidate)
{

nbc_bucket_node *min, *min_next,
		*left_node, *left_node_next,
		*tail, *array,
		*current_candidate;

	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;

	unsigned long left_node_op_id;

	unsigned int size, attempts = 0;
	unsigned int counter;
	pkey_t left_ts;
	double bucket_width, left_limit, right_limit;

	unsigned int ep = 0;
	int con_de = 0;
	bool prob_overflow = false;

	wideptr lnn, new;
	int res;	
	
	tail = queue->tail;

	size = h->size;
	array = h->array;
	bucket_width = h->bucket_width;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	validate_cache(h, current);

	do
	{
		// Too many attempts: is there some problem? recheck the table
		if (h->read_table_period == attempts)
		{
			*result = NULL;
			return -1; //return error - need resize (?)
		}
		attempts++;

		// get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;

		// get the physical bucket
		min = array + (index % (size));
		left_node = min_next = min->next;
		left_node = read_last_min(left_node);

		// get the left limit
		left_limit = ((double)index) * bucket_width;

		index++;

		// get the right limit
		right_limit = ((double)index) * bucket_width;
		// check for a possible overflow
		prob_overflow = (index > MASK_EPOCH);

		// reset variables for a new scan
		counter = ep = 0;

		// a reshuffle has been detected => restart
		if (is_marked(min_next, MOV))
		{
			*result = NULL;
			return -1; //return error - resize is happening 
		}

		do
		{

			// get data from the current node
			left_node_next = left_node->next;
			left_node_op_id = left_node->op_id;
			left_ts = left_node->timestamp;

			// increase count of traversed deleted items
			counter++;

			// Skip marked nodes, invalid nodes and nodes with timestamp out of range
			if (is_marked(left_node_next, DEL) || is_marked(left_node_next, INV) || (left_ts < left_limit && left_node != tail))
				continue;

			// Abort the operation since there is a resize or a possible insert in the past
			if (is_marked(left_node_next, MOV) || left_node->epoch > epoch)
			{
				*result = NULL;
				return -1; //return error - insetrion in the past/resize
			}

			if (left_node_op_id == 1)
				continue;

			// The virtual bucket is empty
			if (left_ts >= right_limit || left_node == tail)
				break;

			// the node is a good candidate for extraction! lets try for it
			//int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));

			current_candidate = __sync_val_compare_and_swap(candidate, NULL, left_node);
			if (current_candidate == NULL)
				current_candidate = left_node;

			// someone already set the candidate
			if (current_candidate != left_node)
			{
				if (((unsigned long long) current_candidate) == 0x1ull)
				{	
					// someone returned empty
					*result = NULL;
					*ret_ts = INFTY;
					return 1;
				}
				// try to extract the candidate then 
				lnn.next = current_candidate->next;
				lnn.op_id = 0;

				new.next = get_marked(current_candidate->next, DEL);//((unsigned long) current_candidate->next) | DEL;
				new.op_id = op_id; //add our id

				// try extract the candidate - help the dequeue of other threads
				BOOL_CAS(&current_candidate->widenext, lnn.widenext, new.widenext);

				// check if someone extracted the candidate
				if (is_marked(current_candidate->next, DEL))
				{
					// was I?
					if (current_candidate->op_id == op_id)
					{
						*result = current_candidate->payload;
						*ret_ts = current_candidate->timestamp;
						return 1;
					}
					else
					{
						// no, reset the candidate and restart
						__sync_bool_compare_and_swap(candidate, current_candidate, NULL);
						*result = NULL;
						//return -1;
						/*
						 * Here the candidate is different from the minimum of the bucket
						 * This means that or the candidate has been already extracted or
						 * the left we have is earlier in the bucket (but inserted after the beginning of the extraction).
						 * When we reach here the candidate has been already extracte by someone else.
						 * So we can simply retry the extraction of left?
						 * */
						continue;
					}
				}
				//che succede se è stato marcato come mov o non è stato marcato?
			}

			// try extract left node
			do {
				lnn.next = left_node->next;
				lnn.op_id = 0;

				if (is_marked(lnn.next, DEL))
				{
					res = 0;
					break;
				}

				new.next = get_marked(left_node_next, DEL); //((unsigned long)left_node_next) | DEL;
				new.op_id = op_id; //add our id
			} while(!(res = BOOL_CAS(&left_node->widenext, lnn.widenext, new.widenext)));
			

			// the extraction is failed
			if (!res)
			{
				//read again left
				left_node_next = left_node->next;
				left_node_op_id = left_node->op_id;
			}
			
			//left_node_next = FETCH_AND_OR(&left_node->next, DEL);

			// the node cannot be extracted && is marked as MOV	=> restart
			if (is_marked(left_node_next, MOV))
			{
				// try reset the candidate
				//*candidate = NULL;
				__sync_bool_compare_and_swap(candidate, current_candidate, NULL);
				*result = NULL;
				return -1; // return error - MOV
			}

			// the node cannot be extracted && is marked as DEL
			// check who extracted it, in case skip
			if (is_marked(left_node_next, DEL)) 
			{
				if (left_node_op_id != op_id) 
				{
					// reset candidate e try again
					__sync_bool_compare_and_swap(candidate, current_candidate, NULL);
					continue;
				}
				else
				{
					// the node has been already extracted by someone with my op
					*result = left_node->payload;
					*ret_ts = left_ts;
					return 1;
				}
			}

			// we have extracted the node, so we do the update of the stats

			// use it for count the average number of traversed node per dequeue
			scan_list_length += counter;
			// use it for count the average of completed extractions
			concurrent_dequeue += (unsigned long long)(__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);
			performed_dequeue++;

			update_last_min(left_node);

			*result = left_node->payload;
			*ret_ts = left_ts;
			return 1;

		} while ((left_node = get_unmarked(left_node_next)));

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		// how to avoid a dequeue which lose update? Try set the current atomically, in case of failure someone has found a minimum
		if (left_node == tail && size == 1 && !is_marked(min->next, MOV) && *candidate == NULL)
		{
			// the queue is empty and (possibly) nobody has set the candidate
			if (__sync_bool_compare_and_swap(candidate, NULL, 1))
			{
				*result = NULL;
				*ret_ts = INFTY;
				return 1;
			}
		}

		new_current = h->current;
		if (new_current == current)
		{

			if (prob_overflow && h->e_counter.count == 0)
			{
				*result = NULL;
				return -1; //return error
			}

			assertf(prob_overflow, "\nOVERFLOW INDEX:%llu"
								   "BW:%.10f"
								   "SIZE:%u TAIL:%p TABLE:%p\n",
					index, bucket_width, size, tail, h);

			num_cas++;
			old_current = VAL_CAS(&(h->current), current, ((index << 32) | epoch));
			if (old_current == current)
			{
				current = ((index << 32) | epoch);
				num_cas_useful++;
			}
			else
				current = old_current;
		}
		else
			current = new_current;

		validate_cache(h, current);

	} while (1);

	return -1;
}

int pq_enqueue(void* q, pkey_t timestamp, void *payload) 
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");

	nb_calqueue *queue = (nb_calqueue *) q;
	table *h = NULL;
	op_node *operation, *extracted_op,
		*requested_op, *handling_op;
	
	unsigned long long vb_index;
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

	vb_index  = hash(timestamp, h->bucket_width);
	dest_node = NODE_HASH(vb_index % h->size);

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

		if (operation != NULL && !mine)
		{
			// compute vb
			vb_index  = hash(operation->timestamp, h->bucket_width);
			dest_node = NODE_HASH(vb_index);	

			// need to move to another queue?
			if (dest_node != NID) 
			{
				tq_enqueue(&enq_queue[dest_node], (void *)operation, dest_node);
				operation = NULL; // need to extract another op
			}
			// here we keep the operation if it is not null
		}

		extracted_op = operation;

		if (extracted_op == NULL)
		{
			// check if my op was done // no combining on enqueue
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
		
		handling_op = NULL;

		if (!mine) 
			operation = extracted_op;
		
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
	dest_node = NODE_HASH(vb_index % h->size);

	requested_op = operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
	requested_op->op_id = __sync_fetch_and_add(&op_counter, 1);
	requested_op->type = OP_PQ_DEQ;
	requested_op->timestamp = vb_index * (h->bucket_width);
	requested_op->payload = NULL; //DEADBEEF
	requested_op->response = -1;
	requested_op->candidate = NULL;
	requested_op->requestor = &requested_op;

	unsigned long succ = 0;
	unsigned long total = 0;
	unsigned long num = 0;
	do {

		// read table
		h = read_table(&queue->hashtable, th, epb, pub);

		if (operation != NULL && !mine)
		{
			
			vb_index = (h->current) >> 32;
			dest_node = NODE_HASH(vb_index);
			
			// need to move to another queue?
			if (dest_node != NID) 
			{

				tq_enqueue(&deq_queue[dest_node], (void *)operation, dest_node);
				operation = NULL; // need to extract another op

				if (succ != 0)
				{
					total += succ;
					num += 1;
					succ = 0;
				}
			}			
		}

		extracted_op = operation;

		// dequeue one op
		if (extracted_op == NULL)
		{
			if (!tq_dequeue(&deq_queue[NID], &extracted_op)) 
			{
				// check if my op was done - only if no op left (going combining)
				if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) != -1)
				{
					*result = requested_op->payload;
					ret_ts = requested_op->timestamp;
					gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
					critical_exit();
					requested_op = NULL;
					
					total += succ;
					num++;
					
					/*
					if (total > 1)
						LOG("Series: len %llu, num %llu \n", (unsigned long) total/num, num);
					*/
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
			succ++;
			performed_dequeue++;
			handling_op->payload = new_payload;
			handling_op->timestamp = ret_ts;
			__sync_bool_compare_and_swap(&(handling_op->response), -1, 1); /* Is this an overkill? */
			operation = NULL;
			continue;
		}
		
		handling_op = NULL;
		if (!mine) 
			operation = extracted_op;
		
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
