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

//OP macros
#define USE_TASK_QUEUE
#ifdef USE_TASK_QUEUE
#define USE_TASK_STEAL
#endif

#define NODE_HASH(bucket_id) ((bucket_id >> 2ull) % _NUMA_NODES)

/* 
 * @TODO improve op node definition
 * @TODO move update of queue from single step to wrapper
 * @TODO refactor of wrappers
 * @TODO use only active numa nodes
 *  */

/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

int gc_aid[32];
int gc_hid[4];

unsigned long op_counter = 2;
task_queue op_queue[_NUMA_NODES]; // (!new) per numa node queue

//unsigned int ACTIVE_NUMA_NODES;

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

// (!new) new fields
__thread op_node *requested_op;
__thread op_node *handling_op;

/**
 * This function commits a value in the current field of a queue. It retries until the timestamp
 * associated with current is strictly less than the value that has to be committed
 *
 * @param h: the interested set table
 * @param newIndex: index of the bucket where the new node belongs
 * @param node: pointer to the new item
 */
void flush_current(table *h, unsigned long long newIndex, nbc_bucket_node *node)
{
	unsigned long long oldCur, oldIndex, oldEpoch;
	unsigned long long newCur, tmpCur = ULONG_MAX;
	bool mark = false; // <----------------------------------------

	// Retrieve the old index and compute the new one
	oldCur = h->current;
	oldEpoch = oldCur & MASK_EPOCH;
	oldIndex = oldCur >> 32;

	newCur = newIndex << 32;

	// Try to update the current if it need
	if (
		// if the new item falls into a subsequent bucket of current we can return
		newIndex > oldIndex
		// if the new item has been marked as MOV or DEL we can complete (INV cannot be reached here)
		|| is_marked(node->next)
		// if we do not fall in the previous cases we try to update current and return if we succeed
		|| oldCur == (tmpCur = VAL_CAS(
						  &(h->current),
						  oldCur,
						  (newCur | (oldEpoch + 1)))))
	{
		// collect statistics
		if (tmpCur != ULONG_MAX)
			near++;
		return;
	}

	//At this point someone else has updated the current from the begin of this function (Failed CAS)
	do
	{
		// get old data from the previous failed CAS
		oldCur = tmpCur;
		oldEpoch = oldCur & MASK_EPOCH;
		oldIndex = oldCur >> 32;

		// keep statistics
		near += mark;
		mark = false;
	} while (
		// retry
		// if the item is in a previous bucket of current and
		newIndex < oldIndex
		// if the item is still valid and
		&& is_marked(node->next, VAL) && (mark = true)
		// if the cas has failed
		&& oldCur != (tmpCur = VAL_CAS(
						  &(h->current),
						  oldCur,
						  (newCur | (oldEpoch + 1)))));
}

/**
 * This function implements the search of a node that contains a given timestamp t. It finds two adjacent nodes,
 * left and right, such that: left.timestamp <= t and right.timestamp > t.
 *
 * Based on the code by Timothy L. Harris. For further information see:
 * Timothy L. Harris, "A Pragmatic Implementation of Non-Blocking Linked-Lists"
 * Proceedings of the 15th International Symposium on Distributed Computing, 2001
 *
 * @author Romolo Marotta
 *
 * @param head: the head of the list in which we have to perform the search
 * @param timestamp: the value to be found
 * @param tie_breaker: tie breaker of the key (if 0 means left_ts <= timestamp < right_ts, otherwise left_ts < timestamp <= right_ts
 * @param left_node: a pointer to a pointer used to return the left node
 * @param left_node_next: a pointer to a pointer used to return the next field of the left node 
 *
 */
void search(nbc_bucket_node *head, pkey_t timestamp, unsigned int tie_breaker,
			nbc_bucket_node **left_node, nbc_bucket_node **right_node, int flag)
{
	nbc_bucket_node *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int tmp_tie_breaker;
	pkey_t tmp_timestamp;
	bool marked, ts_equal, tie_lower, go_to_next;

	do
	{
		/// Fetch the head and its next node
		left = tmp = head;
		left_next = tmp_next = tmp->next;
		tail = tmp->tail;
		assertf(head == NULL, "PANIC %s\n", "");
		//assertf(flag == REMOVE_DEL_INV && is_marked_for_search(left_next, flag), "PANIC2 %s\n", "");
		counter = 0;
		marked = is_marked_for_search(tmp_next, flag);

		do
		{

			//Find the first unmarked node that is <= timestamp
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				counter = 0;
			}
			counter += marked;

			// Retrieve timestamp and next field from the current node (tmp)

			tmp = get_unmarked(tmp_next);
			tmp_next = tmp->next;
			tmp_timestamp = tmp->timestamp;
			tmp_tie_breaker = tmp->counter;

			// Check if the node is marked
			marked = is_marked_for_search(tmp_next, flag);
			// Check timestamp
			go_to_next = LESS(tmp_timestamp, timestamp);
			ts_equal = D_EQUAL(tmp_timestamp, timestamp);
			// Check tie breaker
			tie_lower = (tie_breaker == 0 ||
						 (tie_breaker != 0 && tmp_tie_breaker <= tie_breaker));
			go_to_next = go_to_next || (ts_equal && tie_lower);

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (tmp != tail &&
				 (marked ||
				  go_to_next));

		// Set right node and copy the mark of left node
		right = get_marked(tmp, get_mark(left_next));

		//left node and right node have to be adjacent. If not try with CAS
		if (!is_marked_for_search(left_next, flag) && left_next != right)
		//if (left_next != right)
		{
			// if CAS succeeds connect the removed nodes to to_be_freed_list
			if (!BOOL_CAS(&(left->next), left_next, right))
				continue;
			connect_to_be_freed_node_list(left_next, counter);
		}

		// at this point they are adjacent
		*left_node = left;
		*right_node = right;

		return;

	} while (1);
}

/**
 * This function implements the search of a node needed for inserting a new item.
 * 
 * It runs in two modes according to the value of the parameter 'flag':
 * 
 * REMOVE_DEL_INV:
 * 		With this value of 'flag', the routine searches for 2 subsequent (not necessarily adjacent)
 * 		key K1 and K2 such that: K1 <= timestamp < K2
 * 		The algorithm tries to insert the new key with a single cas on the next field of the node containing K1
 * 		If K1 == timestamp it sets the tie_breaker of the new node as T1+1, where T1 is the tie_breaker of K1
 * 		If the CAS succeeds it collects the disconnected nodes making the 3 nodes adjacent
 * 
 * REMOVE_DEL:
 * 		This value of flag is used during a resize operation. 
 * 		It searches for 2 subsequent (not necessarily adjacent)
 * 		key K1 and K2 such that: K1 <= timestamp < K2
 * 		and T1 <= tie_breaker < T2
 * 		If <K1, T1> != <timestamp, tie_breaker> 
 * 			insert the node (which was previously inserted in the previous set table> with a cas
 * 		If <K1, T1> == <timestamp, tie_breaker> 
 * 			it follows that a replica of the interested key has been already inserted, thus is exchanges the parameter node with the found one
 * 			 and frees the node passed as parameter 
 * 		
 *
 * Based on the code by Timothy L. Harris. For further information see:
 * Timothy L. Harris, "A Pragmatic Implementation of Non-Blocking Linked-Lists"
 * Proceedings of the 15th International Symposium on Distributed Computing, 2001
 *
 * @author Romolo Marotta
 *
 * @param head: the head of the list in which we have to perform the search
 * @param timestamp: the value to be found
 * @param tie_breaker: tie breaker of the key 
 * @param left_node a pointer to a pointer used to return the left node
 * @param left_node_next a pointer to a pointer used to return the next field of the left node 
 *
 */
int search_and_insert(nbc_bucket_node *head, pkey_t timestamp, unsigned int tie_breaker,
					  int flag, nbc_bucket_node *new_node_pointer, nbc_bucket_node **new_node)
{
	nbc_bucket_node *left, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int left_tie_breaker, tmp_tie_breaker;
	unsigned int len;
	pkey_t left_timestamp, tmp_timestamp;
	double rand;
	bool marked, ts_equal, tie_lower, go_to_next;
	bool is_new_key = flag == REMOVE_DEL_INV;
	drand48_r(&seedT, &rand);

	// clean the heading zone of the bucket
	nbc_bucket_node *lnode, *rnode;
	search(head, -1.0, 0, &lnode, &rnode, flag);

	// read tail from head (this is done for avoiding an additional cache miss)
	tail = head->tail;
	do
	{
		len = 0;
		/// Fetch the head and its next node
		left = tmp = head;
		// read all data from the head (hopefully only the first access is a cache miss)
		left_next = tmp_next = tmp->next;

		// since such head does not change, probably we can cache such data with a local copy
		left_tie_breaker = tmp_tie_breaker = tmp->counter;
		left_timestamp = tmp_timestamp = tmp->timestamp;

		// SANITY CHECKS
		assertf(head == NULL, "PANIC %s\n", "");
		assertf(tmp_next == NULL, "PANIC1 %s\n", "");
		assertf(is_marked_for_search(left_next, flag), "PANIC2 %s\n", "");

		// init variables useful during iterations
		counter = 0;
		marked = is_marked_for_search(tmp_next, flag);

		do
		{
			len++;
			//Find the left node compatible with value of 'flag'
			// potentially this if can be removed
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				left_tie_breaker = tmp_tie_breaker;
				left_timestamp = tmp_timestamp;
				counter = 0;
			}

			// increase the count of marked nodes met during scan
			counter += marked;

			// get an unmarked reference to the tmp node
			tmp = get_unmarked(tmp_next);

			// Retrieve timestamp and next field from the current node (tmp)
			tmp_next = tmp->next;
			tmp_timestamp = tmp->timestamp;
			tmp_tie_breaker = tmp->counter;

			// Check if the right node is marked
			marked = is_marked_for_search(tmp_next, flag);

			// Check if the right node timestamp and tie_breaker satisfies the conditions
			go_to_next = LESS(tmp_timestamp, timestamp);
			ts_equal = D_EQUAL(tmp_timestamp, timestamp);
			tie_lower = (is_new_key ||
						 (!is_new_key && tmp_tie_breaker <= tie_breaker));
			go_to_next = go_to_next || (ts_equal && tie_lower);

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (tmp != tail &&
				 (marked ||
				  go_to_next));

		// if the right or the left node is MOV signal this to the caller
		if (is_marked(tmp, MOV) || is_marked(left_next, MOV))
			return MOV_FOUND;

		// mark the to-be.inserted node as INV if flag == REMOVE_DEL
		new_node_pointer->next = get_marked(tmp, INV & (-(!is_new_key)));

		// set the tie_breaker to:
		// 1+T1 		IF K1 == timestamp AND flag == REMOVE_DEL_INV
		// 1			IF K1 != timestamp AND flag == REMOVE_DEL_INV
		// UNCHANGED 	IF flag != REMOVE_DEL_INV
		new_node_pointer->counter = (((unsigned int)(-(is_new_key))) & (1 + (((unsigned int)-D_EQUAL(timestamp, left_timestamp)) & left_tie_breaker))) +
									(~((unsigned int)(-(is_new_key))) & tie_breaker);

		// node already exists
		if (!is_new_key && D_EQUAL(timestamp, left_timestamp) && left_tie_breaker == tie_breaker)
		{
			node_free(new_node_pointer);
			*new_node = left;
			return OK;
		}

#if KEY_TYPE != DOUBLE
		if (is_new_key && D_EQUAL(timestamp, left_timestamp))
		{
			node_free(new_node_pointer);
			*new_node = left;
			return PRESENT;
		}
#endif

		// copy left node mark
		if (BOOL_CAS(&(left->next), left_next, get_marked(new_node_pointer, get_mark(left_next))))
		{
			if (is_new_key)
			{
				scan_list_length_en += len;
			}
			if (counter > 0)
				connect_to_be_freed_node_list(left_next, counter);
			return OK;
		}

		// this could be avoided
		return ABORT;

	} while (1);
}


int search_and_insert_NUMAQ(nbc_bucket_node *head, pkey_t timestamp, unsigned int tie_breaker,
					  int flag, nbc_bucket_node *new_node_pointer, nbc_bucket_node **new_node)
{
	nbc_bucket_node *left, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int left_tie_breaker, tmp_tie_breaker;
	unsigned int len;
	pkey_t left_timestamp, tmp_timestamp;
	double rand;
	bool marked, ts_equal, tie_lower, go_to_next;
	bool is_new_key = flag == REMOVE_DEL_INV;
	drand48_r(&seedT, &rand);

	// clean the heading zone of the bucket
	nbc_bucket_node *lnode, *rnode;
	search(head, -1.0, 0, &lnode, &rnode, flag);
	
	// read tail from head (this is done for avoiding an additional cache miss)
	tail = head->tail;
	do
	{
		len = 0;
		/// Fetch the head and its next node
		left = tmp = head;
		// read all data from the head (hopefully only the first access is a cache miss)
		left_next = tmp_next = tmp->next;

		// since such head does not change, probably we can cache such data with a local copy
		left_tie_breaker = tmp_tie_breaker = tmp->counter;
		left_timestamp = tmp_timestamp = tmp->timestamp;

		// SANITY CHECKS
		assertf(head == NULL, "PANIC %s\n", "");
		assertf(tmp_next == NULL, "PANIC1 %s\n", "");
		assertf(is_marked_for_search(left_next, flag), "PANIC2 %s\n", "");

		// init variables useful during iterations
		counter = 0;
		marked = is_marked_for_search(tmp_next, flag);

		do
		{
			len++;
			//Find the left node compatible with value of 'flag'
			// potentially this if can be removed
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				left_tie_breaker = tmp_tie_breaker;
				left_timestamp = tmp_timestamp;
				counter = 0;
			}

			// increase the count of marked nodes met during scan
			counter += marked;

			// get an unmarked reference to the tmp node
			tmp = get_unmarked(tmp_next);

			// Retrieve timestamp and next field from the current node (tmp)
			tmp_next = tmp->next;
			tmp_timestamp = tmp->timestamp;
			tmp_tie_breaker = tmp->counter;

			// Check if the right node is marked
			marked = is_marked_for_search(tmp_next, flag);

			// Check if the right node timestamp and tie_breaker satisfies the conditions
			go_to_next = LESS(tmp_timestamp, timestamp);
			ts_equal = D_EQUAL(tmp_timestamp, timestamp);
			tie_lower = (is_new_key ||
						 (!is_new_key && tmp_tie_breaker <= tie_breaker));
			go_to_next = go_to_next || (ts_equal && tie_lower);

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (tmp != tail &&
				 (marked ||
				  go_to_next));

		// if the right or the left node is MOV signal this to the caller
		if (is_marked(tmp, MOV) || is_marked(left_next, MOV))
			return MOV_FOUND;

		// mark the to-be.inserted node as INV if flag == REMOVE_DEL
		new_node_pointer->next = get_marked(tmp, INV & (-(!is_new_key)));

		// set the tie_breaker to:
		// 1+T1 		IF K1 == timestamp AND flag == REMOVE_DEL_INV
		// 1			IF K1 != timestamp AND flag == REMOVE_DEL_INV
		// UNCHANGED 	IF flag != REMOVE_DEL_INV
		new_node_pointer->counter = (((unsigned int)(-(is_new_key))) & (1 + (((unsigned int)-D_EQUAL(timestamp, left_timestamp)) & left_tie_breaker))) +
									(~((unsigned int)(-(is_new_key))) & tie_breaker);

		// node already exists
		if (!is_new_key && D_EQUAL(timestamp, left_timestamp) && left_tie_breaker == tie_breaker)
		{
			node_free(new_node_pointer);
			*new_node = left;
			return OK;
		}

#if KEY_TYPE != DOUBLE
		if (is_new_key && D_EQUAL(timestamp, left_timestamp))
		{
			node_free(new_node_pointer);
			*new_node = left;
			return PRESENT;
		}
#endif

		// copy left node mark
		if (BOOL_CAS(&(left->next), left_next, get_marked(new_node_pointer, get_mark(left_next))))
		{
			if (is_new_key)
			{
				scan_list_length_en += len;
			}
			if (counter > 0)
				connect_to_be_freed_node_list(left_next, counter);
			return OK;
		}

		// this could be avoided
		return ABORT;

	} while (1);
}

void set_new_table(table *h, unsigned int threshold, double pub, unsigned int epb, unsigned int counter)
{
	//double pub_per_epb = pub*epb;
	//new_size += ((unsigned int)(-(size >= thp2 && counter > 2   * pub_per_epb * (size))  ))	&(size <<1 );
	//new_size += ((unsigned int)(-(size >  thp2 && counter < 0.5 * pub_per_epb * (size))  ))	&(size >>1);
	//new_size += ((unsigned int)(-(size == 1    && counter > thp2) 					     ))	& thp2;
	//new_size += ((unsigned int)(-(size == thp2 && counter < threshold)				     ))	& 1;

	nbc_bucket_node *tail;
	table *new_h = NULL;
	double current_num_items = pub * epb * h->size;
	int res = 0;
	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int new_size = 0;
	unsigned int thp2 = 1;
	double log_size = 1.0;

	i = size;
	while (i != 0)
	{
		log_size += 1.0;
		i >>= 1;
	}
	while (thp2 < threshold * 2)
		thp2 <<= 1;

	// check if resize is needed due to num of items
	if (size >= thp2 && counter > 2 * current_num_items)
		new_size = size << 1;
	else if (size > thp2 && counter < 0.5 * current_num_items)
		new_size = size >> 1;
	else if (size == 1 && counter > thp2)
		new_size = thp2;
	else if (size == thp2 && counter < threshold)
		new_size = 1;

	// is time for periodic resize?
	if (new_size == 0 && (h->e_counter.count + h->d_counter.count) > RESIZE_PERIOD && h->resize_count / log_size < 0.75)
		new_size = h->size;
	// the num of items is doubled/halved but it is not enough for change size
	//if(new_size == 0 && h->last_resize_count != 0 && (counter >  h->last_resize_count*2 || counter < h->last_resize_count/2 ) )
	//	new_size = h->size;

	if (new_size != 0)
	{
		// allocate new table
		res = posix_memalign((void **)&new_h, CACHE_LINE_SIZE, sizeof(table));
		if (res != 0)
		{
			printf("No enough memory to new table structure\n");
			return;
		}

		res = posix_memalign((void **)&new_h->array, CACHE_LINE_SIZE, new_size * sizeof(nbc_bucket_node));
		if (res != 0)
		{
			free(new_h);
			printf("No enough memory to new table structure\n");
			return;
		}

		tail = h->array->tail;
		new_h->bucket_width = -1.0;
		new_h->size = new_size;
		new_h->new_table = NULL;
		new_h->d_counter.count = 0;
		new_h->e_counter.count = 0;
		new_h->last_resize_count = counter;
		new_h->resize_count = h->resize_count + 1;
		new_h->current = ((unsigned long long)-1) << 32;
		new_h->read_table_period = h->read_table_period;

		for (i = 0; i < new_size; i++)
		{
			new_h->array[i].next = tail;
			new_h->array[i].tail = tail;
			new_h->array[i].counter = 0;
			new_h->array[i].epoch = 0;
		}

		// try to publish the table
		if (!BOOL_CAS(&(h->new_table), NULL, new_h))
		{
			// attempt failed, thus release memory
			free(new_h->array);
			free(new_h);
		}
		else
			LOG("%u - CHANGE SIZE from %u to %u, items %u OLD_TABLE:%p NEW_TABLE:%p\n", TID, size, new_size, counter, h, new_h);
	}
}

void block_table(table *h)
{
	unsigned int i = 0;
	unsigned int size = h->size;
	nbc_bucket_node *array = h->array;
	nbc_bucket_node *bucket, *bucket_next;
	nbc_bucket_node *left_node, *right_node;
	nbc_bucket_node *right_node_next, *left_node_next;
	nbc_bucket_node *tail = array->tail;
	double rand = 0.0;
	unsigned int start = 0;

	drand48_r(&seedT, &rand);
	// start blocking table from a random physical bucket
	start = (unsigned int)rand * size;

	for (i = 0; i < size; i++)
	{
		bucket = array + ((i + start) % size);

		//Try to mark the head as MOV
		do
		{
			bucket_next = bucket->next;
		} while (!is_marked(bucket_next, MOV) &&
				 !BOOL_CAS(&(bucket->next), bucket_next, get_marked(bucket_next, MOV)));

		//Try to mark the first VALid node as MOV
		do
		{
			search(bucket, -1.0, 0, &left_node, &left_node_next, REMOVE_DEL_INV);
			right_node = get_unmarked(left_node_next);
			right_node_next = right_node->next;
		} while (
			right_node != tail &&
			(is_marked(right_node_next, DEL) ||
			 (is_marked(right_node_next, VAL) && !BOOL_CAS(&(right_node->next), right_node_next, get_marked(right_node_next, MOV)))));
	}
}

double compute_mean_separation_time(table *h,
									unsigned int new_size, unsigned int threashold, unsigned int elem_per_bucket)
{

	unsigned int i = 0, index;

	table *new_h = h->new_table;
	nbc_bucket_node *array = h->array;
	double old_bw = h->bucket_width;
	unsigned int size = h->size;
	double new_bw = new_h->bucket_width;

	nbc_bucket_node *tail = array->tail;
	unsigned int sample_size;
	double average = 0.0;
	double newaverage = 0.0;
	pkey_t tmp_timestamp;
	acc_counter = 0;
	unsigned int counter = 0;

	pkey_t min_next_round = INFTY;
	pkey_t lower_bound, upper_bound;

	nbc_bucket_node *tmp, *tmp_next;

	index = (unsigned int)(h->current >> 32);

	if (new_bw >= 0)
		return new_bw;

	//if(new_size < threashold*2)
	//	return 1.0;

	sample_size = (new_size <= 5) ? (new_size / 2) : (5 + (unsigned int)((double)new_size / 10));
	sample_size = (sample_size > SAMPLE_SIZE) ? SAMPLE_SIZE : sample_size;

	pkey_t sample_array[SAMPLE_SIZE + 1]; //<--TODO: DOES NOT FOLLOW STANDARD C90

	//read nodes until the total samples is reached or until someone else do it
	acc = 0;
	while (counter != sample_size && new_h->bucket_width == -1.0)
	{

		for (i = 0; i < size; i++)
		{
			tmp = array + (index + i) % size; //get the head of the bucket
			tmp = get_unmarked(tmp->next);	//pointer to first node

			lower_bound = (pkey_t)((index + i) * old_bw);
			upper_bound = (pkey_t)((index + i + 1) * old_bw);

			while (tmp != tail && counter < sample_size)
			{
				tmp_timestamp = tmp->timestamp;
				tmp_next = tmp->next;
				//I will consider ognly valid nodes (VAL or MOV) In realtà se becco nodi MOV posso uscire!
				if (!is_marked(tmp_next, DEL) && !is_marked(tmp_next, INV))
				{
					if ( //belong to the current bucket
						LESS(tmp_timestamp, upper_bound) && GEQ(tmp_timestamp, lower_bound) &&
						D_EQUAL(tmp_timestamp, sample_array[counter]))
					{
						acc++;
					}
					if ( //belong to the current bucket
						LESS(tmp_timestamp, upper_bound) && GEQ(tmp_timestamp, lower_bound) &&
						!D_EQUAL(tmp_timestamp, sample_array[counter]))
					{
						sample_array[++counter] = tmp_timestamp;
					}
					else if (GEQ(tmp_timestamp, upper_bound) && LESS(tmp_timestamp, min_next_round))
					{
						min_next_round = tmp_timestamp;
						break;
					}
				}
				tmp = get_unmarked(tmp_next);
			}
		}
		//if the calendar has no more elements I will go out
		if (min_next_round == INFTY)
			break;
		//otherwise I will restart from the next good bucket
		index = hash(min_next_round, old_bw);
		min_next_round = INFTY;
	}

	if (counter < sample_size)
		sample_size = counter;

	for (i = 2; i <= sample_size; i++)
		average += sample_array[i] - sample_array[i - 1];

	// Get the average
	average = average / (double)(sample_size - 1);

	int j = 0;
	// Recalculate ignoring large separations
	for (i = 2; i <= sample_size; i++)
	{
		if ((sample_array[i] - sample_array[i - 1]) < (average * 2.0))
		{
			newaverage += (sample_array[i] - sample_array[i - 1]);
			j++;
		}
	}

	// Compute new width
	newaverage = (newaverage / j) * elem_per_bucket; /* this is the new width */
	//	LOG("%d- my new bucket %.10f for %p\n", TID, newaverage, h);

	if (newaverage <= 0.0)
		newaverage = 1.0;
	//if(newaverage <  pow(10,-4))
	//	newaverage = pow(10,-3);
	if (isnan(newaverage))
		newaverage = 1.0;

	//  LOG("%d- my new bucket %.10f for %p AVG REPEAT:%u\n", TID, newaverage, h, acc/counter);
	return newaverage;
}

void migrate_node(nbc_bucket_node *right_node, table *new_h)
{
	nbc_bucket_node *replica;
	nbc_bucket_node **new_node;
	nbc_bucket_node *right_replica_field, *right_node_next;

	nbc_bucket_node *bucket, *new_node_pointer;
	unsigned int index;

	unsigned int new_node_counter;
	pkey_t new_node_timestamp;

	int res = 0;

	//Create a new node to be inserted in the new table as as INValid
	replica = numa_node_malloc(right_node->payload, right_node->timestamp, right_node->counter, NODE_HASH(hash(right_node->timestamp, new_h->bucket_width)));
	replica->op_id = right_node->op_id;

	new_node = &replica;
	new_node_pointer = (*new_node);
	new_node_counter = new_node_pointer->counter;
	new_node_timestamp = new_node_pointer->timestamp;

	index = hash(new_node_timestamp, new_h->bucket_width);

	// node to be added in the hashtable
	bucket = new_h->array + (index % new_h->size);

	do
	{
		right_replica_field = right_node->replica;
	}
	// try to insert the replica in the new table
	while (right_replica_field == NULL && (res =
											   search_and_insert(bucket, new_node_timestamp, new_node_counter, REMOVE_DEL, new_node_pointer, new_node)) == ABORT);
	// at this point we have at least one replica into the new table

	// try to decide which is the right replica and if I won the challenge increase the counter of enqueued items into the new set table
	if (right_replica_field == NULL &&
		BOOL_CAS(
			&(right_node->replica),
			NULL,
			replica))
		ATOMIC_INC(&(new_h->e_counter));

	right_replica_field = right_node->replica;

	// make the replica being valid
	do
	{
		right_node_next = right_replica_field->next;
	} while (
		is_marked(right_node_next, INV) &&
		!BOOL_CAS(&(right_replica_field->next),
				  right_node_next,
				  get_unmarked(right_node_next)));

	// now the insertion is completed so flush the current of the new table
	flush_current(new_h, index, right_replica_field);

	// invalidate the node MOV to DEL (11->01)
	right_node_next = FETCH_AND_AND(&(right_node->next), MASK_DEL);
}

table *read_table(table *volatile *curr_table_ptr, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket)
{
#if ENABLE_EXPANSION == 0
	return *curr_table_ptr;
#endif

	nbc_bucket_node *tail;
	nbc_bucket_node *bucket, *array;
	nbc_bucket_node *right_node, *left_node, *right_node_next, *node;
	table *new_h;
	table *h = *curr_table_ptr;
	double new_bw;
	double newaverage;
	double rand;
	int a, b, signed_counter;
	int samples[2];
	int sample_a;
	int sample_b;
	unsigned int counter;
	unsigned int start;
	unsigned int i, size = h->size;

	// this is used to break synchrony while invoking readtable
	read_table_count = (((unsigned int)(-(read_table_count == UINT_MAX))) & TID) +
					   (((unsigned int)(-(read_table_count != UINT_MAX))) & read_table_count);

	// after READTABLE_PERIOD iterations check if a new set table is required
	if (read_table_count++ % h->read_table_period == 0)
	{
		// make two reads of op. counters in order to reduce probability of a descheduling between each read
		for (i = 0; i < 2; i++)
		{
			b = ATOMIC_READ(&h->d_counter);
			a = ATOMIC_READ(&h->e_counter);
			samples[i] = a - b;
		}

		// compute two samples
		sample_a = abs(samples[0] - ((int)(size * perc_used_bucket)));
		sample_b = abs(samples[1] - ((int)(size * perc_used_bucket)));

		// take the minimum of the samples
		signed_counter = (sample_a < sample_b) ? samples[0] : samples[1];

		// take the maximum between the signed_counter and ZERO
		counter = (unsigned int)((-(signed_counter >= 0)) & signed_counter);

		// call the set_new_table
		if (h->new_table == NULL)
			set_new_table(h, threshold, perc_used_bucket, elem_per_bucket, counter);
	}

	// if a reshuffle is started execute the protocol
	if (h->new_table != NULL)
	{
		new_h = h->new_table;
		array = h->array;
		new_bw = new_h->bucket_width;
		tail = array->tail;

		if (new_bw < 0)
		{
			block_table(h);																		   // avoid that extraction can succeed after its completition
			newaverage = compute_mean_separation_time(h, new_h->size, threshold, elem_per_bucket); // get the new bucket width
			if																					   // commit the new bucket width
				(
					BOOL_CAS(
						UNION_CAST(&(new_h->bucket_width), unsigned long long *),
						UNION_CAST(new_bw, unsigned long long),
						UNION_CAST(newaverage, unsigned long long)))
				LOG("COMPUTE BW -  OLD:%.20f NEW:%.20f %u SAME TS:%u\n", new_bw, newaverage, new_h->size, acc_counter != 0 ? acc / acc_counter : 0);
		}

		//First speculative try: try to migrate the nodes, if a conflict occurs, continue to the next bucket
		drand48_r(&seedT, &rand);
		start = (unsigned int)rand * size; // start to migrate from a random bucket

		for (i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size); // get the head
			node = get_unmarked(bucket->next);	 // get the successor of the head (unmarked because heads are MOV)
			do
			{
				if (node == tail)
					break; // the bucket is empty

				search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV); // compact and get the successor of node
				right_node = get_unmarked(right_node);
				right_node_next = right_node->next;

				// Skip the current bucket if
				if (right_node == tail ||		  // the successor of node is a tail ??? WTF???
					is_marked(right_node_next) || // the successor of node is already MOV
					!BOOL_CAS(					  // the successor of node has been concurrently set as MOV
						&(right_node->next),
						right_node_next,
						get_marked(right_node_next, MOV)))
					break;

				migrate_node(node, new_h); // migrate node
				node = right_node;		   // go to the next node

			} while (true);
		}

		//Second conservative try: migrate the nodes and continue until each bucket is empty
		drand48_r(&seedT, &rand);

		start = (unsigned int)rand + size;
		for (i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size); // get the head
			node = get_unmarked(bucket->next);	 // get the successor of the head (unmarked because heads are MOV)
			do
			{
				if (node == tail)
					break; // the bucket is empty

				do
				{
					search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV); // compact and get the successor of node
					right_node = get_unmarked(right_node);
					right_node_next = right_node->next;
				} while (				  // repeat because
					right_node != tail && // the successor of node is not a tail
					(
						is_marked(right_node_next, DEL) || // the successor of node is DEL (we need to check the next one)
						(
							is_marked(right_node_next, VAL) // the successor of node is VAL and the cas to mark it as MOV is failed
							&& !BOOL_CAS(&(right_node->next), right_node_next, get_marked(right_node_next, MOV)))));

				if (is_marked(node->next, MOV))
					migrate_node(node, new_h); // if node is marked as MOV we can migrate it
				node = right_node;			   // proceed to the next node (which is also MOV)

			} while (true);

			search(bucket, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV); // perform a compact to remove all DEL nodes (make head and tail adjacents again)
			assertf(get_unmarked(right_node) != tail, "Fail in line 972 %p %p %p %p %p %p\n",
					bucket,
					left_node,
					right_node,
					((nbc_bucket_node *)get_unmarked(right_node))->next,
					((nbc_bucket_node *)get_unmarked(right_node))->replica,
					tail);
		}

		if (BOOL_CAS(curr_table_ptr, h, new_h))
		{ //Try to replace the old table with the new one
			// I won the challenge thus I collect memory
			node_gc_add_ptr_to_hook_list(ptst, h, gc_hid[0], NID);
			node_gc_add_ptr_to_hook_list(ptst, h->array, gc_hid[0], NID);
		}

		h = new_h;
	}

	// return the current set table
	return h;
}

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
	//ceil(a / b) = (a / b) + ((a % b) != 0);
	/*
	ACTIVE_NUMA_NODES = (((THREADS * _NUMA_NODES)) / NUM_CPUS) + ((((THREADS * _NUMA_NODES)) % NUM_CPUS) != 0); // (!new) compute the number of active numa nodes 
	ACTIVE_NUMA_NODES = ACTIVE_NUMA_NODES < _NUMA_NODES? ACTIVE_NUMA_NODES:_NUMA_NODES;
	printf("\n#######\nt %d, nn %d, cpu %d, ann%d\n########\n", THREADS, _NUMA_NODES, NUM_CPUS, ACTIVE_NUMA_NODES);
	*/
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
	res->tail = node_malloc(NULL, INFTY, 0);
	res->tail->next = NULL;

	// (!new) initialize numa queues
	for (i = 0; i < _NUMA_NODES; i++)
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

/* (!new) single step dequeuq*/
static inline pkey_t single_step_pq_dequeue(table *h, nb_calqueue *queue, void **result, unsigned long op_id, nbc_bucket_node ** candidate)
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
	tail = queue->tail;

	size = h->size;
	array = h->array;
	bucket_width = h->bucket_width;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	do
	{
		// Too many attempts: is there some problem? recheck the table
		if (h->read_table_period == attempts)
		{
			*result = NULL;
			return -1; //return error
		}
		attempts++;

		// get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;

		// get the physical bucket
		min = array + (index % (size));
		left_node = min_next = min->next;

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
			return -1; //return error
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
				return -1; //return error
			}

			// The virtual bucket is empty
			if (left_ts >= right_limit || left_node == tail)
				break;

			// the node is a good candidate for extraction! lets try for it
			//int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));

			current_candidate = __sync_val_compare_and_swap(candidate, NULL, left_node);
			if (current_candidate == NULL)
				current_candidate = left_node;

			if (current_candidate != left_node)
			{
				//printf("Diffenrent candidate: %p, %p\n", current_candidate, left_node);
				// 	someone extracted the candidate
				if (is_marked(current_candidate->next, DEL))
				{
					if (current_candidate->op_id == op_id)
					{
						*result = current_candidate->payload;
						return current_candidate->timestamp;
					}
					else
					{
						__sync_bool_compare_and_swap(candidate, current_candidate, NULL);
						*result = NULL;
						return -1;
					}
				}
			}

			//	printf("\n%p", current_candidate);
			else if (current_candidate == 1)
			{
				// someone returned empty
				*result = NULL;
				return INFTY;
			}
			

			wideptr lnn;
			lnn.next = left_node_next;
			lnn.op_id = 0;

			wideptr new;
			new.next = ((unsigned long)left_node_next) | DEL;
			new.op_id = op_id; //add our id

			int res = __sync_bool_compare_and_swap(&left_node->widenext, lnn.widenext, new.widenext); // this will change also the candidate

			// the extraction is failed
			if (!res)
			{
				left_node_next = left_node->next;
				left_node_op_id = left_node->op_id;
			}
			/* the extraction failed, why?
			 * Someone stole my op
			 * Someone returned the node
			 * A migration is happening
			 * */

			//left_node_next = FETCH_AND_OR(&left_node->next, DEL);

			// the node cannot be extracted && is marked as MOV	=> restart
			if (is_marked(left_node_next, MOV))
			{
				// try reset the candidate
				__sync_bool_compare_and_swap(candidate, current_candidate, NULL);
				*result = NULL;
				return -1; // return error
			}

			// the node cannot be extracted && is marked as DEL
			// check who extracted it, in case skip
			if (is_marked(left_node_next, DEL) && left_node_op_id != op_id)
				continue;

			// the node has been extracted

			// use it for count the average number of traversed node per dequeue
			scan_list_length += counter;
			// use it for count the average of completed extractions
			concurrent_dequeue += (unsigned long long)(__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

			*result = left_node->payload;

			return left_ts;

		} while ((left_node = get_unmarked(left_node_next)));

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		// how to avoid a dequeue which lose update? Try set the current atomically, in case of failure someone has found a minimum
		if (left_node == tail && size == 1 && !is_marked(min->next, MOV) && *candidate == NULL)
		{
			if (__sync_bool_compare_and_swap(candidate, NULL, 1))
			{
				*result = NULL;
				return INFTY;
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

	} while (1);

	return INFTY;
}

/* (!new) new enqueue*/
static inline int single_step_pq_enqueue(table *h, pkey_t timestamp, void *payload, nbc_bucket_node ** candidate)
{
	nbc_bucket_node *bucket, *new_node;

	unsigned int index, size;
	unsigned long long newIndex = 0;

	int res, con_en = 0;

	// get actual size
	size = h->size;
	// compute virtual bucket index
	newIndex = hash(timestamp, h->bucket_width);
	
	// allocate node on right NUMA NODE
	new_node = numa_node_malloc(payload, timestamp, 0, NODE_HASH(newIndex));
	new_node->op_id = 1; // (!new) now nodes cannot be dequeued until resize
	// read actual epoch
	new_node->epoch = (h->current & MASK_EPOCH);

	// compute the index of physical bucket
	index = ((unsigned int)newIndex) % size;

	// get the bucket
	bucket = h->array + index;

	//read the number of executed enqueues for statistical purposes
	con_en = h->e_counter.count;

	res = ABORT;

	do
	{
		res = search_and_insert_NUMAQ(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node);
		/* Can return MOV_FOUND, OK, PRESENT, ABORT */
	} while (res == ABORT); // @TODO must be handled in a different way?

	if (res == MOV_FOUND)
	{
		// no allocation done
		node_free(new_node);
		return -1;
	}

#if KEY_TYPE != DOUBLE
	// se uno torna present, può accadere che il secondo thread torni un valore differente
	if (res == PRESENT)
	{
		return 0;
	}
#endif

	if (res == OK)
	{
		wideptr curr_state;
		wideptr new_state;
		nbc_bucket_node *tmp;
		// the CAS succeeds, thus we want to ensure that the insertion becomes visible
		// il nodo è stato inserito, possibilmente più volte da operazioni parallele - non può essere ancora rimosso
		// provo a settare il mio nodo come candidato, finchè non c'è qualcosa.
		tmp = __sync_val_compare_and_swap(candidate, NULL, new_node);
		if (tmp == NULL) {
			tmp = new_node;
		}
		else {
			// marco il mio nodo come DEL | 0 se non l'ho inserito.
			
			do {
				
				curr_state.next = new_node->next;
				curr_state.op_id = new_node->op_id;

				new_state.next = ( (unsigned long) new_node->next) | DEL;
				new_state.op_id = 0;

			} while(!__sync_bool_compare_and_swap(&new_node->widenext, curr_state.widenext, new_state.widenext));
		}

		// leggo il candidato e provo a sbloccarlo
		// curr_state.next = tmp->next;
		// curr_state.op_id = tmp->op_id;
		// new_state.next = tmp->next;
		// new_state.op_id = 0;

		tmp->op_id = 0; // sblocca il nodo.

		// @TODO muovere questa parte prima della return del wrapper
		new_node->op_id = 0; // il nodo può essere estratto
		flush_current(h, newIndex, new_node);
		performed_enqueue++;

		// updates for statistics
		concurrent_enqueue += (unsigned long long)(__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);

#if COMPACT_RANDOM_ENQUEUE == 1
		// clean a random bucket
		unsigned long long oldCur = h->current;
		unsigned long long oldIndex = oldCur >> 32;
		unsigned long long dist = 1;
		double rand;
		nbc_bucket_node *left_node, *right_node;
		drand48_r(&seedT, &rand);
		search(h->array + ((oldIndex + dist + (unsigned int)(((double)(size - dist)) * rand)) % size), -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
#endif

		return 1;
	}

	return res;
}

/* (!new) new enqueue*/
// @TODO Improve path in case of compilation with queues not active

int pq_enqueue(void *q, pkey_t timestamp, void *payload)
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");
	nb_calqueue *queue = (nb_calqueue *)q;
	pkey_t ret_ts, ts = timestamp;
	op_node *operation = NULL;

	int ret;
	unsigned long long vb_index;

	unsigned int node = NID, i = 0;

	unsigned int dest_node;
	void *new_payload;

	critical_enter();

	table *h = NULL;

	//table configuration
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;

	do
	{
		//read table
		h = read_table(&queue->hashtable, th, epb, pub);

		//compute vb
		vb_index = hash(ts, h->bucket_width);
		dest_node = NODE_HASH(vb_index);

		if (requested_op == NULL && operation == NULL)
		{
			//first iteration
			requested_op = operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);
			// se avviene una resize e il nodo di destinazione cambia non peggioro la situazione?

			//@TODO The structure must be reshuffled to be handled correctly
			requested_op->op_id = 1;//__sync_fetch_and_add(&op_counter, 1);
			requested_op->type = OP_PQ_ENQ;
			requested_op->timestamp = ts;
			requested_op->payload = payload; //DEADBEEF
			requested_op->response = -1;
			requested_op->candidate = NULL;
		}

#ifdef USE_TASK_QUEUE
		if (operation != NULL)
			tq_enqueue(&op_queue[dest_node], (void *)operation, dest_node);
		op_node *extracted_op = NULL;
#else
		op_node *extracted_op = requested_op;
#endif

		if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) != -1)
		{
			gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
			critical_exit();
			requested_op = NULL;
			// dovrebbe essere come se il thread fosse stato deschedulato prima della return
			return ret; // someone did my op, we can return
		}
#ifdef USE_TASK_QUEUE
#ifndef USE_TASK_STEAL
		// extract from one queue
		i = node = NID;
		do
		{
			if (tq_dequeue(&op_queue[i], &extracted_op))
			{
				break;
			}
			i = (i + 1) % _NUMA_NODES;
		} while (i != node);
		// all queues are empty
		if (extracted_op == NULL)
		{
			assertf(requested_op == NULL, "ENQ - No op available to deq%s\n", "");
			//@TODO how to handle this case?
			continue;
		}
#else
		node = NID;
		if (!tq_dequeue(&op_queue[node], &extracted_op))
		{
			extracted_op = requested_op;
			i = 1;
		}
#endif
#endif
		//extracted op, executing
		handling_op = extracted_op;
		if (handling_op->type == OP_PQ_ENQ)
		{
			ret = single_step_pq_enqueue(h, handling_op->timestamp, handling_op->payload, &handling_op->candidate);
			if (ret != -1) //enqueue succesful
			{
				__sync_bool_compare_and_swap(&(handling_op->response), -1, ret);
				operation = NULL;
				continue;
			}
		}
		else if (handling_op->type == OP_PQ_DEQ)
		{
			// table* h, nb_calqueue *queue, void** result
			ret_ts = single_step_pq_dequeue(h, queue, &new_payload, handling_op->op_id, &handling_op->candidate);
			if (ret_ts != -1) //dequeue succesful
			{
				performed_dequeue++;
				handling_op->payload = new_payload;
				handling_op->timestamp = ret_ts;
				__sync_bool_compare_and_swap(&(handling_op->response), -1, 1);
				operation = NULL;
				continue;
			}
		}

		// @TODO in case of failure update ts of vb
		//op failed

#ifdef USE_TASK_QUEUE
#ifdef USE_TASK_STEAL
		if (i == 0)
		{
#endif
			handling_op = NULL;
			operation = extracted_op;
			ts = operation->timestamp;
#ifdef USE_TASK_STEAL
		}
		i = 0;
#endif
#endif 
	} while (true);
	//critical_exit()
	//return code
}

/* !(new) dequeue */
// @TODO Improve path in case of compilation with queues not active
pkey_t pq_dequeue(void *q, void **result)
{
	nb_calqueue *queue = (nb_calqueue *)q;
	table *h = NULL;
	pkey_t ret_ts, ts = 1;
	op_node *operation = NULL;

	int ret;

	//for read table
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;

	unsigned int node = NID, i = 0;

	unsigned long long vb_index;
	unsigned int dest_node = NID;

	void *new_payload;

	critical_enter();

	do
	{
		h = read_table(&queue->hashtable, th, epb, pub);

		// first iteration are dummy
		vb_index = hash(ts, h->bucket_width);
		dest_node = NODE_HASH(vb_index);

		if (requested_op == NULL && operation == NULL)
		{
			//taken only first iteration

			// take index of minimum
			vb_index = (h->current) >> 32;
			dest_node = NODE_HASH(vb_index);

			//compute ts in order to take right node
			ts = vb_index * (h->bucket_width);

			requested_op = operation = gc_alloc_node(ptst, gc_aid[GC_OPNODE], dest_node);

			requested_op->op_id = __sync_fetch_and_add(&op_counter, 1);
			requested_op->type = OP_PQ_DEQ;
			requested_op->timestamp = ts;
			requested_op->payload = NULL;
			requested_op->candidate = NULL;
			requested_op->response = -1;
		}

#ifdef USE_TASK_QUEUE
		if (operation != NULL)
			tq_enqueue(&op_queue[dest_node], (void *)operation, dest_node);
		op_node *extracted_op = NULL;

#else
		op_node *extracted_op = operation;
#endif

		if ((ret = __sync_fetch_and_add(&(requested_op->response), 0)) != -1)
		{
			ret_ts = requested_op->timestamp;
			*result = requested_op->payload;
			gc_free(ptst, requested_op, gc_aid[GC_OPNODE]);
			critical_exit();

			requested_op = NULL;
			return ret_ts;
		}

#ifdef USE_TASK_QUEUE
#ifndef USE_TASK_STEAL
		// extract from one queue
		i = node = NID;
		do
		{
			if (tq_dequeue(&op_queue[i], &extracted_op))
				break;
			i = (i + 1) % _NUMA_NODES;
		} while (i != node);

		// all queues are empty
		if (extracted_op == NULL)
		{
			assertf(requested_op == NULL, "DEQ - No op available to deq%s\n", "");
			//@TODO how to handle this case?
			continue;
		}

#else
		/*
		 * If local queue is empty, execute directly my op.
		 * Do not enqueue it again, but execute it till success
		 * */
		node = NID;
		if (!tq_dequeue(&op_queue[node], &extracted_op))
		{
			extracted_op = requested_op;
			i = 1;
		}
#endif

#endif

		//extracted op, executing
		handling_op = extracted_op;
		if (handling_op->type == OP_PQ_ENQ)
		{
			ret = single_step_pq_enqueue(h, handling_op->timestamp, handling_op->payload, &handling_op->candidate);
			if (ret != -1) //enqueue succesful
			{
				__sync_bool_compare_and_swap(&(handling_op->response), -1, ret); /* Is this an overkill? */
				operation = NULL;
				continue;
			}
		}
		else if (handling_op->type == OP_PQ_DEQ)
		{
			/* table* h, nb_calqueue *queue, void** result */
			ret_ts = single_step_pq_dequeue(h, queue, &new_payload, handling_op->op_id, &handling_op->candidate);
			if (ret_ts != -1)
			{ //dequeue failed
				performed_dequeue++;
				handling_op->payload = new_payload;
				handling_op->timestamp = ret_ts;
				__sync_bool_compare_and_swap(&(handling_op->response), -1, 1); /* Is this an overkill? */
				operation = NULL;
				continue;
			}
		}

		// @TODO in case of failure update ts of vb if dequeue
		//op failed
#ifdef USE_TASK_QUEUE
#ifdef USE_TASK_STEAL
		if (i == 0)
		{
#endif
			handling_op = NULL;
			operation = extracted_op;
			ts = operation->timestamp;
#ifdef USE_TASK_STEAL
		}
		i = 0;
#endif
#endif
	} while (true);
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
