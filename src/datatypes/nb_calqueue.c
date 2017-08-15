/*****************************************************************************
*
*	This file is part of NBQueue, a lock-free O(1) priority queue.
*
*   Copyright (C) 2015, Romolo Marotta
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
 * nonblocking_queue.c
 *
 *  Created on: July 13, 2015
 *  Author: Romolo Marotta
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <math.h>

//#include "atomic.h"
#include "nb_calqueue.h"
#include "../utils/hpdcs_utils.h"
#include "../mm/garbagecollector.h"
//#include "core.h"


#define LOG_DEQUEUE 0
#define LOG_ENQUEUE 0

#define BOOL_CAS_ALE(addr, old, new)  CAS_x86(\
										UNION_CAST(addr, volatile unsigned long long *),\
										UNION_CAST(old,  unsigned long long),\
										UNION_CAST(new,  unsigned long long)\
									  )
									  	
#define BOOL_CAS_GCC(addr, old, new)  __sync_bool_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )

#define VAL_CAS_GCC(addr, old, new)  __sync_val_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )

#define VAL_CAS  VAL_CAS_GCC 
#define BOOL_CAS BOOL_CAS_GCC

#define FETCH_AND_AND 				__sync_fetch_and_and
#define FETCH_AND_OR 				__sync_fetch_and_or

#define ATOMIC_INC					atomic_inc_x86
#define ATOMIC_DEC					atomic_dec_x86

//#define ATOMIC_INC(x)					__sync_fetch_and_add( &((x)->count), 1)
//#define ATOMIC_DEC(x)					__sync_fetch_and_add( &((x)->count), -1)

#define ATOMIC_READ					atomic_read
//#define ATOMIC_READ(x)					__sync_fetch_and_add( &((x)->count), 0)

#define VAL (0ULL)
#define DEL (1ULL)
#define INV (2ULL)
#define MOV (3ULL)

#define MASK_PTR 	(-4LL)
#define MASK_MRK 	(3ULL)
#define MASK_DEL 	(-3LL)

#define MAX_UINT 			  (0xffffffffU)
#define MASK_EPOCH	(0x00000000ffffffffULL)
#define MASK_CURR	(0xffffffff00000000ULL)


#define REMOVE_DEL		 0
#define REMOVE_DEL_INV	 1

#define is_marked(...) macro_dispatcher(is_marked, __VA_ARGS__)(__VA_ARGS__)
#define is_marked2(w,r) is_marked_2(w,r)
#define is_marked1(w)   is_marked_1(w)
#define is_marked_2(pointer, mask)	( (UNION_CAST(pointer, unsigned long long) & MASK_MRK) == mask )
#define is_marked_1(pointer)		(UNION_CAST(pointer, unsigned long long) & MASK_MRK)
#define get_unmarked(pointer)		(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_PTR), void *))
#define get_marked(pointer, mark)	(UNION_CAST((UNION_CAST(pointer, unsigned long long)|(mark)), void *))
#define get_mark(pointer)			(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_MRK), unsigned long long))


__thread hpdcs_gc_status malloc_status =
{
	.free_nodes_lists 			= NULL,
	.to_free_nodes 				= NULL,
	.to_free_nodes_old 			= NULL,
	.block_size 				= sizeof(nbc_bucket_node),
	.offset_next 				= offsetof(nbc_bucket_node, next),
	.to_remove_nodes_count 		= 0LL
};

__thread nbc_bucket_node *to_free_nodes = NULL;
__thread nbc_bucket_node *to_free_nodes_old = NULL;

__thread nbc_bucket_node *to_free_tables_old = NULL;
__thread nbc_bucket_node *to_free_tables_new = NULL;


__thread unsigned long long concurrent_dequeue = 0;
__thread unsigned long long performed_dequeue  = 0;
__thread unsigned long long attempt_dequeue  = 0;
__thread unsigned long long scan_list_length  = 0;
                  
                  
__thread unsigned long long concurrent_enqueue = 0;
__thread unsigned long long performed_enqueue  = 0;
__thread unsigned long long attempt_enqueue  = 0;
__thread unsigned long long flush_current_attempt	 = 0;
__thread unsigned long long flush_current_success	 = 0;
__thread unsigned long long flush_current_fail	 = 0;
__thread unsigned long long read_table_count	 = 0;

static unsigned int * volatile prune_array;
static unsigned int threads;

static nbc_bucket_node *g_tail;

/**
 * This function blocks the execution of the process.
 * Used for debug purposes.
 */
static inline void error(const char *msg, ...) {
	char buf[1024];
	va_list args;

	va_start(args, msg);
	vsnprintf(buf, 1024, msg, args);
	va_end(args);

	printf("%s", buf);
	exit(1);
}


/**
 * This function computes the index of the destination bucket in the hashtable
 *
 * @author Romolo Marotta
 *
 * @param timestamp the value to be hashed
 * @param bucket_width the depth of a bucket
 *
 * @return the linear index of a given timestamp
 */
static inline unsigned long long hash(double timestamp, double bucket_width)
{
	double tmp1;
	double tmp2;
	double res_d = (timestamp / bucket_width);
	unsigned long long res =  (unsigned long long) res_d;
	int upA = 0;
	int upB = 0;


	if(__builtin_expect(res_d > 4294967295, 0))
	{
		error("Probable Overflow when computing the index: "
				"TS=%e,"
				"BW:%e, "
				"TS/BW:%e, "
				"2^32:%e\n",
				timestamp, bucket_width, res_d,  pow(2, 32));
	}

	//tmp1 = res * bucket_width;
	//if(__builtin_expect(LESS(timestamp, tmp1), 0))
	//	//return --res;
	//	upA = -1;
	//tmp2 = tmp1 + bucket_width;
	//if(__builtin_expect(GEQ(timestamp, tmp2), 0))
	//	//return ++res;
	//	upB = +1;

	tmp1 = res * bucket_width;
	tmp2 = tmp1 + bucket_width;
	
	upA = - LESS(timestamp, tmp1);
	upB = GEQ(timestamp, tmp2 );
		
	return res+ upA + upB;

}

static inline bool is_marked_for_search(void *pointer, unsigned int research_flag)
{
	unsigned long long mask_value = (UNION_CAST(pointer, unsigned long long) & MASK_MRK);
	
	return 
		(/*research_flag == REMOVE_DEL &&*/ mask_value == DEL) 
		|| (research_flag == REMOVE_DEL_INV && (mask_value == INV) );
}


/**
 *  This function is an helper to allocate a node and filling its fields.
 *
 *  @author Romolo Marotta
 *
 *  @param payload is a pointer to the referred payload by the node
 *  @param timestamp the timestamp associated to the payload
 *
 *  @return the pointer to the allocated node
 *
 */
static nbc_bucket_node* node_malloc(void *payload, double timestamp, unsigned int tie_breaker)
{
	nbc_bucket_node* res;
	
	res = mm_node_malloc(&malloc_status);
	
	if (unlikely(is_marked(res) || res == NULL))
	{
		error("%lu - Not aligned Node or No memory\n", pthread_self());
		abort();
	}

	res->counter = tie_breaker;
	res->next = NULL;
	res->replica = NULL;
	res->payload = payload;
	res->epoch = 0;
	res->timestamp = timestamp;

	return res;
}

static void node_free(nbc_bucket_node *pointer)
{	
	mm_node_free(&malloc_status, pointer);
}

/**
 * This function connect to a private structure marked
 * nodes in order to free them later, during a synchronisation point
 *
 * @author Romolo Marotta
 *
 * @param queue used to associate freed nodes to a queue
 * @param start the pointer to the first node in the disconnected sequence
 * @param number of node to connect to the to_be_free queue
 * @param timestamp   the timestamp of the last disconnected node
 *
 */
static inline void connect_to_be_freed_node_list(nbc_bucket_node *start, unsigned int counter)
{
	mm_node_trash(&malloc_status, get_unmarked(start), counter);
}

static inline void connect_to_be_freed_table_list(table *h)
{
	nbc_bucket_node *tmp = node_malloc(h, INFTY, 0);
	tmp->next = to_free_tables_new;
	to_free_tables_new = tmp;
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
 * @param queue the queue that contains the bucket
 * @param head the head of the list in which we have to perform the search
 * @param timestamp the value to be found
 * @param left_node a pointer to a pointer used to return the left node
 * @param left_node_next a pointer to a pointer used to return the next field of the left node 
 *
 */   

static void search(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						nbc_bucket_node **left_node, nbc_bucket_node **right_node, int flag)
{
	nbc_bucket_node *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	double tmp_timestamp;
	tail = g_tail;
	bool marked;

	do
	{
		/// Fetch the head and its next node
		left = tmp = head;
		left_next = tmp_next = tmp->next;
		assertf(head == NULL, "PANIC %s\n", "");
		assertf(tmp_next == NULL, "PANIC1 %s\n", "");
		assertf(is_marked_for_search(left_next, flag), "PANIC2 %s\n", "");
		counter = 0;
		
		do
		{
			// Check if the node is marked
			marked = is_marked_for_search(tmp_next, flag);

			//Find the first unmarked node that is <= timestamp
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				counter = 0;
			}
			counter+=marked;
			
			// Retrieve timestamp and next field from the current node (tmp)
			
			tmp = get_unmarked(tmp_next);
			tmp_timestamp = tmp->timestamp;
			tmp_next = tmp->next;

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (	tmp != tail &&
					(
						is_marked_for_search(tmp_next, flag) ||
						LESS(tmp_timestamp, timestamp)	||  
						(
							D_EQUAL(tmp_timestamp, timestamp) &&
							(
								tie_breaker == 0 || 
								(tie_breaker != 0 && tmp->counter <= tie_breaker)
							)
						)
					)
				);

		// Set right node and copy the mark of left node
		right = get_marked(tmp,get_mark(left_next));
	
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


static nbc_bucket_node* search_next_valid(nbc_bucket_node *head, int flag)
{
	nbc_bucket_node *right, *tmp, *tmp_next, *tail;
	tail = g_tail;

	/// Fetch the head and its next node
	tmp = head;
	tmp_next = tmp->next;
	
	
	tmp = get_unmarked(tmp_next);
	tmp_next = tmp->next;
	
	while (	tmp != tail && is_marked_for_search(tmp_next, flag)  )
	{
		tmp = get_unmarked(tmp_next);
		tmp_next = tmp->next;
	}
	
	right = tmp;

	return right;
}


/**
 * This function commits a value in the current field of a queue. It retries until the timestamp
 * associated with current is strictly less than the value that has to be committed
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 * @param left_node the candidate node for being next current
 *
 */
static inline void nbc_flush_current(table* h, nbc_bucket_node* node)
{
	unsigned long long oldCur, oldIndex, oldEpoch;
	unsigned long long newIndex, newCur, tmpCur = -1;
	bool mark = false;	// <----------------------------------------
		
	
	// Retrieve the old index and compute the new one
	oldCur = h->current;
	oldEpoch = oldCur & MASK_EPOCH;
	oldIndex = oldCur >> 32;
	newIndex = ( unsigned long long ) hash(node->timestamp, h->bucket_width);
	newCur =  newIndex << 32;
	
//	printf("EPOCH %llu \n", 
	
	// Try to update the current if it need	
	if(
		newIndex >	oldIndex 
		|| is_marked(node->next)
		|| oldCur 	== 	(tmpCur =  VAL_CAS(
										&(h->current), 
										oldCur, 
										(newCur | (oldEpoch + 1))
									) 
						)
		)
	{
		if(tmpCur != -1)
		{	
			flush_current_attempt += 1;	
			flush_current_success += 1;
		}
		return;
	}
						 
	//At this point someone else has update the current from the begin of this function
	do
	{
		
		oldCur = tmpCur;
		oldEpoch = oldCur & MASK_EPOCH;
		oldIndex = oldCur >> 32;
		mark = false;
		
		//double rand;		
		//drand48_r(&seedT, &rand); 
		//if(rand > 1.0/16	)
		//{
		//	if(newIndex <	oldIndex 
		//		&& is_marked(node->next, VAL))
		//	return;
		//	else
		//	continue;
		//}
		flush_current_attempt += 1;	
		flush_current_fail += 1;
	}
	while (
		newIndex <	oldIndex 
		&& is_marked(node->next, VAL)
		&& (mark = true)
		&& oldCur 	!= 	(tmpCur = 	VAL_CAS(
										&(h->current), 
										oldCur, 
										(newCur | (oldEpoch + 1))
									) 
						)
		);
		if(mark)
		{
			flush_current_success += 1;
			flush_current_attempt += 1;	
		}
}

/**
 * This function insert a new event in the nonblocking queue.
 * The cost of this operation when succeeds should be O(1) as calendar queue
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 * @param timestamp the timestamp of the event
 * @param payload the event to be enqueued
 *
 */
static bool insert_std(table* hashtable, nbc_bucket_node** new_node, int flag)
{
	nbc_bucket_node *left_node, *right_node, *bucket, *new_node_pointer;
	//nbc_bucket_node *left_node_next;
	unsigned int index;

	unsigned int new_node_counter 	;
	//unsigned int skipped_nodes 	;
	double 		 new_node_timestamp ;

	new_node_pointer 	= (*new_node);
	new_node_counter 	= new_node_pointer->counter;
	new_node_timestamp 	= new_node_pointer->timestamp;

	index = hash(new_node_timestamp, hashtable->bucket_width) % hashtable->size;

	// node to be added in the hashtable
	bucket = hashtable->array + index;

	 
	search(bucket, new_node_timestamp, new_node_counter, &left_node, &right_node, flag);
	//search_for_insert(bucket, new_node_timestamp, new_node_counter, &left_node, &left_node_next, &right_node, flag, &skipped_nodes);

	if(!is_marked(right_node, MOV))
	{
		switch(flag)
		{
		case REMOVE_DEL_INV:

			new_node_pointer->next = right_node;
			// set tie_breaker
			new_node_pointer->counter = 1 + ( -D_EQUAL(new_node_timestamp, left_node->timestamp ) & left_node->counter );


			if (BOOL_CAS
						(
							&(left_node->next),
							right_node,
							new_node_pointer
						)
			)
 			{
				#if LOG_ENQUEUE == 1
 				LOG("ENQUEUE: %f %u - %u %u\n", new_node_pointer->timestamp, new_node_pointer->counter,	hash(new_node_timestamp, hashtable->bucket_width), index );
 				#endif
 				return true;
 			}

			// reset tie breaker for the new search
			new_node_pointer->counter = 0;
			break;

		case REMOVE_DEL:

			// mark the to-be.inserted node as INV
			new_node_pointer->next = get_marked(right_node, INV);
			// node already exists
			if(D_EQUAL(new_node_timestamp, left_node->timestamp ) && left_node->counter == new_node_counter)
			{
				node_free(new_node_pointer);
				*new_node = left_node;
				return true;
			}
			// copy left node mark			
			new_node_pointer = get_marked(new_node_pointer,get_mark(right_node));

			if (BOOL_CAS(
						&(left_node->next),
						right_node,
						new_node_pointer
					)
			)
				return true;
			break;
		}
	}
	return false;

}

static void set_new_table(table* h, unsigned int threshold, double pub, unsigned int epb, unsigned int counter)
{
	nbc_bucket_node *tail = g_tail;
	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int thp2;//, size_thp2;
	unsigned int new_size = 0;
	unsigned int res = 0;
	double pub_per_epb = pub*epb;
	table *new_h;
	nbc_bucket_node *array;
	
	thp2 = threshold *2;
	//size_thp2 = (unsigned int) ((thp2) / ( pub * epb ));
	//if(thp2 > size_thp2) thp2 = size_thp2;

//	if 		(size >= thp2/pub_per_epb && counter > 2   * (pub_per_epb*size))
//		new_size = 2   * size;
//	else if (size >  thp2/pub_per_epb && counter < 0.5 * (pub_per_epb*size))
//		new_size = 0.5 * size;
//	else if	(size == 1    && counter > thp2)
//		new_size = thp2/pub_per_epb;
//	else if (size == thp2/pub_per_epb && counter < threshold/pub_per_epb)
//		new_size = 1;

	//if 		(size >= thp2 && counter > 2*size)
	//	new_size = 2*size;
	//else if (size > thp2 && counter < 0.5*size)
	//	new_size =  0.5*size;
	//else if	(size == 1 && counter > thp2)
	//	new_size = thp2;
	//else if (size == thp2 && counter < threshold)
	//	new_size = 1;
	
	
	
	//if 		(size >= thp2 && counter > 2   * (pub_per_epb*size))
	//	new_size = 2   * size;
	//else if (size >  thp2 && counter < 0.5 * (pub_per_epb*size))
	//	new_size = 0.5 * size;
	//else if	(size == 1    && counter > thp2)
	//	new_size = thp2;
	//else if (size == thp2 && counter < threshold)
	//	new_size = 1;
	
	
	new_size += (-(size >= thp2 && counter > 2   * pub_per_epb * (size)) )	&(size <<1 );
	new_size += (-(size >  thp2 && counter < 0.5 * pub_per_epb * (size)) )	&(size >>1);
	new_size += (-(size == 1    && counter > thp2) 					   )	& thp2;
	new_size += (-(size == thp2 && counter < threshold)				   )	& 1;
	
	if(new_size != 0 && new_size <= MAXIMUM_SIZE)
	{
		//new_h = malloc(sizeof(table));
		res = posix_memalign((void**)&new_h, CACHE_LINE_SIZE, sizeof(table));
		if(res != 0)
			error("No enough memory to new table structure\n");

		new_h->bucket_width  = -1.0;
		new_h->size 		 = new_size;
		new_h->new_table 	 = NULL;
		new_h->d_counter.count = 0;
		new_h->e_counter.count = 0;
		new_h->current 		 = ((unsigned long long)-1) << 32;

		//array =  calloc(new_size, sizeof(nbc_bucket_node));
		array =  alloc_array_nodes(&malloc_status, new_size);
		if(array == NULL)
		{
			free(new_h);
			error("No enough memory to allocate new table array %u\n", new_size);
		}

		for (i = 0; i < new_size; i++)
		{
			array[i].next = tail;
			array[i].counter = 0;
			array[i].epoch = 0;
		}
		new_h->array = array;

		if(!BOOL_CAS(&(h->new_table), NULL,	new_h))
		{
			free_array_nodes(&malloc_status, new_h->array);
			free(new_h);
		}
		else
			LOG("%u - CHANGE SIZE from %u to %u, items %u OLD_TABLE:%p NEW_TABLE:%p\n", TID, size, new_size, counter, h, new_h);
	}
}

static void block_table(table* h)
{
	unsigned int i=0;
	unsigned int size = h->size;
	nbc_bucket_node *array = h->array;
	nbc_bucket_node *bucket, *bucket_next;
	nbc_bucket_node *left_node, *right_node; 
	nbc_bucket_node *right_node_next, *left_node_next;
	nbc_bucket_node *tail = g_tail;
	double rand = 0.0;			// <----------------------------------------
	unsigned int start = 0;		// <----------------------------------------
	
	drand48_r(&seedT, &rand); // <----------------------------------------
	start = (unsigned int) rand * size;	// <----------------------------
	for(i = 0; i < size; i++)
	{
		bucket = array + ((i + start) % size);	// <---------------------
		//bucket = array + ((i + (TID)) % size);
		//Try to ark the head as MOV
		do
		{
			bucket_next = bucket->next;
		}
		while( !is_marked(bucket_next, MOV) &&
				!BOOL_CAS(&(bucket->next), bucket_next,	get_marked(bucket_next, MOV)) 
		);
		//Try to mark the first VALid node as MOV
		do
		{
			search(bucket, -1.0, 0, &left_node, &left_node_next, REMOVE_DEL_INV);
			right_node = get_unmarked(left_node_next);
			right_node_next = right_node->next;	
		}
		while(
				right_node != tail &&
				(
					is_marked(right_node_next, DEL) ||
					(
						is_marked(right_node_next, VAL) 
						&& !BOOL_CAS(&(right_node->next), right_node_next, get_marked(right_node_next, MOV))
					)
				)
		);
	}
}

static double compute_mean_separation_time(table* h,
		unsigned int new_size, unsigned int threashold, unsigned int elem_per_bucket)
{
	nbc_bucket_node *tail = g_tail;

	unsigned int i = 0, index;

	table *new_h = h->new_table;
	nbc_bucket_node *array = h->array;
	double old_bw = h->bucket_width;
	unsigned int size = h->size;
	double new_bw = new_h->bucket_width;
	
	unsigned int sample_size;
	double average = 0.0;
	double newaverage = 0.0;
	double tmp_timestamp;
	unsigned int counter = 0;
	
	double min_next_round = INFTY;
	double lower_bound, upper_bound;
    
    nbc_bucket_node *tmp, *tmp_next;
	
	index = (unsigned int)(h->current >> 32);
	
	if(new_bw >= 0)
		return new_bw;
	
	if(new_size <= threashold*2)
		return 1.0;
	
	sample_size = (new_size <= 5) ? (new_size/2) : (5 + (unsigned int)((double)new_size / 10));
	sample_size = (sample_size > SAMPLE_SIZE) ? SAMPLE_SIZE : sample_size;
    
	double sample_array[SAMPLE_SIZE+1]; //<--DA SISTEMARE STANDARD C90
    
    //read nodes until the total samples is reached or until someone else do it
	while(counter != sample_size && new_h->bucket_width == -1.0)
	{   
		for (i = 0; i < size; i++)
		{
			tmp = array + (index + i) % size; 	//get the head of the bucket
			tmp = get_unmarked(tmp->next);		//pointer to first node
			
			lower_bound = (index + i) * old_bw;
			upper_bound = (index + i + 1) * old_bw;
		
			while( tmp != tail && counter < sample_size )
			{
				tmp_timestamp = tmp->timestamp;
				tmp_next = tmp->next;
				//I will consider ognly valid nodes (VAL or MOV) In realtà se becco nodi MOV posso uscire!
				if(!is_marked(tmp_next, DEL) && !is_marked(tmp_next, INV))
				{
					if( //belong to the current bucket
						LESS(tmp_timestamp, upper_bound) &&	GEQ(tmp_timestamp, lower_bound) &&
						!D_EQUAL(tmp_timestamp, sample_array[counter])
					)
					{
						sample_array[++counter] = tmp_timestamp;
					}
					else if(GEQ(tmp_timestamp, upper_bound) && LESS(tmp_timestamp, min_next_round))
					{
							min_next_round = tmp_timestamp;
							break;
					}
				}
				tmp = get_unmarked(tmp_next);
			}
		}
		//if the calendar has no more elements I will go out
		if(min_next_round == INFTY)
			break;
		//otherwise I will restart from the next good bucket
		index = hash(min_next_round, old_bw);
		min_next_round = INFTY;
	}


	
	if( counter < sample_size)
		sample_size = counter;
    
	for(i = 2; i<=sample_size;i++)
		average += sample_array[i] - sample_array[i - 1];
    
		// Get the average
	average = average / (double)(sample_size - 1);
    
	int j=0;
	// Recalculate ignoring large separations
	for (i = 2; i <= sample_size; i++) {
		if ((sample_array[i] - sample_array[i - 1]) < (average * 2.0))
		{
			newaverage += (sample_array[i] - sample_array[i - 1]);
			j++;
		}
	}
    
	// Compute new width
	newaverage = (newaverage / j) * elem_per_bucket;	/* this is the new width */
	if(newaverage <= 0.0)
		newaverage = 1.0;
	//if(newaverage <  pow(10,-4))
	//	newaverage = pow(10,-3);
	if(isnan(newaverage))
		newaverage = 1.0;	
    
	return newaverage;
}

static void migrate_node(nbc_bucket_node *right_node,	table *new_h)
{
	//nbc_bucket_node * volatile replica;
	//nbc_bucket_node volatile **replica_new;
	nbc_bucket_node *replica;
	nbc_bucket_node *replica_new;
	nbc_bucket_node *right_replica_field, *right_node_next;
	int res = 0;
	
	
	//Create a new node inserted in the new table as as INValid
	replica = node_malloc(right_node->payload, right_node->timestamp,  right_node->counter);
	replica_new = replica;
	         
    do
	{ 
		right_replica_field = right_node->replica;
	}        
	while(right_replica_field == NULL && (res = insert_std(new_h, &replica, REMOVE_DEL)) == 0);
			
		
	
	if( right_replica_field == NULL && 
			BOOL_CAS(
				&(right_node->replica),
				NULL,
				replica
				)
		)
	{
		ATOMIC_INC(&(new_h->e_counter));
//		__sync_fetch_and_add(&(new_h->e_counter.count), 1);
    }
             
	right_replica_field = right_node->replica;
            
	do
	{
		right_node_next = right_replica_field->next;
	}while( 
		is_marked(right_node_next, INV) && 
		!BOOL_CAS(	&(right_replica_field->next),
					right_node_next,
					get_unmarked(right_node_next)
				)
		);

	nbc_flush_current(new_h, right_replica_field);
	
	
	right_node_next = FETCH_AND_AND(&(right_node->next), MASK_DEL);
	//right_node_next = __sync_and_and_fetch(&(right_node->next), MASK_DEL);
	//LOG("VALUE after fetch&and %p\n", right_node_next);
	
	//do{
	//	right_node_next = right_node->next;
	//}while( 
	//	!is_marked(right_node_next, DEL) && 
	//	!BOOL_CAS(	&(right_node->next),
	//				right_node_next,
	//				get_marked(get_unmarked(right_node_next), DEL)
	//			) 
	//	) ;
	
}

static table* read_table(nb_calqueue *queue)
{
	table *h = queue->hashtable		;
#if ENABLE_EXPANSION == 0
	return h;
#endif
	nbc_bucket_node *tail = g_tail	;
	unsigned int i, size = h->size	;

	table 			*new_h 			;
	double 			 new_bw 		;
	double 			 newaverage		;
	double 			 pub_per_epb	;
	nbc_bucket_node *bucket, *array	;
	int a,b,signed_counter;
	unsigned int counter;
	nbc_bucket_node *right_node, *left_node, *right_node_next, *node;
	unsigned long long  prova;
	//double avg_diff;
	//int *prova2;
	int samples[2];
	int sample_a;
	int sample_b;
	
	if(read_table_count++ %2 == 0)
	{
		//read_table_count = 0;
		//__sync_synchronize();
		//__asm__ __volatile__ ("" : : : "memory");

		//avg_diff= 0.0;
		for(int i=0;i<2;i++)
		{
			b = ATOMIC_READ( &h->d_counter );
			a = ATOMIC_READ( &h->e_counter );
			samples[i] = a-b;
		}
		
		//counter = signed_counter = 0;
		//for(int i=0;i<2;i++)
		//	avg_diff += abs(samples[i+1] - samples[i]);
		//avg_diff/=2;
		//
		//for(int i=0;i<2;i++)
		//{
		////	printf("%u - PROVA %d %d %d %p\n", TID, a,b, samples[i], h);
		//	if(samples[i+1] - samples[i] <= avg_diff)
		//	{	counter++;
		//		signed_counter += samples[i];
		//	}
		//}
		//signed_counter/=counter;
		
		sample_a = abs(samples[0] - size*queue->pub_per_epb);
		sample_b = abs(samples[1] - size*queue->pub_per_epb);
		
		signed_counter =  (sample_a < sample_b) ? samples[0] : samples[1];
		
		//if(signed_counter < 32000)
		//	printf("%u - PROVA %d %d %d %p\n", TID, a,b, signed_counter, h);
		
		//if(signed_counter < 0  )
		//{
		//	printf("%u - PROVA %d %d %d %p\n", TID, a,b, signed_counter, h);
		//	goto recheck;
		//}
		counter = (unsigned int) ( (-(signed_counter >= 0)) & signed_counter);
		
		//printf("SIZE H %d\n", h->counter.count);
		
		//pub_per_epb = queue->pub_per_epb;
		if( 
			//(	
			//	counter < pub_per_epb * 0.5 * size ||
			//	counter > pub_per_epb * 2   * size
			//)
			////(counter < 0.5*size || counter > 2*size)
			//& 
			(h->new_table == NULL)
			)
			set_new_table(h, queue->threshold, queue->perc_used_bucket, queue->elem_per_bucket, counter);
	}
	
	if(h->new_table != NULL)
	{
		//printf("%u - MOVING BUCKETS\n", tid);
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;

		if(new_bw < 0)
		{
			block_table(h);
			newaverage = compute_mean_separation_time(h, new_h->size, queue->threshold, queue->elem_per_bucket);
			if
			(
				BOOL_CAS(
						UNION_CAST(&(new_h->bucket_width), unsigned long long *),
						UNION_CAST(new_bw,unsigned long long),
						UNION_CAST(newaverage, unsigned long long)
					)
			)
				LOG("COMPUTE BW -  OLD:%.20f NEW:%.20f %u\n", new_bw, newaverage, new_h->size);
		}

		//First try: try to migrate the nodes, if a marked node is found, continue to the next bucket
		double rand;			// <----------------------------------------
		unsigned int start;		// <----------------------------------------
		
		drand48_r(&seedT, &rand); // <----------------------------------------
		start = (unsigned int) rand * size;	// <----------------------------
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// <---------------------
		//for(i=0; i < size; i++)
		//{
			//bucket = array + ((i + (TID)) % size);
			node = get_unmarked(bucket->next);		//node = left_node??
			do
			{
				if(node == tail)
					break;
				//Try to mark the top node
				search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
				//right_node = search_next_valid(node, REMOVE_DEL_INV);
			
				right_node = get_unmarked(right_node);
				right_node_next = right_node->next;
        
				if( right_node == tail ||
					is_marked(right_node_next) || 
						!BOOL_CAS(
								&(right_node->next),
								right_node_next,
								get_marked(right_node_next, MOV)
							)								
				)
					break;
				
				migrate_node(node, new_h);
				node = right_node;
				
			}while(true);
		}
	
		//Second try: try to migrate the nodes and continue until each bucket is empty
		drand48_r(&seedT, &rand); // <----------------------------------------
		start = (unsigned int) rand + size;	// <----------------------------
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// <---------------------
		//for(i=0; i < size; i++)
		//{
		//	bucket = array + ((i + (TID)) % size);
			node = get_unmarked(bucket->next);		//node = left_node??
			do
			{
				if(node == tail)
					break;
				//Try to mark the top node
					
				do
				{
					search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
					//right_node = search_next_valid(node, REMOVE_DEL_INV);
					right_node = get_unmarked(right_node);
					right_node_next = right_node->next;
				}
				while(
						right_node != tail &&
						(
							is_marked(right_node_next, DEL) ||
							(
								is_marked(right_node_next, VAL) 
								&& !BOOL_CAS(&(right_node->next), right_node_next, get_marked(right_node_next, MOV))
							)
						)
				);
			
				if(is_marked(node->next, MOV))
				{
					migrate_node(node, new_h);
				}
				node = right_node;
				
			}while(true);
	
			search(bucket, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
			assertf(get_unmarked(right_node) != tail, "Fail in line 972 %p %p %p %p %p %p\n",
			 bucket,
			  left_node,
			   right_node, 
			   ((nbc_bucket_node*)get_unmarked(right_node))->next, 
			   ((nbc_bucket_node*)get_unmarked(right_node))->replica, 
			 //  ((nbc_bucket_node*)get_unmarked(right_node))->replica->next, 
			   tail); 
	
		}

		//Try to replace the old table with the new one
		if( BOOL_CAS(&(queue->hashtable), h, new_h) )
		{
			connect_to_be_freed_table_list(h);
		}

		h = new_h;
	}
	return h;
}

/**
 * This function create an instance of a non-blocking calendar queue.
 *
 * @author Romolo Marotta
 *
 * @param queue_size is the inital size of the new queue
 *
 * @return a pointer a new queue
 */
nb_calqueue* nb_calqueue_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{
	unsigned int i = 0;
	//unsigned int new_threshold = 1;
	unsigned int res_mem_posix = 0;

	threads = threshold;
	prune_array = calloc(threshold*threshold, sizeof(unsigned int));

	nb_calqueue* res = calloc(1, sizeof(nb_calqueue));
	if(res == NULL)
		error("No enough memory to allocate queue\n");

	//while(new_threshold <= threshold)
	//	new_threshold <<=1;

	res->threshold = threshold;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;

	//res->hashtable = malloc(sizeof(table));
	res_mem_posix = posix_memalign((void**)&res->hashtable, CACHE_LINE_SIZE, sizeof(table));
	if(res_mem_posix != 0)
	{
		free(res);
		error("No enough memory to allocate queue\n");
	}
	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;

	//res->hashtable->array = calloc(MINIMUM_SIZE, sizeof(nbc_bucket_node) );
	
	res->hashtable->array =  alloc_array_nodes(&malloc_status, MINIMUM_SIZE);
	
	if(res->hashtable->array == NULL)
	{
		error("No enough memory to allocate queue\n");
		free(res->hashtable);
		free(res);
	}

	g_tail = node_malloc(NULL, INFTY, 0);
	g_tail->next = NULL;

	for (i = 0; i < MINIMUM_SIZE; i++)
	{
		res->hashtable->array[i].next = g_tail;
		res->hashtable->array[i].timestamp = i * 1.0;
		res->hashtable->array[i].counter = 0;
	}

	res->hashtable->current = 0;

	return res;
}

/**
 * This function implements the enqueue interface of the non-blocking queue.
 * Should cost O(1) when succeeds
 *
 * @author Romolo Marotta
 *
 * @param queue
 * @param timestamp the key associated with the value
 * @param payload the event to be enqueued
 *
 * @return true if the event is inserted in the hashtable, else false
 */
void nbc_enqueue(nb_calqueue* queue, double timestamp, void* payload)
{
	nbc_bucket_node *new_node = node_malloc(payload, timestamp, 0);
	table * h = NULL;		
	unsigned int conc_enqueue;
	unsigned int conc_enqueue_2;
	//table *old_h = NULL;	

	//do
	{
		//if(old_h != (h = read_table(queue)))
		//{
		//	old_h = h;
		//	new_node->epoch = (h->current & MASK_EPOCH);
		//}
		h = read_table(queue);
		new_node->epoch = (h->current & MASK_EPOCH);	
		conc_enqueue =  ATOMIC_READ(&h->e_counter);	
		attempt_enqueue++;
		
	} while(!insert_std(h, &new_node, REMOVE_DEL_INV)) //;
	{
		h = read_table(queue);
		new_node->epoch = (h->current & MASK_EPOCH);
		conc_enqueue_2 = ATOMIC_READ(&h->e_counter);
		concurrent_enqueue += conc_enqueue_2 - conc_enqueue;
		conc_enqueue = conc_enqueue_2;		
		attempt_enqueue++;
	}

	nbc_flush_current(h, new_node);
	
				
	conc_enqueue_2 = ATOMIC_READ(&h->e_counter);
	performed_enqueue++;		
	concurrent_enqueue += conc_enqueue_2 - conc_enqueue;
		
	ATOMIC_INC(&(h->e_counter));
	
}

static inline bool CAS_for_mark( nbc_bucket_node* right_node, nbc_bucket_node* right_node_next)
{
	return BOOL_CAS(
			&(right_node->next),
			get_unmarked(right_node_next),
			get_marked(right_node_next, DEL)
			);

}

static inline bool CAS_for_increase_cur(table* h, unsigned long long current, unsigned long long newCur)
{
	return BOOL_CAS(
			&(h->current),
			current,
			newCur				);
}

/**
 * This function dequeue from the nonblocking queue. The cost of this operation when succeeds should be O(1)
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 *
 * @return a pointer to a node that contains the dequeued value
 *
 */
double nbc_dequeue(nb_calqueue *queue, void** result)
{
	nbc_bucket_node *min, *min_next, 
					*left_node, *left_node_next, 
					*res, *tail, *array;
	table * h;
	
	unsigned long long current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned int size;
	unsigned int counter;
	unsigned int conc_dequeue;
	double bucket_width, left_ts;

	tail = g_tail;
	
	do
	{
		
		counter = 0;
		h = read_table(queue);
		current = h->current;
		size = h->size;
		array = h->array;
		bucket_width = h->bucket_width;
		conc_dequeue = ATOMIC_READ(&h->d_counter);

		index = current >> 32;
		epoch = current & MASK_EPOCH;

		assertf(
				index+1 > MASK_EPOCH, 
				"\nOVERFLOW INDEX:%llu  BW:%.10f SIZE:%u TAIL:%p TABLE:%p NUM_ELEM:%u\n",
				index, bucket_width, size, tail, h, ATOMIC_READ(&h->counter)
			);
		min = array + (index++ % size);

		left_node = min_next = min->next;
		
		if(is_marked(min_next, MOV))
			continue;
		
		while(left_node->epoch <= epoch)
		{	
			left_node_next = left_node->next;
			left_ts = left_node->timestamp;
			res = left_node->payload;
						
			if(!is_marked(left_node_next))
			{
				if(left_ts < index*bucket_width)
				{
					left_node_next = FETCH_AND_OR(&left_node->next, DEL);
					if(!is_marked(left_node_next))
					{
						concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
						scan_list_length += counter;						
						performed_dequeue++;
						attempt_dequeue++;
						#if LOG_DEQUEUE == 1
							LOG("DEQUEUE: %f %u - %llu %llu\n",left_ts, left_node->counter, index, index % size);
						#endif
						*result = res;
						ATOMIC_INC(&(h->d_counter));
						return left_ts;
					}
				}
				else
				{
					if(counter > 0 && BOOL_CAS(&(min->next), min_next, left_node))
					{
						connect_to_be_freed_node_list(min_next, counter);
					}
					if(left_node == tail && size == 1 )
					{
						#if LOG_DEQUEUE == 1
						LOG("DEQUEUE: NULL 0 - %llu %llu\n", index, index % size);
						#endif
						*result = NULL;
						concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
						scan_list_length += counter;						
						attempt_dequeue++;
						return INFTY;
					}
					//double rand;			// <----------------------------------------
					//drand48_r(&seedT, &rand); 
					//if(rand < 1.0/2)
					if(h->current == current)
						BOOL_CAS(&(h->current), current, ((index << 32) | epoch) );
					concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
					scan_list_length += counter;						
					attempt_dequeue++;
					break;	
				}
				
			}
			
			if(is_marked(left_node_next, MOV))
				break;
			
			left_node = get_unmarked(left_node_next);
			counter++;
		}

	}while(1);
	
	
	return INFTY;
}

/**
 * This function frees any node in the hashtable with a timestamp strictly less than a given threshold,
 * assuming that any thread does not hold any pointer related to any nodes
 * with timestamp lower than the threshold.
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 * @param timestamp the threshold such that any node with timestamp strictly less than it is removed and freed
 *
 */
double nbc_prune(nb_calqueue *queue)
{
#if ENABLE_PRUNE == 0
	return 0.0;
#endif
	
	nbc_bucket_node  *tmp, *tmp_next;
	unsigned int counter;
	
	
	if(!mm_safe(prune_array, threads, TID))
		return 0.0;
		
	
	while(to_free_tables_old != NULL)
	{
		nbc_bucket_node *my_tmp = to_free_tables_old;
		to_free_tables_old = to_free_tables_old->next;
    
		table *h = (table*) my_tmp->payload;
		free_array_nodes(&malloc_status, h->array); //<-------NEW
		free(h);
		node_free(my_tmp); //<-------NEW
	}
	
	do 														//<-----NEW
    {	                                                    //<-----NEW
		tmp = mm_node_collect(&malloc_status, &counter);    //<-----NEW
		while(tmp != NULL && counter-- != 0)                //<-----NEW
		{                                                   //<-----NEW
			tmp_next = tmp->next;                           //<-----NEW
			node_free(tmp);                                 //<-----NEW
			tmp =  get_unmarked(tmp_next);                  //<-----NEW
		}                                                   //<-----NEW
	}                                                       //<-----NEW
    while(tmp != NULL);										//<-----NEW

	to_free_tables_old = to_free_tables_new;
	to_free_tables_new = NULL;
	
	mm_new_era(&malloc_status, prune_array, threads, TID);
	
	return 0.0;
}

