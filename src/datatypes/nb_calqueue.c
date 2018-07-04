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

#include "nb_calqueue.h"


/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

unsigned int threads;
unsigned int * volatile prune_array;

/*************************************
 * VARIABLES FOR GARBAGE COLLECTION  *
 *************************************/

__thread hpdcs_gc_status malloc_status =
{
	.free_nodes_lists 			= NULL,
	.free_chunk		 			= NULL,
	.to_free_nodes 				= NULL,
	.to_free_nodes_old 			= NULL,
	.block_size 				= sizeof(nbc_bucket_node),
	.to_remove_nodes_count 		= 0LL
};

__thread nbc_bucket_node *to_free_tables_old = NULL;
__thread nbc_bucket_node *to_free_tables_new = NULL;



/*************************************
 * VARIABLES FOR ENQUEUE  *
 *************************************/

// number of enqueue invokations
__thread unsigned int flush_eq = 0;

/*************************************
 * VARIABLES FOR READ TABLE  *
 *************************************/

__thread unsigned long long read_table_count	 = 0;
  
/*************************************
 * VARIABLES FOR DEQUEUE	  		 *
 *************************************/
  
__thread unsigned int local_monitor = -1;
__thread unsigned int flush_de = 0;

__thread unsigned long long last_curr = 0ULL;
__thread unsigned long long cached_curr = 0ULL;
__thread unsigned long long num_cas = 0ULL;
__thread unsigned long long num_cas_useful = 0ULL;

__thread unsigned long long near = 0;
__thread unsigned long long dist = 0;


#define MOV_FOUND 	3
#define OK			1
#define ABORT		0


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

void search(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						nbc_bucket_node **left_node, nbc_bucket_node **right_node, int flag)
{
	nbc_bucket_node *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int tmp_tie_breaker;
	double tmp_timestamp;
	bool marked, ts_equal, tie_lower, go_to_next;

	do
	{
		/// Fetch the head and its next node
		left = tmp = head;
		left_next = tmp_next = tmp->next;
		tail = tmp->tail;
		assertf(head == NULL, "PANIC %s\n", "");
		assertf(tmp_next == NULL, "PANIC1 %s\n", "");
		assertf(is_marked_for_search(left_next, flag), "PANIC2 %s\n", "");
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
			counter+=marked;
			
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
			tie_lower = 		(
								tie_breaker == 0 || 
								(tie_breaker != 0 && tmp_tie_breaker <= tie_breaker)
							);
			go_to_next =  go_to_next || (ts_equal && tie_lower);

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (	tmp != tail &&
					(
						marked ||
						go_to_next
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
 
static unsigned int search_and_insert(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						 int flag, nbc_bucket_node *new_node_pointer, nbc_bucket_node **new_node)
{
	nbc_bucket_node *left, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int left_tie_breaker, tmp_tie_breaker;
	unsigned int len;
	double left_timestamp, tmp_timestamp, rand;
	bool marked, ts_equal, tie_lower, go_to_next;
	bool is_new_key = flag == REMOVE_DEL_INV;
	drand48_r(&seedT, &rand);
	
	// clean the heading zone of the bucket
	nbc_bucket_node *lnode, *rnode;
	search(head, -1.0, 0, &lnode, &rnode, REMOVE_DEL_INV);
	
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
		left_tie_breaker = tmp_tie_breaker = tmp->timestamp;
		left_timestamp 	= tmp_timestamp    = tmp->counter;
		
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
			counter+=marked;
			
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
			tie_lower = 		(
								is_new_key || 
								(!is_new_key && tmp_tie_breaker <= tie_breaker)
							);
			go_to_next =  go_to_next || (ts_equal && tie_lower);

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (	tmp != tail &&
					(
						marked ||
						go_to_next
					)
				);

		
		// if the right or the left node is MOV signal this to the caller
		if(is_marked(tmp, MOV) || is_marked(left_next, MOV) )
			return MOV_FOUND;
		
		// mark the to-be.inserted node as INV if flag == REMOVE_DEL
		new_node_pointer->next = get_marked(tmp, INV & (-(!is_new_key)) );
		
		// set the tie_breaker to:
		// 1+T1 		IF K1 == timestamp AND flag == REMOVE_DEL_INV 
		// 1			IF K1 != timestamp AND flag == REMOVE_DEL_INV
		// UNCHANGED 	IF flag != REMOVE_DEL_INV
		new_node_pointer->counter =  ( (-(is_new_key)) & (1 + ( -D_EQUAL(timestamp, left_timestamp ) & left_tie_breaker ))) +
									 (~(-(is_new_key)) & tie_breaker);

		// node already exists
		if(!is_new_key && D_EQUAL(timestamp, left_timestamp ) && left_tie_breaker == tie_breaker)
		{
			node_free(new_node_pointer);
			*new_node = left;
			return OK;
		}
		
		// copy left node mark			
		if (BOOL_CAS(&(left->next), left_next, get_marked(new_node_pointer,get_mark(left_next))))
		{
			if(is_new_key)
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
void flush_current(table* h, unsigned long long newIndex, unsigned int size, nbc_bucket_node* node)
{
	unsigned long long oldCur, oldIndex, oldEpoch;
	unsigned long long newCur, tmpCur = -1;
	signed long long distance = 0;
	bool mark = false;	// <----------------------------------------
		
	
	// Retrieve the old index and compute the new one
	oldCur = h->current;
	oldEpoch = oldCur & MASK_EPOCH;
	oldIndex = oldCur >> 32;

	newCur =  newIndex << 32;
	distance = newIndex - oldIndex;
	dist = size*DISTANCE_FROM_CURRENT;

	if(distance > 0 && distance < dist){
		newIndex = oldIndex;
		newCur =  newIndex << 32;
		near+= distance!=0;
	}
	
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
			near++;
		return;
	}
						 
	//At this point someone else has update the current from the begin of this function
	do
	{
		
		oldCur = tmpCur;
		oldEpoch = oldCur & MASK_EPOCH;
		oldIndex = oldCur >> 32;
		mark = false;
		near++;
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
}

void set_new_table(table* h, unsigned int threshold, double pub, unsigned int epb, unsigned int counter)
{
	nbc_bucket_node *tail;
	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int thp2;//, size_thp2;
	unsigned int new_size = 0;
	unsigned int res = 0;
	double pub_per_epb = pub*epb;
	table *new_h = NULL;
	nbc_bucket_node *array;
	unsigned int thp2_tmp = 1;
	thp2 = threshold *2;
	
	while(thp2_tmp < thp2)
		thp2_tmp <<= 1;
	
	thp2 = thp2_tmp;
	
	//if 	(size >= thp2 && counter > 2   * (pub_per_epb*size))
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
	
	
	// is time for periodic resize?
	if(new_size == 0 && (h->e_counter.count + h->d_counter.count) > RESIZE_PERIOD)
		new_size = h->size;
	
	if(new_size == 0 && h->last_resize_count != 0 && (counter >  h->last_resize_count*2 || counter < h->last_resize_count/2 ) )
		new_size = h->size;

	if(new_size != 0 && new_size <= MAXIMUM_SIZE)
	{
		res = posix_memalign((void**)&new_h, CACHE_LINE_SIZE, sizeof(table));
		if(res != 0)
			error("No enough memory to new table structure\n");
		
		tail = h->array->tail;
		new_h->bucket_width  = -1.0;
		new_h->size 		 = new_size;
		new_h->new_table 	 = NULL;
		new_h->d_counter.count = 0;
		new_h->e_counter.count = 0;
		new_h->last_resize_count = counter;
		new_h->current 		 = ((unsigned long long)-1) << 32;
		new_h->read_table_period = h->read_table_period;

		//array =  alloc_array_nodes(&malloc_status, new_size);
		array =  malloc(new_size*sizeof(nbc_bucket_node));
		if(array == NULL)
		{
			free(new_h);
			error("No enough memory to allocate new table array %u\n", new_size);
		}
		

		for (i = 0; i < new_size; i++)
		{
			array[i].next = tail;
			array[i].tail = tail;
			array[i].counter = 0;
			array[i].epoch = 0;
		}
		new_h->array = array;

		if(!BOOL_CAS(&(h->new_table), NULL,	new_h))
		{
			//free_array_nodes(&malloc_status, new_h->array);
			free(new_h->array);
			free(new_h);
		}
		else
			LOG("%u - CHANGE SIZE from %u to %u, items %u OLD_TABLE:%p NEW_TABLE:%p\n", TID, size, new_size, counter, h, new_h);
	}
}

void block_table(table* h)
{
	unsigned int i=0;
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
	start = (unsigned int) rand * size;	

	for(i = 0; i < size; i++)
	{
		bucket = array + ((i + start) % size);	
		
		//Try to mark the head as MOV
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

double compute_mean_separation_time(table* h,
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
	double tmp_timestamp;
	unsigned int counter = 0;
	
	double min_next_round = INFTY;
	double lower_bound, upper_bound;
    
    nbc_bucket_node *tmp, *tmp_next;
	
	index = (unsigned int)(h->current >> 32);
	
	if(new_bw >= 0)
		return new_bw;
	
	if(new_size < threashold*2)
		return 1.0;
	
	sample_size = (new_size <= 5) ? (new_size/2) : (5 + (unsigned int)((double)new_size / 10));
	sample_size = (sample_size > SAMPLE_SIZE) ? SAMPLE_SIZE : sample_size;
    
	double sample_array[SAMPLE_SIZE+1]; //<--TODO: DOES NOT FOLLOW STANDARD C90
    
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
	//	LOG("%d- my new bucket %.10f for %p\n", TID, newaverage, h);   

	if(newaverage <= 0.0)
		newaverage = 1.0;
	//if(newaverage <  pow(10,-4))
	//	newaverage = pow(10,-3);
	if(isnan(newaverage))
		newaverage = 1.0;	
    
	//  LOG("%d- my new bucket %.10f for %p\n", TID, newaverage, h);	
	return newaverage;
}

void migrate_node(nbc_bucket_node *right_node,	table *new_h)
{
	nbc_bucket_node *replica;
	nbc_bucket_node** new_node;
	nbc_bucket_node *right_replica_field, *right_node_next;
	
	nbc_bucket_node *bucket, *new_node_pointer;
	unsigned int index;

	unsigned int new_node_counter 	;
	double 		 new_node_timestamp ;

	
	int res = 0;
	
	//Create a new node inserted in the new table as as INValid
	replica = node_malloc(right_node->payload, right_node->timestamp,  right_node->counter);
	
	new_node 			= &replica;
	new_node_pointer 	= (*new_node);
	new_node_counter 	= new_node_pointer->counter;
	new_node_timestamp 	= new_node_pointer->timestamp;
	
	index = hash(new_node_timestamp, new_h->bucket_width) % new_h->size;

	// node to be added in the hashtable
	bucket = new_h->array + index;
	         
    do
	{ 
		right_replica_field = right_node->replica;
	}        
	while(right_replica_field == NULL && (res = 
	search_and_insert(bucket, new_node_timestamp, new_node_counter, REMOVE_DEL, new_node_pointer, new_node)
	) == ABORT);
			
	
	if( right_replica_field == NULL && 
			BOOL_CAS(
				&(right_node->replica),
				NULL,
				replica
				)
		)
		ATOMIC_INC(&(new_h->e_counter));
             
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

	
	flush_current(new_h, ( unsigned long long ) hash(right_replica_field->timestamp, new_h->bucket_width), new_h->size, right_replica_field);
	
	
	right_node_next = FETCH_AND_AND(&(right_node->next), MASK_DEL);
}

table* read_table(table *volatile *curr_table_ptr, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket)
{
	table *h = *curr_table_ptr		;
#if ENABLE_EXPANSION == 0
	return h;
#endif
	nbc_bucket_node *tail;
	unsigned int i, size = h->size	;

	table 			*new_h 			;
	double 			 new_bw 		;
	double 			 newaverage		;
	//double 			 pub_per_epb	;
	nbc_bucket_node *bucket, *array	;
	int a,b,signed_counter;
	unsigned int counter;
	nbc_bucket_node *right_node, *left_node, *right_node_next, *node;
	
	int samples[2];
	int sample_a;
	int sample_b;
	
	read_table_count = ((-(read_table_count == -1)) & TID) + ((-(read_table_count != -1)) & read_table_count);
	if(read_table_count++ % h->read_table_period == 0)
	{
		for(i=0;i<2;i++)
		{
			b = ATOMIC_READ( &h->d_counter );
			a = ATOMIC_READ( &h->e_counter );
			samples[i] = a-b;
		}
		
		sample_a = abs(samples[0] - size*perc_used_bucket);
		sample_b = abs(samples[1] - size*perc_used_bucket);
		
		signed_counter =  (sample_a < sample_b) ? samples[0] : samples[1];
		
		counter = (unsigned int) ( (-(signed_counter >= 0)) & signed_counter);
		
		
		if( h->new_table == NULL )
			set_new_table(h, threshold, perc_used_bucket, elem_per_bucket, counter);
	}
	
	if(h->new_table != NULL)
	{
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;
		tail = array->tail;	

		if(new_bw < 0)
		{
			block_table(h);
			newaverage = compute_mean_separation_time(h, new_h->size, threshold, elem_per_bucket);
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
		double rand;			
		unsigned int start;		
		
		drand48_r(&seedT, &rand); 
		start = (unsigned int) rand * size;	
		
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	
			node = get_unmarked(bucket->next);		
			do
			{
				if(node == tail)
					break;
					
				//Try to mark the top node
				search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
				
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
		drand48_r(&seedT, &rand); 
		
		start = (unsigned int) rand + size;	
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// <---------------------
		
			node = get_unmarked(bucket->next);		//node = left_node??
			do
			{
				if(node == tail)
					break;
				//Try to mark the top node
					
				do
				{
					search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
				
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
			   tail); 
	
		}
		
		// TODO scan new table for find out what is the minimum bucket 

		//Try to replace the old table with the new one
		if( BOOL_CAS(curr_table_ptr, h, new_h) )
			connect_to_be_freed_table_list(h);
		
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
nb_calqueue* nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{
	unsigned int i = 0;
	//unsigned int new_threshold = 1;
	unsigned int res_mem_posix = 0;

	threads = threshold;
	prune_array = calloc(threshold*threshold, sizeof(unsigned int));

	//nb_calqueue* res = calloc(1, sizeof(nb_calqueue));
	nb_calqueue* res = NULL; 
	res_mem_posix = posix_memalign((void**)(&res), CACHE_LINE_SIZE, sizeof(nb_calqueue));
	if(res_mem_posix != 0)
	{
		error("No enough memory to allocate queue\n");
	}
	//if(res == NULL)
	//	error("No enough memory to allocate queue\n");

	//while(new_threshold <= threshold)
	//	new_threshold <<=1;

	res->threshold = threads;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->read_table_period = READTABLE_PERIOD;

	//res->hashtable = malloc(sizeof(table));
	res_mem_posix = posix_memalign((void**)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table));
	
	if(res_mem_posix != 0)
	{
		free(res);
		error("No enough memory to allocate queue\n");
	}
	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;
	res->hashtable->current = 0;
	res->hashtable->last_resize_count = 0;
	res->hashtable->e_counter.count = 0;
	res->hashtable->d_counter.count = 0;
    res->hashtable->read_table_period = res->read_table_period;	
	//res->hashtable->array =  alloc_array_nodes(&malloc_status, MINIMUM_SIZE);
	res->hashtable->array =  malloc(MINIMUM_SIZE*sizeof(nbc_bucket_node));
	
	if(res->hashtable->array == NULL)
	{
		error("No enough memory to allocate queue\n");
		free(res->hashtable);
		free(res);
	}

	gc_id[0] = gc_add_allocator(sizeof(nbc_bucket_node));
	critical_enter();
	critical_exit();
	
	res->tail = node_malloc(NULL, INFTY, 0);
	res->tail->next = NULL;

	for (i = 0; i < MINIMUM_SIZE; i++)
	{
		res->hashtable->array[i].next = res->tail;
		res->hashtable->array[i].tail = res->tail;
		res->hashtable->array[i].timestamp = i * 1.0;
		res->hashtable->array[i].counter = 0;
	}

	return res;
}


/**
 * This function implements the enqueue interface of the non-blocking calendar queue.
 * Cost O(1) when succeeds
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
	
	critical_enter();
	nbc_bucket_node *bucket, *new_node = node_malloc(payload, timestamp, 0);
	table * h = NULL;		
	unsigned int index, res, size;
	unsigned long long newIndex = 0;
	
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	unsigned int con_en = 0;
	

	//init the result
	res = MOV_FOUND;
	
	//repeat until a successful insert
	while(res != OK){
		
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){
			// check for a resize
			h = read_table(&queue->hashtable, th, epb, pub);
			// get actual size
			size = h->size;
	        // read the actual epoch
        	new_node->epoch = (h->current & MASK_EPOCH);
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			// compute the index of the physical bucket
			index = newIndex % size;
			// get the bucket
			bucket = h->array + index;
			// read the number of executed enqueues for statistics purposes
			con_en = h->e_counter.count;
		}
		// search the two adjacent nodes that surround the new key and try to insert with a CAS 
	    res = search_and_insert(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node);
	}

	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
	flush_current(h, newIndex, size, new_node);
	
	// updates for statistics
	
	concurrent_enqueue += __sync_fetch_and_add(&h->e_counter.count, 1) - con_en;
	performed_enqueue++;
	
	#if COMPACT_RANDOM_ENQUEUE == 1
	// clean a random bucket
	unsigned long long oldCur = h->current;
	unsigned long long oldIndex = oldCur >> 32;
	dist = size*DISTANCE_FROM_CURRENT+1;
	double rand;
	nbc_bucket_node *left_node, *right_node;
	drand48_r(&seedT, &rand);
	search(h->array+((oldIndex + dist + (unsigned int)((size-dist)*rand))%size), -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
	#endif
	
	critical_exit();

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
					*tail, *array;
	table * h = NULL;
	
	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned int size;
	unsigned int counter;
	double bucket_width, left_ts, right_limit;
	
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	unsigned int ep = 0;
	unsigned int con_de = 0;
	
	tail = queue->tail;
	performed_dequeue++;
	
	critical_enter();

begin:
	h = read_table(&queue->hashtable, th, epb, pub);
	size = h->size;
	array = h->array;
	bucket_width = h->bucket_width;
	current = h->current;
	con_de = h->d_counter.count;
	if(last_curr != current){
		cached_curr = last_curr = current;
	}
	else
		current = cached_curr;
	
	do
	{
		counter = 0;
		index = current >> 32;
		epoch = current & MASK_EPOCH;
		
		assertf(index+1 > MASK_EPOCH, "\nOVERFLOW INDEX:%llu  BW:%.10f SIZE:%u TAIL:%p TABLE:%p NUM_ELEM:%u\n", index, bucket_width, size, tail, h, ATOMIC_READ(&h->counter));
		
		min = array + (index++ % (size));
		
		left_node = min_next = min->next;
		right_limit = index*bucket_width;
		ep = 0;
		
		if(is_marked(min_next, MOV))
			goto begin;
		
		do
		{
				
			left_node_next = left_node->next;
			left_ts = left_node->timestamp;
			*result = left_node->payload;
			
			if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV)) continue;
			
			ep += left_node->epoch > epoch;
			
			if(left_node->epoch > epoch) continue;
			
			if(is_marked(left_node_next, MOV)) goto begin;
			
			if(left_ts >= right_limit) break;
			
			int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));
			if(!res) left_node_next = left_node->next;

			//left_node_next = FETCH_AND_OR(&left_node->next, DEL);
				
			if(is_marked(left_node_next, MOV))	goto begin;

			if(is_marked(left_node_next, DEL))	continue;
				
			scan_list_length += counter;

			concurrent_dequeue += __sync_fetch_and_add(&h->d_counter.count, 1) - con_de;
				
			critical_exit();

			return left_ts;
										
		}while( (left_node = get_unmarked(left_node_next)) && ++counter);
		
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		{
			*result = NULL;
				critical_exit();
			return INFTY;
		}
				
		if(ep == 0)
		{
			new_current = h->current;
			if(new_current == last_curr){
				unsigned int dist = (size*DISTANCE_FROM_CURRENT);
				
				assertf((index - (last_curr >> 32) -1) <= dist, "%s", "");
				if(  //1 || 
				 (index - (last_curr >> 32) -1) == dist 
				 )
				{
					num_cas++;
					old_current = VAL_CAS(&(h->current), last_curr, ((index << 32) | epoch) );
					if(old_current == last_curr){
						current = ((index << 32) | epoch);
						num_cas_useful++;
					}
					else
						current = old_current;
					cached_curr = last_curr = current;
				}
				else
					cached_curr = current = ((index << 32) | epoch);
			}
			else
				cached_curr = last_curr = current = new_current;
		}
		else
			goto begin;
		
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
double nbc_prune()
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
		//free_array_nodes(&malloc_status, h->array); 
		free(h->array); 
		free(h);
		node_free(my_tmp); 
	}
	
	do 														
    {	                                                    
		tmp = mm_node_get_reusable(&malloc_status, &counter);    
		while(tmp != NULL && counter-- != 0)                
		{                                                   
			tmp_next = tmp->next;                           
			node_free(tmp);                                 
			tmp =  get_unmarked(tmp_next);                  
		}                                                   
	}                                                       
    while(tmp != NULL);										

	to_free_tables_old = to_free_tables_new;
	to_free_tables_new = NULL;
	
	mm_new_era(&malloc_status, prune_array, threads, TID);
	
	return 0.0;
}

void nbc_report(unsigned int TID)
{
	
	printf("%d- "
	"Enqueue: %.10f LEN: %.10f ### "
	"Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ### "
	"NEAR: %llu dist: %llu ### "
	"RTC:%llu,M:%lld\n",
			TID,
			((float)concurrent_enqueue)/performed_enqueue,
			((float)scan_list_length_en)/performed_enqueue,
			((float)concurrent_dequeue)/performed_dequeue,
			((float)scan_list_length)/performed_dequeue,
			num_cas, num_cas_useful,
			near,
			dist,
			read_table_count	  ,
			malloc_status.to_remove_nodes_count);
}
