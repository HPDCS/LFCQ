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


/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

unsigned int threads;
nbc_bucket_node *g_tail;
unsigned int * volatile prune_array;

/*************************************
 * CONFIG VARIABLES					 *
 ************************************/

__thread unsigned int read_table_period = READTABLE_PERIOD;
__thread unsigned int period_monitor = MONITOR_PERIOD;



/*************************************
 * VARIABLES FOR GARBAGE COLLECTION  *
 *************************************/

__thread hpdcs_gc_status malloc_status =
{
	.free_nodes_lists 			= NULL,
	.to_free_nodes 				= NULL,
	.to_free_nodes_old 			= NULL,
	.block_size 				= sizeof(nbc_bucket_node),
	.offset_next 				= offsetof(nbc_bucket_node, next),
	.to_remove_nodes_count 		= 0LL
};

__thread nbc_bucket_node *to_free_tables_old = NULL;
__thread nbc_bucket_node *to_free_tables_new = NULL;


/*************************************
 * VARIABLES FOR ENQUEUE  *
 *************************************/

// number of sampled enqueue
__thread unsigned long long en_samples = 0;
// number of total attempts  
__thread unsigned long long attempt_enqueue  = 0;
// number of enqueues after two sample
__thread unsigned long long concurrent_enqueue = 0;
// number of enqueue invokations
__thread unsigned int flush_eq = 0;

/*************************************
 * VARIABLES FOR INSERT  *
 *************************************/
 
 // number of steps per attempt
__thread unsigned long long enqueue_steps = 0;

/*************************************
 * VARIABLES FOR FLUSH CURRENT  *
 *************************************/

__thread unsigned long long flush_current_attempt	 = 0;
__thread unsigned long long flush_current_success	 = 0;
__thread unsigned long long flush_current_fail	 = 0;


/*************************************
 * VARIABLES FOR READ TABLE  *
 *************************************/

__thread unsigned long long read_table_count	 = 0;
  
/*************************************
 * VARIABLES FOR DEQUEUE	  		 *
 *************************************/
  
// number of sampled dequeue
__thread unsigned long long de_samples = 0;
__thread unsigned long long concurrent_dequeue = 0;
__thread unsigned long long performed_dequeue  = 0;
__thread unsigned long long attempt_dequeue  = 0;
__thread unsigned long long scan_list_length  = 0;
            
            



__thread unsigned int local_monitor = -1;




__thread unsigned int flush_de = 0;


__thread unsigned long long dequeue_steps = 0;
__thread unsigned long long compact = 0;
__thread unsigned long long c_success = 0;
__thread unsigned long long buckets = 0;


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
 * @param queue the queue that contains the bucket
 * @param head the head of the list in which we have to perform the search
 * @param timestamp the value to be found
 * @param left_node a pointer to a pointer used to return the left node
 * @param left_node_next a pointer to a pointer used to return the next field of the left node 
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
			marked = is_marked_for_search(tmp_next, flag);
			tmp_timestamp = tmp->timestamp;
			tmp_tie_breaker = tmp->counter;
			// Check if the node is marked
			go_to_next = LESS(tmp_timestamp, timestamp);
			ts_equal = D_EQUAL(tmp_timestamp, timestamp);
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
 * 			it follows that a replica of the interested key has been already inserted, thus is exchanges the parameter node with the found one and frees the node passed as parameter 
 * 		
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
 
static unsigned int search_and_insert(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						 int flag, nbc_bucket_node *new_node_pointer, nbc_bucket_node **new_node)
{
	nbc_bucket_node *left, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int left_tie_breaker, tmp_tie_breaker;
	double left_timestamp, tmp_timestamp, rand;
	bool marked, ts_equal, tie_lower, go_to_next;
	bool is_new_key = flag == REMOVE_DEL_INV;
	drand48_r(&seedT, &rand);
	if(rand < 0.05){
		nuc_bucket_node *lnode, *rnode;
		search(head, -1.0, 0, &lnode, &rnode, REMOVE_DEL_INV);
	}
	// read tail from head (this is done for avoiding an additional cache miss)
	tail = head->tail;
	do
	{
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
__thread unsigned long long near = 0;
__thread unsigned long long dist = 0;

void flush_current(table* h, unsigned long long newIndex, nbc_bucket_node* node)
{
	unsigned long long oldCur, oldIndex, oldEpoch;
	unsigned long long newCur, tmpCur = -1;
	signed long long distance = 0;
	bool mark = false;	// <----------------------------------------
		
	
	// Retrieve the old index and compute the new one
	oldCur = h->current;
	oldEpoch = oldCur & MASK_EPOCH;
	oldIndex = oldCur >> 32;
	//newIndex = ( unsigned long long ) hash(node->timestamp, h->bucket_width);
	newCur =  newIndex << 32;
	distance = newIndex - oldIndex;
	dist = h->size*DISTANCE_FROM_CURRENT;
	if(distance > 0 && distance < dist){
		newIndex = oldIndex;
		newCur =  newIndex << 32;
		near+= distance!=0;
	}
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
		//if(tmpCur != -1)
		//{	
		//	flush_current_attempt += 1;	
		//	flush_current_success += 1;
		//}
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
		
		//flush_current_attempt += 1;	
		//flush_current_fail += 1;
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
		//if(mark)
		//{
		//	flush_current_success += 1;
		//	flush_current_attempt += 1;	
		//}
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
bool insert_std(table* hashtable, nbc_bucket_node** new_node, int flag)
{
	nbc_bucket_node *bucket, *new_node_pointer;
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

	 
	return search_and_insert(bucket, new_node_timestamp, new_node_counter, flag, new_node_pointer, new_node);
	//search_for_insert(bucket, new_node_timestamp, new_node_counter, &left_node, &left_node_next, &right_node, flag, &skipped_nodes);

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
		
		tail = g_tail;
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
			array[i].tail = tail;
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

void block_table(table* h)
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

double compute_mean_separation_time(table* h,
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
    
    //printf("%d- my new bucket %.10f for %p\n", TID, newaverage, h);	
	return newaverage;
}

void migrate_node(nbc_bucket_node *right_node,	table *new_h)
{
	//nbc_bucket_node * volatile replica;
	//nbc_bucket_node volatile **replica_new;
	nbc_bucket_node *replica;
	//nbc_bucket_node *replica_new;
	nbc_bucket_node *right_replica_field, *right_node_next;
	int res = 0;
	
	
	//Create a new node inserted in the new table as as INValid
	replica = node_malloc(right_node->payload, right_node->timestamp,  right_node->counter);
	//replica_new = replica;
	         
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

	
	flush_current(new_h, ( unsigned long long ) hash(right_replica_field->timestamp, new_h->bucket_width), right_replica_field);
	
	
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

table* read_table(table *volatile *curr_table_ptr, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket)
{
	//printf("ENTER READTABLE\n", TID);
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
	//unsigned long long  prova;
	//double avg_diff;
	//int *prova2;
	int samples[2];
	int sample_a;
	int sample_b;
	
	read_table_count = ((-(read_table_count == -1)) & TID) + ((-(read_table_count != -1)) & read_table_count);
	if(read_table_count++ % read_table_period == 0)
	{
		//read_table_count = 0;
		//__sync_synchronize();
		//__asm__ __volatile__ ("" : : : "memory");

		//avg_diff= 0.0;
		for(i=0;i<2;i++)
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
		
		sample_a = abs(samples[0] - size*perc_used_bucket);
		sample_b = abs(samples[1] - size*perc_used_bucket);
		
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
			set_new_table(h, threshold, perc_used_bucket, elem_per_bucket, counter);
	}
	
	if(h->new_table != NULL)
	{
		//printf("%u - MOVING BUCKETS\n", tid);
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;
		tail = g_tail;	

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
		if( BOOL_CAS(curr_table_ptr, h, new_h) )
		{
			connect_to_be_freed_table_list(h);
		}

		h = new_h;
	}
	//printf("EXIT READTABLE\n", TID);
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
	res->hashtable->e_counter.count = 0;
	res->hashtable->d_counter.count = 0;

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
		res->hashtable->array[i].tail = g_tail;
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
	nbc_bucket_node *bucket, *new_node = node_malloc(payload, timestamp, 0);
	table * h = NULL;		
	unsigned int index, res, dist;
	unsigned long long cur_enqueues = 0;
	unsigned long long newIndex = 0;
	
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	//init the result
	res = MOV_FOUND;
	local_monitor = ((-(local_monitor == -1)) & TID) + ((-(local_monitor != -1)) & local_monitor);
	bool monitor = ++local_monitor % period_monitor == 0;
	en_samples+=monitor;
	flush_eq++;

	//repeat until a successful insert
	while(res != OK){
		
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){
			// check for a resize
			h = read_table(&queue->hashtable, th, epb, pub);
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			// compute the index of the physical bucket
			index = newIndex % h->size;
			// get the bucket
			bucket = h->array + index;
			// read the number of executed enqueues for statistics purposes
			if(monitor){
				cur_enqueues =  ATOMIC_READ(&h->e_counter);
			}
		}
		// read the actual epoch
		new_node->epoch = (h->current & MASK_EPOCH);
		// search the two adjacent nodes that surround the new key and try to insert with a CAS 
	    res = search_and_insert(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node);
	}

	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
	flush_current(h, newIndex, new_node);
	
	// updates for statistics
	if(monitor)
	{
		
		unsigned long long oldCur = h->current;
		unsigned long long oldIndex = oldCur >> 32;
		dist = h->size*DISTANCE_FROM_CURRENT+1;
		concurrent_enqueue += __sync_fetch_and_add(&h->e_counter.count, flush_eq) - cur_enqueues;
		flush_eq = 0;
		
		#if COMPACT_RANDOM_ENQUEUE == 1
		// clean a random bucket
		double rand;
		nbc_bucket_node *left_node, *right_node;
		drand48_r(&seedT, &rand);
		search(h->array+((oldIndex + dist + (unsigned int)((h->size-dist)*rand))%h->size), -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
		#endif
	}
	
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
 
__thread unsigned long long last_curr = 0ULL;
__thread unsigned long long cached_curr = 0ULL;
__thread unsigned long long num_cas = 0ULL;
	double nbc_dequeue(nb_calqueue *queue, void** result)
{
	nbc_bucket_node *min, *min_next, 
					*left_node, *left_node_next, 
					*tail, *array;
	nbc_bucket_node *left_nodea, *right_node;
	table * h = NULL;
	
	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	unsigned long long cur_dequeues = 0;
	
	unsigned int size, dist;
	unsigned int counter;
	double bucket_width, left_ts, right_limit;
	
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
		tail = queue->tail;
	unsigned int ep = 0;
	double rand = 0.0;                      // <----------------------------------------
	
	
	local_monitor = ((-(local_monitor == -1)) & TID) + ((-(local_monitor != -1)) & local_monitor);
	bool monitor = local_monitor++ % period_monitor == 0;
	de_samples+=monitor;
	
	
begin:
	h = read_table(&queue->hashtable, th, epb, pub);
	
	if(monitor)
	{
		cur_dequeues = ATOMIC_READ(&h->d_counter);
	}
	size = h->size;
	array = h->array;
	bucket_width = h->bucket_width;
	current = h->current;
	
	if(last_curr != current){
		cached_curr = last_curr = current;
	}
	else
		current = cached_curr;
	
	#if COMPACT_RANDOM_DEQUEUE == 1
	if(monitor){
		dist = h->size*DISTANCE_FROM_CURRENT+1;
		drand48_r(&seedT, &rand);
		index = current >> 32;
		search(array+((index + dist + (unsigned int)( (size-dist)*rand))%size), -1.0, 0,  &left_nodea, &right_node, REMOVE_DEL_INV);
	}
	#endif
	
	
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
		
		//while( 1//left_node->epoch <= epoch
		//)
		do
		{
				
			left_node_next = left_node->next;
			left_ts = left_node->timestamp;
			*result = left_node->payload;
			
			if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV))
				continue;
			
			ep += left_node->epoch > epoch;
			
			if(left_node->epoch > epoch)
				continue;
				
			
			if(is_marked(left_node_next, MOV))
				goto begin;
			
			//if(!is_marked(left_node_next) && left_node->epoch <= epoch)
			//{
			if(left_ts >= right_limit)
				break;
			
			//{
				//left_node_next = VAL_CAS(&left_node->next, left_node_next, get_marked(left_node_next, DEL));
				left_node_next = FETCH_AND_OR(&left_node->next, DEL);
				
				if(is_marked(left_node_next, MOV))
					goto begin;

				if(is_marked(left_node_next, DEL))
					continue;

				
				flush_de++;
				//if(!is_marked(left_node_next))
				//{
				if(monitor)
				{
					concurrent_dequeue += __sync_fetch_and_add(&h->d_counter.count, flush_de) - cur_dequeues;
					flush_de = 0;
				}
				//scan_list_length += counter;				
				
				#if LOG_DEQUEUE == 1
					LOG("DEQUEUE: %f %u - %llu %llu\n",left_ts, left_node->counter, index, index % size);
				#endif
								
				return left_ts;
				//}
			//}
				
			//}
						
			//left_node = get_unmarked(left_node_next);
			//counter++;
		}while( (left_node = get_unmarked(left_node_next)) && ++counter);
		
		
		drand48_r(&seedT, &rand);

		/*
		if( //0 &&
			ep == 0 &&
			(successful_cas = counter > 0) && //concurr  && 
			//(successful_cas = rand < 2.0/concurr) && 
			BOOL_CAS(&(min->next), min_next, left_node)
		)
		{
			//scan_list_length+=counter;
			compact++;	
			connect_to_be_freed_node_list(min_next, counter);
			counter = 0;
		}
		
		c_success+=successful_cas;
		*/
		
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		{
			#if LOG_DEQUEUE == 1
			LOG("DEQUEUE: NULL 0 - %llu %llu\n", index, index % size);
			#endif
			*result = NULL;
			if(monitor)
			{
				concurrent_dequeue += ATOMIC_READ(&h->d_counter) - cur_dequeues;
			}
			return INFTY;
		}

				
		if(ep == 0)
		{
			new_current = h->current;
			//if(new_current == current){
			if(new_current == last_curr){
				unsigned int dist = (size*DISTANCE_FROM_CURRENT);
				//printf("index: %llu last_cur:%llu, dist: %lu, size: %lu\n",  index , (last_curr >> 32), dist, size);
				if((index - (last_curr >> 32) -1) > dist){
					printf("FUCK\n");
				//	exit(1);
				}
				if(  //1 || 
				 (index - (last_curr >> 32) -1) == dist 
				 )
				{
					num_cas++;
					//old_current = VAL_CAS(&(h->current), current, ((index << 32) | epoch) );
					old_current = VAL_CAS(&(h->current), last_curr, ((index << 32) | epoch) );
					if(old_current == last_curr)
						current = ((index << 32) | epoch);
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

void nbc_report(unsigned int TID)
{
	
	printf("%d- "
	"Dequeue: Conc.:%.3f NUMCAS: %llu ### "
	"Enqueue: Conc.:%.3f ### "
	"NEAR: %llu dist: %llu ### "
	"RTC:%llu,M:%lld\n",
			TID,
			concurrent_dequeue*1.0/de_samples, num_cas,
			concurrent_enqueue*1.0/en_samples,
			near,
			dist,
			read_table_count	  ,
			malloc_status.to_remove_nodes_count);
}
