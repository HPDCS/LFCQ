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
#include <stdbool.h>
#include <pthread.h>
#include <math.h>

#include "common_nb_calqueue.h"


/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/


__thread unsigned long long concurrent_enqueue;
__thread unsigned long long performed_enqueue ;
__thread unsigned long long concurrent_dequeue;
__thread unsigned long long performed_dequeue ;
__thread unsigned long long scan_list_length;
__thread unsigned long long scan_list_length_en ;


__thread ptst_t *ptst;
int gc_aid[1];
int gc_hid[1];





/*************************************
 * VARIABLES FOR GARBAGE COLLECTION  *
 *************************************/

__thread unsigned long long malloc_count;


/*************************************
 * VARIABLES FOR ENQUEUE  *
 *************************************/

// number of enqueue invokations
__thread unsigned int flush_eq = 0;

/*************************************
 * VARIABLES FOR READ TABLE  *
 *************************************/

/*************************************
 * VARIABLES FOR DEQUEUE	  		 *
 *************************************/
  
__thread unsigned int local_monitor = -1;
__thread unsigned int flush_de = 0;

__thread unsigned long long last_curr = 0ULL;
__thread unsigned long long cached_curr = 0ULL;


#define MOV_FOUND 	3
#define OK			1
#define ABORT		0


void my_hook(ptst_t *p, void *ptr){	free(ptr); }


/**
 * This function create an instance of a non-blocking calendar queue.
 *
 * @author Romolo Marotta
 *
 * @param queue_size is the inital size of the new queue
 *
 * @return a pointer a new queue
 */
nb_calqueue* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
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

	gc_aid[0] = gc_add_allocator(sizeof(nbc_bucket_node));
	gc_hid[0] = gc_add_hook(my_hook);
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
void pq_enqueue(nb_calqueue* queue, double timestamp, void* payload)
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
double pq_dequeue(nb_calqueue *queue, void** result)
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
