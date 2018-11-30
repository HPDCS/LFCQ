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

__thread unsigned long long last_curr 	= 0ULL;
__thread unsigned long long cached_curr = 0ULL;
__thread nbc_bucket_node *last_head 	= NULL;
__thread nbc_bucket_node *last_min 		= NULL;

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
		last_head 	= NULL;
		last_min  	= NULL;
	}
	
	do
	{
		counter = 0;
		index = current >> 32;
		epoch = current & MASK_EPOCH;
		
		assertf(index+1 > MASK_EPOCH, "\nOVERFLOW INDEX:%llu  BW:%.10f SIZE:%u TAIL:%p TABLE:%p NUM_ELEM:%u\n", index, bucket_width, size, tail, h, ATOMIC_READ(&h->counter));
		
		min = array + (index++ % (size));
		
		left_node = min_next = min->next;

		if(left_node != last_head){ 
			last_head = left_node;
			last_min = NULL;
		}
		else if(last_min){
			left_node = last_min;
		}

		right_limit = index*bucket_width;
		ep = 0;
		
		if(is_marked(min_next, MOV)) goto begin;
		
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
			last_min = left_node;
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
