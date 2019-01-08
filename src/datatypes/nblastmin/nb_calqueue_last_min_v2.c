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

__thread unsigned long long last_seen_curr 	= 17ULL;
__thread nbc_bucket_node *local_head 		= NULL;
__thread nbc_bucket_node* last_heads[MAX_SKIP_BUCKETS];
__thread	unsigned int heads_index	= 0;


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
	
	unsigned int size, attempts;
	unsigned int counter;
	double bucket_width, left_ts, right_limit;
	
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	unsigned int ep = 0;
	unsigned int con_de = 0;
	unsigned int i = 0;
	bool prob_overflow  = false;
	
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
	attempts = 0;

	if(last_seen_curr != current){
		last_seen_curr = current;
		local_head = NULL;
		heads_index = 0;
	}
	

			if(ptst->gc == (void*)0x1){
				local_head = NULL;
				printf("FUCK I NEVER SHOULD BE HERE C\n");
				local_head->next=0;
			}

	do
	{
		if( h->read_table_period == attempts){
			goto begin;
		}
		attempts++;



			if(ptst->gc == (void*)0x1){
				local_head = NULL;
				printf("FUCK I NEVER SHOULD BE HERE D\n");
				local_head->next=0;
			}

		// VALIDATE OLD HEADS
		//printf("ITERAION %d\n", heads_index);
		
		index = last_seen_curr >> 32;
		epoch = last_seen_curr & MASK_EPOCH;

		min = array +(index % (size));
		left_node = min_next = min->next;

		if(min_next == NULL){
			local_head = NULL;
			printf("FUCK I NEVER SHOULD BE HERE 1\n");
			local_head->next=0;
		}
		
		for(i=0;i<heads_index;i++){	

			prob_overflow = (index+1 > MASK_EPOCH);
		
			if(is_marked(min_next, MOV))	goto begin;
			
			if(min_next != last_heads[i])
				heads_index = i;
			else{
				index += 1;
				min = array +(index % (size));	
				left_node = min_next = min->next;
			
				if(min_next == NULL){
					local_head = NULL;
					printf("FUCK I NEVER SHOULD BE HERE 2\n");
					local_head->next=0;
				}
			}

		}

		index++;

		if(is_marked(min_next, MOV))	goto begin;

		if(min_next != last_heads[heads_index]){ 
			last_heads[heads_index] = min_next;
			local_head = NULL;
		}
		// THIS IS ONLY FOR DEBUG
		else if(local_head){
			local_head = NULL;
			printf("FUCK I NEVER SHOULD BE HERE");
			local_head->next=0;
		}

		counter = 0;
		right_limit = index*bucket_width;
		//printf("RIGHT LIMIT: [%f %f) %llu %llu %llu\n", (index-1)*bucket_width, right_limit, current >> 32, last_seen_curr >> 32, index);
		ep = 0;
		
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
			//local_head = left_node;
			//printf("DONE\n");
			if(ptst->gc == (void*)0x1){
				local_head = NULL;
				printf("FUCK I NEVER SHOULD BE HERE A\n");
				local_head->next=0;
			}
			return left_ts;
										
		}while( (left_node = get_unmarked(left_node_next)) && ++counter);
		
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		{
			*result = NULL;
				critical_exit();
			//printf("DONE\n");
						if(ptst->gc == (void*)0x1){
				local_head = NULL;
				printf("FUCK I NEVER SHOULD BE HER B\n");
				local_head->next=0;
			}

			return INFTY;
		}
				
		if(ep == 0)
		{
			new_current = h->current;
			if(new_current == last_seen_curr){
				unsigned int dist = (size*PERC_SKIP_BUCKET);
				dist = dist > MAX_SKIP_BUCKETS ? MAX_SKIP_BUCKETS : dist;
				
				if(prob_overflow && h->e_counter.count == 0) goto begin;
				assertf(prob_overflow, "\nOVERFLOW INDEX:%llu  BW:%.10f SIZE:%u TAIL:%p TABLE:%p\n", index, bucket_width, size, tail, h);


				// BEYOND THE LIMIT
				if(  //1 || 
				 ( 	 (index - (last_seen_curr >> 32) -1) == dist  )
				|| (index == 0 && 0 == (last_seen_curr >> 32)) 
				|| (heads_index == (MAX_SKIP_BUCKETS-1))
				)
				{
					num_cas++;
					old_current = VAL_CAS(&(h->current), last_seen_curr, ((index << 32) | epoch) );
					goto begin;
					/*
					if(old_current == last_seen_curr){
						current = ((index << 32) | epoch);
						num_cas_useful++;
					}
					else{
						current = old_current;
						int i = 0;
						for(i=0;i<MAX_SKIP_BUCKETS;i++)
							last_heads[i] = NULL;
					}

					local_curr = last_seen_curr = current;
					*/
				}
				else{
					//printf("Aggiorno current locale index:%d lsc:%d \n", heads_index, last_seen_curr>>32);
					last_heads[++heads_index] = NULL;

					if(ptst->gc == (void*)0x1){
						local_head = NULL;
						printf("FUCK I NEVER SHOULD BE HERE F\n");
						local_head->next=0;
					}
				}
				
			}
			else
				goto begin;
				//local_curr = last_seen_curr = current = new_current;
		}
		else
			goto begin;
		
	}while(1);
	
	return INFTY;
}
