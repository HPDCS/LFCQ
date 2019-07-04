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
 *  nb_calqueue.c
 *
 *  Author: Romolo Marotta
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <pthread.h>
#include <math.h>

#include "common_nb_calqueue.h"
#include "../../utils/common.h"

/**
 * This function extract the minimum item from the NBCQ. 
 * The cost of this operation when succeeds should be O(1)
 *
 * @param  q: pointer to the interested queue
 * @param  result: location to save payload address
 * @return the highest priority 
 *
 */

unsigned int ex = 0;

pkey_t pq_dequeue(void *q, void** result)
{
	//printf("EX: %u\n", ex++);
	nb_calqueue *queue = (nb_calqueue*)q;
	bucket_t *min, *min_next, 
					*left_node, *left_node_next, 
					*tail, *array, *right_node;
	table * h = NULL;
	unrolled_node_t *ex_node;
	
	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	unsigned long long extraction;
	unsigned long long extraction2;
	
	unsigned int size, attempts = 0;
	unsigned int counter;
	pkey_t left_ts;
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	int con_de = 0;
	bool prob_overflow = false;
	tail = queue->tail;
	performed_dequeue++;
	
	critical_enter();

begin:
	// Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub, tail);

	// Get data from the table
	size = h->size;
	array = h->array;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	//while(1);
	do
	{	

		*result  = NULL;
		left_ts  = INFTY;

		// To many attempts: there is some problem? recheck the table
		if( h->read_table_period == attempts){
			goto begin;
		}
		attempts++;

		// get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;

		//printf("search for key %d\n", index);

		// get the physical bucket
		min = array + (index % (size));
		left_node = search(min, &left_node_next, &right_node, &counter, index);
		min_next = min->next;
		
		// if i'm here it means that the physical bucket was empty. Check for queue emptyness
		if(left_node->type == HEAD  && right_node->type == TAIL   && size == 1 && !is_marked(min->next, MOV))
		{
		//	printf("EMPTY 	QUEUE\n");
			critical_exit();
			*result = NULL;
			return INFTY;
		}

		// check for a possible overflow
		prob_overflow = (index > MASK_EPOCH);
		
		// a reshuffle has been detected => restart
		if(is_marked(min_next, MOV)) goto begin;

		// BUcket present
		if(left_node->index == index && left_node->type != HEAD){
	
			if(left_node->epoch > epoch) goto begin;
			
			extraction = __sync_fetch_and_add(&left_node->cas128_field.c[0], 1);
			extraction2 = extraction;

			//printf("Extraction %llu vs %u : is freezed mov %llu : is freezed %llu \n", extraction, left_node->cas128_field.a.entries->count, extraction & (1ULL<<63), extraction >> 32);

			if(extraction & (1ULL<<63)) goto begin;

			if(extraction >> 32) goto begin; // this is blocking for dequeues

			ex_node = left_node->cas128_field.a.entries;

			if(extraction < ex_node->count){
				while(extraction > UNROLLED_FACTOR){
					extraction -= UNROLLED_FACTOR;
					ex_node = ex_node->next;
				}
				if(ex_node->array[extraction].valid){
					*result  = ex_node->array[extraction].payload;
					left_ts  = ex_node->array[extraction].timestamp; 
				}
				else
					continue;
			}
			extraction = extraction2;
			
			//if(extraction == 25)
			//	printf("A result %p, ts %u\n", *result, left_ts);

			// The bucket was not empty
			if(left_ts != INFTY){
				if(extraction == 250)
					critical_exit();
					concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);
					return left_ts;
			}
		}

		// bucket empty or absent
		new_current = h->current;
		if(new_current == current){
			// save from livelock with empty queue
			if(prob_overflow && h->e_counter.count == 0) goto begin;

			num_cas++;
			index++;
			// try to mark the bucket as empty
			if(left_node->type == ITEM)
				BOOL_CAS( &(left_node->next), left_node_next, get_marked(left_node_next,DEL) );


			// increase current
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
