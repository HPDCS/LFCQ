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

int do_pq_dequeue(void *q, pkey_t* ret_ts, void **result, unsigned long op_id, nbc_bucket_node* volatile *candidate)
{

	nb_calqueue *queue = (nb_calqueue*)q;
	nbc_bucket_node *min, *min_next, 
					*left_node, *left_node_next, 
					*tail, *array,
					*current_candidate;
	table * h = NULL;
	
	wideptr lnn, new;

	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned long left_node_op_id;

	unsigned int size, attempts = 0;
	unsigned int counter, dest_node;
	pkey_t left_ts;
	double bucket_width, left_limit, right_limit;

	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	unsigned int ep = 0;
	int con_de = 0;
	int res;
	bool prob_overflow = false;
	tail = queue->tail;
	performed_dequeue++;
	
	bool remote = false;

begin:
	// Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub);

	// Get data from the table
	size = h->size;
	array = h->array;
	bucket_width = h->bucket_width;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	do
	{	
		// To many attempts: there is some problem? recheck the table
		if( h->read_table_period == attempts){
			goto begin;
		}
		attempts++;

		// get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;

		dest_node = NODE_HASH(index % (size));
		if (dest_node != NID)
			remote = true;

		// get the physical bucket
		min = array + (index % (size));
		left_node = min_next = min->next;
		
		dest_node = NODE_HASH(index % (size));
		if (dest_node != NID)
			remote = true;

		// get the left limit
		left_limit = ((double)index)*bucket_width;

		index++;

		// get the right limit
		right_limit = ((double)index)*bucket_width;
		// check for a possible overflow
		prob_overflow = (index > MASK_EPOCH);
		
		// reset variables for a new scan
		counter = ep = 0;
		
		// a reshuffle has been detected => restart
		if(is_marked(min_next, MOV)) goto begin;
		
		do
		{
			
			// get data from the current node	
			left_node_next = left_node->next;
			left_node_op_id = left_node->op_id;
			left_ts = left_node->timestamp;

			// increase count of traversed deleted items
			counter++;

			// Skip marked nodes, invalid nodes and nodes with timestamp out of range
			if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV) || (left_ts < left_limit && left_node != tail)) continue;
			
			// Abort the operation since there is a resize or a possible insert in the past
			if(is_marked(left_node_next, MOV) || left_node->epoch > epoch) goto begin;
			
			// the node is in insertion
			if (left_node_op_id == 1) continue;

			// The virtual bucket is empty
			if(left_ts >= right_limit || left_node == tail) break;
			
			// the node is a good candidate for extraction! lets try for it
			current_candidate = VAL_CAS(candidate, NULL, left_node);
			if (current_candidate == NULL) current_candidate = left_node;
			
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
						// no, try to reset the candidate and restart
						BOOL_CAS(candidate, current_candidate, NULL);
						continue;
					}
				}
			}

			// here left node is the current candidate 
			// try the extraction
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
			


			//int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));

			// the extraction is failed
			if(!res)
			{ 
				left_node_next = left_node->next;
				left_node_op_id = left_node->op_id;
			}

			//left_node_next = FETCH_AND_OR(&left_node->next, DEL);
			
			// the node cannot be extracted && is marked as MOV	=> restart
			if(is_marked(left_node_next, MOV)){
				BOOL_CAS(candidate, current_candidate, NULL);
				goto begin;
			}

			// the node cannot be extracted && is marked as DEL => skip
			if(is_marked(left_node_next, DEL))
			{	
				// who extracted the node?
				if (left_node_op_id != op_id)
				{
					BOOL_CAS(candidate, current_candidate, NULL);
					continue;
				}
				else
				{
					*result = left_node->payload;
					*ret_ts = left_ts;
					return 1;
				}
				
			}
			// the node has been extracted

			// use it for count the average number of traversed node per dequeue
			scan_list_length += counter;
			// use it for count the average of completed extractions
			concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

			*result = left_node->payload;
			*ret_ts = left_ts;
			
			return 1;
										
		}while( (left_node = get_unmarked(left_node_next)));
		

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV) && *candidate == NULL)
		{
			if (BOOL_CAS(candidate, NULL, 1))
			{
				*result = NULL;
				*ret_ts = INFTY;
				return 1;
			}
		}
				
		new_current = h->current;
		if(new_current == current){

			if(prob_overflow && h->e_counter.count == 0) goto begin;
			
			assertf(prob_overflow, "\nOVERFLOW INDEX:%llu" "BW:%.10f"  "SIZE:%u TAIL:%p TABLE:%p\n", index, bucket_width, size, tail, h);
			//assertf((index - (last_curr >> 32) -1) <= dist, "%s\n", "PROVA");

			num_cas++;
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
	
	return -1;

}

unsigned int global_op_id = 2;

/**
 * This function extract the minimum item from the NBCQ. 
 * The cost of this operation when succeeds should be O(1)
 *
 * @param  q: pointer to the interested queue
 * @param  result: location to save payload address
 * @return the highest priority 
 *
 */
pkey_t pq_dequeue(void *q, void** result)
{

	pkey_t ret;
	
	critical_enter();
	op_node* requested_op = gc_alloc_node(ptst, gc_aid[1], NID);
	requested_op->op_id = __sync_fetch_and_add(&global_op_id, 1);
	requested_op->type = OP_PQ_DEQ;
	requested_op->payload = NULL; //DEADBEEF
	requested_op->response = -1;
	requested_op->candidate = NULL;
	requested_op->requestor = requested_op;

	
	int res = do_pq_dequeue(q, &ret, result, requested_op->op_id, &requested_op->candidate);
	gc_free(ptst, requested_op, gc_aid[1]);
	critical_exit();
	
	return ret;
}
