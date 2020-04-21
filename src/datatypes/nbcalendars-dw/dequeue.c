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
#include <stdbool.h>
#include <pthread.h>
#include <math.h>

#include "common_nb_calqueue.h"

/**
 * This function extract the minimum item from the NBCQ. 
 * The cost of this operation when succeeds should be O(1)
 *
 * @param  q: pointer to the interested queue
 * @param  result: location to save payload address
 * @return the highest priority 
 *
 */

extern __thread unsigned long long no_empty_vb;
extern __thread long estr;
extern __thread long conflitti_estr;

extern bool dw_enable;


pkey_t pq_dequeue(void *q, void** result)
{
	nb_calqueue *queue = (nb_calqueue*)q;
	nbc_bucket_node *min, *min_next, 
					*left_node, *left_node_next, 
					*tail, *array;
	table * h = NULL;
	
	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned int size, attempts = 0;
	unsigned int counter;
	pkey_t left_ts;
	double bucket_width, left_limit, right_limit;

	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	unsigned int ep = 0;
	int con_de = 0;
	bool prob_overflow = false;
	tail = queue->tail;
	performed_dequeue++;

	#if NUMA_DW || SEL_DW
	unsigned int dest_node;
	bool remote = false;
	#endif

	critical_enter();

begin:

	// Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub, queue->tail);

	// definizione variabili dw
	dwb* bucket_p = NULL;
	pkey_t dw_node_ts;
	bool no_cq_node, no_dw_node_curr_vb, no_dw_node;
	
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

		no_dw_node = no_cq_node = no_dw_node_curr_vb = false;

		bucket_p = dw_dequeue(h, index, tail);

		no_dw_node = (size == 1 && h->deferred_work->heads[0].next == h->deferred_work->list_tail);
		if(bucket_p != NULL){
			no_dw_node_curr_vb = ((bucket_p->valid_elem == 0) || (get_deq_ind(bucket_p->indexes) > 0 && get_deq_ind(bucket_p->indexes) == bucket_p->valid_elem) 
				|| bucket_p->pro);
			//no_cq_node = (bucket_p->cq_head->next == queue->tail);
			
			if(!no_dw_node_curr_vb && (get_bucket_state(bucket_p->next) == BLK))
				goto begin;
		}else{
			//if (index <50)
			//printf("%llu\n", index);
			//no_dw_node = (size == 1 && h->deferred_work->heads[0].next == h->deferred_work->list_tail);
			//no_cq_node = true;
			index++;
			goto fine;
		}

		//no_dw_node_curr_vb = ((bucket_p = dw_dequeue(h, index)) == NULL);
		//no_dw_node = (size == 1 && h->deferred_work->heads[0].next == h->deferred_work->list_tail);
		

		//no_cq_node = false;
/*
		#if DISABLE_EXTRACTION_FROM_DW
		assertf(!no_dw_node_curr_vb, "dequeue(): bucket non flushato %s\n", "");
		#endif
*/
/*
		if(!no_dw_node_curr_vb && (get_bucket_state(bucket_p->next) == BLK))
			goto begin;
*/
/*
		if(index < 100 && bucket_p != NULL)
			printf("%p %llu\n", bucket_p, bucket_p->index_vb);
		else if(index < 100)
			printf("nullo\n");
*/
		/*
		if(index < 50)
			printf("%p %p\n", h->deferred_work->list_tail, h->deferred_work->heads[0].next);
		*/

		

		// get the physical bucket
		//min = array + (index % (size));
		min = bucket_p->cq_head;
		left_node = min_next = min->next;

		#if NUMA_DW || SEL_DW
		dest_node = NODE_HASH(hash_dw(index, size)/*index % (size)*/);
		if (dest_node != NID)
			remote = true;
		#endif
		
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
			//printf("pippo %llu\n", index);
			// get data from the current node	
			left_node_next = left_node->next;
			left_ts = left_node->timestamp;

			// increase count of traversed deleted items
			counter++;
				
			// Skip marked nodes, invalid nodes and nodes with timestamp out of range
			if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV) || (left_ts < left_limit && left_node != tail)) continue;
			
			if(is_marked(left_node_next, INV2)){
				 validate_or_destroy(left_node);
				 goto begin;
			}
		
			// Abort the operation since there is a resize or a possible insert in the past
			if(is_marked(left_node_next, MOV) || left_node->epoch > epoch) goto begin;

			if(left_ts >= right_limit || left_node == tail){

				no_cq_node = true;

				if(no_dw_node_curr_vb)
					break;
			}

			// DWQ
			if(!no_dw_node_curr_vb){				
				
				dw_node_ts = dw_extraction(bucket_p, result, left_ts);
				
				if(dw_node_ts >= 0){				// estrazione riuscita
					concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);
					if(!no_cq_node) no_empty_vb++;
					critical_exit();
					
					#if NUMA_DW || SEL_DW
						if (!remote)
							local_deq++;
						else
							remote_deq++;
					#endif
				
				//	printf("DW %f\n", dw_node_ts);
					
					return dw_node_ts;
				}else if(dw_node_ts == GOTO)		// bisogna ricominciare da capo
					goto begin;
				else if(dw_node_ts == EMPTY)	// bucket vuoto
					no_dw_node_curr_vb = true;
					
			}

			// se non c'erano elementi ricominciamo
			if(no_cq_node) break;
		
			// the node is a good candidate for extraction! lets try for it
			int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));

			// the extraction is failed
			if(!res) left_node_next = left_node->next;

			//left_node_next = FETCH_AND_OR(&left_node->next, DEL);

			// the node cannot be extracted && is marked as MOV	=> restart
			if(is_marked(left_node_next, MOV))	goto begin;

			// the node cannot be extracted && is marked as DEL => skip
			if(is_marked(left_node_next, DEL))	continue;
			
			// the node has been extracted

			// use it for count the average number of traversed node per dequeue
			scan_list_length += counter;
			// use it for count the average of completed extractions
			concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

			*result = left_node->payload;
				
			critical_exit();

			#if NUMA_DW || SEL_DW
			if (!remote)
				local_deq++;
			else
				remote_deq++;
			#endif

			//printf("CQ %f\n", left_ts);
			return left_ts;
										
		}while( (left_node = get_unmarked(left_node_next)));

fine:		
		
		if(bucket_p != NULL){
			assertf(dw_enable && !bucket_p->pro, "%s", "");
			BOOL_CAS(&(bucket_p->next), bucket_p->next, get_marked_ref(bucket_p->next, DELB));
			assertf(!is_marked_ref(bucket_p->next, DELB), "delb %s\n", "");
		}

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		//if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		//if(left_node == tail && size == 1 && !is_marked(min->next, MOV) && no_dw_node)
		if(no_dw_node)
		{
			critical_exit();
			*result = NULL;
			printf("TID %d Coda completamente vuota\n", TID);
			fflush(stdout);
			return INFTY;
		}

		new_current = h->current;
		if(new_current == current){

			if(prob_overflow && h->e_counter.count == 0) goto begin;
			
			assertf(prob_overflow, "\nOVERFLOW INDEX:%llu" "BW:%.10f"  "SIZE:%u TAIL:%p TABLE:%p\n", index, bucket_width, size, tail, h);
			//assertf((index - (last_curr >> 32) -1) <= dist, "%s\n", "PROVA");

			num_cas++;
			old_current = VAL_CAS( &(h->current), current, ((index << 32) | epoch) );
			if(old_current == current){
				//printf("old_current %llu\n", old_current);
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