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
//#include <stdlib.h>
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


//extern unsigned long long d_to_cq;
//extern unsigned long long d_to_dq;
extern long		conflitti;

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

	critical_enter();

begin:

	// Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub);

	#if DW_USAGE	
		
		nbc_bucket_node *dw_node, *extracted_dw_node;
		dwn* dw_list_node = NULL;
		pkey_t dw_node_ts;
		bool no_cq_node, no_dw_node_curr_vb, no_dw_node;
		int cn = 0;

	#endif

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
		//printf("index %lld\n", index);
		#if DW_USAGE

			if((dw_list_node = dw_dequeue(q, index)) == NULL){ // cerco di portare il bucket virtuale in EXT se esiste
				
				no_dw_node_curr_vb = true;	// se sto qui il bucket virtuale è vuoto
			
				no_dw_node = (size == 1 && h->deferred_work->dwls[0]->head->next == h->deferred_work->dwls[0]->tail);

			}else
				no_dw_node_curr_vb = false;

			no_cq_node = false;
		#endif

		// get the physical bucket
		min = array + (index % (size));
		left_node = min_next = min->next;
		
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
			left_ts = left_node->timestamp;

			// increase count of traversed deleted items
			counter++;
				
			// Skip marked nodes, invalid nodes and nodes with timestamp out of range
			if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV) || (left_ts < left_limit && left_node != tail)) continue;
			
			// Abort the operation since there is a resize or a possible insert in the past
			if(is_marked(left_node_next, MOV) || left_node->epoch > epoch) goto begin;

			if(left_ts >= right_limit || left_node == tail){
				#if DW_USAGE
					no_cq_node = true;

					if(no_dw_node_curr_vb)
						break;
				#else
				break;
				#endif
			}

			#if DW_USAGE

			if(!no_dw_node_curr_vb && !no_dw_node){
				//printf("estra");
			
				dw_retry:
				cn = dw_list_node->deq_cn; // il prossimo da estrarre

				// cerco un possibile candidato
				while(cn < dw_list_node->enq_cn && DW_GET_STATE(dw_list_node->next) == EXT 
					&& (DW_NODE_IS_DEL(dw_list_node->dwv[cn]) || DW_NODE_IS_BLK(dw_list_node->dwv[cn]))){
					if(!BOOL_CAS(&dw_list_node->deq_cn, cn, cn + 1))//	FETCH_AND_ADD(&dw_list_node->deq_cn, 1);
						conflitti++;	
					cn = dw_list_node->deq_cn;
				}

				if (DW_GET_STATE(dw_list_node->next) > EXT)
					goto begin; // è cominciato il resize

				if(cn >= dw_list_node->enq_cn){
					no_dw_node_curr_vb = true;
				}else{
					dw_node = DW_GET_NODE_PTR(dw_list_node->dwv[cn]);// per impedire l'etrazione di uno già estratto
					dw_node_ts = dw_node->timestamp;

					// provo a fare l'estrazione se il nodo della dw viene prima 
					if(dw_node_ts <= left_ts){

						extracted_dw_node = VAL_CAS(&dw_list_node->dwv[cn], dw_node, (nbc_bucket_node*)((unsigned long long)dw_node | DELN));

						if(extracted_dw_node == dw_node){// se estrazione riuscita
							if(!BOOL_CAS(&dw_list_node->deq_cn, cn, cn + 1))
								conflitti++;

							concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

							*result = extracted_dw_node->payload;
							//__sync_fetch_and_add(&d_to_dq, 1ULL);
							/*
							if(dw_node_ts < 5.0){
								printf("TID: %d dw %f\n",TID, dw_node_ts);
								fflush(stdout);
							}*/
							
						//printf("dequeu %p %f \n", extracted_dw_node, extracted_dw_node->timestamp );
							DW_AUDIT{
								printf("ESTRAZIONE_ESTERNA: TID %d: cn %d , index %lld, node %p, timestamp %f\n", TID, cn, index - 1, extracted_dw_node, extracted_dw_node->timestamp);	
								fflush(stdout);	
							}
							critical_exit();
				
							return dw_node_ts;
						}else{// provo a vedere se ci sono altri nodi in dw
							goto dw_retry;
						}
					}
				}
			}

			if(no_cq_node)// se non c'erano elementi ricominciamo
				break;

				// altrimenti provo a estrarre dalla calendar
			#endif
			if(left_node == tail){
				printf("TAIL MARCATA\n");	
			}

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
			/*
			if(left_ts < 5.0){
				printf("TID %d standard %f\n",TID, left_ts);
				fflush(stdout);
			}
			*/
			
			//__sync_fetch_and_add(&d_to_cq, 1ULL);
			return left_ts;
										
		}while( (left_node = get_unmarked(left_node_next)));

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		#if DW_USAGE
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV) && no_dw_node)
		#else
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		#endif
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
