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
__thread unsigned long long no_empty_vb = 0;
extern __thread long ins;
extern __thread long conflitti_ins;
extern __thread long estr;
extern __thread long conflitti_estr;


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
	unsigned int old_v;
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
	#endif
	bool remote = false;

	critical_enter();

begin:

	// Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub);

	// definizione variabili dw
	dwb* bucket_p = NULL;
	pkey_t dw_node_ts;
	bool no_cq_node, no_dw_node_curr_vb, no_dw_node;
	
	nbc_bucket_node *dw_node;
	int indexes_enq, deq_cn;

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

		no_dw_node_curr_vb = ((bucket_p = dw_dequeue(h, index)) == NULL);
		no_dw_node = (size == 1 && h->deferred_work->heads[0].next == h->deferred_work->list_tail);
			
		no_cq_node = false;

		if(!no_dw_node_curr_vb && (get_bucket_state(bucket_p->next) == BLK))
			goto begin;

		// get the physical bucket
		min = array + (index % (size));
		left_node = min_next = min->next;

		#if NUMA_DW || SEL_DW
		dest_node = NODE_HASH(index % (size));
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

			if(!no_dw_node_curr_vb){

				indexes_enq = get_enq_ind(bucket_p->indexes) << ENQ_BIT_SHIFT;// solo la parte inserimento di indexes
				dw_retry:
								 
				old_v = bucket_p->indexes;
				deq_cn = get_deq_ind(old_v); 
				// cerco un possibile indice
				
				while(	deq_cn < bucket_p->valid_elem && is_deleted(bucket_p->dwv_sorted[deq_cn].node) && 
						!is_moving(bucket_p->dwv_sorted[deq_cn].node) && !is_marked_ref(bucket_p->next, DELB))
				{
					//BOOL_CAS(&bucket_p->indexes, (indexes_enq + deq_cn), (indexes_enq + deq_cn + 1));	// aggiorno deq
					deq_cn = 
					deq_cn+1;
					//get_deq_ind(bucket_p->indexes);
				}
				
				old_v = bucket_p->indexes;
				if(get_deq_ind(old_v) == 0 || get_deq_ind(old_v) < deq_cn ){
					BOOL_CAS(&bucket_p->indexes, old_v, (indexes_enq + deq_cn));	// aggiorno deq
				}

				// controllo se sono uscito perchè è iniziata la resize
				if(is_moving(bucket_p->dwv_sorted[deq_cn].node))
					goto begin;

				if(deq_cn >= bucket_p->valid_elem || is_marked_ref(bucket_p->next, DELB)){

					no_dw_node_curr_vb = true;
					if(!is_marked_ref(bucket_p->next, DELB))
						BOOL_CAS(&(bucket_p->next), bucket_p->next, get_marked_ref(bucket_p->next, DELB));
								
					// TODO
					//}else if(bucket_p->dwv[deq_cn].node->epoch > epoch){
					//	goto begin;
				}else{
					assertf(is_blocked(bucket_p->dwv_sorted[deq_cn].node), 
						"pq_dequeue(): si cerca di estrarre un nodo bloccato. stato nodo %p, stato bucket %llu\n", 
						bucket_p->dwv_sorted[deq_cn].node, get_bucket_state(bucket_p->next));

					assertf((bucket_p->dwv_sorted[deq_cn].node == NULL), 
						"pq_dequeue(): si cerca di estrarre un nodo nullo." 
						"\nnumero bucket: %llu"
						"\nstato bucket virtuale: %llu"
						"\nbucket marcato: %d"
						"\nindice estrazione letto: %d"
						"\nindice estrazione nella struttura: %d"
						"\nindice inserimento: %d"
						"\nlimite ciclo: %d"
						"\nelementi validi: %d"
						"\ntimestamp: %f\n",
						bucket_p->index_vb, get_bucket_state(bucket_p->next), is_marked_ref(bucket_p->next, DELB), deq_cn, get_deq_ind(bucket_p->indexes), 
						get_enq_ind(indexes_enq), bucket_p->cicle_limit, bucket_p->valid_elem, bucket_p->dwv_sorted[deq_cn].timestamp);
					
					// provo a fare l'estrazione se il nodo della dw viene prima 
								
					// TODO
					assertf(bucket_p->dwv_sorted[deq_cn].timestamp == INV_TS, "INV_TS in extractions%s\n", ""); 
					if((dw_node_ts = bucket_p->dwv_sorted[deq_cn].timestamp) <= left_ts){
					
						dw_node = get_node_pointer(bucket_p->dwv_sorted[deq_cn].node);	// per impedire l'estrazione di uno già estratto o in movimento
						assertf((dw_node == NULL), "pq_dequeue(): dw_node è nullo %s\n", "");

						if(BOOL_CAS(&bucket_p->dwv_sorted[deq_cn].node, dw_node, get_marked_node(dw_node, DELN))){// se estrazione riuscita
										
							assertf(deq_cn >= VEC_SIZE, "pq_dequeue(): indice di estrazione fuori range %s\n", "");	
							assertf(get_enq_ind(bucket_p->indexes) != get_enq_ind(indexes_enq), "pq_dequeue(): indice di inserimento non costante %s\n", "");
										
							BOOL_CAS(&bucket_p->indexes, (indexes_enq + deq_cn), (indexes_enq + deq_cn + 1));
							concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

							estr++;
							*result = dw_node->payload;

							if(!no_cq_node) no_empty_vb++;

							critical_exit();
							#if NUMA_DW || SEL_DW
							if (!remote)
								local_deq++;
							else
								remote_deq++;
							#endif

							return dw_node_ts;
						}else{// provo a vedere se ci sono altri nodi in dw
							conflitti_estr++;

							// controllo se non ci sono riuscito perchè stiamo nella fase di resize
							if(is_moving(bucket_p->dwv_sorted[deq_cn].node)) 
								goto begin;

							goto dw_retry;
						}
					}
				}
				
				/*
				dw_node_ts = dw_extraction(bucket_p, result, left_ts, remote, no_cq_node);
				
				if(dw_node_ts >= 0){				// estrazione riuscita
					concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);
					critical_exit();
					return dw_node_ts;
				}else if(dw_node_ts == GOTO)		// bisogna ricominciare da capo
					goto begin;
				else if(dw_node_ts == EMPTY)	// bucket vuoto
					no_dw_node_curr_vb = true;
					*/
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

			return left_ts;
										
		}while( (left_node = get_unmarked(left_node_next)));

		// if i'm here it means that the virtual bucket was empty. Check for queue emptyness
		//if(left_node == tail && size == 1 && !is_marked(min->next, MOV))
		if(left_node == tail && size == 1 && !is_marked(min->next, MOV) && no_dw_node)
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
