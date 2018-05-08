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


#include "numa_queue.h"

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
void numa_flush_current(numa_nb_calqueue *queue, table* h, nbc_bucket_node* node, bool update_epoch)
{
	unsigned long long oldCur, oldIndex, oldEpoch;
	unsigned long long newIndex, newCur, tmpCur = -1;
	bool mark = false;	// <----------------------------------------
		
	
	// Retrieve the old index and compute the new one
	oldCur = h->current;
	oldEpoch = oldCur & MASK_EPOCH;
	oldIndex = oldCur >> 32;
	newIndex = ( unsigned long long ) hash(node->timestamp, h->bucket_width);
	newCur =  newIndex << 32;
	
//	printf("EPOCH %llu \n", 
	
	// TODO *** reasoning about updating global epoch...
	
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
		// NO ATTEMPT OF UPDATING CURRENT
		if(tmpCur != -1)
		{	
			
			flush_current_attempt += 1;	
			flush_current_success += 1;
		}
		// A SUCCESSFUL ATTEMPT HAS BEEN PERFORMED
		else if(update_epoch)
		{	
			unsigned long long old = queue->global_epoch;
			__sync_val_compare_and_swap(&queue->global_epoch, old, old+1);
		}
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
		flush_current_attempt += 1;	
		flush_current_fail += 1;
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
		
	if(mark)
	{
		flush_current_success += 1;
		flush_current_attempt += 1;	
		// SOMEONE has already extracted the item
	}
	else if(update_epoch)
	{	
		unsigned long long old = queue->global_epoch;
		__sync_val_compare_and_swap(&queue->global_epoch, old, old+1);
	}
		
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
numa_nb_calqueue* numa_nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{
	unsigned int i = 0, j=0;
	//unsigned int new_threshold = 1;
	unsigned int res_mem_posix = 0;

	threads = threshold;
	prune_array = calloc(threshold*threshold, sizeof(unsigned int));

	numa_nb_calqueue* res = calloc(1, sizeof(numa_nb_calqueue));
	if(res == NULL)
		error("No enough memory to allocate queue\n");

	//while(new_threshold <= threshold)
	//	new_threshold <<=1;

	res->threshold = threads/NUMA_NODES;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->global_epoch = 0;

	//res->hashtable = malloc(sizeof(table));
	
	g_tail = node_malloc(NULL, INFTY, 0);
	g_tail->next = NULL;
	
	printf("\n##################\n");
	printf("INIT NUMA QUEUE\n");
	printf("##################\n");
	for (j = 0; j < NUMA_NODES; j++)
	{
		
		res_mem_posix = posix_memalign( (void**)(&(res->hashtable[j])), CACHE_LINE_SIZE, sizeof(table));
		if(res_mem_posix != 0)
		{
			free(res);
			error("No enough memory to allocate queue\n");
		}
		printf("%p\n", res->hashtable[j]); 
		res->hashtable[j]->bucket_width = 1.0;
		res->hashtable[j]->new_table = NULL;
		res->hashtable[j]->size = MINIMUM_SIZE;
		res->hashtable[j]->current = 0;
		#if SINGLE_COUNTER == 0	
		res->hashtable[j]->e_counter.count = 0;
		res->hashtable[j]->d_counter.count = 0;
		#else
		res->hashtable[j]->counter = 0;
		#endif
		//res->hashtable->array = calloc(MINIMUM_SIZE, sizeof(nbc_bucket_node) );
		
		res->hashtable[j]->array =  alloc_array_nodes(&malloc_status, MINIMUM_SIZE);
		
		if(res->hashtable[j]->array == NULL)
		{
			error("No enough memory to allocate queue\n");
			free(res->hashtable[j]);
			free(res);
		}

		for (i = 0; i < MINIMUM_SIZE; i++)
		{
			res->hashtable[j]->array[i].next = g_tail;
			res->hashtable[j]->array[i].timestamp = i * 1.0;
			res->hashtable[j]->array[i].counter = 0;
		}
	}

	return res;
}

/**
 * This function implements the enqueue interface of the non-blocking queue.
 * Should cost O(1) when succeeds
 *
 * @author Romolo Marotta
 *
 * @param queue
 * @param timestamp the key associated with the value
 * @param payload the event to be enqueued
 *
 * @return true if the event is inserted in the hashtable, else false
 */
void numa_nbc_enqueue(numa_nb_calqueue* queue, double timestamp, void* payload)
{
	nbc_bucket_node *new_node = node_malloc(payload, timestamp, 0);
	table * h = NULL;		
	unsigned int conc_enqueue;
	unsigned int conc_enqueue_2;
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	//table *old_h = NULL;	

	//do
	{
		//if(old_h != (h = read_table(queue)))
		//{
		//	old_h = h;
		//	new_node->epoch = (h->current & MASK_EPOCH);
		//}
		h = read_table(&queue->hashtable[NID], th, epb, pub);
		new_node->epoch = (h->current & MASK_EPOCH);
		new_node->epoch = queue->global_epoch;
		#if SINGLE_COUNTER == 0	
		conc_enqueue =  ATOMIC_READ(&h->e_counter);	
		#endif
		attempt_enqueue++;
		
	} while(!insert_std(h, &new_node, REMOVE_DEL_INV)) //;
	{
		h = read_table(&queue->hashtable[NID], th, epb, pub);
		new_node->epoch = (h->current & MASK_EPOCH);
		new_node->epoch = queue->global_epoch;
		#if SINGLE_COUNTER == 0	
		conc_enqueue_2 = ATOMIC_READ(&h->e_counter);
		#endif
		concurrent_enqueue += conc_enqueue_2 - conc_enqueue;
		conc_enqueue = conc_enqueue_2;		
		attempt_enqueue++;
	}

	numa_flush_current(queue, h, new_node, true);
	
	#if SINGLE_COUNTER == 0				
	conc_enqueue_2 = ATOMIC_READ(&h->e_counter);
	#endif
	performed_enqueue++;		
	concurrent_enqueue += conc_enqueue_2 - conc_enqueue;
	
	#if SINGLE_COUNTER == 0	
		ATOMIC_INC(&(h->e_counter));
	#else
		__sync_fetch_and_add(&h->counter, 1);
	#endif
	
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
double numa_nbc_dequeue(numa_nb_calqueue *queue, void** result)
{
	nbc_bucket_node *min, *min_next, 
					*left_node, *left_node_next, 
					*res, *tail, *array;
	table * h;
	
	unsigned long long current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned int size;
	unsigned int i;
	unsigned int counter = 0;
	unsigned int conc_dequeue = 0;
	unsigned int current_numa_node;
	double bucket_width, left_ts;
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	bool end = false;

	tail = g_tail;
	
	nbc_bucket_node * candidates[NUMA_NODES];
	table * c_h[NUMA_NODES];
	void * c_payloads[NUMA_NODES];
	bool completed[NUMA_NODES];
	bool old_epoch = false;
	for (i =0; i<NUMA_NODES;i++)
		completed[i] = false;
	
begin:
	epoch = queue->global_epoch;
	for (i =0; i<NUMA_NODES;i++)
	{
		counter = 0;
		if (completed[i])
			continue;
		candidates[i] = NULL;
		current_numa_node = i;
		end = false;
		do
		{
			counter = 0;
			h = read_table(&queue->hashtable[current_numa_node], th, epb, pub);
			current = h->current;
			size = h->size;
			array = h->array;
			bucket_width = h->bucket_width;
			#if SINGLE_COUNTER == 0
			conc_dequeue = ATOMIC_READ(&h->d_counter);
			#endif
			index = current >> 32;
			//epoch = current & MASK_EPOCH;

			assertf(
					index+1 > MASK_EPOCH, 
					"\nOVERFLOW INDEX:%llu  BW:%.10f SIZE:%u TAIL:%p TABLE:%p NUM_ELEM:%u\n",
					index, bucket_width, size, tail, h, ATOMIC_READ(&h->counter)
				);
			min = array + (index++ % size);

			left_node = min_next = min->next;
			
			if(is_marked(min_next, MOV))
				continue;
			
			while(left_node->epoch <= epoch)
			{	
				left_node_next = left_node->next;
				left_ts = left_node->timestamp;
				res = left_node->payload;
				c_payloads[i] = res;
				c_h[i] = h;
							
				if(!is_marked(left_node_next))
				{
					if(left_ts < index*bucket_width)
					{
						candidates[i] = left_node;
						completed[i] = end = true;
						
						break;
						/*
						left_node_next = FETCH_AND_OR(&left_node->next, DEL);
						if(!is_marked(left_node_next))
						{
							concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
							scan_list_length += counter;						
							performed_dequeue++;
							attempt_dequeue++;
							#if LOG_DEQUEUE == 1
								LOG("DEQUEUE: %f %u - %llu %llu\n",left_ts, left_node->counter, index, index % size);
							#endif
							*result = res;
							ATOMIC_INC(&(h->d_counter));
							return left_ts;
						}
						*/
					}
					else
					{
						if(counter > 0 && BOOL_CAS(&(min->next), min_next, left_node))
						{
							connect_to_be_freed_node_list(min_next, counter);
						}
						if(left_node == tail && size == 1 )
						{
							#if LOG_DEQUEUE == 1
							LOG("DEQUEUE: NULL 0 - %llu %llu\n", index, index % size);
							#endif
							*result = NULL;
							#if SINGLE_COUNTER == 0	
							concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
							#endif
							scan_list_length += counter;						
							attempt_dequeue++;
							
							candidates[i] = left_node;
							completed[i] = end = true;
							break;
							//return INFTY;
						}
						//double rand;			// <----------------------------------------
						//drand48_r(&seedT, &rand); 
						//if(rand < 1.0/2)
						if(h->current == current)
							BOOL_CAS(&(h->current), current, ((index << 32) | epoch) );
						#if SINGLE_COUNTER == 0	
						concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
						#endif
						scan_list_length += counter;						
						attempt_dequeue++;
						break;	
					}
					
				}
				
				old_epoch = left_node->epoch > epoch;
				if(old_epoch)
					goto begin;
				
				if(is_marked(left_node_next, MOV))
					break;
				
				left_node = get_unmarked(left_node_next);
				counter++;
			}

		}while(!end);
		
	}
	//printf("CANDIDATE A %p %f\n", candidates[0], candidates[0]->timestamp);
	//printf("CANDIDATE B %p %f\n", candidates[1], candidates[1]->timestamp);
	//g_tail->next->next->next = 0; 
	
	if( candidates[0] == candidates[1])
	{
		*result = NULL;
		return INFTY;
	}
	
	if(candidates[0]->timestamp < candidates[1]->timestamp)
		i=0;
	else
		i=1;
	
	
	h = c_h					[i];
	left_node = candidates	[i];
	res = c_payloads		[i];
	left_ts = candidates	[i]->timestamp;
	
	left_node_next = FETCH_AND_OR(&left_node->next, DEL);
	if(!is_marked(left_node_next))
	{
		#if SINGLE_COUNTER == 0	
		concurrent_dequeue += ATOMIC_READ(&h->d_counter) - conc_dequeue;
		#endif
		scan_list_length += counter;						
		performed_dequeue++;
		attempt_dequeue++;
		#if LOG_DEQUEUE == 1
			LOG("DEQUEUE: %f %u - %llu %llu\n",left_ts, left_node->counter, index, index % size);
		#endif
		*result = res;
	
		#if SINGLE_COUNTER == 0	
			ATOMIC_INC(&(h->d_counter));
		#else
			__sync_fetch_and_add(&h->counter, -1);
		#endif
		return left_ts;
	}
	else
	{
		completed[i] = false;
		goto begin;
	}
	return INFTY;
}

