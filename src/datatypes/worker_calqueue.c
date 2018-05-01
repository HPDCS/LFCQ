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

#include "nb_calqueue.h"
#include "worker_calqueue.h"


/**
 * This function create an instance of a non-blocking calendar queue.
 *
 * @author Romolo Marotta
 *
 * @param queue_size is the inital size of the new queue
 *
 * @return a pointer a new queue
 */
 
 __thread unsigned int local_spin = 0;
 
worker_calqueue* worker_nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{
	unsigned int i = 0;
	//unsigned int new_threshold = 1;
	unsigned int res_mem_posix = 0;

	threads = threshold;
	prune_array = calloc(threshold*threshold, sizeof(unsigned int));

	worker_calqueue* ret_val = calloc(1, sizeof(worker_calqueue));
	if(ret_val == NULL)
		error("No enough memory to allocate queue\n");
	
	nb_calqueue *res = &(ret_val->real_queue);
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
		res->hashtable->array[i].timestamp = i * 1.0;
		res->hashtable->array[i].counter = 0;
	}
	
	
	for(i = 0; i< 32; i++){
		ret_val->pending_ops[i*8].id_op = 0;
		ret_val->pending_ops[i*8].timestamp = 0;
		ret_val->pending_ops[i*8].payload = NULL;
	}

	return ret_val;
}


void helper(worker_calqueue* queue)
{
	unsigned long long i = 0, cur_op= 0;
	op_descriptor * volatile desc = NULL;
	for(i = 0; i< threads; i++){
		desc = queue->pending_ops + i*8;
		cur_op = desc->id_op;
		if(cur_op > 2ULL && __sync_bool_compare_and_swap(&desc->id_op, cur_op, RUNNING_DESCRIPTOR))
		{
			if(cur_op == ENQUEUE_DESCRIPTOR)
				nbc_enqueue(&queue->real_queue, desc->timestamp, desc->payload);
			else if (cur_op == DEQUEUE_DESCRIPTOR)
				queue->pending_ops[i*8].timestamp = nbc_dequeue(&queue->real_queue,desc->payload);
			else
			{	
				desc = NULL;
				printf("WTF!!! %llu\n", cur_op);
				desc->id_op = 0;
			}
			__sync_bool_compare_and_swap(&desc->id_op, RUNNING_DESCRIPTOR, DONE_DESCRIPTOR);
		}
	}
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
void worker_nbc_enqueue(worker_calqueue* queue, double timestamp, void* payload)
{
	op_descriptor *desc = NULL;
	if(NID != 0)
	{
		desc = &queue->pending_ops[TID*8];
		desc->timestamp = timestamp;
		desc->payload = payload;
		__sync_bool_compare_and_swap(&desc->id_op, EMPTY_DESCRIPTOR, ENQUEUE_DESCRIPTOR);
		while(desc->id_op != DONE_DESCRIPTOR){
			if(local_spin++ % 2500000000 == 0){
				//printf("%d E -FUCK %llu\n", TID, desc->id_op);
				if(desc->id_op == ENQUEUE_DESCRIPTOR){
					if(__sync_bool_compare_and_swap(&desc->id_op, ENQUEUE_DESCRIPTOR, EMPTY_DESCRIPTOR)){
						nbc_enqueue(&queue->real_queue, timestamp, payload);
						return;
					}
				}
			}
		}
		__sync_bool_compare_and_swap(&desc->id_op, DONE_DESCRIPTOR, EMPTY_DESCRIPTOR);
	}
	else{
		helper(queue);
		nbc_enqueue(&queue->real_queue, timestamp, payload);
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
double worker_nbc_dequeue(worker_calqueue *queue, void** result)
{
	op_descriptor *desc;
	if(NID != 0)
	{
			desc = &queue->pending_ops[TID*8];
			desc->payload = result;
			__sync_bool_compare_and_swap(&desc->id_op, EMPTY_DESCRIPTOR, DEQUEUE_DESCRIPTOR);
			while(desc->id_op != DONE_DESCRIPTOR){
				if(local_spin++ % 25000000 == 0){
					//printf("%d D-FUCK %llu\n", TID, desc->id_op);
					if(desc->id_op == DEQUEUE_DESCRIPTOR){
						if(__sync_bool_compare_and_swap(&desc->id_op, DEQUEUE_DESCRIPTOR, EMPTY_DESCRIPTOR)){
							return nbc_dequeue(&queue->real_queue,result);
						}
					}
				}
			}
			
			__sync_bool_compare_and_swap(&desc->id_op, DONE_DESCRIPTOR, EMPTY_DESCRIPTOR);
			*result = queue->pending_ops[TID*8].payload;
			return queue->pending_ops[TID*8].timestamp;
	}
	else
	{
		helper(queue);
		return nbc_dequeue(&queue->real_queue,result);
	}
}

