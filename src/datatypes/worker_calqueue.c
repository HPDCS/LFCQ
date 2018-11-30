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


#include "common_nb_calqueue.h"

#define NID nid

extern __thread unsigned int NID;
extern unsigned int NUMA_NODES;

#define EMPTY_DESCRIPTOR 	0ULL
#define DONE_DESCRIPTOR 	1ULL
#define RUNNING_DESCRIPTOR 	2ULL
#define ENQUEUE_DESCRIPTOR 	4ULL
#define DEQUEUE_DESCRIPTOR 	8ULL

typedef struct op_descriptor op_descriptor;
struct op_descriptor
{
	volatile unsigned long long id_op;
	volatile double timestamp;
	//16
	void * volatile payload;
	//24
	char pad[40];
	
};

typedef struct worker_calqueue worker_calqueue;
struct worker_calqueue
{
	nb_calqueue *real_queue;
	unsigned long long num_threads;
	op_descriptor *pending_ops;

};

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
 __thread unsigned int from_last_help = 0;
 
#define NUM_MAIN_NODES 0
#define CORE_PER_NODE 4
 
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
	
	nb_calqueue *res = nbc_init(threshold, perc_used_bucket, elem_per_bucket); //&(ret_val->real_queue);
	ret_val->real_queue = res;
	ret_val->pending_ops = calloc(threads, sizeof(op_descriptor));
		
	return ret_val;
}


void helper(worker_calqueue* queue)
{
	unsigned long long i = 0, cur_op= 0;
	
	op_descriptor * volatile desc = NULL;
	//if(from_last_help++%2)
	//	return;
	for(i = 0; i< threads; i++)
	{
		desc = queue->pending_ops + i;
		cur_op = desc->id_op;
		if(cur_op > 2ULL && __sync_bool_compare_and_swap(&desc->id_op, cur_op, RUNNING_DESCRIPTOR))
		{
			if(cur_op == ENQUEUE_DESCRIPTOR)
				nbc_enqueue(queue->real_queue, desc->timestamp, desc->payload);
			else if (cur_op == DEQUEUE_DESCRIPTOR)
				queue->pending_ops[i].timestamp = nbc_dequeue(queue->real_queue,desc->payload);
			else
			{	
				desc = NULL;
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
	if(NID > NUM_MAIN_NODES)
	{
		desc = &queue->pending_ops[TID];
		desc->timestamp = timestamp;
		desc->payload = payload;
		__sync_bool_compare_and_swap(&desc->id_op, EMPTY_DESCRIPTOR, ENQUEUE_DESCRIPTOR);
		while(desc->id_op != DONE_DESCRIPTOR){
			if(0 && (local_spin++ % 2500000000 == 0) ){
				if(desc->id_op == ENQUEUE_DESCRIPTOR){
					if(__sync_bool_compare_and_swap(&desc->id_op, ENQUEUE_DESCRIPTOR, EMPTY_DESCRIPTOR)){
						nbc_enqueue(queue->real_queue, timestamp, payload);
						return;
					}
				}
			}
		}
		__sync_bool_compare_and_swap(&desc->id_op, DONE_DESCRIPTOR, EMPTY_DESCRIPTOR);
	}
	else{
		helper(queue);
		nbc_enqueue(queue->real_queue, timestamp, payload);
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
	if(NID > NUM_MAIN_NODES)
	{
			desc = &queue->pending_ops[TID];
			desc->payload = result;
			__sync_bool_compare_and_swap(&desc->id_op, EMPTY_DESCRIPTOR, DEQUEUE_DESCRIPTOR);
			while(desc->id_op != DONE_DESCRIPTOR){
				if( 0 && (local_spin++ % 25000000 == 0) ){
					if(desc->id_op == DEQUEUE_DESCRIPTOR){
						if(__sync_bool_compare_and_swap(&desc->id_op, DEQUEUE_DESCRIPTOR, EMPTY_DESCRIPTOR)){
							return nbc_dequeue(queue->real_queue,result);
						}
					}
				}
			}
			
			__sync_bool_compare_and_swap(&desc->id_op, DONE_DESCRIPTOR, EMPTY_DESCRIPTOR);
			*result = queue->pending_ops[TID].payload;
			return queue->pending_ops[TID].timestamp;
	}
	else
	{
		helper(queue);
		return nbc_dequeue(queue->real_queue,result);
	}
}

