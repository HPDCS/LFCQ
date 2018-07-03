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
 * nonblockingqueue.h
 *
 *  Created on: Jul 13, 2015    
 *      Author: Romolo Marotta   
 */

#ifndef DATATYPES_WORKER_CALQUEUE_H_
#define DATATYPES_WORKER_CALQUEUE_H_


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

extern worker_calqueue* worker_nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void worker_nbc_enqueue(worker_calqueue* queue, double timestamp, void* payload);
extern double worker_nbc_dequeue(worker_calqueue *queue, void** result);

#endif /* DATATYPES_NONBLOCKING_QUEUE_H_ */
