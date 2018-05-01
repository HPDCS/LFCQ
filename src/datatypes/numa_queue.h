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

#ifndef DATATYPES_NONBLOCKING_NUMA_CALQUEUE_H_
#define DATATYPES_NONBLOCKING_NUMA_CALQUEUE_H_


#include "common_nb_calqueue.h"


#define NID nid


extern __thread unsigned int NID;
extern unsigned int NUMA_NODES;


typedef struct numa_nb_calqueue numa_nb_calqueue;
struct numa_nb_calqueue
{
	unsigned int threshold;
	unsigned int elem_per_bucket;
	double perc_used_bucket;
	double pub_per_epb;
	volatile unsigned int global_epoch;
	char zpad9[36];
	table * volatile hashtable[MAX_NUMA_NODES];
};


extern numa_nb_calqueue* numa_nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void numa_nbc_enqueue(numa_nb_calqueue* queue, double timestamp, void* payload);
extern double numa_nbc_dequeue(numa_nb_calqueue *queue, void** result);

#endif /* DATATYPES_NONBLOCKING_QUEUE_H_ */
