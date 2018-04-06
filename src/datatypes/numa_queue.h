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


#include "nb_calqueue.h"


#define NID nid


extern __thread unsigned int NID;
extern __thread unsigned int NUMA_NODES;


extern nb_calqueue* numa_nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void numa_nbc_enqueue(nb_calqueue* queue, double timestamp, void* payload);
extern double numa_nbc_dequeue(nb_calqueue *queue, void** result);
extern double numa_nbc_prune(nb_calqueue *queue);
extern void numa_nbc_report(unsigned int TID);

#endif /* DATATYPES_NONBLOCKING_QUEUE_H_ */
