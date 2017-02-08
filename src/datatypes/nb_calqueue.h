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

#ifndef DATATYPES_NONBLOCKING_CALQUEUE_H_
#define DATATYPES_NONBLOCKING_CALQUEUE_H_

#include <float.h>
#include <math.h>
#include "../arch/atomic.h"

#define INFTY DBL_MAX
//#define LESS(a,b) 		( (a) < (b) && !D_EQUAL((a), (b)) )
//#define LEQ(a,b)		( (a) < (b) ||  D_EQUAL((a), (b)) )
//#define D_EQUAL(a,b) 	(fabs((a) - (b)) < DBL_EPSILON)
//#define GEQ(a,b) 		( (a) > (b) ||  D_EQUAL((a), (b)) )
//#define GREATER(a,b) 	( (a) > (b) &&  !D_EQUAL((a), (b)) )
#define LESS(a,b) 		( (a) <  (b) )
#define LEQ(a,b)		( (a) <= (b) )
#define D_EQUAL(a,b) 	( (a) == (b) )
#define GEQ(a,b) 		( (a) >  (b) )
#define GREATER(a,b) 	( (a) >= (b) )
#define SAMPLE_SIZE 25
#define HEAD_ID 0
#define MAXIMUM_SIZE 1048576 //524288 //262144 //131072 //65536 //32768
#define MINIMUM_SIZE 1

#define FLUSH_SMART 1
#define ENABLE_EXPANSION 1
#define ENABLE_PRUNE 1

#define TID tid

extern __thread unsigned int TID;
extern __thread struct drand48_data seedT;


/**
 *  Struct that define a node in a bucket
 */ 

typedef struct __bucket_node nbc_bucket_node;
struct __bucket_node
{
	void *payload;  				// general payload
	double timestamp;  				// key
	unsigned long long epoch;		//enqueue's epoch
	unsigned int counter; 			// used to resolve the conflict with same timestamp using a FIFO policy
	unsigned int pad; 			// used to resolve the conflict with same timestamp using a FIFO policy
	nbc_bucket_node * volatile next;	// pointer to the successor
	nbc_bucket_node * volatile replica;	// pointer to the replica
	//char pad2[16];
};


/**
 *
 */

typedef struct table table;
struct table
{
	nbc_bucket_node * array;
	double bucket_width;
	unsigned int size;
	unsigned int pad;
	table * volatile new_table;
	char zpad4[32];
	atomic_t e_counter;
	char zpad3[60];
	atomic_t d_counter;
	char zpad1[60];
	volatile unsigned long long current;
	char zpad2[56];
};


typedef struct nb_calqueue nb_calqueue;
struct nb_calqueue
{
	unsigned int threshold;
	unsigned int elem_per_bucket;
	double perc_used_bucket;
	double pub_per_epb;
	char zpad9[40];
	table * volatile hashtable;
};

extern void nbc_enqueue(nb_calqueue *queue, double timestamp, void* payload);
extern double nbc_dequeue(nb_calqueue *queue, void **payload);
extern double nbc_prune(nb_calqueue *queue);
extern nb_calqueue* nb_calqueue_init(unsigned int threashold, double perc_used_bucket, unsigned int elem_per_bucket);

#endif /* DATATYPES_NONBLOCKING_QUEUE_H_ */
