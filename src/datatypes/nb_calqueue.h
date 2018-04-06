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
#define MAX_NUMA_NODES 16

#define FLUSH_SMART 1
#define ENABLE_EXPANSION 1
#define ENABLE_PRUNE 1

#define TID tid

#define LOG_DEQUEUE 0
#define LOG_ENQUEUE 0

#define BOOL_CAS_ALE(addr, old, new)  CAS_x86(\
										UNION_CAST(addr, volatile unsigned long long *),\
										UNION_CAST(old,  unsigned long long),\
										UNION_CAST(new,  unsigned long long)\
									  )
									  	
#define BOOL_CAS_GCC(addr, old, new)  __sync_bool_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )

#define VAL_CAS_GCC(addr, old, new)  __sync_val_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )

#define VAL_CAS  VAL_CAS_GCC 
#define BOOL_CAS BOOL_CAS_GCC

#define FETCH_AND_AND 				__sync_fetch_and_and
#define FETCH_AND_OR 				__sync_fetch_and_or

#define ATOMIC_INC					atomic_inc_x86
#define ATOMIC_DEC					atomic_dec_x86

//#define ATOMIC_INC(x)					__sync_fetch_and_add( &((x)->count), 1)
//#define ATOMIC_DEC(x)					__sync_fetch_and_add( &((x)->count), -1)

#define ATOMIC_READ					atomic_read
//#define ATOMIC_READ(x)					__sync_fetch_and_add( &((x)->count), 0)

#define VAL (0ULL)
#define DEL (1ULL)
#define INV (2ULL)
#define MOV (3ULL)

#define MASK_PTR 	(-4LL)
#define MASK_MRK 	(3ULL)
#define MASK_DEL 	(-3LL)

#define MAX_UINT 			  (0xffffffffU)
#define MASK_EPOCH	(0x00000000ffffffffULL)
#define MASK_CURR	(0xffffffff00000000ULL)


#define REMOVE_DEL		 0
#define REMOVE_DEL_INV	 1

#define is_marked(...) macro_dispatcher(is_marked, __VA_ARGS__)(__VA_ARGS__)
#define is_marked2(w,r) is_marked_2(w,r)
#define is_marked1(w)   is_marked_1(w)
#define is_marked_2(pointer, mask)	( (UNION_CAST(pointer, unsigned long long) & MASK_MRK) == mask )
#define is_marked_1(pointer)		(UNION_CAST(pointer, unsigned long long) & MASK_MRK)
#define get_unmarked(pointer)		(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_PTR), void *))
#define get_marked(pointer, mark)	(UNION_CAST((UNION_CAST(pointer, unsigned long long)|(mark)), void *))
#define get_mark(pointer)			(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_MRK), unsigned long long))



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
	unsigned int nid; 				// used to resolve the conflict with same timestamp using a FIFO policy
	nbc_bucket_node * volatile next;	// pointer to the successor
	nbc_bucket_node * volatile replica;	// pointer to the replica
	//char pad2[16];
};


extern nbc_bucket_node *g_tail;

/**
 *
 */

typedef struct table table;
struct table
{
	table * volatile new_table;
	char zpad4[56];
	atomic_t e_counter;
	char zpad3[60];
	atomic_t d_counter;
	char zpad1[60];
	volatile unsigned long long current;
	char zpad2[56];
	unsigned int size;
	unsigned int pad;
	double bucket_width;
	nbc_bucket_node* array[MAX_NUMA_NODES];
};


typedef struct nb_calqueue nb_calqueue;
struct nb_calqueue
{
	unsigned int threshold;
	unsigned int elem_per_bucket;
	double perc_used_bucket;
	double pub_per_epb;
	unsigned long long global_epoch;
	char zpad9[32];
	table * volatile hashtable[MAX_NUMA_NODES];
};

extern void nbc_enqueue(nb_calqueue *queue, double timestamp, void* payload);
extern double nbc_dequeue(nb_calqueue *queue, void **payload);
extern double nbc_prune(nb_calqueue *queue);
nb_calqueue* nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void nbc_report(unsigned int);

#endif /* DATATYPES_NONBLOCKING_QUEUE_H_ */
