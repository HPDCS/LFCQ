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
 *  common_nb_calqueue.h
 *
 *  Author: Romolo Marotta
 */

#ifndef DATATYPES_COMMON_NONBLOCKING_CALQUEUE_H_
#define DATATYPES_COMMON_NONBLOCKING_CALQUEUE_H_

#include <float.h>
#include <math.h>

#include "bucket.h"
#include "../../key_type.h"
#include "../../arch/atomic.h"
#include "../../gc/ptst.h"

extern __thread ptst_t *ptst;
extern int gc_aid[];
extern int gc_hid[];


#define SAMPLE_SIZE 25

#define MINIMUM_SIZE 1
#define MAX_NUMA_NODES 16

#define ENABLE_EXPANSION 1
#define READTABLE_PERIOD 63
#define COMPACT_RANDOM_ENQUEUE 0

#define BASE 1000000ULL 
#ifndef RESIZE_PERIOD_FACTOR 
#define RESIZE_PERIOD_FACTOR 4ULL //2000ULL
#endif
#define RESIZE_PERIOD RESIZE_PERIOD_FACTOR*BASE


#define LESS(a,b) 		( (a) <  (b) )
#define LEQ(a,b)		( (a) <= (b) )
#define D_EQUAL(a,b) 	( (a) == (b) )
#define GEQ(a,b) 		( (a) >= (b) )
#define GREATER(a,b) 	( (a) >  (b) )

#define TID tid


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

#define ATOMIC_READ					atomic_read



//extern nbc_bucket_node *g_tail;

typedef struct table table;
struct table
{
	table * volatile new_table;		// 8
	unsigned int size;
    unsigned int pad;				//16        
	double bucket_width;			//24        
	bucket_t* array;			//32
	unsigned int read_table_period; 
	unsigned int last_resize_count; //40
	unsigned int resize_count; 		//44
	char zpad4[20];
	atomic_t e_counter;
	char zpad3[60];
	atomic_t d_counter;
	char zpad1[60];
	volatile unsigned long long current;
};

typedef struct nb_calqueue nb_calqueue;
struct nb_calqueue
{
	unsigned int threshold;
	unsigned int elem_per_bucket;
	double perc_used_bucket;
	// 16
	double pub_per_epb;
	bucket_t * tail;
	// 32
	unsigned int read_table_period;
	unsigned int pad;   
	// 64
	table * volatile hashtable;
	//char pad[24];
	// 64
};



extern __thread unsigned int TID;
extern __thread struct drand48_data seedT;

extern __thread unsigned long long concurrent_dequeue;
extern __thread unsigned long long performed_dequeue ;
extern __thread unsigned long long scan_list_length ;

extern __thread unsigned long long malloc_count;
extern __thread unsigned int read_table_count    ;
extern __thread unsigned long long near;
extern __thread unsigned long long num_cas;
extern __thread unsigned long long num_cas_useful;
extern __thread unsigned long long dist;


 
/**
 * This function computes the index of the destination bucket in the hashtable
 *
 * @author Romolo Marotta
 *
 * @param timestamp the value to be hashed
 * @param bucket_width the depth of a bucket
 *
 * @return the linear index of a given timestamp
 */
static inline unsigned int hash(pkey_t timestamp, double bucket_width)
{
	double tmp1, tmp2, res_d = (timestamp / bucket_width);
	long long res =  (long long) res_d;
	int upA = 0;
	int upB = 0;

	if(__builtin_expect(res_d > 4294967295, 0))
	{
		error("Probable Overflow when computing the index: "
				"TS=%e,"
				"BW:%e, "
				"TS/BW:%e, "
				"2^32:%e\n",
				timestamp, bucket_width, res_d,  pow(2, 32));
	}

	tmp1 = ((double) (res)	 ) * bucket_width;
	tmp2 = ((double) (res+1) )* bucket_width;
	
	upA = - LESS(timestamp, tmp1);
	upB = GEQ(timestamp, tmp2 );
		
	return (unsigned int) (res+ upA + upB);

}


extern table* read_table(table *volatile *curr_table_ptr, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket, bucket_t *tail);
extern bucket_t* search(bucket_t *head, bucket_t **old_left_next, bucket_t **right_bucket, unsigned int *distance, unsigned int index);



#endif /* DATATYPES_COMMON_NONBLOCKING_CALQUEUE_H_ */
