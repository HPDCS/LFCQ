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
 *  Author: Romolo Marotta
 */

#ifndef DATATYPES_COMMON_NONBLOCKING_CALQUEUE_H_
#define DATATYPES_COMMON_NONBLOCKING_CALQUEUE_H_

#include <float.h>
#include <math.h>
#include "../arch/atomic.h"
#include "../mm/garbagecollector.h"
#include "../utils/hpdcs_utils.h"

#include "gc/ptst.h"

extern __thread ptst_t *ptst;
extern int gc_id[];


#define SAMPLE_SIZE 25
#define HEAD_ID 0
#define MAXIMUM_SIZE 1048576 //524288 //262144 //131072 //65536 //32768
#define MINIMUM_SIZE 1
#define MAX_NUMA_NODES 16

#define FLUSH_SMART 1
#define ENABLE_EXPANSION 1
#define ENABLE_PRUNE 0
#define ENABLE_HIGH_STATITISTICS 1

#define READTABLE_PERIOD 63
#define COMPACT_RANDOM_ENQUEUE 1
#define DISTANCE_FROM_CURRENT 0.0 

#define BASE 1000000ULL 
#ifndef RESIZE_PERIOD_FACTOR 
#define RESIZE_PERIOD_FACTOR 2000ULL
#endif
#define RESIZE_PERIOD RESIZE_PERIOD_FACTOR*BASE



#define INFTY DBL_MAX
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


#define REMOVE_DEL	 0
#define REMOVE_DEL_INV	 1

#define is_marked(...) macro_dispatcher(is_marked, __VA_ARGS__)(__VA_ARGS__)
#define is_marked2(w,r) is_marked_2(w,r)
#define is_marked1(w)   is_marked_1(w)
#define is_marked_2(pointer, mask)	( (UNION_CAST(pointer, unsigned long long) & MASK_MRK) == mask )
#define is_marked_1(pointer)		(UNION_CAST(pointer, unsigned long long) & MASK_MRK)
#define get_unmarked(pointer)		(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_PTR), void *))
#define get_marked(pointer, mark)	(UNION_CAST((UNION_CAST(pointer, unsigned long long)|(mark)), void *))
#define get_mark(pointer)			(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_MRK), unsigned long long))

#define MOV_FOUND 	3
#define OK			1
#define ABORT		0



/**
 *  Struct that define a node in a bucket
 */ 

typedef struct __bucket_node nbc_bucket_node;
struct __bucket_node
{
	void *payload;  				// general payload
	unsigned long long epoch;		//enqueue's epoch
	//16
	double timestamp;  				// key
	unsigned int counter; 			// used to resolve the conflict with same timestamp using a FIFO policy
	unsigned int nid; 				// used to resolve the conflict with same timestamp using a FIFO policy
	//32
	nbc_bucket_node * tail;
	nbc_bucket_node * volatile next;	// pointer to the successor
	//48
	nbc_bucket_node * volatile replica;	// pointer to the replica
	nbc_bucket_node * volatile next_next;
	//64
};


//extern nbc_bucket_node *g_tail;

typedef struct table table;
struct table
{
	table * volatile new_table;		// 8
	unsigned int size;
    unsigned int pad;				//16        
	double bucket_width;			//24        
	nbc_bucket_node* array;			//32
	unsigned int read_table_period; 
	unsigned int last_resize_count; //40
	char zpad4[24];
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
	nbc_bucket_node * tail;
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
extern __thread hpdcs_gc_status malloc_status;

extern __thread nbc_bucket_node *to_free_tables_old;
extern __thread nbc_bucket_node *to_free_tables_new;
   
extern __thread unsigned long long concurrent_dequeue;
extern __thread unsigned long long performed_dequeue ;
extern __thread unsigned long long attempt_dequeue ;
extern __thread unsigned long long scan_list_length ;
extern __thread unsigned long long scan_list_length_en ;


extern __thread unsigned long long concurrent_enqueue;
extern __thread unsigned long long performed_enqueue ;
extern __thread unsigned long long attempt_enqueue ;
extern __thread unsigned long long flush_current_attempt       ;
extern __thread unsigned long long flush_current_success       ;
extern __thread unsigned long long flush_current_fail  ;
extern __thread unsigned long long read_table_count    ;


extern unsigned int * volatile prune_array;
extern unsigned int threads;

extern nbc_bucket_node *g_tail;
extern __thread hpdcs_gc_status malloc_status;

extern __thread unsigned long long near;
extern __thread unsigned long long num_cas;
extern __thread unsigned long long num_cas_useful;
extern __thread unsigned long long dist;


extern void set_new_table(table* h, unsigned int threshold, double pub, unsigned int epb, unsigned int counter);
extern bool insert_std(table* hashtable, nbc_bucket_node** new_node, int flag);
extern table* read_table(table * volatile *hashtable, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket);
extern void block_table(table* h);
extern double compute_mean_separation_time(table* h, unsigned int new_size, unsigned int threashold, unsigned int elem_per_bucket);
extern void migrate_node(nbc_bucket_node *right_node,	table *new_h);
extern void search(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker, nbc_bucket_node **left_node, nbc_bucket_node **right_node, int flag);
extern void flush_current(table* h, unsigned long long newIndex, unsigned int size, nbc_bucket_node* node);
extern double nbc_prune();
extern void nbc_report(unsigned int);
extern unsigned int search_and_insert(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						 int flag, nbc_bucket_node *new_node_pointer, nbc_bucket_node **new_node);



/**
 *  This function is an helper to allocate a node and filling its fields.
 *
 *  @author Romolo Marotta
 *
 *  @param payload is a pointer to the referred payload by the node
 *  @param timestamp the timestamp associated to the payload
 *
 *  @return the pointer to the allocated node
 *
 */
static inline nbc_bucket_node* node_malloc(void *payload, double timestamp, unsigned int tie_breaker)
{
	nbc_bucket_node* res;
	
	//res = mm_node_malloc(&malloc_status);
	
    res = gc_alloc(ptst, gc_id[0]);
    
	if (unlikely(is_marked(res) || res == NULL))
	{
		error("%lu - Not aligned Node or No memory\n", TID);
		abort();
	}

	res->counter = tie_breaker;
	res->next = NULL;
	res->replica = NULL;
	res->payload = payload;
	res->epoch = 0;
	res->timestamp = timestamp;

	return res;
}

static inline void node_free(nbc_bucket_node *pointer)
{	
	mm_node_free(&malloc_status, pointer);
}


/**
 * This function connect to a private structure marked
 * nodes in order to free them later, during a synchronisation point
 *
 * @author Romolo Marotta
 *
 * @param queue used to associate freed nodes to a queue
 * @param start the pointer to the first node in the disconnected sequence
 * @param number of node to connect to the to_be_free queue
 * @param timestamp   the timestamp of the last disconnected node
 *
 */
static inline void connect_to_be_freed_node_list(nbc_bucket_node *start, unsigned int counter)
{
	//mm_node_collect_connected_nodes(&malloc_status, get_unmarked(start), counter);
	nbc_bucket_node *tmp_next;
	start = get_unmarked(start);
	while(start != NULL && counter-- != 0)                //<-----NEW
	{                                                   //<-----NEW
		tmp_next = start->next;                           //<-----NEW
		gc_free(ptst, (void *)start, gc_id[0]);
		start =  get_unmarked(tmp_next);                  //<-----NEW
	}                                                   //<-----NEW
}

static inline void connect_to_be_freed_table_list(table *h)
{
	nbc_bucket_node *tmp = node_malloc(h, INFTY, 0);
	tmp->next = to_free_tables_new;
	to_free_tables_new = tmp;
}

static inline bool is_marked_for_search(void *pointer, unsigned int research_flag)
{
	unsigned long long mask_value = (UNION_CAST(pointer, unsigned long long) & MASK_MRK);
	
	return 
		(/*research_flag == REMOVE_DEL &&*/ mask_value == DEL) 
		|| (research_flag == REMOVE_DEL_INV && (mask_value == INV) );
}


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
static inline unsigned long long hash(double timestamp, double bucket_width)
{
	double tmp1;
	double tmp2;
	double res_d = (timestamp / bucket_width);
	unsigned long long res =  (unsigned long long) res_d;
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

	tmp1 = res * bucket_width;
	tmp2 = (res+1) * bucket_width;
	
	upA = - LESS(timestamp, tmp1);
	upB = GEQ(timestamp, tmp2 );
		
	return res+ upA + upB;

}


static inline void clflush(volatile void *p)
{
        asm volatile ("clflush (%0)" :: "r"(p));        
}

static inline void prefetch(void *p)
{
	__builtin_prefetch(p, 1, 3);
}




#endif /* DATATYPES_COMMON_NONBLOCKING_CALQUEUE_H_ */
