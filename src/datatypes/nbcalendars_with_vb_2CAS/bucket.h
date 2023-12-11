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
 *  bucket.h
 *
 *  Author: Romolo Marotta
 */

#ifndef BUCKET_H_
#define BUCKET_H_

#include <stdlib.h>
#include "../../key_type.h"
#include "../../gc/gc.h"
#include "../../utils/hpdcs_utils.h"
#include "nb_lists_defs.h"


extern __thread ptst_t *ptst;
extern int gc_aid[];
extern int gc_hid[];

#define HEAD 0ULL
#define ITEM 1ULL
#define TAIL 2ULL

#define GC_BUCKETS   0
#define GC_INTERNALS 1

#define UNROLLED_FACTOR 128


typedef struct __entry_t entry_t;
struct __entry_t
{
	void *payload;
	pkey_t timestamp;  				// key
	char pad[8-sizeof(pkey_t)];
	unsigned int counter;
	volatile unsigned int valid;
	entry_t * volatile replica;
};

typedef struct __unrolled_node_t unrolled_node_t;
struct __unrolled_node_t
{
	unrolled_node_t *next;
	unsigned int count;
	unsigned int pad;
	entry_t array[UNROLLED_FACTOR];
};


typedef struct __cas128_t cas128_t;
struct __cas128_t
{	
	// TODO ENDIANESS DEPENDENT
	volatile unsigned int extractions_counter;
	volatile unsigned int extractions_counter_old;
	unrolled_node_t * volatile entries;
};


typedef union __atomic128_t atomic128_t;
union __atomic128_t{
	volatile cas128_t    a;
	volatile __uint128_t b;
	volatile unsigned long long c[2];
	void volatile * d[2];
};


/**
 *  Struct that define a bucket
 */ 

typedef struct __bucket_t bucket_t;
struct __bucket_t
{
	unsigned int epoch;			//enqueue's epoch
	unsigned int index; 		// used to resolve the conflict with same timestamp using a FIFO policy
	
	volatile unsigned int new_epoch;
	unsigned int type;
	//48
	volatile atomic128_t cas128_field;
	
	bucket_t *next;
};


extern void init_bucket_subsystem();
extern bucket_t* bucket_alloc();
extern void bucket_unsafe_free(bucket_t *ptr);
extern void bucket_safe_free(bucket_t *ptr);
extern bucket_t* new_bucket_with_entry(unsigned int epoch, unsigned int index, entry_t new_item);
extern int bucket_connect(bucket_t *bckt, atomic128_t field, entry_t item);
extern bucket_t* get_clone(bucket_t *bckt);
extern int bucket_validate_item(bucket_t *bckt,  entry_t item);
extern int bucket_connect_invalid(bucket_t *bckt, atomic128_t field, entry_t item);





static inline int need_compact(atomic128_t field){
	return field.a.entries != NULL && field.a.extractions_counter > field.a.entries->count;
}

static inline int is_freezed(atomic128_t field){
	return field.a.extractions_counter_old != 0;
}

static inline int is_freezed_for_mov(atomic128_t field){
	return field.a.extractions_counter_old & (1ULL<<63) ;
}

static inline int freeze_bucket_for_mov(bucket_t *bckt, atomic128_t old_field){
	atomic128_t new_field = old_field;
	new_field.a.extractions_counter_old = new_field.a.extractions_counter | (1ULL << 63);
	return __sync_bool_compare_and_swap(&bckt->cas128_field.b, old_field.b, new_field.b);
}

static inline int freeze_bucket_for_replacement(bucket_t *bckt, atomic128_t old_field){
	atomic128_t new_field = old_field;
	new_field.a.extractions_counter_old = new_field.a.extractions_counter;
	return __sync_bool_compare_and_swap(&bckt->cas128_field.b, old_field.b, new_field.b);
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
static inline void connect_to_be_freed_node_list(bucket_t *start, unsigned int counter)
{
	bucket_t *tmp_next;
	start = get_unmarked(start);
	while(start != NULL && counter-- != 0)            
	{                                                 
		tmp_next = start->next;                 
		bucket_safe_free(start);
		start =  get_unmarked(tmp_next);              
	}                                                 
}


#endif