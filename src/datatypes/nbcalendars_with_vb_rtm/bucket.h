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

#define FREEZE_FOR_MOV (1ULL << 63)
#define FREEZE_FOR_DEL (1ULL << 62)


#define GC_BUCKETS   0
#define GC_INTERNALS 1


typedef struct __node_t node_t;
struct __node_t
{
	void *payload;
	pkey_t timestamp;  				// key
	char pad[8-sizeof(pkey_t)];
	unsigned int tie_breaker;
	node_t * volatile next;
};

/**
 *  Struct that define a bucket
 */ 

typedef struct __bucket_t bucket_t;
struct __bucket_t
{
	volatile unsigned int epoch;			//enqueue's epoch
	unsigned int index; 		// used to resolve the conflict with same timestamp using a FIFO policy
	unsigned int type;
	unsigned int pad;
	node_t tail;
	node_t head;	
	volatile unsigned long long extractions;
	bucket_t * volatile next;
};


extern void init_bucket_subsystem();
extern bucket_t* bucket_alloc();
extern node_t* node_alloc();
extern void bucket_unsafe_free(bucket_t *ptr);
extern void bucket_safe_free(bucket_t *ptr);

extern int extract_from_bucket(bucket_t *bckt, void ** result, pkey_t *ts, unsigned int epoch);
extern int bucket_connect(bucket_t *bckt, pkey_t timestamp, unsigned int tie_breaker, void* payload);
extern int increase_bucket_epoch(bucket_t *bckt, unsigned int epoch);
extern int need_compact(bucket_t *bckt);
extern int compact_bucket(bucket_t *bckt);


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




static inline int is_freezed(unsigned long long extractions){
	return (extractions >> 32) != 0ULL;
}


static inline int is_freezed_for_mov(unsigned long long extractions){
	return (extractions >> 32) != 0ULL && (extractions & FREEZE_FOR_MOV);
}


static inline int is_freezed_for_del(unsigned long long extractions){
	return (extractions >> 32) != 0ULL && (extractions & FREEZE_FOR_DEL);
}


static inline void complete_freeze(bucket_t *bckt){
	unsigned long long old_extractions = 0;
	void *old_next = NULL;
	
	old_extractions = bckt->extractions;

	do{
		old_next = bckt->next;
	}while(is_marked(old_next, VAL) && is_freezed_for_del(old_extractions) && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(old_next, DEL)));


	do{
		old_next = bckt->next;
	}while(is_marked(old_next, VAL) && is_freezed_for_mov(old_extractions) && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(old_next, MOV)));

}


static inline void freeze_from_mov_to_del(bucket_t *bckt){
	unsigned long long old_extractions = 0;
	unsigned long long new_extractions = 0;
	void *old_next = NULL;
	
	old_extractions = bckt->extractions;
	while(is_freezed_for_mov(old_extractions)){
		new_extractions = (old_extractions  & (~(1ULL << 63))) | (1ULL << 62);
		if(__sync_bool_compare_and_swap(&bckt->extractions, old_extractions, new_extractions))
			break;
		old_extractions = bckt->extractions;
	}
	
	old_extractions = bckt->extractions;
	//printf("%p Is freeze_for_del %d\n", bckt, is_freezed_for_del(old_extractions));
	do{
		old_next = bckt->next;
	}while(is_marked(old_next, MOV) && is_freezed_for_del(old_extractions) && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(get_unmarked(old_next), DEL)));


}


static inline void freeze_for_del(bucket_t *bckt){
	unsigned long long old_extractions = 0;
	unsigned long long new_extractions = 0;
	
	old_extractions = bckt->extractions;
	while(!is_freezed(old_extractions)){
		new_extractions = (old_extractions << 32) | old_extractions | (1ULL << 62);
		if(__sync_bool_compare_and_swap(&bckt->extractions, old_extractions, new_extractions))
			break;
		old_extractions = bckt->extractions;
	}

	complete_freeze(bckt);

}

static inline void freeze_for_mov(bucket_t *bckt){
	unsigned long long old_extractions = 0;
	unsigned long long new_extractions = 0;
	
	old_extractions = bckt->extractions;
	while(!(old_extractions >> 32 )){
		new_extractions = (old_extractions << 32) | old_extractions | (1ULL << 63);
		if(__sync_bool_compare_and_swap(&bckt->extractions, old_extractions, new_extractions))
			break;
		old_extractions = bckt->extractions;
	}

	complete_freeze(bckt);
}



#endif