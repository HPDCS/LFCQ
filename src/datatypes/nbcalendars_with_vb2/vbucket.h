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

#ifndef VBUCKET_H_
#define VBUCKET_H_

#include <stdlib.h>
#include <immintrin.h>

#include "../../key_type.h"
#include "../../utils/rtm.h"
#include "../../gc/gc.h"
#include "../../utils/hpdcs_utils.h"
#include "nb_lists_defs.h"


extern __thread ptst_t *ptst;
extern int gc_aid[];
extern int gc_hid[];
extern __thread unsigned long long scan_list_length_en;
extern __thread unsigned long long scan_list_length;
extern __thread struct drand48_data seedT;



#define HEAD 0ULL
#define ITEM 1ULL
#define TAIL 2ULL

#define FREEZE_FOR_MOV (1ULL << 63)
#define FREEZE_FOR_DEL (1ULL << 62)
#define FREEZE_FOR_EPO (1ULL << 61)


#define GC_BUCKETS   0
#define GC_INTERNALS 1


typedef struct __node_t node_t;
struct __node_t
{
	void *payload;					// 8
	pkey_t timestamp;  				//  
	char pad[8-sizeof(pkey_t)];		// 16
	unsigned int tie_breaker;		// 20
	unsigned int pad2;				// 24
	node_t * volatile next;			// 32
};

/**
 *  Struct that define a bucket
 */ 
typedef struct __bucket_t bucket_t;
struct __bucket_t
{
	volatile unsigned long long extractions;	// 8
	char pad1[54];
	unsigned int epoch;				// 12 //enqueue's epoch
	unsigned int index;							// 16
	unsigned int type; 							// 20 // used to resolve the conflict with same timestamp using a FIFO policy
	volatile unsigned int new_epoch;							// 24
	node_t *tail;
	bucket_t * volatile next;	
	char pad2[32];
#ifndef RTM
	pthread_spinlock_t lock;
	#endif
	//node_t tail;								// 56
	//node_t *tail;
	node_t head;								// 80 + 88
	//bucket_t * volatile next;					// 96
};


/**
 * This function initializes the gc subsystem
 */

static inline void init_bucket_subsystem(){
	gc_aid[GC_BUCKETS] 		= gc_add_allocator(sizeof(bucket_t		));
	gc_aid[GC_INTERNALS] 	= gc_add_allocator(sizeof(node_t 		));
}


/* allocate a unrolled nodes */
static inline node_t* node_alloc(){
	node_t* res;
    res = gc_alloc(ptst, gc_aid[GC_INTERNALS]);

	res->next 					= NULL;
	res->payload				= NULL;
	res->tie_breaker			= 0;
	res->timestamp	 			= INFTY;
	return res;
}


#define node_safe_free(ptr) 			gc_free(ptst, ptr, gc_aid[GC_INTERNALS])
#define node_unsafe_free(ptr) 			gc_free(ptst, ptr, gc_aid[GC_INTERNALS])
#define only_bucket_unsafe_free(ptr)	gc_free(ptst, ptr, gc_aid[GC_BUCKETS])



#define is_freezed(extractions)  ((extractions >> 32) != 0ULL)
#define is_freezed_for_mov(extractions) (is_freezed(extractions) && (extractions & FREEZE_FOR_MOV))
#define is_freezed_for_del(extractions) (is_freezed(extractions) && (extractions & FREEZE_FOR_DEL))
#define is_freezed_for_epo(extractions) (is_freezed(extractions) && (extractions & FREEZE_FOR_EPO))
#define get_freezed(extractions, flag)  ((extractions << 32) | extractions | flag)

/* allocate a bucket */
static inline bucket_t* bucket_alloc(){
	bucket_t* res;
    res 					= gc_alloc(ptst, gc_aid[GC_BUCKETS]);
    res->extractions 		= 0ULL;
    res->epoch				= 0U;
    res->new_epoch			= 0U;

	res->tail = node_alloc();
    res->tail->payload		= NULL;
    res->tail->timestamp		= INFTY;
    res->tail->tie_breaker	= 0U;
    res->tail->next			= NULL;

    res->head.payload		= NULL;
    res->head.timestamp		= MIN;
    res->head.tie_breaker	= 0U;
    res->head.next			= res->tail;
    
    #ifndef RTM
    pthread_spin_init(&res->lock, 0);
    #endif

	return res;
}


static inline void bucket_safe_free(bucket_t *ptr){
	node_t *tmp, *current = ptr->head.next;
	unsigned long long old_extractions = ptr->extractions;
	while(current != ptr->tail && !is_freezed_for_epo(old_extractions)){
		tmp = current;
		current = tmp->next;
		node_safe_free(tmp);
	}
	if(!is_freezed_for_epo(old_extractions))
		node_safe_free(ptr->tail);
	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);
	
}

static inline void bucket_unsafe_free(bucket_t *ptr){
	node_t *tmp, *current = ptr->head.next;
	unsigned long long old_extractions = ptr->extractions;
	while(current != ptr->tail && !is_freezed_for_epo(old_extractions)){
		tmp = current;
		current = tmp->next;
		node_unsafe_free(tmp);
	}
	if(!is_freezed_for_epo(old_extractions))
		node_safe_free(ptr->tail);
	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);

}


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


#define complete_freeze(bckt) do{\
	unsigned long long old_extractions = 0ULL;\
	void *old_next = NULL;\
	old_extractions = (bckt)->extractions;\
	complete_freeze_for_epo(bckt, old_extractions);\
	do{ old_next = (bckt)->next;\
	}while(is_marked(old_next, VAL) && is_freezed_for_del(old_extractions) && !__sync_bool_compare_and_swap(&(bckt)->next, old_next, get_marked(old_next, DEL)));\
	do{ old_next = (bckt)->next;\
	}while(is_marked(old_next, VAL) && is_freezed_for_mov(old_extractions) && !__sync_bool_compare_and_swap(&(bckt)->next, old_next, get_marked(old_next, MOV)));\
}while(0)


static inline void complete_freeze_for_epo(bucket_t *bckt, unsigned long long old_extractions){
	if(!is_freezed_for_epo(old_extractions)) return;
	
	void *old_next = bckt->next;
	bucket_t *res = bucket_alloc();
	bool suc = false;
	
	
	
    res->extractions 		= bckt->extractions ;
    res->extractions 		= (res->extractions & ( ~(FREEZE_FOR_DEL | FREEZE_FOR_MOV | FREEZE_FOR_EPO) ) ) >> 32 ;
    res->epoch				= bckt->new_epoch;
    res->new_epoch			= 0U;
    res->index				= bckt->index;
    res->type				= bckt->type;

    res->head.payload		= NULL;
    res->head.timestamp		= MIN;
    res->head.tie_breaker	= 0U;
    res->head.next			= bckt->head.next;

    node_safe_free(res->tail);
    res->tail = bckt->tail;
        
    do{
		old_next = bckt->next;
		res->next = old_next;
		suc=false;
	}while(is_marked(old_next, VAL) && !(suc = __sync_bool_compare_and_swap(&(bckt)->next, old_next, get_marked(res, DEL))));
	
	if(suc) return;
	only_bucket_unsafe_free(res);
}


//static inline void freeze(bucket_t *bckt, unsigned long long flag) {
#define freeze(bckt, flag) 	{\
	do{\
	unsigned long long old_extractions = 0ULL;\
	unsigned long long new_extractions = 0ULL;\
	old_extractions = (bckt)->extractions;\
	while(!is_freezed(old_extractions)){\
		new_extractions = get_freezed(old_extractions, flag);\
		if(__sync_bool_compare_and_swap(&(bckt)->extractions, old_extractions, new_extractions)) 	break;\
		old_extractions = (bckt)->extractions;\
	}\
	complete_freeze((bckt));\
}while(0);}
//}


#define freeze_from_mov_to_del(bckt) do{\
	unsigned long long old_extractions = 0;\
	unsigned long long new_extractions = 0;\
	void *old_next = NULL;\
	complete_freeze(bckt);\
	old_extractions = bckt->extractions;\
	while(is_freezed_for_mov(old_extractions)){\
		new_extractions = (old_extractions  & (~FREEZE_FOR_MOV)) | FREEZE_FOR_DEL;\
		if(__sync_bool_compare_and_swap(&bckt->extractions, old_extractions, new_extractions))\
			break;\
		old_extractions = bckt->extractions;\
	}\
	old_extractions = bckt->extractions;\
	do{\
		old_next = bckt->next;\
	}while(is_marked(old_next, MOV) && is_freezed_for_del(old_extractions) && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(get_unmarked(old_next), DEL)));\
}while(0)


static inline int check_increase_bucket_epoch(bucket_t *bckt, unsigned int epoch){
	
	if(bckt->epoch >= epoch && bckt->new_epoch == 0) return OK;
	
	__sync_bool_compare_and_swap(&bckt->new_epoch, 0, epoch);
	
	freeze(bckt, FREEZE_FOR_EPO);
	
	return ABORT;
	
	/*
	while(bckt->epoch < epoch){
		extracted = bckt->extractions;
		if(is_freezed(extracted)) return ABORT;
		ATOMIC(&bckt->lock){
			extracted = bckt->extractions;
			if(is_freezed(extracted)) TM_ABORT();
			bckt->epoch = epoch;
			TM_COMMIT();
		}
		FALLBACK(&bckt->lock){
			return ABORT;
		}
		END_ATOMIC(&bckt->lock);
	}
	return OK;
	*/
}

__thread pkey_t last_key = 0;
__thread unsigned long long counter_last_key = 0ULL;

static inline int bucket_connect(bucket_t *bckt, pkey_t timestamp, unsigned int tie_breaker, void* payload){
	
	node_t *tail  = bckt->tail;
	node_t *left  = &bckt->head;
	node_t *curr  = left;
	unsigned long long extracted = 0;
	unsigned long long toskip = 0;
	unsigned long long position = 0;
	node_t *new   = node_alloc();
	
	new->timestamp = timestamp;
	new->payload	= payload;
	complete_freeze(bckt);

  begin:
	curr = left = &bckt->head;
	if(last_key != timestamp){
		last_key = timestamp;
		counter_last_key =0 ;
	}
	counter_last_key++;
	new->tie_breaker = 1;
  	extracted 	= bckt->extractions;

  	if(is_freezed_for_mov(extracted)) {node_unsafe_free(new); return MOV_FOUND;	}
  	if(is_freezed(extracted)) {node_unsafe_free(new); return ABORT; 	}
  	
  	toskip		= extracted;
	position = 0;
  	while(toskip > 0ULL && curr != tail){
  		curr = curr->next;
  		toskip--;
  		scan_list_length_en++;
  		position++;
  	}

  	if(curr == tail && toskip > 0ULL) {
  		freeze(bckt, FREEZE_FOR_DEL);
  		node_unsafe_free(new);
  		return ABORT;
  	}

  	while(curr->timestamp <= timestamp){
		if(counter_last_key > 1000000)
			printf("L: %p-%u C: %p-%u\n", left, left->timestamp, curr, curr->timestamp);
  		left = curr;
  		curr = curr->next;
  		scan_list_length_en++;
  		position++;
  	}

  	if(left->timestamp == timestamp)
  		new->tie_breaker+= left->tie_breaker;
  	new->next = curr;

//	{
		unsigned int __local_try=0;
		long  rand;
		unsigned int __status;
		unsigned int fallback;
//CMB();
	  retry_tm:

        if(position < bckt->extractions)  {rtm_debug++;goto begin;}
		if(left->next != curr)	{rtm_nested++;goto begin;}
//retry_tm:
		++rtm_prova;
		__status = 0;
//CMB();
		if ((__status = _xbegin ()) == _XBEGIN_STARTED)
		{
			if(position < bckt->extractions){ TM_ABORT(0xf2);}
			if(left->next != curr){ TM_ABORT(0xf1);}
			left->next = new; 
			TM_COMMIT();
		}
		else
		{
			rtm_failed++;
			/*  Transaction retry is possible. */
			if(__status & _XABORT_RETRY) {DEB("RETRY\n");rtm_retry++;}
			/*  Transaction abort due to a memory conflict with another thread */
			if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n");rtm_conflict++;}
			/*  Transaction abort due to the transaction using too much memory */
			if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n");rtm_capacity++;}
			/*  Transaction abort due to a debug trap */
			if(__status & _XABORT_DEBUG) {DEB("DEBUG\n");rtm_debug++;}
			/*  Transaction abort in a inner nested transaction */
			if(__status & _XABORT_NESTED) {DEB("NESTES\n");rtm_nested++;}
			/*  Transaction explicitely aborted with _xabort. */
			if(__status & _XABORT_EXPLICIT){DEB("EXPLICIT\n");rtm_explicit++;}
			if(__status == 0){DEB("Other\n");rtm_other++;}
			if(_XABORT_CODE(__status) == 0xf2) {rtm_a++;}
			if(_XABORT_CODE(__status) == 0xf1) {rtm_b++;}
			if(__status & _XABORT_RETRY && __local_try++ < 51200) {
				lrand48_r(&seedT, &rand);
				fallback = rand & 511L;
				while(fallback & 1L && fallback--!=0)_mm_pause();
				goto retry_tm;
			}
			goto begin;
		}

	rtm_insertions++;
	return OK;
}


static inline int extract_from_bucket(bucket_t *bckt, void ** result, pkey_t *ts, unsigned int epoch){
	node_t *curr  = &bckt->head;
	node_t *tail  = bckt->tail;
	unsigned long long extracted = 0;
	
	assertf(bckt->type != ITEM, "trying to extract from a head bucket%s\n", "");

	if(bckt->epoch > epoch) return ABORT;

  #ifdef RTM
  	extracted 	= __sync_add_and_fetch(&bckt->extractions, 1ULL);
  #else
	pthread_spin_lock(&bckt->lock);
		extracted 	= __sync_add_and_fetch(&bckt->extractions, 1ULL);
  	pthread_spin_unlock(&bckt->lock);
  #endif
  	if(is_freezed_for_mov(extracted)) return MOV_FOUND;
  	if(is_freezed(extracted)) return EMPTY;
  	
  	while(extracted > 0 && curr != tail){
  		curr = curr->next;
  		extracted--;
		scan_list_length++;
  	}

  	if(curr == tail){
		unsigned long rand=0;
                lrand48_r(&seedT, &rand);
		rand &= 511;
//                if(rand < 128)
  		freeze(bckt, FREEZE_FOR_DEL);
		return EMPTY; // try to compact
  	} 
  	*result = curr->payload;
  	*ts		= curr->timestamp;
  	
	return OK;
}

#endif
