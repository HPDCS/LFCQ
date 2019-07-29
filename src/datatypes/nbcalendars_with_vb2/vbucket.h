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
extern __thread unsigned int tid;
extern __thread int nid;

//#define VALIDATE_BUCKETS

#define GC_BUCKETS   0
#define GC_INTERNALS 1

#define RTM_RETRY 32
#define RTM_FALLBACK 1
#define RTM_BACKOFF 1L


#define NOOP			0ULL
#define CHANGE_EPOCH	1ULL
#define	DELETE			2ULL
#define SET_AS_MOV		3ULL

#define MICROSLEEP_TIME 5
#define CLUSTER_WAIT 2500000
#define WAIT_LOOPS CLUSTER_WAIT //2

#define get_op_type(x) ((x) >> 32)


#define BACKOFF_LOOP() {\
long rand;\
long fallback;\
    lrand48_r(&seedT, &rand);\
    fallback = (rand & RTM_BACKOFF) /* + nid*350*/;\
    /*if(fallback & 1L)*/ while(fallback--!=0) _mm_pause();\
}

typedef struct __node_t node_t;
typedef struct __bucket_t bucket_t;

struct __node_t
{
	void *payload;						// 8
	pkey_t timestamp;  					//  
	char __pad_1[8-sizeof(pkey_t)];		// 16
	unsigned int tie_breaker;			// 20
	int hash;							// 24
	node_t * next;				// 32
	bucket_t *  bucket;
	void * volatile replica;
	char __pad_2[16];
};

/**
 *  Struct that define a bucket
 */ 
struct __bucket_t
{
	volatile unsigned long long extractions;	//  8
	char __pad_2[56];
	unsigned int epoch;				
	unsigned int index;				// 16
	unsigned int type;
	volatile int socket;		// 24
	unsigned long long __pad_4;       // 52
	node_t * tail;					// 32
	volatile unsigned long long op_descriptor;
	node_t * volatile pending_insert;		// 40
	bucket_t * volatile next;			// 48
	volatile long hash;				// 64
//-----------------------------
	node_t head;					// 32
	char __pad_3[64-sizeof(node_t)];
//-----------------------------
  #ifndef RTM
	pthread_spinlock_t lock;
  #endif
};

#include "mm_vbucket.h"

static inline void validate_bucket(bucket_t *bckt){
  #ifdef VALIDATE_BUCKETS 
	node_t *ln;
	ln = &bckt->head;
	while(ln->timestamp != INFTY)
		ln = ln->next;
	assert(ln == bckt->tail);
  #endif
}

unsigned long tacc_rdtscp(int *chip, int *core)
{
    unsigned long a,d,c;
    __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
    *chip = (c & 0xFFF000)>>12;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << 32);;
}


static inline void acquire_node(volatile int *socket){
//return;
//	int core, l_nid;
//	tacc_rdtscp(&l_nid, &core);
//	if(nid != l_nid) printf("NID: %d  LNID: %d\n", nid, l_nid);
//	nid = l_nid;
	int old_socket = *socket;
	int loops = WAIT_LOOPS;
	if(old_socket != nid){
		if(old_socket != -1) 
			while(
				loops-- && 
				(old_socket = *socket) != nid
			)
			_mm_pause(); 
//			usleep(MICROSLEEP_TIME);	
//		if(old_socket == -1) printf("old_socket:%d\n", old_socket);
		if(old_socket != nid)
			__sync_bool_compare_and_swap(socket, old_socket, nid);
//			__sync_lock_test_and_set(socket, nid);
	}
}


static inline void complete_freeze_for_epo(bucket_t *bckt, unsigned long long old_extractions){
	unsigned int present	=	0;
	unsigned long long toskip;
	node_t *tail  = bckt->tail;
	node_t *left  = &bckt->head;
	node_t *curr  = left;
	node_t *head  = curr;
	node_t *new;
	bucket_t *res;
	bucket_t *old_next = bckt->next;
	bool suc = false;

	// phase 2: check if there is a pending op
	if(bckt->pending_insert != ((void*)1ULL) ){
		toskip			= get_cleaned_extractions(old_extractions);
		new   = node_alloc();
		new->timestamp  = bckt->pending_insert->timestamp;
		new->payload	= bckt->pending_insert->payload;
		new->hash		= bckt->pending_insert->hash;
		
		// check if there is space in the bucket
	  	while(toskip > 0ULL && curr != tail){
	  		curr = curr->next;
	  		toskip--;
	  	}
		head  = curr;

	  	// there is no space so the whoever has tried a fallback has to retry
	  	if(curr == tail && toskip >= 0ULL) 	node_unsafe_free(new);
	  	else{
		  	// Connect...
			do{
	  			new->tie_breaker = 1;
	  			left = curr = head;
			  	curr = curr->next;
			  	while(curr->timestamp <= new->timestamp){
	            	// keep memory if it was inserted to release memory
	                if(curr->timestamp == new->timestamp && curr->hash == new->hash) present = 1;

			  		left = curr;
			  		curr = curr->next;

			  	}

			  	if(left->timestamp == new->timestamp)	new->tie_breaker+= left->tie_breaker;

			  	new->next = curr;
			}while(!present && !__sync_bool_compare_and_swap(&left->next, curr, new));

			if(present) node_unsafe_free(new);
			// now either it was present or i'm inserted so communicate that the op has compleated
		}
	}
	
	// phase 3: replace the bucket for new epoch
	res = bucket_alloc(bckt->tail);
    res->extractions 		= get_cleaned_extractions(old_extractions);
    res->epoch				= bckt->op_descriptor & MASK_EPOCH;
    res->epoch				= res->epoch < bckt->epoch ? bckt->epoch :  res->epoch;
    res->index				= bckt->index;
    res->type				= bckt->type;

    res->head.payload		= NULL;
    res->head.timestamp		= MIN;
    res->head.tie_breaker	= 0U;
    res->head.next			= bckt->head.next;
       
    do{
		old_next = bckt->next;
		res->next = old_next;
		suc=false;
	}while(is_marked(old_next, VAL) && !(suc = __sync_bool_compare_and_swap(&(bckt)->next, old_next, get_marked(res, DEL))));
    atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);

	if(suc) return;

	only_bucket_unsafe_free(res);
}


__thread unsigned long long count_epoch_ops = 0ULL;
__thread unsigned long long rq_epoch_ops = 0ULL;
__thread unsigned long long bckt_connect_count = 0ULL;

static void post_operation(bucket_t *bckt, unsigned long long ops_type, unsigned int epoch, node_t *node){
	unsigned long long pending_op_descriptor = bckt->op_descriptor;
	unsigned long long pending_op_type	  = get_op_type(pending_op_descriptor);
	
	if(pending_op_type != NOOP) return;
	if(node != NULL) __sync_bool_compare_and_swap(&bckt->pending_insert, NULL, node);
	pending_op_descriptor = (ops_type << 32) | epoch;
	__sync_bool_compare_and_swap(&bckt->op_descriptor, NOOP, pending_op_descriptor);
}



static inline void execute_operation(bucket_t *bckt){
	unsigned long long pending_op_type = get_op_type(bckt->op_descriptor);
	unsigned long long old_extractions = bckt->extractions;
	unsigned long long new_extractions = 0ULL;
	bucket_t *old_next = NULL;

	if(pending_op_type == NOOP) return;
	else if(pending_op_type == DELETE){
        if(!is_freezed_for_del(old_extractions)) 		atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);
        if(is_marked(bckt->next, VAL))		 			atomic_bts_x64(&bckt->next, 0);
	}
	else if(pending_op_type == SET_AS_MOV){
		while(!is_freezed(old_extractions)){
			new_extractions = get_freezed(old_extractions, FREEZE_FOR_MOV);
			if(__sync_bool_compare_and_swap(&bckt->extractions, old_extractions, new_extractions)) 	
				break;
			old_extractions = bckt->extractions;
		}
	
		do{ old_next = bckt->next;
		}while(is_marked(old_next, VAL) && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(old_next, MOV)));

	}
	else if(pending_op_type == CHANGE_EPOCH){
		while(!is_freezed(old_extractions)){
			new_extractions = get_freezed(old_extractions, FREEZE_FOR_EPO);
			old_extractions = __sync_val_compare_and_swap(&bckt->extractions, old_extractions, new_extractions);
		}

		if(bckt->pending_insert == NULL) __sync_bool_compare_and_swap(&bckt->pending_insert, NULL, (void*)1ULL);
		complete_freeze_for_epo(bckt, old_extractions);
	}
	else 
		assert(0);

}




static inline void finalize_set_as_mov(bucket_t *bckt){
	bucket_t *old_next = NULL;

	if(is_freezed_for_mov(bckt->extractions)) atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);

    do{
            old_next = bckt->next;
    }while(is_marked(old_next, MOV)  && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(get_unmarked(old_next), DEL)));
}


__thread unsigned long long fallback_insertions = 0ULL;
__thread pkey_t last_key_fall = 0;
__thread unsigned long long counter_last_key_fall = 0ULL;


static inline int bucket_connect_fallback(bucket_t *bckt, node_t *node, unsigned int epoch){
	fallback_insertions++;
	int res = ABORT;

	post_operation(bckt, CHANGE_EPOCH, epoch, node);
	execute_operation(bckt);

	if(get_op_type(bckt->op_descriptor) == CHANGE_EPOCH && bckt->pending_insert == node) res = OK;

	if(last_key_fall != node->timestamp){
		last_key_fall = node->timestamp;
		counter_last_key_fall = 0;
	}
	else{
		counter_last_key_fall++;
		if(counter_last_key_fall > 100000) printf("Problems during bucket connect fallback\n");
	}
	
	return res;
}


__thread unsigned long long acc_contention = 0ULL;
__thread unsigned long long cnt_contention = 0ULL;
__thread unsigned long long min_contention = -1LL;
__thread unsigned long long max_contention = 0ULL;

__thread pkey_t last_key = 0;
__thread unsigned long long counter_last_key = 0ULL;




static inline bucket_t* increase_epoch(bucket_t *bckt, unsigned int epoch){
	unsigned int __status,  original_index = bckt->index;
	bucket_t *res = bckt; 

	count_epoch_ops++;
	if((__status = _xbegin ()) == _XBEGIN_STARTED)
	{
		if(is_freezed(bckt->extractions)){ TM_ABORT(0xf2);}
		if(get_op_type(bckt->op_descriptor)){TM_ABORT(0xf2);}
		if(bckt->epoch < epoch) bckt->epoch = epoch;
		TM_COMMIT();
	}
	else{
		post_operation(bckt, CHANGE_EPOCH, epoch, NULL);
		execute_operation(bckt);
		res = get_unmarked(bckt->next);
		rq_epoch_ops++;
		
		if(get_op_type(bckt->op_descriptor) != CHANGE_EPOCH) return NULL;
		if(res->index != original_index) return NULL;
	}

	return res;
}


int bucket_connect(bucket_t *bckt, pkey_t timestamp, unsigned int tie_breaker, void* payload, unsigned int epoch){
	bckt_connect_count++;

//	acquire_node(&bckt->socket);

    while(bckt != NULL && bckt->epoch < epoch){
    	bckt = increase_epoch(bckt, epoch);
    } 
    if(bckt == NULL) return ABORT;

	node_t *tail  = bckt->tail;
	node_t *left  = &bckt->head;
	node_t *curr  = left;
	unsigned long long extracted = 0;
	unsigned long long toskip = 0;
	unsigned long long position = 0;
	unsigned int __global_try = 0;
	unsigned int __local_try=0;
	unsigned int __status = 0;
	unsigned int mask = 1;
	int res = OK;
    long long contention = 0; //__sync_fetch_and_add(&bckt->pad3, 1LL);
    acc_contention+=contention;
    cnt_contention++;
    min_contention = contention <= min_contention ? contention : min_contention;
    max_contention = contention >= max_contention ? contention : max_contention;

	node_t *new   = node_alloc(); //node_alloc_by_index(bckt->index);
	new->timestamp = timestamp;
	new->payload	= payload;

	validate_bucket(bckt);

  begin:
  	__global_try++;
	curr = left = &bckt->head;
	PREFETCH(curr->next, 0);
	if(last_key != timestamp){
		last_key = timestamp;
		counter_last_key =0 ;
	}
	counter_last_key++;
	new->tie_breaker = 1;
  	extracted 	= bckt->extractions;

  	if(get_op_type(bckt->op_descriptor) == SET_AS_MOV) 	{node_unsafe_free(new); res = MOV_FOUND; goto out;	}
  	if(get_op_type(bckt->op_descriptor) != NOOP) 		{node_unsafe_free(new); res = ABORT; 	 goto out; 	}

  	toskip		= extracted;
	position = 0;
  	while(toskip > 0ULL && curr != tail){
  		curr = curr->next;
		if(curr) 	PREFETCH(curr->next, 0);
  		toskip--;
  		scan_list_length_en++;
  		position++;
  	}

  	if(curr == tail && toskip >= 0ULL) {
		post_operation(bckt, DELETE, 0ULL, NULL);
  		node_unsafe_free(new);
  		res = ABORT; 
  		goto out;
  	}

	left = curr;
	curr = curr->next;
  	while(curr->timestamp <= timestamp){
		if(counter_last_key > 1000000ULL)	printf("L: %p-" KEY_STRING " C: %p-" KEY_STRING "\n", left, left->timestamp, curr, curr->timestamp);
  		left = curr;
  		curr = curr->next;
		if(curr) PREFETCH(curr->next, 1);
		scan_list_length_en++;
  		position++;
  	}
	
	assert(curr->timestamp != INFTY || curr == tail);

  	if(left->timestamp == timestamp) new->tie_breaker+= left->tie_breaker;
  	new->next = curr;


	__local_try=0;
	__status = 0;
	mask = 1;
  retry_tm:

	BACKOFF_LOOP();
	//if(!BOOL_CAS(&left->next, curr, curr))  {rtm_nested++;goto begin;}
	// if(position < VAL_CAS(&bckt->extractions, 0, 0))  {rtm_debug++;goto begin;}

    if(position < bckt->extractions)  {rtm_debug++;goto begin;}
	if(left->next != curr)	{rtm_nested++;goto begin;}

	++rtm_prova;
	__status = 0;

	assert(new != NULL && left->next != NULL && curr != NULL);
	if((__status = _xbegin ()) == _XBEGIN_STARTED)
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
		if(
			//__status & _XABORT_RETRY ||
			__local_try++ < RTM_RETRY
		) 
		{
			mask = mask | (mask <<1);
			goto retry_tm;
		}
		if(__global_try < RTM_FALLBACK || __status & _XABORT_EXPLICIT) 
			goto begin;
//		__sync_fetch_and_add(&bckt->pad3, -1LL);

		
    	res = bucket_connect_fallback(bckt, new, epoch); 
    	if(res != OK) 	node_unsafe_free(new);
    	return res;
	}

	rtm_insertions++;
  out:

	validate_bucket(bckt);
//	__sync_fetch_and_add(&bckt->pad3, -1LL);

	#if ENABLE_CACHE == 1
	if(res == OK){
	 	__cache_bckt[bckt->index % INSERTION_CACHE_LEN] = bckt;
	 	__cache_hash[bckt->index % INSERTION_CACHE_LEN] = bckt->hash;
	 }
	#endif
	return res;
}


__thread node_t   *__last_node 	= NULL;
__thread unsigned long long __last_val = 0;

static inline int extract_from_bucket(bucket_t *bckt, void ** result, pkey_t *ts, unsigned int epoch){
//	acquire_node(&bckt->socket);
	node_t *curr  = &bckt->head;
	node_t *tail  = bckt->tail;

	
	//node_t *head  = &bckt->head;
	unsigned long long old_extracted = 0;	
	unsigned long long extracted = 0;
	unsigned skipped = 0;
	PREFETCH(curr->next, 0);
	assertf(bckt->type != ITEM, "trying to extract from a head bucket%s\n", "");

	if(bckt->epoch > epoch) return ABORT;

//  unsigned int count = 0;	long rand;  lrand48_r(&seedT, &rand); count = rand & 7L; while(count-->0)_mm_pause();

  	old_extracted = extracted 	= __sync_add_and_fetch(&bckt->extractions, 1ULL);

  	if(is_freezed_for_mov(extracted)) return MOV_FOUND;
  	if(is_freezed_for_epo(extracted)) return ABORT;
  	if(is_freezed_for_del(extracted)) return EMPTY;

	validate_bucket(bckt);

	if(__last_node != NULL){
  		curr = __last_node;
  		extracted -= __last_val;
  	}

  	while(extracted > 0ULL && curr != tail){
  		/*if(skipped > 25 && extracted > 2){
  			if((__status = _xbegin ()) == _XBEGIN_STARTED)
			{
				if(old_extracted != bckt->extractions){ TM_ABORT(0xf2);}
				head->next = curr;
				old_extracted -= skipped; 
				TM_COMMIT();
				skipped = 0;
			}
			else{}

  		}*/
  		curr = curr->next;
		if(curr) 	PREFETCH(curr->next, 0);
  		extracted--;
  		skipped++;
		scan_list_length++;
  	}

	if(curr->timestamp == INFTY)	assert(curr == tail);
  	if(curr == tail){
		post_operation(bckt, DELETE, 0ULL, NULL);
		return EMPTY; 
  	} 
  	__last_node = curr;
  	__last_val  = old_extracted;
  	*result = curr->payload;
  	*ts		= curr->timestamp;
  	
	return OK;
}

#endif
