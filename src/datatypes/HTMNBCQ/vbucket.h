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
 *  Second Author (30/11/2020): Fortunato Tocci
 */

#ifndef VBUCKET_H
#define VBUCKET_H

#include <stdlib.h>
#include <numa.h>
#include <immintrin.h>

#include "../../key_type.h"
#include "../../utils/rtm.h"
#include "../../gc/gc.h"
#include "../../utils/hpdcs_utils.h"
#include "./nb_lists_defs.h"

extern __thread ptst_t *ptst;
extern int gc_aid[];
extern int gc_hid[];
extern __thread unsigned long long scan_list_length_en;
extern __thread unsigned long long scan_list_length;
extern __thread struct drand48_data seedT;
extern __thread unsigned int tid;
extern __thread int nid;

#define GC_BUCKETS   0
#define GC_INTERNALS 1

#define RTM_RETRY 32
#define RTM_FALLBACK 1
#define RTM_BACKOFF 1L

#define NOOP			0ULL
#define CHANGE_EPOCH	1ULL
#define	DELETE			2ULL
#define SET_AS_MOV		3ULL

#define VB_NUM_LEVELS	4
#define VB_MAX_LEVEL	(VB_NUM_LEVELS-1)		
#define DISABLE_SKIPLIST 0

#define MICROSLEEP_TIME 5
#define CLUSTER_WAIT 200000
#define WAIT_LOOPS CLUSTER_WAIT //2

#define get_op_type(x) ((x) >> 32)

#define BACKOFF_LOOP() {\
long rand;\
long fallback;\
    lrand48_r(&seedT, &rand);\
    fallback = (rand & RTM_BACKOFF) /** + nid*350*/;\
    /**if(fallback & 1L)*/ while(fallback--!=0) _mm_pause();\
}

typedef struct __node_t node_t;
typedef struct __bucket_t bucket_t;
typedef unsigned int sl_key_t;

/**
 *  Struct that define a list of buckets
 */ 
typedef struct listNode_t {
	sl_key_t key;
	int topLevel;
	bucket_t * volatile bottom_level;
	bucket_t * volatile value;
	volatile long hash;
	struct listNode_t* volatile next[];
} ListNode;

typedef struct skipList_t {
	ListNode *head;
	ListNode *tail;
} SkipList;

typedef ListNode* markable_ref;

/**
 *  Struct that define a node
 */ 
struct __node_t {
	void *payload;						// 8
	pkey_t timestamp;  					// 
	char inList;
	char __pad_1[7];		// 16
	unsigned int tie_breaker;			// 20
	int hash;							// 24
	void * volatile replica;			// 40
	node_t * volatile next;				// 32
	node_t * volatile upper_next[VB_NUM_LEVELS-2];	// 64
};

/**
 *  Struct that define a bucket
 */ 
struct __bucket_t {
	volatile unsigned long long extractions;	//  8
	char __pad_2[56];
	unsigned int epoch;				
	unsigned int index;				// 16
	unsigned int type;
	volatile int socket;		// 24
	ListNode *inode;       // 52
	node_t * tail;					// 32
	volatile unsigned long long op_descriptor;
	node_t * volatile pending_insert;		// 40
	bucket_t * volatile next;			// 48
	volatile long hash;				// 64
	//-----------------------------
	node_t head;					// 32
	char __pad_3[64-sizeof(node_t)];
	//-----------------------------
	// LUCKY: 
	unsigned int numaNodes;
	unsigned int tot_arrays;
	struct __arrayNodes_t** ptr_arrays;
	struct __arrayNodes_t* arrayOrdered;
	struct __bucket_t** destBuckets;
	// LUCKY: End
	//-----------------------------
  #ifndef RTM
	pthread_spinlock_t lock;
  #endif
};

#include "./mm_vbucket.h"
#include "./cache.h"
#include "./array.h"

/**
* Function that do a validation of the bucket
*/
static inline void validate_bucket(bucket_t *bckt){
  #ifdef VALIDATE_BUCKETS 
	node_t *ln;
	ln = &bckt->head;
	while(ln->timestamp != INFTY)
		ln = ln->next;
	assert(ln == bckt->tail);
  #endif
}

/**
* Function that reads the current value of the processor's timestamp counter
*/
unsigned long tacc_rdtscp(int *chip, int *core){
    unsigned long a,d,c;
    __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
    *chip = (c & 0xFFF000)>>12;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << 32);;
}


static inline void acquire_node(volatile int *socket){
	int core, l_nid;
	tacc_rdtscp(&l_nid, &core);
//	if(nid != l_nid) printf("NID: %d  LNID: %d\n", nid, l_nid);
	nid = l_nid;
	int old_socket = *socket;
	int loops = WAIT_LOOPS;
//	if(nid == 0) loops >>=1;
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

/**
* Function that freeze the bucket, compute eventual insert and then change the bucket epoch
*/
static inline void complete_freeze_for_epo(bucket_t *bckt, unsigned long long old_extractions){
	unsigned int present = 0;
	unsigned long long toskip;
	node_t *tail = bckt->tail;
	node_t *left = &bckt->head;
	node_t *curr = left;
	node_t *head = curr;
	node_t *newN;
	bucket_t *res;
	bucket_t *old_next = bckt->next;
	bool suc = false;

	// phase 2: check if there is a pending op
	if(bckt->pending_insert != ((void*)1ULL) ){
		toskip = get_cleaned_extractions(old_extractions);
		newN = node_alloc();
		newN->inList = 1;
		newN->timestamp  = bckt->pending_insert->timestamp;
		newN->payload	= bckt->pending_insert->payload;
		newN->hash		= bckt->pending_insert->hash;
		
		// check if there is space in the bucket
		while(toskip > 0ULL && curr != tail){
			curr = curr->next;
			toskip--;
		}
		head  = curr;

		// there is no space so the whoever has tried a fallback has to retry
		if(curr == tail && toskip >= 0ULL) 	node_unsafe_free(newN);
		else{
			// connect...
			do{
					newN->tie_breaker = 1;
					left = curr = head;
					curr = curr->next;
					while(curr->timestamp <= newN->timestamp){
						// keep memory if it was inserted to release memory
						if(curr->timestamp == newN->timestamp && curr->hash == newN->hash) present = 1;
						left = curr;
						curr = curr->next;
					}
					if(left->timestamp == newN->timestamp)	newN->tie_breaker+= left->tie_breaker;
					newN->next = curr;
			}while(!present && !__sync_bool_compare_and_swap(&left->next, curr, newN));
			// CAS has done insert of the new node

			if(present) node_unsafe_free(newN);
			// now either it was present or i'm inserted so communicate that the op has compleated
		}
	}

	// phase 3: replace the bucket for new epoch
	res = bucket_alloc_epo(bckt->tail);
	// LUCKY: Tenere traccia del flag linked
	unsigned long long newExt = 0;
	newExt = get_cleaned_extractions(old_extractions);
	if(is_freezed_for_lnk(old_extractions))
		newExt = newExt | (1ULL << LNK_BIT_POS);
	res->extractions = newExt;
	res->epoch = bckt->op_descriptor & MASK_EPOCH;
	res->epoch = res->epoch < bckt->epoch ? bckt->epoch :  res->epoch;
	res->index = bckt->index;
	res->type	= bckt->type;

	res->head.payload = NULL;
	res->head.timestamp	= MIN;
	res->head.tie_breaker	= 0U;
	res->head.next = bckt->head.next;

	// LUCKY: Copy the old array information
	res->numaNodes = bckt->numaNodes;
	res->tot_arrays = bckt->tot_arrays;
	res->ptr_arrays = bckt->ptr_arrays;
	res->arrayOrdered = bckt->arrayOrdered;
	res->destBuckets = bckt->destBuckets;
			
	do{
		old_next = bckt->next;
		res->next = old_next;
		suc=false;
	}while(is_marked(old_next, VAL) && 
		!(suc = __sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(res, DEL))));
	// With the CAS in the while condition I mark old bucket as delete and substitute it with the new bucket
	atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);
	old_next = get_unmarked(bckt->next);
	
	// if(old_next->index == bckt->index) update_cache(old_next);

	if(suc) return;

	only_bucket_unsafe_free(res);
}


__thread unsigned long long count_epoch_ops = 0ULL;
__thread unsigned long long rq_epoch_ops = 0ULL;
__thread unsigned long long bckt_connect_count = 0ULL;
/**
* Function that publish an operation to execute on a bucket
*/
static void post_operation(bucket_t *bckt, unsigned long long ops_type, unsigned int epoch, node_t *node){
	unsigned long long pending_op_descriptor = bckt->op_descriptor;
	unsigned long long pending_op_type	  = get_op_type(pending_op_descriptor);
	
	if(pending_op_type != NOOP) return;
	if(node != NULL) __sync_bool_compare_and_swap(&bckt->pending_insert, NULL, node);
	pending_op_descriptor = (ops_type << 32) | epoch;
	__sync_bool_compare_and_swap(&bckt->op_descriptor, NOOP, pending_op_descriptor);
}

/**
* Function that executes an operation on the passed bucket
*/
static inline void execute_operation(bucket_t *bckt){
	unsigned long long pending_op_type = get_op_type(bckt->op_descriptor);
	unsigned long long old_extractions = bckt->extractions;
	unsigned long long new_extractions = 0ULL;
	bucket_t *old_next = NULL;

	// No operation to execute
	if(pending_op_type == NOOP) return;
	// A DELETE operation to execute
	else if(pending_op_type == DELETE){
		if(!is_freezed_for_del(old_extractions))
			atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);
		if(is_marked(bckt->next, VAL))
			atomic_bts_x64(&bckt->next, 0);
	}
	// A SET_AS_MOV operation to execute
	else if(pending_op_type == SET_AS_MOV){
		while(!is_freezed(get_extractions_wtoutlk(old_extractions))){
			// Gets the count of the extract by freezing which will become the new value of the extract begins
			new_extractions = get_freezed(old_extractions, FREEZE_FOR_MOV);
			if(__sync_bool_compare_and_swap(&bckt->extractions, old_extractions, new_extractions)) 	
				break;
			old_extractions = bckt->extractions;
		}
		do{
			old_next = bckt->next;
		}while(is_marked(old_next, VAL) && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(old_next, MOV)));
		// With the CAS in the while condition, I mark old bucket as delete and substitute it with the new bucket

	}
	// A CHANGE_EPOCH operation to execute
	else if(pending_op_type == CHANGE_EPOCH){
		while(!is_freezed(get_extractions_wtoutlk(old_extractions))){
			// Gets the count of the extract by freezing which will become the new value of the extract begins
			new_extractions = get_freezed(old_extractions, FREEZE_FOR_EPO);
			old_extractions = __sync_val_compare_and_swap(&bckt->extractions, old_extractions, new_extractions);
		}
		if(bckt->pending_insert == NULL) __sync_bool_compare_and_swap(&bckt->pending_insert, NULL, (void*)1ULL);
		// Continue ...
		complete_freeze_for_epo(bckt, old_extractions);
	}
	else 
		assert(0);
}

/**
* Function that try to terminate the SET_AS_MOV operation
*/
static inline void finalize_set_as_mov(bucket_t *bckt){
	bucket_t *old_next = NULL;

	if(is_freezed_for_mov(bckt->extractions)) atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);
    do{
			old_next = bckt->next;
    }while(is_marked(old_next, MOV)  && !__sync_bool_compare_and_swap(&bckt->next, old_next, get_marked(get_unmarked(old_next), DEL)));
		// With the CAS in the while condition I mark old bucket as delete
}


__thread unsigned long long fallback_insertions = 0ULL;
__thread pkey_t last_key_fall = 0;
__thread unsigned long long counter_last_key_fall = 0ULL;
/**
* Function that implements the fallback path of bucket connection
*/
static inline int bucket_connect_fallback(bucket_t *bckt, node_t *node, unsigned int epoch){
	fallback_insertions++;
	int res = ABORT;

	post_operation(bckt, CHANGE_EPOCH, epoch, node);
	execute_operation(bckt);

	// Check to know if the operation executed is what I have requested rows above
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

/**
 * Variables for statistics
*/
__thread unsigned long long acc_contention = 0ULL;
__thread unsigned long long cnt_contention = 0ULL;
__thread unsigned long long min_contention = -1LL;
__thread unsigned long long max_contention = 0ULL;

__thread pkey_t last_key = 0;
__thread unsigned long long counter_last_key = 0ULL;


/**
* Function that increase the epoch of the passed bucket
*/
static inline bucket_t* increase_epoch(bucket_t *bckt, unsigned int epoch){
	unsigned int __status,  original_index = bckt->index;
	bucket_t *res = bckt; 

	count_epoch_ops++;
	// Try change the epoch using a transaction
	if((__status = _xbegin ()) == _XBEGIN_STARTED)
	{
		if(is_freezed(bckt->extractions)){ TM_ABORT(0xf2);}
		if(get_op_type(bckt->op_descriptor)){TM_ABORT(0xf2);}
		if(bckt->epoch < epoch) bckt->epoch = epoch;
		TM_COMMIT();
	}
	// Fallback path using atomic ops
	else{
		post_operation(bckt, CHANGE_EPOCH, epoch, NULL);
		execute_operation(bckt);
		res = get_unmarked(bckt->next);
		rq_epoch_ops++;
		
		// Check to verify if I have executed the operation requested rows above
		if(get_op_type(bckt->op_descriptor) != CHANGE_EPOCH) return NULL;
		if(res->index != original_index) return NULL;
	}

	return res;
}

/**
* Function that goes over the extracted nodes modifing the passed parameters
*/
unsigned long long skip_extracted(node_t *tail, node_t **curr, unsigned long long toskip){
	unsigned long long position = 0;  
	while(toskip > 0ULL && *curr != tail){
  		*curr = (*curr)->next;
		if(*curr) 	PREFETCH((*curr)->next, 0);
  		toskip--;
  		position++;
  	}
  	return position;
}

/**
 * Function that obtain the position where to insert the new node
*/
unsigned long long fetch_position(node_t **curr, node_t **left, pkey_t timestamp, int level){
	unsigned long long position = 0;
	while((*curr)->timestamp <= timestamp){
		if(counter_last_key > 1000000ULL)	printf("L: %p-" KEY_STRING " C: %p-" KEY_STRING "\n", *left, (*left)->timestamp, *curr, (*curr)->timestamp);
			*left = *curr;
			if(level > 0) 
				*curr = (*curr)->upper_next[level-1];
			else
				*curr = (*curr)->next;
		if(*curr) PREFETCH((*curr)->next, 1);
			position++;
	}
	return position;
}

// LUCKY: Only for Debug
void printList(node_t* now){
	printf("%u, List: \n", syscall(SYS_gettid));
	while(now->next != NULL){
		printf("%p %f\n", now->payload, now->timestamp);
		now = now->next;
	}
	printf("\n");
}

__thread unsigned long long rtm_skip_insertion = 0;
__thread unsigned long long accelerated_searches = 0;
__thread unsigned long long searches_count = 0;
/**
 * Function that inserts a new node in the list stored in the bucket
*/
int bucket_connect(bucket_t *bckt, pkey_t timestamp, unsigned int tie_breaker, void* payload, unsigned int epoch){
	bckt_connect_count++;

//	acquire_node(&bckt->socket);

	// Get amount of extractions, important
	unsigned long long extracted = 0;
	extracted = bckt->extractions;

	// If another operation is in progress, return the operation
	if(is_freezed_for_mov(extracted)) return MOV_FOUND;
	if(is_freezed_for_epo(extracted)) return ABORT;
	if(is_freezed_for_del(extracted)) return EMPTY;

	while(bckt != NULL && bckt->epoch < epoch){
		bckt = increase_epoch(bckt, epoch);
	} 
	if(bckt == NULL) return ABORT;

	node_t *tail  = bckt->tail;
	node_t *left  = &bckt->head;
	node_t *curr  = left;
	unsigned long long toskip = 0;
	unsigned long long position = 0;
	unsigned int __global_try = 0;
	unsigned int __local_try=0;
	unsigned int __status = 0;
	unsigned int mask = 1;
	int res = OK;
	int level = 0;
	long long contention = 0;
	acc_contention+=contention;
	cnt_contention++;
	min_contention = contention <= min_contention ? contention : min_contention;
	max_contention = contention >= max_contention ? contention : max_contention;

	node_t *newN   = node_alloc(); //node_alloc_by_index(bckt->index);
	newN->timestamp = timestamp;
	newN->payload	= payload;
	newN->inList = 1;

	// Check the validity of the bucket
	validate_bucket(bckt);

	// LUCKY: Codice della skiplist
	// int i = 0;
	// long rand, record = 0;
	// lrand48_r(&seedT, &rand);
	// for(i=0;i<VB_NUM_LEVELS-1;i++){
	// 	if(rand & 1) level++;
	// 	else break;
	// 	rand >>= 1;
	// }
	// assert(level <= VB_MAX_LEVEL);

  begin:
	__global_try++;
	curr = left = &bckt->head;
	PREFETCH(curr->next, 0);
	if(last_key != timestamp){
		last_key = timestamp;
		counter_last_key =0 ;
	}
	counter_last_key++;
	newN->tie_breaker = 1;

  // If the request operation is another type then I return and notify that
	if(get_op_type(bckt->op_descriptor) == SET_AS_MOV){
		node_unsafe_free(newN);
		res = MOV_FOUND;
		goto out;	
	}

	if(get_op_type(bckt->op_descriptor) != NOOP){
		node_unsafe_free(newN);
		res = ABORT;
		goto out;
	}

	// LUCKY:
	int numaNode = getNumaNode();
	unsigned long long idxWrite = 0;
	if(validContent(idxWrite))
		idxWrite = VAL_FAA(&bckt->ptr_arrays[numaNode]->indexWrite, 1);

	// Evaluate State Machine
	stateMachine(bckt, ENQUEUE);

	extracted = bckt->extractions;

	// If another operation is in progress, return the operation
	if(is_freezed_for_mov(extracted)) return MOV_FOUND;
	if(is_freezed_for_epo(extracted)) return ABORT;
	if(is_freezed_for_del(extracted)) return EMPTY;

	if(!is_freezed_for_lnk(extracted) && validContent(idxWrite)){
		arrayNodes_t* array = bckt->ptr_arrays[numaNode];
		int idx = getDynamic(idxWrite);
		//assert(array->nodes+idx != NULL);
		void* ptr = array->nodes[idx].ptr;
		//assert(ptr != BLOCK_ENTRY && ptr == NULL);
		return nodesInsert(bckt->ptr_arrays[numaNode], getDynamic(idxWrite), payload, timestamp) == MYARRAY_INSERT ? OK : ABORT;
	}

  // The amount of elements to skip are equal to the extracted value
  toskip = get_extractions_wtoutlk(extracted);
  
	// Codice della skiplist
  // Set the level at 0 if we have been extractions
	long record = is_freezed_for_lnk(extracted);
	// if(record) level  = 0;
	// LUCKY: end

	position = skip_extracted(tail, &curr, toskip);
	scan_list_length_en+=position;

	toskip -= position;
	searches_count++;
	// In case I have arrived at the end of list and I need to go along
	// so this means that the bucket is empty then I mark it as deleted
	// and return an abort
	if(curr == tail && toskip >= 0ULL) {
		post_operation(bckt, DELETE, 0ULL, NULL);
		node_unsafe_free(newN);
		res = ABORT; 
		goto out;
	}

	// If the skiplist is disabled or if there were any extractions 
	// previously so I insert the event in the lowest level
	// LUCKY:
	if(DISABLE_SKIPLIST || record){
		left = curr;
		curr = curr->next;
		// Level is set to 0 (the lowest)
		position += fetch_position(&curr, &left, timestamp, 0);
		scan_list_length_en+=position;
	}

	record = 0;
	// LUCKY: Remove asset, maybe for new code is an incorrect check
	//assert(curr->timestamp != INFTY || curr == tail);

	if(left->timestamp == timestamp){ newN->tie_breaker+= left->tie_breaker; }
	
	newN->next = curr;
	__local_try=0;
	__status = 0;
	mask = 1;
  retry_tm:

	BACKOFF_LOOP();
	//if(!BOOL_CAS(&left->next, curr, curr))  {rtm_nested++;goto begin;}
	// if(position < VAL_CAS(&extracted, 0, 0))  {rtm_debug++;goto begin;}

  if(position < get_extractions_wtoutlk(extracted))  {rtm_debug++;goto begin;}
	if(left->next != curr)	{rtm_nested++;goto begin;}

	++rtm_prova;
	__status = 0;

	assert(newN != NULL && left->next != NULL && curr != NULL);
	if((__status = _xbegin ()) == _XBEGIN_STARTED){
		// If has been done any extractions then abort
		if(position < get_extractions_wtoutlk(extracted)){ TM_ABORT(0xf2);}
		// If there was any changes in the structure then abort
		if(left->next != curr){ TM_ABORT(0xf1);}
		// Insert the node
		left->next = newN; 
		TM_COMMIT();
	}
	else
	{
		rtm_failed++;
		/**  Transaction retry is possible. */
		if(__status & _XABORT_RETRY) {DEB("RETRY\n");rtm_retry++;}
		/**  Transaction abort due to a memory conflict with another thread */
		if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n");rtm_conflict++;}
		/**  Transaction abort due to the transaction using too much memory */
		if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n");rtm_capacity++;}
		/**  Transaction abort due to a debug trap */
		if(__status & _XABORT_DEBUG) {DEB("DEBUG\n");rtm_debug++;}
		/**  Transaction abort in a inner nested transaction */
		if(__status & _XABORT_NESTED) {DEB("NESTES\n");rtm_nested++;}
		/**  Transaction explicitely aborted with _xabort. */
		if(__status & _XABORT_EXPLICIT){DEB("EXPLICIT\n");rtm_explicit++;}
		if(__status == 0){DEB("Other\n");rtm_other++;}
		if(_XABORT_CODE(__status) == 0xf2) {rtm_a++;}
		if(_XABORT_CODE(__status) == 0xf1) {rtm_b++;}
		//FIXME:
		if(/*__status & _XABORT_RETRY ||*/ __local_try++ < RTM_RETRY){
			mask = mask | (mask <<1);
			goto retry_tm;
		}
		if(__global_try < RTM_FALLBACK || __status & _XABORT_EXPLICIT) 
			goto begin;
//		__sync_fetch_and_add(&bckt->pad3, -1LL);

		// Execute the fallback path because TSX path always fail
		res = bucket_connect_fallback(bckt, newN, epoch);
		// In case of error, delete the created node and return the error status
		if(res != OK) 	node_unsafe_free(newN);
		// printList(bckt->head.next);
		return res;
	}

	rtm_insertions++;
	rtm_skip_insertion+=record;
  out:

	validate_bucket(bckt);
//	__sync_fetch_and_add(&bckt->pad3, -1LL);

	update_cache(bckt);
	// printList(bckt->head.next);
	return res;
}


__thread node_t   *__last_node 	= NULL;
__thread unsigned long long __last_val = 0;
/**
 * Function that extract the node that has the minimum timestamp from the array or list of the passed bucket
*/
static inline int extract_from_ArrayOrList(bucket_t *bckt, void ** result, pkey_t *ts, unsigned int epoch){

	node_t *curr  = NULL;
	node_t *tail  = NULL;

	unsigned long long idxRead = 0;
	int res = -1;


	begin:
	// acquire_node(&bckt->socket);
	curr  = &bckt->head;
	tail  = bckt->tail;

	//unsigned skipped = 0;
	PREFETCH(curr->next, 0);
	assertf(bckt->type != ITEM, "trying to extract from a head bucket%s\n", "");

	// If the bucket epoch is greater than the parameter epoch then abort,
	// I can't do an extract something that can be more recent of epoch from the bucket
	if(bckt->epoch > epoch) return ABORT;

	//  unsigned int count = 0;	long rand;  lrand48_r(&seedT, &rand); count = rand & 7L; while(count-->0)_mm_pause();

	// LUCKY: When using the list, I must always be one position ahead
	//old_extracted = extracted = __sync_add_and_fetch(&bckt->extractions, 1ULL);
	// old_extracted = extracted = bckt->extractions;

	// // If another operation is in progress, return the operation
	// if(is_freezed_for_mov(extracted)) return MOV_FOUND;
	// if(is_freezed_for_epo(extracted)) return ABORT;
	// if(is_freezed_for_del(extracted)) return EMPTY;

	validate_bucket(bckt);

	// LUCKY:
	int numaNode = getNumaNode();
	if(validContent(bckt->ptr_arrays[numaNode]->indexWrite)){
		// Invalido inserimenti sugli array per numa node
		setUnvalidContent(bckt);
	}	
	assert(validContent(bckt->ptr_arrays[numaNode]->indexWrite) == false);

	// Applico la state machine per costruire l'array ordinato
	// che comprende tutti gli elementi
	stateMachine(bckt, DEQUEUE);
	//assert(validContent(bckt->ptr_arrays[numaNode]->indexWrite) == false || unordered(bckt) == false);
	idxRead = VAL_FAA(&bckt->extractions, 1);

	// If another operation is in progress, return the operation
	if(is_freezed_for_mov(idxRead)) return MOV_FOUND;
	if(is_freezed_for_epo(idxRead)) return ABORT;
	if(is_freezed_for_del(idxRead)) return EMPTY;

	arrayNodes_t* ordered = bckt->arrayOrdered;
	if(!is_freezed_for_lnk(idxRead) && ordered == NULL) return ABORT;

	// FIXME: Forse risolto
	if(!is_freezed_for_lnk(idxRead) && getDynamic(idxRead) > getFixed(ordered->indexWrite)){
		post_operation(bckt, DELETE, 0ULL, NULL);
		return EMPTY;
	}

	if(!is_freezed_for_lnk(idxRead)){
		// Assert sui flags (SUPER IMPORTANTE)
/* 		assert(validContent(unBlock(bckt->ptr_arrays[numaNode])->indexWrite) == false && unordered(bckt) == false && is_freezed_for_lnk(bckt->extractions) == false
			&& bckt->head.next == bckt->tail); */
		if(getDynamic(idxRead) < bckt->arrayOrdered->length && getDynamic(idxRead) < getFixed(bckt->arrayOrdered->indexWrite)){
			res = nodesDequeue(bckt->arrayOrdered, getDynamic(idxRead), result, ts);
			//assert(*result != NULL && *ts >= MIN && *ts < INFTY);
			if(res == MYARRAY_EXTRACT) return OK;
			else return ABORT;
		}else{
			goto begin;
		}
	}
	// LUCKY: end

	assert(curr != NULL);

	// if(__last_node != NULL){
	// 	curr = __last_node;
	// 	extracted -= __last_val;
	// }

	// LUCKY:
	unsigned long long pos = get_extractions_wtoutlk(idxRead)+1;
	scan_list_length += skip_extracted(tail, &curr, pos);

	// If it arrived at the end of the list then return EMPTY state
	if(curr->timestamp == INFTY)	assert(curr == tail);
	if(curr == tail){
		post_operation(bckt, DELETE, 0ULL, NULL);
		return EMPTY; 
	}
	// __last_node = curr;
	// __last_val  = old_extracted;
	//	__sync_bool_compare_and_swap(&curr->taken, 0, 1);

	// Return the important information of extracted node
	*result = curr->payload;
	*ts		= curr->timestamp;

	return OK;
}

#endif
