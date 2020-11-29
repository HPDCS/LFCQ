#ifndef VBUCKET_ARRAY_H
#define VBUCKET_ARRAY_H

#include <stdlib.h>
#include <immintrin.h>

#include "../../../key_type.h"
#include "../../../utils/rtm.h"
#include "../../../gc/gc.h"
#include "../../../utils/hpdcs_utils.h"
#include "../mm_vbucket.h"
#include "../cache.h"
#include "../nb_lists_defs.h"
#include "./array.h"

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
typedef struct __node_t {
	void *payload;						// 8
	pkey_t timestamp;  					//  
	char __pad_1[8-sizeof(pkey_t)];		// 16
	unsigned int tie_breaker;			// 20
	int hash;							// 24
	void * volatile replica;			// 40
	node_t * volatile next;				// 32
	node_t * volatile upper_next[VB_NUM_LEVELS-1];	// 64
} node_t;

/**
 *  Struct that define a bucket
 */ 
typedef struct __bucket_t {
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
	// Fortunato
	arrayNodes_t* array;
	arrayNodes_t* arrayOrdered;
	unsigned long long indexWrite;
	unsigned long long indexRead;
	//-----------------------------
  #ifndef RTM
	pthread_spinlock_t lock;
  #endif
} bucket_t;

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

// TODO: TO DELETE BECAUSE UNUSED FUNCTION
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
	res = bucket_alloc(bckt->tail);
	res->extractions = get_cleaned_extractions(old_extractions);
	res->epoch = bckt->op_descriptor & MASK_EPOCH;
	res->epoch = res->epoch < bckt->epoch ? bckt->epoch :  res->epoch;
	res->index = bckt->index;
	res->type	= bckt->type;

	res->head.payload = NULL;
	res->head.timestamp	= MIN;
	res->head.tie_breaker	= 0U;
	res->head.next = bckt->head.next;
			
	do{
		old_next = bckt->next;
		res->next = old_next;
		suc=false;
	}while(is_marked(old_next, VAL) && 
		!(suc = __sync_bool_compare_and_swap(&(bckt)->next, old_next, get_marked(res, DEL))));
	// With the CAS in the while condition I mark old bucket as delete and substitute it with the new bucket
	atomic_bts_x64(&bckt->extractions, DEL_BIT_POS);
	old_next = get_unmarked(bckt->next);
	
	// TODO: TO DELETE becuse cache
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
		while(!is_freezed(old_extractions)){
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
		while(!is_freezed(old_extractions)){
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

__thread unsigned long long rtm_skip_insertion = 0;
__thread unsigned long long accelerated_searches = 0;
__thread unsigned long long searches_count = 0;
/**
 * Function that inserts a new node in the list stored in the bucket
*/
int bucket_connect(bucket_t *bckt, pkey_t timestamp, unsigned int tie_breaker, void* payload, unsigned int epoch){
	bckt_connect_count++;

// TODO: To DELETE the row below
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
	int level = 0;
	long long contention = 0;
	acc_contention+=contention;
	cnt_contention++;
	min_contention = contention <= min_contention ? contention : min_contention;
	max_contention = contention >= max_contention ? contention : max_contention;
    // FIXME: Romolo, chiedere per la alloc by index
	node_t *newN   = node_alloc(); //node_alloc_by_index(bckt->index);
	newN->timestamp = timestamp;
	newN->payload	= payload;

	// Check the validity of the bucket
	validate_bucket(bckt);

	int i = 0;
	long rand, record = 0;
	lrand48_r(&seedT, &rand);
	for(i=0;i<VB_NUM_LEVELS-1;i++){
		if(rand & 1) level++;
		else break;
		rand >>= 1;
	}
	assert(level <= VB_MAX_LEVEL);

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
	// Get amount of extractions, important
	extracted = bckt->extractions;

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

  // The amount of elements to skip are equal to the extracted value
  toskip = extracted;
    
  // Set the level at 0 if we have been extractions
	record = extracted == 0;
	if(!record) level  = 0;
	
	/** TODO: TO DELETE, the same code of skip_extracted
  	position = 0;
	while(toskip > 0ULL && curr != tail){
  		curr = curr->next;
		if(curr) 	PREFETCH(curr->next, 0);
  		toskip--;
  		scan_list_length_en++;
  		position++;
  	}
	*/

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
	if(DISABLE_SKIPLIST || !record){
		left = curr;
		curr = curr->next;
		// Level is set to 0 (the lowest)
		position += fetch_position(&curr, &left, timestamp, 0);
		scan_list_length_en+=position;
	}else{
		// Insert using the skiplist ...
		node_t *preds[VB_NUM_LEVELS], *succs[VB_NUM_LEVELS];
		accelerated_searches++;

		for(i=VB_MAX_LEVEL;i>=0;i--){
			preds[i] = curr;
			succs[i] = curr->upper_next[i-1];
			scan_list_length_en+=fetch_position(&succs[i], &preds[i], timestamp, i);
			curr 	 = preds[i];
		}
// TODO: TO DELETE, unrolled of above loop
/**
		preds[3] = curr;
		succs[3] = curr->upper_next[2];
		scan_list_length_en+=fetch_position(&succs[3], &preds[3], timestamp, 3);
		curr 	 = preds[3];

		preds[2] = curr;
		succs[2] = curr->upper_next[1];
		scan_list_length_en+=fetch_position(&succs[2], &preds[2], timestamp, 2);
		curr	 = preds[2];

		preds[1] = curr;
		succs[1] = curr->upper_next[0];
		scan_list_length_en+=fetch_position(&succs[1], &preds[1], timestamp, 1);
		curr	 = preds[1];

		preds[0] = curr;
		succs[0] = curr->next;
		scan_list_length_en+=fetch_position(&succs[0], &preds[0], timestamp, 0);
		curr	 = preds[0];
*/
		left = preds[0];
		curr = succs[0];


		if(level > 0){
			// FIXME: Devo farlo sempre su preds[0] questo check oppure doveva essere left
			if(preds[0]->timestamp == timestamp) newN->tie_breaker+= preds[0]->tie_breaker;

			for(i=0;i<VB_NUM_LEVELS;i++)
				newN->upper_next[i-1] = level >= i ? succs[i] : NULL;

			// TODO: TO DELETE, unrolled above loop
/**				if(level >= 0)			newN->upper_next[-1]	= succs[0];
				if(level >= 1) 			newN->upper_next[ 0] 	= succs[1];
				if(level >= 2) 			newN->upper_next[ 1] 	= succs[2];
				if(level >= 3) 			newN->upper_next[ 2] 	= succs[3];
*/

/**										newN->next 			= succs[0];
				if(level >= 1) 			newN->upper_next[0] 	= succs[1];
				if(level >= 2) 			newN->upper_next[1] 	= succs[2];
				if(level >= 3) 			newN->upper_next[2] 	= succs[3];
*/

				__local_try=0;
				__status = 0;
				mask = 1;
			  retry_tm2:

				BACKOFF_LOOP();

				if(bckt->extractions != 0){ rtm_debug++; goto begin; }

				for(i=0;i<VB_NUM_LEVELS;i++)
					if(level >= i && preds[i]->upper_next[i-1] != succs[i]){ rtm_nested++; goto begin; }

/**			    
				if(				 preds[0]->next 	     != succs[0])	{rtm_nested++;goto begin;}
				if(level >= 1 && preds[1]->upper_next[0] != succs[1])	{rtm_nested++;goto begin;}
				if(level >= 2 && preds[2]->upper_next[1] != succs[2])	{rtm_nested++;goto begin;}
				if(level >= 3 && preds[3]->upper_next[2] != succs[3])	{rtm_nested++;goto begin;}
*/

/**
			  	for(i=0;i<level;i++)
					if(preds[i+1]->upper_next[i] != succs[i+1])	{rtm_nested++;goto begin;}
*/
//				if((rand & 3) == 3 && preds[2]->upper_next[1] != succs[2])	{rtm_nested++;goto begin;}

				++rtm_prova;
				__status = 0;

				assert(newN != NULL && preds[0]->next != NULL && succs[0] != NULL);
				if((__status = _xbegin ()) == _XBEGIN_STARTED){
					// If it was done an extraction then abort
					if(bckt->extractions != 0) TM_ABORT(0xf2);

					// If there is some difference from previous structure state then abort
					for(i=0;i<VB_NUM_LEVELS;i++)
						if(level >= i && preds[i]->upper_next[i-1] != succs[i]){ TM_ABORT(0xf1);}
// TODO: DELETE
/**					if(				 preds[0]->next 	     != succs[0])	{ TM_ABORT(0xf1);}
					if(level >= 1 && preds[1]->upper_next[0] != succs[1])	{ TM_ABORT(0xf1);}
					if(level >= 2 && preds[2]->upper_next[1] != succs[2])	{ TM_ABORT(0xf1);}
					if(level >= 3 && preds[3]->upper_next[2] != succs[3])	{ TM_ABORT(0xf1);}
*/
				// Insert the new node in the lost
				for(i=0;i<VB_NUM_LEVELS;i++)
					if(level >= i) preds[i]->upper_next[i-1] = newN;

// TODO: DELETE
/**					if(level >= 0 ) preds[0]->upper_next[-1]= newN; //						preds[0]->next 			= newN;
					if(level >= 1 ) preds[1]->upper_next[0] = newN;
					if(level >= 2 ) preds[2]->upper_next[1] = newN;
					if(level >= 3 ) preds[3]->upper_next[2] = newN;
*/

/**				  	for(i=0;i<level;i++)
						if(preds[i+1]->upper_next[i] != succs[i+1])	{ TM_ABORT(0xf1);}
*///
//					if(preds[1]->upper_next[0] != succs[1])	{ TM_ABORT(0xf1);}
//					if((rand & 3) == 3 && preds[2]->upper_next[1] != succs[2])	{ TM_ABORT(0xf1);}

/**
					preds[0]->next 			= newN; 
				  	for(i=0;i<level;i++)
				  		preds[i+1]->upper_next[i] 	= newN;
*/
//					preds[1]->upper_next[0] 	= newN; 
//					if((rand & 3) == 3)			preds[2]->upper_next[1] 	= newN; 
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
					if( /*__status & _XABORT_RETRY || */ __local_try++ < RTM_RETRY){
						mask = mask | (mask <<1);
						goto retry_tm2;
					}
					if(__global_try < RTM_FALLBACK || __status & _XABORT_EXPLICIT)
						goto begin;
					// FIXME: Romolo a cosa serve questa FAA? è statistica?
					//		__sync_fetch_and_add(&bckt->pad3, -1LL);
					// Execute the fallback path because TSX path always fail
					res = bucket_connect_fallback(bckt, newN, epoch); 
					// In case of error, delete the created node and return the error status
					if(res != OK) node_unsafe_free(newN);
					return res;
				}
				rtm_insertions++;
				rtm_skip_insertion+=1;
			  goto out;
		}
	}

	/**
  	while(curr->timestamp <= timestamp){
		if(counter_last_key > 1000000ULL)	printf("L: %p-" KEY_STRING " C: %p-" KEY_STRING "\n", left, left->timestamp, curr, curr->timestamp);
  		left = curr;
  		curr = curr->next;
		if(curr) PREFETCH(curr->next, 1);
		scan_list_length_en++;
  		position++;
  	}
	*/ 
	record = 0;
	assert(curr->timestamp != INFTY || curr == tail);

	if(left->timestamp == timestamp){ newN->tie_breaker+= left->tie_breaker; }
	
	newN->next = curr;
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

	assert(newN != NULL && left->next != NULL && curr != NULL);
	if((__status = _xbegin ()) == _XBEGIN_STARTED){
		// If has been done any extractions then abort
		if(position < bckt->extractions){ TM_ABORT(0xf2);}
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
		return res;
	}

	rtm_insertions++;
	rtm_skip_insertion+=record;
  out:

	validate_bucket(bckt);
//	__sync_fetch_and_add(&bckt->pad3, -1LL);

	update_cache(bckt);
	return res;
}


__thread node_t   *__last_node 	= NULL;
__thread unsigned long long __last_val = 0;
/**
 * Function that extract the node that has the minimum timestamp from the passed bucket
*/
static inline int extract_from_bucket(bucket_t *bckt, void ** result, pkey_t *ts, unsigned int epoch){
//	acquire_node(&bckt->socket);
	node_t *curr  = &bckt->head;
	node_t *tail  = bckt->tail;

	//node_t *head  = &bckt->head;
	unsigned long long old_extracted = 0;	
	unsigned long long extracted = 0;
	//unsigned skipped = 0;
	PREFETCH(curr->next, 0);
	assertf(bckt->type != ITEM, "trying to extract from a head bucket%s\n", "");

	// If the bucket epoch is greater than the parameter epoch then abort,
	// I can't do an extract something that can be more recent of epoch from the bucket
	if(bckt->epoch > epoch) return ABORT;

//  unsigned int count = 0;	long rand;  lrand48_r(&seedT, &rand); count = rand & 7L; while(count-->0)_mm_pause();

	old_extracted = extracted = __sync_add_and_fetch(&bckt->extractions, 1ULL);

	// If another operation is in progress, return the operation
	if(is_freezed_for_mov(extracted)) return MOV_FOUND;
	if(is_freezed_for_epo(extracted)) return ABORT;
	if(is_freezed_for_del(extracted)) return EMPTY;

	validate_bucket(bckt);

	// FIXME: Romolo, condition always false
	if(__last_node != NULL){
  		curr = __last_node;
  		extracted -= __last_val;
	}

	scan_list_length += skip_extracted(tail, &curr, extracted);

	/**
  	while(extracted > 0ULL && curr != tail){
  		//if(skipped > 25 && extracted > 2){
  		//	if((__status = _xbegin ()) == _XBEGIN_STARTED)
		//	{
		//		if(old_extracted != bckt->extractions){ TM_ABORT(0xf2);}
		//		head->next = curr;
		//		old_extracted -= skipped; 
		//		TM_COMMIT();
		//		skipped = 0;
		//	}
		//	else{}
		//
  		//}
  		curr = curr->next;
		if(curr) 	PREFETCH(curr->next, 0);
  		extracted--;
  		skipped++;
		scan_list_length++;
  	}
	*/

	// If it arrived at the end of the list then return EMPTY state
	if(curr->timestamp == INFTY)	assert(curr == tail);
	if(curr == tail){
		post_operation(bckt, DELETE, 0ULL, NULL);
		return EMPTY; 
	}
	__last_node = curr;
	__last_val  = old_extracted;
//	__sync_bool_compare_and_swap(&curr->taken, 0, 1);

	// Return the important information of extracted node
	*result = curr->payload;
	*ts		= curr->timestamp;
	return OK;
}

#endif