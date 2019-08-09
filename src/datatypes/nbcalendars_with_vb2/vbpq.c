#include <stdlib.h>
#include <math.h>

#include "vbpq.h"
#include "../../key_type.h"
#include "../../arch/atomic.h"
#include "../../utils/hpdcs_utils.h"
#include "../../gc/ptst.h"




/*************************************
 * THREAD LOCAL VARIABLES			 *
 ************************************/

__thread unsigned long long concurrent_enqueue;
__thread unsigned long long performed_enqueue ;
__thread unsigned long long concurrent_dequeue;
__thread unsigned long long performed_dequeue ;
__thread unsigned long long scan_list_length;
__thread unsigned long long scan_list_length_en ;
__thread unsigned int 		read_table_count	 = UINT_MAX;
__thread unsigned long long num_cas = 0ULL;
__thread unsigned long long num_cas_useful = 0ULL;
__thread unsigned long long near = 0;
__thread unsigned int 		acc = 0;
__thread unsigned int 		acc_counter = 0;


__thread double last_bw = 0.0;


int gc_aid[2];
int gc_hid[1];



void std_free_hook(ptst_t *p, void *ptr){	free(ptr); }

/**
 * This function create an instance of a NBCQ.
 *
 * @param threshold: ----------------
 * @param perc_used_bucket: set the percentage of occupied physical buckets 
 * @param elem_per_bucket: set the expected number of items for each virtual bucket
 * @return a pointer a new queue
 */
void* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{
	unsigned int i = 0;
	int res_mem_posix = 0;
	vbpq* res = NULL;

	// init fraser garbage collector/allocator 
	_init_gc_subsystem();
	// add allocator of nbc_bucket_node
	init_bucket_subsystem();
	__init_skipList_subsystem();
	// add callback for set tables and array of nodes whene a grace period has been identified
	gc_hid[0] = gc_add_hook(std_free_hook);
	critical_enter();
	critical_exit();

	// allocate memory
	res_mem_posix = posix_memalign((void**)(&res), CACHE_LINE_SIZE, sizeof(vbpq));
	if(res_mem_posix != 0) 	error("No enough memory to allocate queue\n");
	res_mem_posix = posix_memalign((void**)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table_t));
	if(res_mem_posix != 0)	error("No enough memory to allocate set table\n");
	

	
	res->hashtable->threshold = threshold;
	res->hashtable->perc_used_bucket = perc_used_bucket;
	res->hashtable->elem_per_bucket = elem_per_bucket;
	res->hashtable->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->hashtable->read_table_period = READTABLE_PERIOD;
	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;
	res->hashtable->current = 0;
	res->hashtable->socket = -1;
	res->hashtable->last_resize_count = 0;
	res->hashtable->resize_count = 0;
	res->hashtable->e_counter.count = 0;
	res->hashtable->d_counter.count = 0;
	res->hashtable->b_tail.extractions = 0ULL;
	res->hashtable->b_tail.epoch = 0U;
	res->hashtable->b_tail.index = UINT_MAX;
	res->hashtable->b_tail.type = TAIL;
	res->hashtable->b_tail.next = NULL; 
	tail_node_init(&res->hashtable->n_tail);
	
	res->hashtable->cached_node = NULL;
	for (i = 0; i < MINIMUM_SIZE; i++)
	{
		res->hashtable->array[i].next = &res->hashtable->b_tail;
		res->hashtable->array[i].tail = &res->hashtable->n_tail;
		res->hashtable->array[i].type = HEAD;
		res->hashtable->array[i].epoch = 0U;
		res->hashtable->array[i].index = 0U;
		res->hashtable->array[i].socket = -1;
		res->hashtable->array[i].extractions = 0ULL;
	}

	res->hashtable->index = alloc_index(MINIMUM_SIZE);
	init_index(res->hashtable);
	return res;
}



/**
 * This function implements the enqueue interface of the NBCQ.
 * Cost O(1) when succeeds
 *
 * @param q: pointer to the queueu
 * @param timestamp: the key associated with the value
 * @param payload: the value to be enqueued
 * @return true if the event is inserted in the set table else false
 */
int pq_enqueue(void* q, pkey_t timestamp, void* payload)
{
	t_state = 1;
	insertions++;
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");
	vbpq* queue = (vbpq*) q; 	
	bucket_t *bucket;
	table_t *h = NULL;		
	unsigned int index, size, epoch;
	unsigned long long newIndex = 0;
	int res, con_en = 0;
	SkipList *lookup_table = NULL;
	

	//init the result
	res = MOV_FOUND;

	critical_enter();
	
	//repeat until a successful insert
	while(res != OK){
		
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){

			// check for a resize
			t_state = 0;
			h = read_table(&queue->hashtable);
			// get actual size
			size = h->size;
	        // read the actual epoch
	        	epoch = (h->current & MASK_EPOCH);
			last_bw = h->bucket_width;
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			// compute the index of the physical bucket
			index = virtual_to_physical(((unsigned int) newIndex), size);

//			lookup_table = h->index->array[index];
			// get the bucket
			bucket = h->array + index;
			// read the number of executed enqueues for statistics purposes
			con_en = h->e_counter.count;
		}

		#if KEY_TYPE != DOUBLE
		if(res == PRESENT){
			res = 0;
			goto out;
		}
		#endif
		t_state = 2;
		// search the two adjacent nodes that surround the new key and try to insert with a CAS 
	    res = search_and_insert(bucket, lookup_table, newIndex, timestamp, 0, epoch, payload);
	}


	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
	flush_current(h, newIndex);
	performed_enqueue++;
	res=1;
	
	// updates for statistics
	
	concurrent_enqueue += (unsigned long long) (__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);
	
	#if COMPACT_RANDOM_ENQUEUE == 1
	// clean a random bucket
	unsigned long long oldCur = h->current;
	unsigned long long oldIndex = oldCur >> 32;
	unsigned long long dist = 1;
	double rand;
	bucket_t *left_node, *left_node_next, *right_node;
	drand48_r(&seedT, &rand);
	unsigned int counter = 0;
	left_node = search(h->array+((oldIndex + dist + (unsigned int)( ( (double)(size-dist) )*rand )) % size), &left_node_next, &right_node, &counter, 0);
	if(is_marked(left_node_next, VAL) && left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, right_node))
		connect_to_be_freed_node_list(left_node_next, counter);
	#endif

  #if KEY_TYPE != DOUBLE
  out:
  #endif
	critical_exit();
	return res;
}




/**
 * This function extract the minimum item from the NBCQ. 
 * The cost of this operation when succeeds should be O(1)
 *
 * @param  q: pointer to the interested queue
 * @param  result: location to save payload address
 * @return the highest priority 
 *
 */
unsigned int ex = 0;

__thread unsigned long long __last_current 	= -1;
__thread unsigned long long __last_index 	= -1;
__thread table_t *__last_table 				= NULL;
__thread bucket_t *__last_bckt 				= NULL;


pkey_t pq_dequeue(void *q, void** result)
{
t_state = 0;
	vbpq *queue = (vbpq*)q;
	bucket_t *min, *min_next, 
					*left_node, *left_node_next, 
					*array, *right_node;
	table_t * h = NULL;
	
	unsigned long long current, old_current, new_current;
	unsigned long long index;
	unsigned long long epoch;
	
	unsigned int size, attempts = 0;
	unsigned int counter;
	int res = ABORT;
	pkey_t left_ts;
	int con_de = 0;
	performed_dequeue++;
	
	critical_enter();

begin:
	// Get the current set table
t_state =0;
	h = read_table(&queue->hashtable);
t_state =1;
//   acquire_node(&h->socket);
	// Get data from the table
	size = h->size;
	array = h->array;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	if(__last_table != h){
		__last_table 	= h;
		__last_current  = -1;
		__last_index  = -1;
		__last_bckt 	= NULL;
		__last_node 	= NULL;
		__last_val 	= 0;
	}

	do
	{
		*result  = NULL;
		left_ts  = INFTY;

		// To many attempts: there is some problem? recheck the table
		if( h->read_table_period == attempts){	goto begin; }
		attempts++;

		if(__last_current != current)
		{
			// get fields from current
			index = current >> 32;

			__last_current  = current;
			__last_index	= index;
			__last_bckt 	= NULL;
			__last_node 	= NULL;
			__last_val 		= 0;
		}
		else index = __last_index;

		epoch = current & MASK_EPOCH;

		// get the physical bucket
		min = array + virtual_to_physical(index, size);
		min_next = min->next;

		// a reshuffle has been detected => restart
		if(is_marked(min_next, MOV)) goto begin;

		left_node = __last_bckt;

		if(left_node == NULL || left_node->index != index || is_freezed(left_node->extractions))
		{
			left_node = search(min, &left_node_next, &right_node, &counter, index);
			if(is_marked(left_node_next, VAL) && left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, right_node))
				connect_to_be_freed_node_list(left_node_next, counter);
	

			// if i'm here it means that the physical bucket was empty. Check for queue emptyness
			if(left_node->type == HEAD  && right_node->type == TAIL   && size == 1 && !is_freezed_for_mov(min->extractions))
			{
				critical_exit();
				*result = NULL;
				return INFTY;
			}
		}

		// Bucket present
		if(left_node->index == index && left_node->type != HEAD){

			if(left_node->epoch > epoch) goto begin;
			if(left_node != __last_bckt){
				__last_bckt 	= left_node;
				__last_node 	= &left_node->head;
				__last_val 	= 0;
			}
			res = extract_from_bucket(left_node, result, &left_ts, (unsigned int)epoch);
			
			if(res == MOV_FOUND) goto begin;

			// The bucket was not empty
			if(res == OK){
					critical_exit();
					concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);
					return left_ts;
			}
		}

		// bucket empty or absent
		new_current = h->current;
		if(new_current == current){
			
			// save from livelock with empty queue
			if(index == MASK_EPOCH && h->e_counter.count == 0) goto begin;

			index++;
			__last_index++;

			if( (__last_index - (__last_current>>32)) == EXTRACTION_VIRTUAL_BUCKET_LEN){
				// increase current

				num_cas++;
				double rand, prob;
				drand48_r(&seedT, &rand);
				prob = (double)h->d_counter.count - (double)con_de ;
				if(prob > 1.0) prob = 1/prob;
				else prob = 1.0;

				if(rand < prob){
					old_current = VAL_CAS( &(h->current), current, ((index << 32) | epoch) );
					if(old_current == current){
						current = ((index << 32) | epoch);
						num_cas_useful++;
					}
					else
						current = old_current;
				}
				else
					current = h->current;
			}
		}
		else
			current = new_current;
		
		
	}while(1);
	
	return INFTY;
}

__thread unsigned int t_state = 0;
__thread unsigned long long search_en = 0ULL;
__thread unsigned long long search_de = 0ULL;

__thread unsigned long long rtm_other=0ULL, rtm_prova=0ULL, rtm_failed=0ULL, rtm_retry=0ULL, rtm_conflict=0ULL, rtm_capacity=0ULL, rtm_debug=0ULL,  rtm_explicit=0ULL,  rtm_nested=0ULL, rtm_insertions=0ULL, insertions=0ULL, rtm_a=0ULL, rtm_b=0ULL;
__thread unsigned long long rtm_other2=0ULL, rtm_prova2=0ULL, rtm_failed2=0ULL, rtm_retry2=0ULL, rtm_conflict2=0ULL, rtm_capacity2=0ULL, rtm_debug2=0ULL,  rtm_explicit2=0ULL,  rtm_nested2=0ULL, rtm_insertions2=0ULL, insertions2=0ULL, rtm_a2=0ULL, rtm_b2=0ULL;
//__thread double last_bw = 0.0;

void pq_report(int TID)
{

unsigned long long cache_load = 0, cache_hit = 0, cache_invs = 0;

cache_load	= get_loads();
cache_hit	= get_hits();
cache_invs	= get_invs();

printf("CHANGE EPOCH REQ: CHANGE_EPO_SUCC %llu - CHANGE_EPO_RQ %llu - BUCKET_CONNECT %llu - Insertion cache efficiency %f %llu %llu\n", count_epoch_ops, rq_epoch_ops, bckt_connect_count,  ((float)cache_hit/(float)cache_load), cache_hit, cache_load);

printf("TID:%u NID:%u BCKT CONNECT "
"ABORTRATE:%f, "
"TSX:%llu, "
"RTM_OTHER:%f, "
//"RTM_ABORTED:%f, "
"RETRY:%f, "
"CONFLICT:%f, "
"CAPACITY %f, "
"DEBUG %f, "
"EXPLICIT %f, "
"NESTED %f, "
"A %f, "
"B %f, "
"PERC_OF_SKIPSEARCH %f, "
"RTM_INSERTIONS %llu, "
"SKIP_RTM_INSERTIONS %llu, "
"FALL_INSERTIONS %llu, "
"NO_RTM INSERTIONS %llu \n", 
tid,nid,
((double)rtm_failed)/((double)rtm_prova),
rtm_prova, 
((double)rtm_other)	/((double)rtm_prova), 	
((double)rtm_retry)		/((double)rtm_prova), 
((double)rtm_conflict)	/((double)rtm_prova), 
((double)rtm_capacity)	/((double)rtm_prova),  
((double)rtm_debug)		/((double)rtm_prova),
((double)rtm_explicit)	/((double)rtm_prova),  
((double)rtm_nested)	/((double)rtm_prova), 
((double)rtm_a)    /((double)rtm_prova),
((double)rtm_b)    /((double)rtm_prova),
((double)accelerated_searches)    /((double)searches_count),
rtm_insertions, 
rtm_skip_insertion,
fallback_insertions,
insertions-rtm_insertions);	


printf("MIGRATE "
"ABORTRATE:%f, "
"TSX:%llu, "
"RTM_OTHER:%f, "
//"RTM_ABORTED:%f, "
"RETRY:%f, "
"CONFLICT:%f, "
"CAPACITY %f, "
"DEBUG %f, "
"EXPLICIT %f, "
"NESTED %f, "
"A %f, "
"B %f, "
"RTM_INSERTIONS %llu, "
"NO_RTM INSERTIONS %llu \n", 
((double)rtm_failed2)/((double)rtm_prova2),
rtm_prova2, 
((double)rtm_other2)	/((double)rtm_prova2), 	
((double)rtm_retry2)		/((double)rtm_prova2), 
((double)rtm_conflict2)	/((double)rtm_prova2), 
((double)rtm_capacity2)	/((double)rtm_prova2),  
((double)rtm_debug2)		/((double)rtm_prova2),
((double)rtm_explicit2)	/((double)rtm_prova2),  
((double)rtm_nested2)	/((double)rtm_prova2), 
((double)rtm_a2)    /((double)rtm_prova2),
((double)rtm_b2)    /((double)rtm_prova2),
rtm_insertions2, 
insertions2-rtm_insertions2);	


	printf("%d- "
"BCKT contention %.10f - %llu - %llu - %llu ### "
	"Enqueue: %.10f LEN: %.10f ### "
	"Dequeue: %.10f LEN: %.10f ### "
	"SEARCH STEPS EN: %.10f ### "
 "SEARCH STEPS DE: %.10f ### "
	"NUMCAS: %llu : %llu ### "
	"NEAR: %llu "
	"RTC:%d,M:%lld, BW:%f\n",
			TID,
			((float)acc_contention)	    /((float)cnt_contention),	
			min_contention, 
			max_contention,
			cnt_contention,
			((float)concurrent_enqueue) /((float)performed_enqueue),
			((float)scan_list_length_en)/((float)performed_enqueue),
			((float)concurrent_dequeue) /((float)performed_dequeue),
			((float)scan_list_length)   /((float)performed_dequeue),
			((float)search_en)		/((float)performed_enqueue),
			((float)search_de)              /((float)performed_dequeue),
			num_cas, num_cas_useful,
			near,
			read_table_count	  ,
			malloc_count, last_bw);
}

void pq_reset_statistics(){
		near = 0;
		num_cas = 0;
		num_cas_useful = 0;	
}

unsigned int pq_num_malloc(){ return (unsigned int) malloc_count; }
