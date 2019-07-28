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

#include "channel.h"



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
	// add callback for set tables and array of nodes whene a grace period has been identified
	gc_hid[0] = gc_add_hook(std_free_hook);
	critical_enter();
	critical_exit();

	// allocate memory
	res_mem_posix = posix_memalign((void**)(&res), CACHE_LINE_SIZE, sizeof(vbpq));
	if(res_mem_posix != 0) 	error("No enough memory to allocate queue\n");
	res_mem_posix = posix_memalign((void**)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table_t));
	if(res_mem_posix != 0)	error("No enough memory to allocate set table\n");
	res_mem_posix = posix_memalign((void**)(&communication_channels), CACHE_LINE_SIZE, threshold*sizeof(channel_t));
	if(res_mem_posix != 0)	error("No enough memory to allocate communication_channels\n");
	
	
	res->hashtable->threshold = threshold;
	res->hashtable->perc_used_bucket = perc_used_bucket;
	res->hashtable->elem_per_bucket = elem_per_bucket;
	res->hashtable->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->hashtable->read_table_period = READTABLE_PERIOD;
	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;
	res->hashtable->current = 0;
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
		res->hashtable->array[i].extractions = 0ULL;
	}
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
	insertions++;
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");
	void *old_payload = payload;
	pkey_t old_timestamp = timestamp;
	acquire_channels_ids();


	do{
		if(am_i_sender(q, timestamp)){
			unsigned int __status;
			retry_post:
			if((__status = _xbegin ()) == _XBEGIN_STARTED)
			{
				communication_channels[my_snd_id].payload   = old_payload;
				communication_channels[my_snd_id].timestamp = old_timestamp;
				communication_channels[my_snd_id].state 	= OP_PENDING;
				TM_COMMIT();
			}
			else goto retry_post;
		}
		else{
			communication_channels[my_snd_id].state = OP_COMPLETED;
			while(internal_enqueue(q, old_timestamp, old_payload, false) != OK);
		}

		while(communication_channels[my_snd_id].state == OP_PENDING || communication_channels[my_rcv_id].state == OP_PENDING){
			if(communication_channels[my_rcv_id].state == OP_PENDING){
				payload 	= communication_channels[my_rcv_id].payload;
				timestamp 	= communication_channels[my_rcv_id].timestamp;
				int res = internal_enqueue(q, timestamp, payload, false);
				if(res == OK) 	 __sync_bool_compare_and_swap(&communication_channels[my_rcv_id].state, OP_PENDING, OP_COMPLETED);
				if(res == ABORT) __sync_bool_compare_and_swap(&communication_channels[my_rcv_id].state, OP_PENDING, OP_ABORTED );
			} 
			// do other work
		}
	}while(communication_channels[my_snd_id].state != OP_COMPLETED);

	return 1;

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

__thread unsigned long long __last_current = -1;
__thread table_t *__last_table = NULL;
__thread bucket_t *__last_bckt 	= NULL;


pkey_t pq_dequeue(void *q, void** result)
{
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
	h = read_table(&queue->hashtable);

	// Get data from the table
	size = h->size;
	array = h->array;
	current = h->current;
	con_de = h->d_counter.count;
	attempts = 0;

	if(__last_table != h){
		__last_table 	= h;
		__last_current  = -1;
		__last_bckt 	= NULL;
		__last_node 	= NULL;
		__last_val 	= 0;
	}

	do
	{
		*result  = NULL;
		left_ts  = INFTY;

		// To many attempts: there is some problem? recheck the table
		if( h->read_table_period == attempts){
			goto begin;
		}
		attempts++;

		// get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;

		// get the physical bucket
		min = array + (index % (size));
		min_next = min->next;

		// a reshuffle has been detected => restart
		if(is_marked(min_next, MOV)) goto begin;

		if(__last_current != current)
		{
			__last_current  = current;
			__last_bckt 	= NULL;
			__last_node 	= NULL;
			__last_val 	= 0;
		}
		left_node = __last_bckt;

		if(left_node == NULL || left_node->index != index || is_freezed(left_node->extractions))
		{
			left_node = search(min, &left_node_next, &right_node, &counter, index);

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

			num_cas++;
			index++;

			// increase current
			old_current = VAL_CAS( &(h->current), current, ((index << 32) | epoch) );
			if(old_current == current){
				current = ((index << 32) | epoch);
				num_cas_useful++;
			}
			else
				current = old_current;
		}
		else
			current = new_current;
		
	}while(1);
	
	return INFTY;
}



__thread unsigned long long rtm_other=0ULL, rtm_prova=0ULL, rtm_failed=0ULL, rtm_retry=0ULL, rtm_conflict=0ULL, rtm_capacity=0ULL, rtm_debug=0ULL,  rtm_explicit=0ULL,  rtm_nested=0ULL, rtm_insertions=0ULL, insertions=0ULL, rtm_a=0ULL, rtm_b=0ULL;
__thread unsigned long long rtm_other2=0ULL, rtm_prova2=0ULL, rtm_failed2=0ULL, rtm_retry2=0ULL, rtm_conflict2=0ULL, rtm_capacity2=0ULL, rtm_debug2=0ULL,  rtm_explicit2=0ULL,  rtm_nested2=0ULL, rtm_insertions2=0ULL, insertions2=0ULL, rtm_a2=0ULL, rtm_b2=0ULL;
//__thread double last_bw = 0.0;

void pq_report(int TID)
{

unsigned long long cache_load = 0, cache_hit = 0;
int h=0;
for(h=0; h<INSERTION_CACHE_LEN-1;h++){
       cache_load+=__cache_load[h];
       cache_hit+=__cache_hit[h];
}
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
"RTM_INSERTIONS %llu, "
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
rtm_insertions, 
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
	"Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ### "
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
