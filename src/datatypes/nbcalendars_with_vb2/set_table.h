#ifndef SET_TABLE_H_
#define SET_TABLE_H_

#include "../../arch/atomic.h"
#include "vbucket.h"
#include "skipList.h"


#define TID tid

extern __thread unsigned long long near;
extern __thread unsigned long long num_cas;
extern __thread unsigned long long num_cas_useful;
extern __thread unsigned long long malloc_count;
extern __thread unsigned int TID;
extern __thread unsigned int acc;
extern __thread unsigned int acc_counter;
extern __thread struct drand48_data seedT;
extern __thread unsigned long long scan_list_length_en;
extern __thread unsigned long long scan_list_length;
extern __thread unsigned int read_table_count;

extern __thread unsigned long long concurrent_enqueue;
extern __thread unsigned long long performed_enqueue ;
extern __thread unsigned long long concurrent_dequeue;
extern __thread unsigned long long performed_dequeue ;
extern __thread unsigned long long scan_list_length;
extern __thread unsigned long long scan_list_length_en ;

#define MINIMUM_SIZE 1
#define SAMPLE_SIZE 50

#define ENABLE_EXPANSION 1
#define READTABLE_PERIOD 64
#define COMPACT_RANDOM_ENQUEUE 1

#define BASE 1000000ULL 
#ifndef RESIZE_PERIOD_FACTOR 
#define RESIZE_PERIOD_FACTOR 4ULL //2000ULL
#endif
#define RESIZE_PERIOD RESIZE_PERIOD_FACTOR*BASE


#define PERC_RESIZE_COUNT 0.25

typedef struct __index index_t;
struct __index
{
	unsigned int length;
	SkipList ** volatile array;
};



typedef struct __table table_t;
struct __table
{
	table_t * volatile new_table;		// 8
	volatile double bucket_width;	//16  
	double perc_used_bucket;		//24
	double pub_per_epb;				//32
	unsigned int size;				//36
	unsigned int threshold;			//40        
	unsigned int read_table_period; //44
	unsigned int last_resize_count; //48
	unsigned int resize_count; 		//52
	unsigned int elem_per_bucket;	//56
	index_t		*index;				//64
	//char zpad1[8];				//64


	atomic_t e_counter;	
	char zpad2[60];

	atomic_t d_counter;
	char zpad3[60];
	
	volatile unsigned long long current; 
	char zpad4[56];     
	
	bucket_t * volatile cached_node; 
	char zpad5[56];     

	volatile int socket; 
	char zpad6[60];     
	
	node_t n_tail;

	bucket_t b_tail;
//	char zpad5[256-sizeof(bucket_t)];

	bucket_t array[1];			//32


};

#include "index.h"
#include "utils_set_table.h"


static inline bucket_t* get_next_valid(bucket_t *bckt){
	bucket_t *res, *res_next, *bckt_next;
	unsigned int count = -1;
	bckt_next = res_next = bckt->next;

	do{
		res = get_unmarked(res_next);
		execute_operation(res);
		res_next = res->next;
		count++;
	}while(res->type != TAIL && is_marked(res_next, DEL));

	if( count && __sync_bool_compare_and_swap(&bckt->next, bckt_next, get_marked(res, MOV)) ){
		connect_to_be_freed_node_list(bckt_next, count);
	}
	return res;
}


static int search_and_insert(bucket_t *head, SkipList *lookup_table, unsigned int index, pkey_t timestamp, unsigned int tie_breaker, unsigned int epoch, void* payload){
	bucket_t *left, *left_next, *right, *lookup_res;
	unsigned int distance;

	left = load_from_cache(index);
	if(left != NULL && bucket_connect(left, timestamp, tie_breaker, payload, epoch) == OK) return OK;
	invalidate_cache(index);

	do{
		long old_hash = 0;
//		int res = skipListContains(lookup_table, index, &lookup_res, &old_hash);

/*		if(
			0 &&
			res &&
			lookup_res != NULL && 
			lookup_res->index == index &&
			lookup_res->hash  == old_hash 
			//!is_freezed(lookup_res->extractions) && 
			//is_marked(lookup_res->next, VAL)
		)
		{
			__cache_load[0]++;
			if(
                        !is_freezed(lookup_res->extractions) 	&&
                        is_marked(lookup_res->next, VAL)	&&
			bucket_connect(lookup_res, timestamp, tie_breaker, payload, epoch) == OK
			) 
				return OK;
		}
*/		if( 0 &&
			lookup_res != NULL && 
			lookup_res->index <= index &&
                        !is_freezed(lookup_res->extractions)    &&
                        is_marked(lookup_res->next, VAL) &&
                        lookup_res->hash  == old_hash
		  )
		{
			__cache_load[0]++;
			do{
				left		= lookup_res;
				left_next	= left->next;
				lookup_res 	= right		= get_next_valid(left);
				
			}while(right->index <= index);
		}
	
/*	}

		if(!found){
			ListNode *pred = preds[MIN_LEVEL];
			ListNode *succ = succs[MIN_LEVEL];

			ListNode *newNode = makeNormalNode(key, topLevel, value);
			for (level = MIN_LEVEL; level <= topLevel; level++) {
				ListNode *succ = succs[level];
				SET_ATOMIC_REF(&(newNode->next[level]), succ, FALSE_MARK);
			}
			ListNode *pred = preds[MIN_LEVEL];
			ListNode *succ = succs[MIN_LEVEL];
			if (  !(REF_CAS(&(pred->next[MIN_LEVEL]), succ, newNode, FALSE_MARK, FALSE_MARK))  ) {
				gc_free(ptst, newNode, gc_id[topLevel]);
				continue;
			}
		}

		
		{
*/
		// first get bucket
		left = search(head, &left_next, &right, &distance, index);
		if(is_marked(left_next, VAL) && left_next != right && BOOL_CAS(&left->next, left_next, right))
			connect_to_be_freed_node_list(left_next, distance);
		

		//scan_list_length_en += distance;
		// if the right or the left node is MOV signal this to the caller
		if(is_marked(left_next, MOV) ) 	return MOV_FOUND;

		// the virtual bucket is missing thus create a new one with the item
		if(left->index != index || left->type == HEAD){
			bucket_t *newb 		= bucket_alloc(left->tail);
			newb->index 		= index;
			newb->type 			= ITEM;
			newb->extractions 	= 0ULL;
			newb->epoch 		= epoch;
			newb->next 			= right;
		
			newb->head.next			= node_alloc();
			newb->head.tie_breaker 	= 0;
			
			newb->head.next->next 				= newb->tail;
			newb->head.next->payload			= payload;
			newb->head.next->tie_breaker 		= 1;
			newb->head.next->timestamp	  		= timestamp;	


			if(!BOOL_CAS(&left->next, left_next, newb)){
				bucket_unsafe_free(newb);
				continue;
			}

			if(left_next != right)	
				connect_to_be_freed_node_list(left_next, distance);

			update_cache(newb);

			//skipListAdd(lookup_table, index, newb);
			return OK;
		}
/*		
		if(
		0 && 
		left != lookup_res
		)
		{
			skipListRemove(lookup_table, index);
			skipListAdd(lookup_table, index, left);
		}
*/
			update_cache(left);
		 	return bucket_connect(left, timestamp, tie_breaker, payload, epoch);
		}while(1);

	return ABORT;
}


__thread void *last_bckt = NULL;
__thread unsigned long long last_bckt_count = 0ULL;

int  migrate_node(bucket_t *bckt, table_t *new_h)
{
	bucket_t *left, *left_next, *right;
	unsigned int new_index, distance;
	unsigned long long extractions;
	unsigned long long toskip;
	node_t *head = &bckt->head;
	node_t *tail = bckt->tail;
	node_t *curr = head->next;
	node_t *curr_next;
	node_t *ln, *rn, *tn;
	unsigned blocked;
  
  begin:
  	blocked = 0;	

	if(last_bckt == bckt){
		if(++last_bckt_count == 1000000){
			blocked = 1;
			printf("HUSTON abbiamo un problema %p\n", bckt);
//			while(1);
		}
	}
	else
	
	last_bckt = bckt;
	extractions = bckt->extractions;
	head = &bckt->head;
	assertf(!is_freezed(extractions), "Migrating bucket not freezed%s\n", "");

/*
	if(!is_freezed_for_mov(extractions)){
		assertf(!is_freezed_for_del(extractions), "Migrating bucket not del%s\n", "");
		return OK;
	}
*/

	toskip = get_cleaned_extractions(extractions);

	while(toskip > 0ULL && head != tail){
		head = head->next;
		toskip--;
	}

	//assertf(head == tail, "While migrating detected extracted tail%s\n", "");
	curr = head;

	if(head != tail) curr = head->next;
	else curr = tail;

//	if(curr != tail)
	while
//( 
(
curr
// = curr->next) 
!= tail)
{
	//		printf("Moving first\n");
			curr_next = curr->next; 

			if(curr->replica) {	
				curr = curr_next; 
				continue;
			}

			new_index = hash(curr->timestamp, new_h->bucket_width);
			do{
				// first get bucket
				left = search(&new_h->array[new_index % new_h->size], &left_next, &right, &distance, new_index);

				//printf("A left: %p right:%p\n", left, right);
				extractions = left->extractions;
			  	toskip		= extractions;
				// if the left node is freezed signal this to the caller
				if(is_freezed(extractions)) 	return ABORT;
				// if left node has some extraction signal this to the caller
				if(toskip) return ABORT;
				
				assertf(left->type != HEAD && left->tail->timestamp != INFTY, "HUGE Problems....%s\n", "");

				// the virtual bucket is missing thus create a new one with the item
				if(left->index != new_index || left->type == HEAD){
					bucket_t *new 			= bucket_alloc(&new_h->n_tail);
					new->index 				= new_index;
					new->type 				= ITEM;
					new->extractions 		= 0ULL;
					new->epoch 				= 0;
					new->next 				= right;

					tn = new->tail;
					ln = &new->head;
					assertf(ln->next != tn, "Problems....%s\n", "");

					if(BOOL_CAS(&left->next, left_next, new)){
						connect_to_be_freed_node_list(left_next, distance);
						left = new;
						break;
					}
					bucket_unsafe_free(new);
					return ABORT;
				}
				else
					break;
			}while(1);

			tn = left->tail;
			ln = &left->head;
			rn = ln->next;
			while(rn != tn){
				if(rn->timestamp == curr->timestamp && rn->tie_breaker == curr->tie_breaker && rn->hash == curr->hash){
					if(curr->replica == NULL && __sync_bool_compare_and_swap(&curr->replica, NULL, rn))
						__sync_fetch_and_add(&new_h->e_counter.count, 1);
					return ABORT;
				}
				if(rn->timestamp < curr->timestamp || (rn->timestamp == curr->timestamp && rn->tie_breaker < curr->tie_breaker) ){
					ln = rn;
					rn = rn->next;
				}
				else
					break;

			}

			

			assert(curr_next != NULL && curr != NULL && rn != NULL && curr->timestamp != INFTY);

			node_t *replica = node_alloc();

			replica->timestamp 		= curr->timestamp;
			replica->payload   		= curr->payload;
			replica->tie_breaker    = curr->tie_breaker;
			replica->hash    		= curr->hash;
			replica->replica		= NULL;
			replica->next 			= rn;

			if(ln->next   != rn  || curr->replica){
				node_unsafe_free(replica);
				return ABORT; //abort
			} 

			assert(curr_next != NULL && curr != NULL && rn != NULL && curr->timestamp != INFTY);

			// copy node

			//atomic
			rtm_insertions2++;
//CMB();
//if(0)	
bool committed = false;		
unsigned int __status = 0;
//ATOMIC2(&bckt->lock, &left->lock)
if((__status = _xbegin ()) == _XBEGIN_STARTED)
			     {
//CMB();
					if(ln->next   != rn  		) TM_ABORT(0xf3); //abort
					if(curr->replica  != NULL	) TM_ABORT(0xf4); //abort
				
					ln->next   = replica;
//					curr->replica = replica;
					committed = true;
					TM_COMMIT();
//				CMB();
//					__sync_fetch_and_add(&new_h->e_counter.count, 1);
//					CMB();
				}
else
//				FALLBACK2(&bckt->lock, &left->lock)
{
committed = false;
				long rand;
				    lrand48_r(&seedT, &rand);
				    rand = (rand & 1) ;
					if(rand & 1) {node_unsafe_free(replica);return ABORT;}
					int res = bucket_connect_fallback(left, replica, 0);
					if(res == ABORT){
						node_unsafe_free(replica);
						if(rand & 2) goto begin;
						return ABORT;
					}
					if(__sync_bool_compare_and_swap(&curr->replica, NULL, replica))
						__sync_fetch_and_add(&new_h->e_counter.count, 1);
					
				}
//			END_ATOMIC2(&bckt->lock, &left->lock);
//CMB();
//if(committed)__sync_fetch_and_add(&new_h->e_counter.count, 1);
			#ifdef VALIDATE_BUCKETS
				node_t *tmp1 = &left->head;
				while(tmp1->timestamp != INFTY)
					tmp1 = tmp1->next;

				assert(tmp1 == left->tail);
				CMB();
				int meet_head = 0;
				//tmp1 = NULL;
				CMB();
				node_t *tmp2 = &bckt->head;
				while(tmp2->timestamp != INFTY){
					if(tmp2 == head) {meet_head = 1;assert(tmp2->next == head->next);}
					tmp2 = tmp2->next;
				}

				assert(tmp2 == bckt->tail && meet_head);
			#endif

			flush_current(new_h, new_index);
//			curr = head->next;
	}
/*	
	head = &bckt->head;
	tail = bckt->tail;
	toskip = get_cleaned_extractions(bckt->extractions);

	while(toskip > 0ULL && head != tail){
		head = head->next;
		toskip--;
	}
	unsigned long long miss = 0;
	while( (head = head->next) != tail){
		if(!head->replica) miss++;
	//head = head->next;
	}
	if(miss) LOG("%llu items not migrated\n", miss);
*/
	finalize_set_as_mov(bckt);
	return OK;
}


static inline table_t* read_table(table_t * volatile *curr_table_ptr){
  #if ENABLE_EXPANSION == 0
  	return *curr_table_ptr;
  #else

	bucket_t *bucket, *array	;
    #ifndef NDEBUG
	bucket_t *btail;
    #endif
	bucket_t *right_node, *left_node, *left_node_next;
	table_t 			*h = *curr_table_ptr 			;
	table_t 			*new_h 			;
  	double 			 new_bw 		;
	double 			 newaverage		;
	double rand;			
	int a,b,signed_counter;
	int samples[2];
	int sample_a;
	int sample_b;
	unsigned int counter;
	unsigned int distance;
	unsigned int start;		
	unsigned int i, size = h->size;
	init_cache();
	

	// this is used to break synchrony while invoking readtable
	read_table_count = 	( ((unsigned int)( -(read_table_count == UINT_MAX) ))   & TID				) 
						+ 
						( ((unsigned int)( -(read_table_count != UINT_MAX) )) 	& read_table_count	);
	
	// after READTABLE_PERIOD iterations check if a new set table is required 
	if(read_table_count++ % h->read_table_period == 0)
	{
	
		// make two reads of op. counters in order to reduce probability of a descheduling between each read
		for(i=0;i<2;i++)
		{
			b = ATOMIC_READ( &h->d_counter );
			a = ATOMIC_READ( &h->e_counter );
			samples[i] = a-b;
		}
		
		// compute two samples
		sample_a = abs(samples[0] - ((int)(size*h->perc_used_bucket)));
		sample_b = abs(samples[1] - ((int)(size*h->perc_used_bucket)));
		
		// take the minimum of the samples		
		signed_counter =  (sample_a < sample_b) ? samples[0] : samples[1];
		
		// take the maximum between the signed_counter and ZERO
		counter = (unsigned int) ( (-(signed_counter >= 0)) & signed_counter);
		
		// call the set_new_table
		if( h->new_table == NULL ) set_new_table(h, counter);
	}

	// if a reshuffle is started execute the protocol
	if(h->new_table != NULL)
	{
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;
	    #ifndef NDEBUG 
		btail 			= &h->b_tail;	
	    #endif
//		init_index(new_h);
		if(new_bw < 0)
		{
			block_table(h); 
																				// avoid that extraction can succeed after its completition
			newaverage = compute_mean_separation_time(h, new_h->size, new_h->threshold, new_h->elem_per_bucket);	// get the new bucket width
			if 																						// commit the new bucket width
			(
				BOOL_CAS(
						UNION_CAST(&(new_h->bucket_width), unsigned long long *),
						UNION_CAST(new_bw,unsigned long long),
						UNION_CAST(newaverage, unsigned long long)
					)
			)
				LOG("COMPUTE BW -  OLD:%.20f NEW:%.20f %u SAME TS:%u\n", new_bw, newaverage, new_h->size, acc_counter != 0 ? acc/acc_counter : 0);
		}
		
		unsigned int bucket_done = 0;
		
		//for(retry_copy_phase = 0;retry_copy_phase<10;retry_copy_phase++){
		while(bucket_done != size){
		bucket_done = 0;
		//First speculative try: try to migrate the nodes, if a conflict occurs, continue to the next bucket
		drand48_r(&seedT, &rand); 			
		start = (unsigned int) (rand * size);	// start to migrate from a random bucket
//		LOG("Start: %u\n", start);
		
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// get the head 
			// get the successor of the head (unmarked because heads are MOV)
			do{
				bucket_t *left_node2 = get_next_valid(bucket);
				if(left_node2->type == TAIL) 	break;			// the bucket is empty	

				post_operation(left_node2, SET_AS_MOV, 0ULL, NULL);
				execute_operation(left_node2);

				left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

				if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, distance);
				
				if(left_node2 != left_node) break;
				assertf(!is_freezed(left_node->extractions), "%s\n", "NODE not FREEZED");
				if(right_node->type != TAIL){
					post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
					execute_operation(right_node);
				}
				if(left_node->type != HEAD) {	
					int res = migrate_node(left_node, new_h);
					if(res == ABORT) break;
				}
			}while(1);
	
			// perform a compact to remove all DEL nodes (make head and tail adjacents again)
			left_node = search(bucket, &left_node_next, &right_node, &distance, 0);
			if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
				connect_to_be_freed_node_list(left_node_next, distance);
			
			if(left_node->type == HEAD  && right_node->type == TAIL)
				bucket_done++;
	
		}
		}

		//First speculative try: try to migrate the nodes, if a conflict occurs, continue to the next bucket
		drand48_r(&seedT, &rand); 			
		start = (unsigned int) (rand * size);	// start to migrate from a random bucket
//		LOG("Start: %u\n", start);
		
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// get the head 
			// get the successor of the head (unmarked because heads are MOV)
			do{
				bucket_t *left_node2 = get_next_valid(bucket);
				if(left_node2->type == TAIL) 	break;			// the bucket is empty	

				post_operation(left_node2, SET_AS_MOV, 0ULL, NULL);
				execute_operation(left_node2);
				left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

				if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, distance);
				
				if(left_node2 != left_node) continue;
				assertf(!is_freezed(left_node->extractions), "%s\n", "NODE not FREEZED");
				if(right_node->type != TAIL){
					post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
					execute_operation(right_node);
				}
				if(left_node->type != HEAD) 	migrate_node(left_node, new_h);
			}while(1);
	
			// perform a compact to remove all DEL nodes (make head and tail adjacents again)
			left_node = search(bucket, &left_node_next, &right_node, &distance, 0);
			if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
				connect_to_be_freed_node_list(left_node_next, distance);
			
			assertf(get_unmarked(right_node) != btail, "Fail in line 972 %p %p %p %p\n",
			 bucket,
			  left_node,
			   right_node, 
			   btail); 
	
		}
		
unsigned long long a,b;
a = ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter );
b =  ATOMIC_READ( &new_h->e_counter ) - ATOMIC_READ( &new_h->d_counter );
assert(a == b || *curr_table_ptr != h);
//LOG("OLD ELEM COUNT: %llu NEW ELEM_COUNT %llu\n", ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter ), ATOMIC_READ( &new_h->e_counter ) - ATOMIC_READ( &new_h->d_counter ));		
		if( BOOL_CAS(curr_table_ptr, h, new_h) ){ //Try to replace the old table with the new one
		 	// I won the challenge thus I collect memory
//			LOG("OLD ELEM COUNT: %llu\n", ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter ));
//LOG("OLD ELEM COUNT: %llu NEW ELEM_COUNT %llu\n",a,b);
		 	gc_add_ptr_to_hook_list(ptst, h, 		 gc_hid[0]);
		 }

		h = new_h;
	}


	if(h != __cache_tblt){

		count_epoch_ops = 0ULL;
		bckt_connect_count = 0ULL;
		rq_epoch_ops = 0ULL;
		num_cas = 0ULL; 
		num_cas_useful = 0ULL;
		near = 0ULL;
		__cache_tblt = h;
performed_enqueue=0;
performed_dequeue=0;
concurrent_enqueue = 0;
scan_list_length_en = 0;
concurrent_dequeue = 0;
scan_list_length = 0;   
search_en= 0;			
search_de = 0;
		flush_cache();
	}


//	acquire_node(&h->socket);
	return h;



	#endif
}



#endif
