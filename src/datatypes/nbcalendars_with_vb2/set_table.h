#ifndef SET_TABLE_H_
#define SET_TABLE_H_

#include "../../arch/atomic.h"
#include "vbucket.h"

#define TID tid

extern __thread unsigned long long near;
extern __thread unsigned long long malloc_count;
extern __thread unsigned int TID;
extern __thread unsigned int acc;
extern __thread unsigned int acc_counter;
extern __thread struct drand48_data seedT;
extern __thread unsigned long long scan_list_length_en;
extern __thread unsigned long long scan_list_length;
extern __thread unsigned int read_table_count;


#define ENABLE_CACHE 1
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
	char zpad1[8];					//64

	atomic_t e_counter;	
	char zpad2[60];

	atomic_t d_counter;
	char zpad3[60];
	
	volatile unsigned long long current; 
	char zpad4[56];     
	
	bucket_t * volatile cached_node; 
	char zpad5[56];     
	
	node_t n_tail;

	bucket_t b_tail;
//	char zpad5[256-sizeof(bucket_t)];

	bucket_t array[1];			//32


};

#include "utils_set_table.h"

#if ENABLE_CACHE == 1
static unsigned int hash64shift(unsigned int a)
{
return a;
   a = (a+0x7ed55d16) + (a<<12);
   a = (a^0xc761c23c) ^ (a>>19);
   a = (a+0x165667b1) + (a<<5);
   a = (a+0xd3a2646c) ^ (a<<9);
   a = (a+0xfd7046c5) + (a<<3);
   a = (a^0xb55a4f09) ^ (a>>16);
   return a;
/*
  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ (key >> 24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ (key >> 14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ (key >> 28);
  key = key + (key << 31);
  return key;*/
}
#endif

#define INSERTION_CACHE_LEN 65536

__thread bucket_t* __cache_bckt[INSERTION_CACHE_LEN];
__thread node_t*   __cache_node[INSERTION_CACHE_LEN];
__thread long 	   __cache_hash[INSERTION_CACHE_LEN];
__thread unsigned long long __cache_hit[INSERTION_CACHE_LEN];
__thread unsigned long long __cache_load[INSERTION_CACHE_LEN];
__thread table_t*  __cache_tblt = NULL;

static int search_and_insert(bucket_t *head, unsigned int index, pkey_t timestamp, unsigned int tie_breaker, unsigned int epoch, void* payload){
	bucket_t *left, *left_next, *right;
	unsigned int distance;

  #if ENABLE_CACHE == 1
	unsigned int key = hash64shift(index) % INSERTION_CACHE_LEN;
	__cache_load[key]++;
	left = __cache_bckt[key];

	if(left != NULL && left->index == index && left->hash == __cache_hash[key] && !is_freezed(left->extractions) && is_marked(left->next, VAL)){
		__cache_hit[key]++;
		if(check_increase_bucket_epoch(left, epoch) == OK && bucket_connect(left, timestamp, tie_breaker, payload) == OK)
		 	return OK;
		__cache_bckt[key] = NULL; 	
	}
  #endif

	left = search(head, &left_next, &right, &distance, index);
	if(is_marked(left_next, VAL) && left_next != right && BOOL_CAS(&left->next, left_next, right))
	connect_to_be_freed_node_list(left_next, distance);

	do{
		// first get bucket
		left = search(head, &left_next, &right, &distance, index);
		
		scan_list_length_en += distance;
		// if the right or the left node is MOV signal this to the caller
		if(is_marked(left_next, MOV) ) 	return MOV_FOUND;

		// the virtual bucket is missing thus create a new one with the item
		if(left->index != index || left->type == HEAD){
			bucket_t *newb 		= bucket_alloc(left->tail);
			newb->index 		= index;
			newb->type 			= ITEM;
			newb->extractions 	= 0ULL;
			newb->epoch 		= epoch;
			newb->new_epoch 	= 0U;
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

			return OK;
		}

		if(check_increase_bucket_epoch(left, epoch) == OK){
	  #if ENABLE_CACHE == 1
		 	__cache_bckt[index % INSERTION_CACHE_LEN] = left;
		 	__cache_hash[index % INSERTION_CACHE_LEN] = left->hash;
	  #endif
		 	return bucket_connect(left, timestamp, tie_breaker, payload);
		 }
	}while(1);

	return ABORT;
}



static inline bucket_t* get_next_valid(bucket_t *bckt){
	bucket_t *res, *res_next, *bckt_next;
	unsigned int count = -1;
	bckt_next = res_next = bckt->next;

	do{
		res = get_unmarked(res_next);
		complete_freeze(res);
		res_next = res->next;
		count++;
	}while(res->type != TAIL && is_marked(res_next, DEL));

	if( count && __sync_bool_compare_and_swap(&bckt->next, bckt_next, get_marked(res, MOV)) ){
		connect_to_be_freed_node_list(bckt_next, count);
	}
	return res;
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
	unsigned blocked = 0;	

	if(last_bckt == bckt){
		if(++last_bckt_count == 1000000){
			blocked = 1;
			printf("YUSTON abbiamo un problema %p\n", bckt);
//			while(1);
		}
	}
	else
		last_bckt = bckt;
	extractions = bckt->extractions;
	assertf(!is_freezed(extractions), "Migrating bucket not freezed%s\n", "");
	if(!is_freezed_for_mov(extractions)){
		assertf(!is_freezed_for_del(extractions), "Migrating bucket not del%s\n", "");
		return OK;
	}
	toskip = get_cleaned_extractions(extractions);

	while(toskip > 0ULL && head != tail){
		head = head->next;
		toskip--;
	}
	
	//assertf(head == tail, "While migrating detected extracted tail%s\n", "");
	if(head != tail) curr = head->next;
	else curr = tail;
	
	while( curr != tail){
	//		printf("Moving first\n");
			curr_next = curr->next; 

			if(curr->replica) {	curr = curr_next; continue;}

			new_index = hash(curr->timestamp, new_h->bucket_width);
			do{
				// first get bucket
				left = search(&new_h->array[new_index % new_h->size], &left_next, &right, &distance, new_index);

				//printf("A left: %p right:%p\n", left, right);
				extractions = left->extractions;
			  	toskip		= extractions;
				// if the left node is freezed signal this to the caller
				if(is_freezed(extractions)) 	return OK;
				// if left node has some extraction signal this to the caller
				if(toskip) return OK;
				
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
				if(rn->timestamp < curr->timestamp || (rn->timestamp == curr->timestamp && rn->tie_breaker < curr->tie_breaker) ){
					ln = rn;
					rn = rn->next;
				}
				else
					break;

			}

			

			assert(curr_next != NULL && curr != NULL && rn != NULL);

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


			// copy node

			//atomic
			rtm_insertions++;
			ATOMIC2(&bckt->lock, &left->lock){
					if(ln->next   != rn  		) TM_ABORT(0xf3); //abort
					if(curr->replica 	) TM_ABORT(0xf4); //abort
				
					ln->next   = replica;
					curr->replica = replica;
					TM_COMMIT();
					__sync_fetch_and_add(&new_h->e_counter.count, 1);
				}
				FALLBACK2(&bckt->lock, &left->lock){
					node_unsafe_free(replica);
					return ABORT;
				}
			END_ATOMIC2(&bckt->lock, &left->lock);

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

	freeze_from_mov_to_del(bckt);
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

				freeze(left_node2, FREEZE_FOR_MOV);
				left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

				if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, distance);
				
				if(left_node2 != left_node) break;
				assertf(!is_freezed(left_node->extractions), "%s\n", "NODE not FREEZED");
				if(right_node->type != TAIL) 	freeze(right_node, FREEZE_FOR_MOV);
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

				freeze(left_node2, FREEZE_FOR_MOV);
				left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

				if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, distance);
				
				if(left_node2 != left_node) continue;
				assertf(!is_freezed(left_node->extractions), "%s\n", "NODE not FREEZED");
				if(right_node->type != TAIL) 	freeze(right_node, FREEZE_FOR_MOV);
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
		
		
		if( BOOL_CAS(curr_table_ptr, h, new_h) ){ //Try to replace the old table with the new one
		 	// I won the challenge thus I collect memory
		 	gc_add_ptr_to_hook_list(ptst, h, 		 gc_hid[0]);
		 }

		h = new_h;
	}

	return h;
	#endif
}



#endif
