#ifndef __MM_VBUCKET_H
#define __MM_VBUCKET_H

#include "../../key_type.h"


//#define ENABLE_CACHE_PARTITION 

//#ifdef ENABLE_CACHE_PARTITION
#define CACHE_INDEX_POS 6
#define CACHE_INDEX_LEN 6
#define CACHE_INDEX_MASK ( 63ULL << CACHE_INDEX_POS )
#define CACHE_INDEX(x) ((((unsigned long long)(x)) & CACHE_INDEX_MASK  ) >> CACHE_INDEX_POS)
#define CACHE_LIMIT 15
//#endif

#define NUM_INDEX 1

/**
 * This function initializes the gc subsystem
 */

static inline void init_bucket_subsystem(){
	printf("SIZES: %lu %lu\n", sizeof(bucket_t), sizeof(node_t));
	gc_aid[GC_BUCKETS] 	=  gc_add_allocator(sizeof(bucket_t	));
	gc_aid[GC_INTERNALS] 	=  gc_add_allocator(sizeof(node_t	));
	
	//int i; for(i=0;i<30;i++)  //printf("AID: %u\n", 
	//	 gc_add_allocator(sizeof(node_t       ));
	//);
}

#define node_safe_free(ptr) 			gc_free(ptst, ptr, gc_aid[GC_INTERNALS])
#define node_unsafe_free(ptr) 			gc_unsafe_free(ptst, ptr, gc_aid[GC_INTERNALS])
#define only_bucket_unsafe_free(ptr)	gc_unsafe_free(ptst, ptr, gc_aid[GC_BUCKETS])


static inline node_t* node_alloc_by_index(unsigned int i){
	unsigned int j;
	long rand;
	i = i % NUM_INDEX;
	node_t *res = gc_alloc(ptst, 2+i);
	j = CACHE_INDEX(res) % NUM_INDEX ;
	while( j != i){
        	gc_unsafe_free(ptst, res, 2+j);
	        res = gc_alloc(ptst, 2+i);
		j = CACHE_INDEX(res) % NUM_INDEX ;
        }

        res->next                       = NULL;
        res->payload                    = NULL;
        res->tie_breaker                = 0;
        res->timestamp                  = INFTY;
        lrand48_r(&seedT, &rand);
        res->hash = rand;
	return res;
}

/* allocate a node */
static inline node_t* node_alloc(){
	node_t* res;
	long rand;
	do{
		res = gc_alloc(ptst, gc_aid[GC_INTERNALS]);
	    #ifdef ENABLE_CACHE_PARTITION
	    	unsigned long long index = CACHE_INDEX(res);
			assert(index <= CACHE_INDEX_MASK);
	    	if(index >= CACHE_LIMIT  )
	    #endif
	    		{break;}
    }while(1);
    bzero(res, sizeof(node_t));
	res->next 			= NULL;
	res->payload			= NULL;
	res->tie_breaker		= 0;
	res->timestamp	 		= INFTY;
	res->replica = NULL;
	lrand48_r(&seedT, &rand);
	res->hash = rand;
	return res;
}

/* allocate a node */
static inline void tail_node_init(node_t* res){
	long rand;
	int i;
    bzero(res, sizeof(node_t));
	res->next 				= NULL;
	for(i=0;i<VB_NUM_LEVELS-1;i++)
		res->upper_next[i] 		= NULL;
	res->payload			= NULL;
	res->tie_breaker		= 0;
	res->timestamp	 		= INFTY;
	res->replica 			= NULL;
	lrand48_r(&seedT, &rand);
	res->hash = rand;
}

#include "./array.h"

/* allocate a bucket */
static inline bucket_t* bucket_alloc(node_t *tail){
	bucket_t* res;
	int i;
	do{
		res = gc_alloc(ptst, gc_aid[GC_BUCKETS]);
	  #ifdef ENABLE_CACHE_PARTITION
	   	unsigned long long index = CACHE_INDEX(res);
		assert(index <= CACHE_INDEX_MASK);
		if(index <= ( CACHE_LIMIT - sizeof(bucket_t)/CACHE_LINE_SIZE ) ) {
	  #endif
			break;
	  #ifdef ENABLE_CACHE_PARTITION
		}
	   	node_t *tmp = (node_t*)res;
	   	node_t *tmp_end = (node_t*)(res+1);
	   	while(tmp < tmp_end){
			node_unsafe_free(tmp);
			tmp += 1;
	    }
	  #endif
    }while(1);
    long hash = res->hash;
    hash++;
    if(hash == 0) hash++;
    res->extractions 		= 0ULL;
    res->epoch				= 0U;
    res->pending_insert		= NULL;
    res->op_descriptor 		= 0ULL;
    res->tail 				= tail;
    res->socket 			= -1;
/*    res->tail = node_alloc();
    res->tail->payload		= NULL;
    res->tail->timestamp	= INFTY;
    res->tail->tie_breaker	= 0U;
    res->tail->next			= NULL;
    bzero(res, sizeof(bucket_t));
    res->tail->bucket = res;*/
    res->head.payload		= NULL;
    res->head.timestamp		= MIN;
    res->head.tie_breaker	= 0U;
    res->head.next			= res->tail;
    for(i=0;i<VB_NUM_LEVELS-1;i++)
			res->head.upper_next[i]	= res->tail;
    
		// LUCKY:
		//res->numaNodes = numa_num_configured_nodes();
		int length = 4;
		res->numaNodes = 1;
		res->ptr_arrays = (arrayNodes_t**)malloc(sizeof(arrayNodes_t*)*res->numaNodes);
		for(int i=0; i < res->numaNodes; i++){
			res->ptr_arrays[i] = initArray(length);
		}
		res->app = initArray(length*res->numaNodes);
		assert(res->head.next == res->tail && res->ptr_arrays[0]->indexWrite == 0);
		// LUCKY: End
    __sync_bool_compare_and_swap(&res->hash, res->hash, hash);
    #ifndef RTM
    pthread_spin_init(&res->lock, 0);
    #endif

	return res;
}


static inline void bucket_safe_free(bucket_t *ptr){

	node_t *tmp, *current = ptr->head.next;
	while(current != ptr->tail && get_op_type(ptr->op_descriptor) != CHANGE_EPOCH){
		tmp = current;
		current = tmp->next;
		if(tmp->timestamp == INFTY)	assert(tmp == ptr->tail);
		node_safe_free(tmp);
	}

	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);
	
}

static inline void bucket_unsafe_free(bucket_t *ptr){

	node_t *tmp, *current = ptr->head.next;
	while(current != ptr->tail && get_op_type(ptr->op_descriptor) != CHANGE_EPOCH){
		tmp = current;
		current = tmp->next;
		if(tmp->timestamp == INFTY)	assert(tmp == ptr->tail);
		node_unsafe_free(tmp);
	}

	gc_unsafe_free(ptst, ptr, gc_aid[GC_BUCKETS]);

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




#endif
