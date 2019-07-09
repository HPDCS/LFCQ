#include "bucket.h"



void init_bucket_subsystem(){
	gc_aid[GC_BUCKETS] 		= gc_add_allocator(sizeof(bucket_t		  ));
	gc_aid[GC_INTERNALS] 	= gc_add_allocator(sizeof(node_t ));
}

/* allocate a bucket */
bucket_t* bucket_alloc(){
	bucket_t* res;
    res = gc_alloc(ptst, gc_aid[GC_BUCKETS]);
	return res;
}

/* allocate a unrolled nodes */
node_t* node_alloc(){
	node_t* res;
    res = gc_alloc(ptst, gc_aid[GC_INTERNALS]);

	res->next 					= NULL;
	res->payload				= NULL;
	res->tie_breaker			= 0;
	res->timestamp	 			= INFTY;
	return res;
}


void node_unsafe_free(node_t *ptr){
	gc_free(ptst, ptr, gc_aid[GC_INTERNALS]);
}



void bucket_safe_free(bucket_t *ptr){
	node_t *tmp, *current = ptr->head.next;
	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);
	while(current != &ptr->tail){
		tmp = current;
		current = tmp->next;
		gc_free(ptst, tmp, gc_aid[GC_INTERNALS]);
	}
}

void bucket_unsafe_free(bucket_t *ptr){
	node_t *tmp, *current = ptr->head.next;
	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);
	while(current != &ptr->tail){
		tmp = current;
		current = tmp->next;
		gc_free(ptst, tmp, gc_aid[GC_INTERNALS]);
	}

}


int bucket_connect(bucket_t *bckt, pkey_t timestamp, unsigned int tie_breaker, void* payload){
	node_t *head  = &bckt->head;
	node_t *tail  = &bckt->tail;
	node_t *left  = &bckt->tail;
	node_t *right = &bckt->tail;
	node_t *curr  = head;
	unsigned long long extracted = 0;
	unsigned long long toskip = 0;
	node_t *new   = node_alloc();
	new->timestamp = timestamp;
	new->payload	= payload;

  begin:
	new->tie_breaker = 1;

  	extracted 	= bckt->extractions;

  	if(is_freezed_for_mov(extracted)) {node_unsafe_free(new); return MOV_FOUND;	}

  	if(is_freezed_for_del(extracted)) {node_unsafe_free(new); return ABORT; 	}
  	

  	toskip		= extracted & MASK_EPOCH;

  	while(toskip > 0 && curr != tail){
  		curr = curr->next;
  		toskip--;
  	}

  	if(curr == tail && extracted != 0ULL) {
  		freeze_for_del(bckt);
  		node_unsafe_free(new);
  		return ABORT;
  	}

  	while(curr->timestamp <= timestamp){
  		left = curr;
  		curr = curr->next;
  	}

  	if(left->timestamp == timestamp)
  		new->tie_breaker+= left->tie_breaker;
  	right = curr;
  	new->next = right;

  	// atomic
  	{
  		if(extracted != bckt->extractions || left->next != right) goto begin; // abort transaction
  		left->next = new; 
  		// commit
  	}

	return OK;
}


int extract_from_bucket(bucket_t *bckt, void ** result, pkey_t *ts, unsigned int epoch){
	node_t *head  = &bckt->head;
	node_t *tail  = &bckt->tail;
	node_t *curr  = head;
	unsigned long long extracted = 0;
	unsigned long long toskip = 0;

	if(bckt->epoch > epoch) return MOV_FOUND;

  	extracted 	= __sync_add_and_fetch(&bckt->extractions, 1ULL);

  	if(is_freezed_for_mov(extracted)) return MOV_FOUND;
  	if(is_freezed_for_del(extracted)) return EMPTY;
  	
  	toskip		= extracted & MASK_EPOCH;

  	while(toskip > 0 && curr != tail){
  		curr = curr->next;
  		toskip--;
  	}

  	if(curr == tail){
		freeze_for_del(bckt);
		return EMPTY; // try to compact
  	} 
  	*result = curr->payload;
  	*ts		= curr->timestamp;
  	
	return OK;
}

