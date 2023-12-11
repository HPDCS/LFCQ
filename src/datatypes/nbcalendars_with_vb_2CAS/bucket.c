#include "bucket.h"
#include <stdbool.h>


/* allocate a bucket */
bucket_t* bucket_alloc(){
	bucket_t* res;
    res = gc_alloc(ptst, gc_aid[GC_BUCKETS]);
	return res;
}

/* allocate a unrolled nodes */
static unrolled_node_t* node_alloc(){
	unsigned int i;
	unrolled_node_t* res;
    res = gc_alloc(ptst, gc_aid[GC_INTERNALS]);

	res->next 					= NULL;
	res->count 					= 0;
	res->pad 					= 0;
	for(i=0;i<UNROLLED_FACTOR;i++){
		res->array[i].payload   = NULL;
		res->array[i].replica   = NULL;
		res->array[i].timestamp = INFTY;
		res->array[i].valid 	= 0;
		res->array[i].counter 	= 0;
	}
	return res;
}


void bucket_safe_free(bucket_t *ptr){
	unrolled_node_t *tmp, *current = ptr->cas128_field.a.entries;
	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);
	while(current){
		tmp = current;
		current = tmp->next;
		gc_free(ptst, tmp, gc_aid[GC_INTERNALS]);
	}
}

void bucket_unsafe_free(bucket_t *ptr){
	unrolled_node_t *tmp, *current = ptr->cas128_field.a.entries;
	gc_free(ptst, ptr, gc_aid[GC_BUCKETS]);
	while(current){
		tmp = current;
		current = tmp->next;
		gc_free(ptst, tmp, gc_aid[GC_INTERNALS]);
	}

}


static inline void release_private_unrolled_list(unrolled_node_t *new){
	unrolled_node_t *tmp;
	while(new){
		tmp = new;
		new = tmp->next;
		gc_free(ptst, tmp, gc_aid[GC_BUCKETS]);
	}
}


static inline void init_new_bucket(bucket_t *bckt, unsigned int epoch, unsigned int index, unsigned int type, unrolled_node_t *node){
	bckt->epoch 										= epoch;
	bckt->new_epoch 									= 0;
	bckt->index 										= index;
	bckt->type  										= type;
	bckt->cas128_field.a.entries 						= node;
	bckt->cas128_field.a.extractions_counter 			= 0UL;
	bckt->cas128_field.a.extractions_counter_old 		= 0UL;
}


bucket_t* new_bucket_with_entry(unsigned int epoch, unsigned int index, entry_t new_item){
	bucket_t *new = bucket_alloc();
	init_new_bucket(new, epoch, index, ITEM, node_alloc());

	new->cas128_field.a.entries->count = 1;
	new->cas128_field.a.entries->array[0] 			= new_item;
	new->cas128_field.a.entries->array[0].counter 	= 1;
	new->cas128_field.a.entries->array[0].valid 	= 1;
	new->cas128_field.a.entries->array[0].replica	= NULL;

	return new;
}


bucket_t* get_clone(bucket_t *bckt){
	unrolled_node_t *current_old, *current_new;
	unsigned int extracted 	= bckt->cas128_field.a.extractions_counter_old;
	unsigned int num_items 	= bckt->cas128_field.a.entries->count;
	unsigned int epoch 		= bckt->new_epoch != 0 ? bckt->new_epoch : bckt->epoch;
	unsigned int index		= bckt->index;
	unsigned int i = 0, j = 0;

	bucket_t *new = bucket_alloc();
	init_new_bucket(new, epoch, index, ITEM, node_alloc());

	current_new = new->cas128_field.a.entries;
	current_old = bckt->cas128_field.a.entries;
	new->cas128_field.a.entries->count 			= num_items - extracted;


	while(extracted > UNROLLED_FACTOR){
		current_old = current_old->next;
		extracted-=UNROLLED_FACTOR; 
		num_items-=UNROLLED_FACTOR;
	}

	i = extracted;
	j = 0;

	while(current_old != NULL){
		current_new->array[j++] = current_old->array[i++];
		if(i == UNROLLED_FACTOR){
			current_old = current_old->next;
			i = 0;
		}
		if(j == UNROLLED_FACTOR){
			current_new->next = node_alloc();
			current_new = current_new->next;
			current_new->next 			= NULL;
			current_new->count 			= 0;
			current_new->pad 			= 0;
			j = 0;
		}
	}

	for(;j<UNROLLED_FACTOR;j++){
		current_new->array[j].payload   = NULL;
		current_new->array[j].timestamp = INFTY;
		current_new->array[j].counter   = 0;
		current_new->array[j].valid  	= 0;
		current_new->array[j].replica  	= NULL;
	}

	return new;
}





int bucket_connect_invalid(bucket_t *bckt, atomic128_t field, entry_t item){
	unsigned int extracted = bckt->cas128_field.a.extractions_counter_old;
	unsigned int num_items = bckt->cas128_field.a.entries->count;
	unsigned int i = 0, j = 0;
	bool inserted = false;
	atomic128_t new_field;
	unrolled_node_t *current_old, *current_new, *new, *old, *tmp;

	new = current_new = node_alloc();
	old = current_old = bckt->cas128_field.a.entries;

	new_field.b = field.b;
	new_field.a.entries = current_new;
	
	while(extracted > UNROLLED_FACTOR){
		*current_new = *current_old;
		current_old = current_old->next;
		extracted-=UNROLLED_FACTOR; 
		num_items-=UNROLLED_FACTOR;
	}

	new->count = old->count+1;
	i = extracted;	
	j = 0;

	while(current_old != NULL){

		// end to copy extracted items
		if(extracted != 0){
			current_new->array[j++] = current_old->array[i++];
			extracted--;
			num_items--;
		}
		// just copy higher priority keys
		else if(current_old->array[i].timestamp < item.timestamp ){
			current_new->array[j++] = current_old->array[i++];
			num_items--;
		}

		// just copy higher priority keys with same timestamp
		else if(current_old->array[i].timestamp == item.timestamp && current_old->array[i].counter < item.counter){
			current_new->array[j++] = current_old->array[i++];
			num_items--;
		}

		// the item is already inserted as invalid
		else if(current_old->array[i].timestamp == item.timestamp && current_old->array[i].counter == item.counter){
			release_private_unrolled_list(new);
			return PRESENT;
		}
		
		// if the old unrolled node has an item with a timestamp greater than the one to be insert its time to 
		else if(!inserted ){
			inserted = true;			
			current_new->array[j] 			= item;
			current_new->array[j].valid 	= 0;
			current_new->array[j].replica 	= NULL;
			j++;
		}
		// copy the rest of items
		else{
			current_new->array[j++] = current_old->array[i++];
			num_items--;
		}

		if(i == UNROLLED_FACTOR){
			current_old = current_old->next;
			i = 0;
		}
		if(j == UNROLLED_FACTOR){
			current_new->next = node_alloc();
			current_new = current_new->next;
			current_new->next 			= NULL;
			current_new->count 			= 0;
			current_new->pad 			= 0;
			j = 0;
		}
	}

	for(;j<UNROLLED_FACTOR;j++){
		current_new->array[j].payload   = NULL;
		current_new->array[j].timestamp = INFTY;
		current_new->array[j].counter   = 0;
		current_new->array[j].valid  = 0;
	}


	if(__sync_bool_compare_and_swap(&bckt->cas128_field.b, field.b, new_field.b)) return OK;
	else release_private_unrolled_list(new);

	return ABORT;
}





int bucket_validate_item(bucket_t *bckt,  entry_t item){
	unsigned int extracted = bckt->cas128_field.a.extractions_counter_old;
	unsigned int num_items = bckt->cas128_field.a.entries->count;
	unsigned int i = 0;
	bool inserted = false;
	entry_t last_valid;
	atomic128_t new_field;
	unrolled_node_t *current_old, *old, *tmp;

	old = current_old = bckt->cas128_field.a.entries;

	while(extracted > UNROLLED_FACTOR){
		current_old = current_old->next;
		extracted-=UNROLLED_FACTOR; 
		num_items-=UNROLLED_FACTOR;
	}

	i = extracted;	

	while(current_old != NULL){
		if(current_old->array[i].timestamp == item.timestamp && current_old->array[i].counter == item.counter){
			if(current_old->array[i].valid == 1 && old == bckt->cas128_field.a.entries)
				return PRESENT;
			if(__sync_bool_compare_and_swap(&current_old->array[i].valid, 0, 1) && old == bckt->cas128_field.a.entries)
				return OK;
			else
				return ABORT;

		}
		i++;
		if(i == UNROLLED_FACTOR){
			i=0;
			current_old = current_old->next;
		}

	}

	return ABORT;
}











/* this do not need a new bucket 
thus we just try to replace the unrolled list with a new one while 
no extraction completes*/


int bucket_connect(bucket_t *bckt, atomic128_t field, entry_t item){
	unsigned int extracted = bckt->cas128_field.a.extractions_counter_old;
	unsigned int num_items = bckt->cas128_field.a.entries->count;
	unsigned int i = 0, j = 0;
	bool inserted = false;
	entry_t last_valid;
	atomic128_t new_field;
	unrolled_node_t *current_old, *current_new, *new, *old, *tmp;

	new = current_new = node_alloc();
	old = current_old = bckt->cas128_field.a.entries;

	new_field.b = field.b;
	new_field.a.entries = current_new;
	
	// copy all extracted items
	while(extracted > UNROLLED_FACTOR){
		*current_new = *current_old;
		current_old = current_old->next;
		extracted-=UNROLLED_FACTOR; 
		num_items-=UNROLLED_FACTOR;
	}

	// increase the counter in the new unrolled list
	new->count = old->count+1;
	i = extracted;	
	j = 0;

	// copy not extracted items and insert the new one ordered
	while(current_old != NULL){
		// end to copy extracted items
		if(extracted != 0){
			current_new->array[j++] = current_old->array[i++];
			extracted--;
			num_items--;
		}
		// just copy higher priority keys
		else if(current_old->array[i].timestamp <= item.timestamp){
			last_valid = current_old->array[i];
			current_new->array[j++] = current_old->array[i++];
			num_items--;
		}
		// if the old unrolled node has an item with a timestamp greater than the one to be insert its time to 
		else if(!inserted ){
			inserted = true;			
			current_new->array[j] 			= item;
			current_new->array[j].counter 	= 1+ last_valid.timestamp == item.timestamp ? last_valid.counter : 0;
			current_new->array[j].valid 	= 1;
			current_new->array[j].replica 	= NULL;
			j++;
		}
		else{
			current_new->array[j++] = current_old->array[i++];
			num_items--;
		}

		// the old unrolled node is ended so go to the next one
		if(i == UNROLLED_FACTOR){
			current_old = current_old->next;
			i = 0;
		}

		// the new unrolled node is ended so allocate a new one
		if(j == UNROLLED_FACTOR){
			current_new->next = node_alloc();
			current_new = current_new->next;
			current_new->next 			= NULL;
			current_new->count 			= 0;
			current_new->pad 			= 0;
			j = 0;
		}
	}

	//this could be avoided
	for(;j<UNROLLED_FACTOR;j++){
		current_new->array[j].payload   = NULL;
		current_new->array[j].timestamp = INFTY;
		current_new->array[j].counter   = 0;
		current_new->array[j].valid  = 0;
	}


	// try to exchange
	if(__sync_bool_compare_and_swap(&bckt->cas128_field.b, field.b, new_field.b))  	return OK;
	else release_private_unrolled_list(new);

	return ABORT;
}


void init_bucket_subsystem(){
	gc_aid[GC_BUCKETS] 		= gc_add_allocator(sizeof(bucket_t		  ));
	gc_aid[GC_INTERNALS] 	= gc_add_allocator(sizeof(unrolled_node_t ));
}