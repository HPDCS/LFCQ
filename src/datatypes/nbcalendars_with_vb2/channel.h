#ifndef __CHANNEL_H
#define __CHANNEL_H

#define OP_NONE			0ULL
#define OP_PENDING		1ULL
#define OP_COMPLETED	2ULL
#define OP_ABORTED		3ULL


typedef struct __channel channel_t;
struct __channel{
	void * payload;						// 8
	pkey_t timestamp;  					//  
	char __pad_1[8-sizeof(pkey_t)];		// 16
	volatile unsigned long long state;			// 24
	void * result_payload;				// 32
	pkey_t result_timestamp;			//  
	char __pad_2[8-sizeof(pkey_t)];		// 40
	unsigned long long op_id;
	char __pad_3[16];
};


channel_t *communication_channels = NULL;
unsigned long long snd_id = 0ULL;
unsigned long long rcv_id = 0ULL;

__thread bool have_channel_id = false;
__thread unsigned long long my_rcv_id = 0ULL;
__thread unsigned long long my_snd_id = 0ULL;



static inline void acquire_channels_ids(){
	if(have_channel_id) return;
	if(nid == 0) {
		my_snd_id = __sync_fetch_and_add(&rcv_id, 1ULL);
		my_rcv_id = my_snd_id + 24;
	}
	else{
		my_snd_id = __sync_fetch_and_add(&snd_id, 1ULL) + 24;
		my_rcv_id = my_snd_id;
	}
	have_channel_id = true;
}

static inline int am_i_sender(void* q, pkey_t timestamp){
	
	vbpq* queue = (vbpq*) q; 	
	table_t *h = NULL;		
	unsigned int index, size, newIndex;
	critical_enter();
	
	// check for a resize
	h = read_table(&queue->hashtable);
	// get actual size
	size = h->size;
	// compute the index of the virtual bucket
	newIndex = hash(timestamp, h->bucket_width);
	// compute the index of the physical bucket
	index = ((unsigned int) newIndex) % size;
	critical_exit();
	return (index % 2 != nid);
}





static int internal_enqueue(void* q, pkey_t timestamp, void* payload){
	
	insertions++;
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");
	vbpq* queue = (vbpq*) q; 	
	bucket_t *bucket;
	table_t *h = NULL;		
	unsigned int index, size, epoch;
	unsigned long long newIndex = 0;
	int res, con_en = 0;


	res = MOV_FOUND;
	critical_enter();
	
	//repeat until a successful insert
	do{
		
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){
			// check for a resize
			h = read_table(&queue->hashtable);
			// get actual size
			size = h->size;
	        // read the actual epoch
        	epoch = (h->current & MASK_EPOCH);
			last_bw = h->bucket_width;
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			// compute the index of the physical bucket
			index = ((unsigned int) newIndex) % size;
			if(index % 2 != nid) return ABORT;
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

		// search the two adjacent nodes that surround the new key and try to insert with a CAS 
	    res = search_and_insert(bucket, newIndex, timestamp, 0, epoch, payload);
	}while(res != OK);


	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
	if(res == OK){
		flush_current(h, newIndex);
		// updates for statistics
		concurrent_enqueue += (unsigned long long) (__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);
		performed_enqueue++;
		res=OK;
	}

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
	if(is_marked(left_node, VAL) && left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, right_node))
		connect_to_be_freed_node_list(left_node_next, counter);
	#endif

	critical_exit();
	return OK;

}



#endif
