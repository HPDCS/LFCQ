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

#define FIXED_BW 0.0015
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
	
	bucket_t tail;
//	char zpad5[256-sizeof(bucket_t)];

	bucket_t array[1];			//32


};


/**
 * This function computes the index of the destination bucket in the hashtable
 *
 * @author Romolo Marotta
 *
 * @param timestamp the value to be hashed
 * @param bucket_width the depth of a bucket
 *
 * @return the linear index of a given timestamp
 */
static inline unsigned int hash(pkey_t timestamp, double bucket_width)
{
	double tmp1, tmp2, res_d = (timestamp / bucket_width);
	long long res =  (long long) res_d;
	int upA = 0;
	int upB = 0;

	assertf(res_d > 4294967295, "Probable Overflow when computing the index: "
				"TS=" KEY_STRING ","
 				"BW:%e, "
				"TS/BW:%e, "
				"2^32:%e\n",
				timestamp, bucket_width, res_d,  pow(2, 32));
	

	tmp1 = ((double) (res)	 ) * bucket_width;
	tmp2 = ((double) (res+1) )* bucket_width;
	
	upA = - LESS(timestamp, tmp1);
	upB = GEQ(timestamp, tmp2 );
		
	return (unsigned int) (res+ upA + upB);

}

/**
 * This function commits a value in the current field of a queue. It retries until the timestamp
 * associated with current is strictly less than the value that has to be committed
 *
 * @param h: the interested set table
 * @param newIndex: index of the bucket where the new node belongs
 * @param node: pointer to the new item
 */
static inline void flush_current(table_t* h, unsigned long long newIndex)
{
	unsigned long long oldCur, oldIndex, oldEpoch;
	unsigned long long newCur, tmpCur = ULONG_MAX;
	bool mark = false;	// <----------------------------------------
		
	
	// Retrieve the old index and compute the new one
	oldCur = h->current;
	oldEpoch = oldCur & MASK_EPOCH;
	oldIndex = oldCur >> 32;

	newCur =  newIndex << 32;
	
	// Try to update the current if it need	
	if(
		// if the new item falls into a subsequent bucket of current we can return
		newIndex >	oldIndex 
		// if we do not fall in the previous cases we try to update current and return if we succeed
		|| oldCur 	== 	(tmpCur =  VAL_CAS(
										&(h->current), 
										oldCur, 
										(newCur | (oldEpoch + 1))
									) 
						)
		)
	{
		// collect statistics
		if(tmpCur != ULONG_MAX) near++;
		return;
	}
						 
	//At this point someone else has updated the current from the begin of this function (Failed CAS)
	do
	{
		// get old data from the previous failed CAS
		oldCur = tmpCur;
		oldEpoch = oldCur & MASK_EPOCH;
		oldIndex = oldCur >> 32;

		// keep statistics
		near+=mark;
		mark = false;
	}
	while (
		// retry 
		// if the item is in a previous bucket of current and
		newIndex <	oldIndex 
		&& (mark = true)
		// if the cas has failed
		&& oldCur 	!= 	(tmpCur = 	VAL_CAS(
										&(h->current), 
										oldCur, 
										(newCur | (oldEpoch + 1))
									) 
						)
		);
}


/**
 * This function implements the search of a node that contains a given timestamp t. It finds two adjacent nodes,
 * left and right, such that: left.timestamp <= t and right.timestamp > t.
 *
 * Based on the code by Timothy L. Harris. For further information see:
 * Timothy L. Harris, "A Pragmatic Implementation of Non-Blocking Linked-Lists"
 * Proceedings of the 15th International Symposium on Distributed Computing, 2001
 *
 * @author Romolo Marotta
 *
 * @param head: the head of the list in which we have to perform the search
 * @param timestamp: the value to be found
 * @param tie_breaker: tie breaker of the key (if 0 means left_ts <= timestamp < right_ts, otherwise left_ts < timestamp <= right_ts
 * @param left_node: a pointer to a pointer used to return the left node
 * @param left_node_next: a pointer to a pointer used to return the next field of the left node 
 *
 */   

static inline bucket_t* search(bucket_t *head, bucket_t **old_left_next, bucket_t **right_bucket, unsigned int *distance, unsigned int index)
{
	bucket_t *left, *left_next, *tmp, *tmp_next;
	unsigned int counter;
	unsigned int len;
	unsigned int tmp_index;
	bool marked = false;
	*old_left_next = NULL;
	*right_bucket = NULL;
	*distance = 0;
	len = 0;
	/// Fetch the head and its next node
	left = tmp = head;
	// read all data from the head (hopefully only the first access is a cache miss)
	left_next = tmp_next = tmp->next;
		
	tmp_index = tmp->index;
	
	// init variables useful during iterations
	counter = 0;
	// this should never happen
	marked = is_marked(tmp_next, DEL);
	assertf(marked, "HEAD is DEL %p\n", tmp_next);
	
	do
	{
		len++;
		//Find the left node compatible with value of 'flag'
		// potentially this if can be removed
		if (!marked)
		{
			left = tmp;
			left_next = tmp_next;
			counter = 0;
		}
		
		// increase the count of marked nodes met during scan
		counter+=marked;
		
		// get an unmarked reference to the tmp node
		tmp = get_unmarked(tmp_next);
		
		// Retrieve timestamp and next field from the current node (tmp)
		tmp_next = tmp->next;
		tmp_index = tmp->index;
		
		// Check if the right node is marked
		complete_freeze(tmp);
		marked = is_marked(tmp_next, DEL);
		
		// Exit if tmp is a tail or its timestamp is > of the searched key
	} while (	tmp->type != TAIL && (marked ||  tmp_index <= index));
	
	// the virtual bucket is missing thus create a new one
	*old_left_next = left_next;
	*right_bucket = tmp;
	*distance = counter;
	return left;		
}


#define INSERTION_CACHE_LEN 4096

__thread bucket_t* __cache_bckt[INSERTION_CACHE_LEN];
__thread node_t*   __cache_node[INSERTION_CACHE_LEN];
__thread table_t*  __cache_hash = NULL;

static int search_and_insert(bucket_t *head, unsigned int index, pkey_t timestamp, unsigned int tie_breaker, unsigned int epoch, void* payload){
	bucket_t *left, *left_next, *right;
	unsigned int distance;

	left = __cache_bckt[index % INSERTION_CACHE_LEN];
	if(left != NULL && left->index == index){
		if(check_increase_bucket_epoch(left, epoch) == OK && bucket_connect(left, timestamp, tie_breaker, payload) == OK)
		 	return OK;
		__cache_bckt[index % INSERTION_CACHE_LEN] = NULL; 	
	}

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
			bucket_t *newb 		= bucket_alloc();
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
		 	__cache_bckt[index % INSERTION_CACHE_LEN] = left;
		 	return bucket_connect(left, timestamp, tie_breaker, payload);
		 }
	}while(1);

	return ABORT;
}


static void set_new_table(table_t *h, unsigned int counter)
{
	table_t *new_h = NULL;
	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int new_size = 0;
	unsigned int thp2	  = 1;
	double log_size = 1.0; 
	double current_num_items = h->perc_used_bucket*h->elem_per_bucket*size;
	int res = 0;

	i=size;
	while(i != 0){ log_size+=1.0;i>>=1; }
	while(thp2 < h->threshold *2)
		thp2 <<= 1;
	

	// check if resize is needed due to num of items 
	if(		size >= thp2 && counter > 2   * current_num_items)
		new_size = size << 1;
	else if(size >  thp2 && counter < 0.5 * current_num_items)
		new_size = size >> 1;
	else if(size == 1    && counter > thp2)
		new_size = thp2;
	else if(size == thp2 && counter < h->threshold)
		new_size = 1;
	
	
	// is time for periodic resize?
	if(new_size == 0 && (h->e_counter.count + h->d_counter.count) > RESIZE_PERIOD && h->resize_count/log_size < PERC_RESIZE_COUNT)
		new_size = h->size;
	// the num of items is doubled/halved but it is not enough for change size
	//if(new_size == 0 && h->last_resize_count != 0 && (counter >  h->last_resize_count*2 || counter < h->last_resize_count/2 ) )
	//	new_size = h->size;

	if(new_size != 0) 
	{
		// allocate new table
		res = posix_memalign((void**)&new_h, CACHE_LINE_SIZE, sizeof(table_t) + (new_size-1)*sizeof(bucket_t));
		if(res != 0) {printf("No enough memory to new table structure\n"); return;}


		new_h->bucket_width 		= -1.0;
		new_h->size 				= new_size;
		new_h->new_table 			= NULL;
		new_h->d_counter.count 		= 0;
		new_h->e_counter.count 		= 0;
		new_h->last_resize_count 	= counter;
		new_h->current 		 		= ((unsigned long long)-1) << 32;
		new_h->read_table_period 	= h->read_table_period;
		new_h->resize_count 		= h->resize_count+1;
		new_h->threshold 			= h->threshold;
		new_h->perc_used_bucket 	= h->perc_used_bucket;
		new_h->elem_per_bucket 		= h->elem_per_bucket;
		new_h->pub_per_epb 			= h->perc_used_bucket * h->elem_per_bucket;
		new_h->cached_node		= NULL;
		new_h->tail.extractions 	= 0ULL;
		new_h->tail.epoch 			= 0U;
		new_h->tail.index 			= UINT_MAX;
		new_h->tail.type 			= TAIL;
		new_h->tail.next 			= NULL; 

		for (i = 0; i < new_size; i++)
		{
			new_h->array[i].next = &new_h->tail;
			new_h->array[i].type = HEAD;
			new_h->array[i].epoch = 0U;
			new_h->array[i].index = i;
			new_h->array[i].extractions = 0ULL;
		}

		// try to publish the table
		if(!BOOL_CAS(&(h->new_table), NULL,	new_h))
		{
			// attempt failed, thus release memory
			free(new_h);
		}
		else
			LOG("%u - CHANGE SIZE from %u to %u, items %u OLD_TABLE:%p NEW_TABLE:%p\n", TID, size, new_size, counter, h, new_h);
	}
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


static inline void block_table(table_t *h)
{
	unsigned int i=0;
	unsigned int size = h->size;
	unsigned int counter = 0;
	bucket_t *array = h->array;
	bucket_t *bucket;
	bucket_t *right_node; 
	bucket_t *left_node_next, *right_node_next;
	double rand = 0.0;			
	unsigned int start = 0;		
		
	drand48_r(&seedT, &rand); 
	// start blocking table from a random physical bucket
	start = (unsigned int) rand * size;	

	for(i = 0; i < size; i++)
	{
		bucket = array + ((i + start) % size);	
		
		//Try to mark the head as MOV
		freeze(bucket, FREEZE_FOR_MOV);

		//Try to mark the first VALid node as MOV
		do
		{
			search(bucket, &left_node_next, &right_node, &counter, 0);
			break;
			if(right_node->type != TAIL) freeze(right_node, FREEZE_FOR_MOV);
			//right_node = get_unmarked(left_node_next);
			break;
			right_node_next = right_node->next;	
		}
		while(
				right_node->type != TAIL &&
				(
					is_marked(right_node_next, DEL) ||
					is_marked(right_node_next, VAL) 
				)
		);
	}
}
 

double compute_mean_separation_time(table_t *h,
		unsigned int new_size, unsigned int threashold, unsigned int elem_per_bucket)
{

	unsigned int i = 0, index, j=0, distance;
	unsigned int size = h->size;
	unsigned int sample_size;
	unsigned int new_min_index = -1;
	unsigned int counter = 0;

	table_t *new_h = h->new_table;
	bucket_t *tmp, *left, *left_next, *right, *array = h->array;
	node_t *curr;
	unsigned long long toskip = 0;

	double new_bw = new_h->bucket_width;
	double average = 0.0;
	double newaverage = 0.0;

	acc_counter  = 0;
	
    
	index = (unsigned int)(h->current >> 32);
	
	if(new_bw >= 0) return new_bw;
	
	//if(new_size < threashold*2)
	//	return 1.0;
	
	sample_size = (new_size <= 5) ? (new_size/2) : (5 + (unsigned int)((double)new_size / 10));
	sample_size = (sample_size > SAMPLE_SIZE) ? SAMPLE_SIZE : sample_size;
    
	pkey_t sample_array[SAMPLE_SIZE+1]; //<--TODO: DOES NOT FOLLOW STANDARD C90
	for(i=0;i<SAMPLE_SIZE+1;i++)
		sample_array[i] = 0;
	i=0;
    
    //read nodes until the total samples is reached or until someone else do it
	acc = 0;
	while(counter != sample_size && new_h->bucket_width == -1.0)
	{   

		for (i = 0; i < size; i++)
		{
			tmp = array + (index + i) % size; 	//get the head of the bucket
			//tmp = get_unmarked(tmp->next);		//pointer to first node
			
			left = search(tmp, &left_next, &right, &distance, index);

			// the bucket is not empty
			if(left->index == index && left->type != HEAD){
//				if(tid == 1)LOG("%d- INDEX: %u\n",tid, index);
				curr = &left->head;
				unsigned long long toskip = left->extractions;
				if(is_freezed(toskip)) toskip = get_cleaned_extractions(left->extractions);
			  	while(toskip > 0ULL && curr != left->tail){
			  		curr = curr->next;
			  		toskip--;
			  	}
 				if(curr != left->tail){
					node_t *prev = curr;
					assert(curr->next != NULL);
					curr = curr->next;
					while(curr != left->tail){
						if(curr->timestamp == INFTY) assert(curr != left->tail);
//						if(tid == 1)LOG("%d- TS: " "%.10f" "\n", tid, curr->timestamp);
						sample_array[++counter] = curr->timestamp; 
						assert(curr->next!=NULL || h->new_table->bucket_width == -1.0 );
						if(h->new_table->bucket_width != -1.0) return h->new_table->bucket_width;
						curr = curr->next;
						if(counter == sample_size) break;
					}
				}
			}


			if(right->type != TAIL) new_min_index = new_min_index < right->index ? new_min_index : right->index;
		
		}
		//if the calendar has no more elements I will go out
		if(new_min_index == -1)
			break;
		//otherwise I will restart from the next good bucket
		index = new_min_index;
		new_min_index = -1;
	}


	
	if( counter < sample_size)
		sample_size = counter;
    
	for(i = 2; i<=sample_size;i++)
		average += sample_array[i] - sample_array[i - 1];
	
		// Get the average
	average = average / (double)(sample_size - 1);
	
	// Recalculate ignoring large separations
	for (i = 2; i <= sample_size; i++) {
		if ((sample_array[i] - sample_array[i - 1]) < (average * 2.0))
		{
			newaverage += (sample_array[i] - sample_array[i - 1]);
			j++;
		}
	}
    

	// Compute new width
	newaverage = (newaverage / j) * elem_per_bucket;	/* this is the new width */
	//	LOG("%d- my new bucket %.10f for %p\n", TID, newaverage, h);   

	if(newaverage <= 0.0)
		newaverage = 1.0;
	//if(newaverage <  pow(10,-4))
	//	newaverage = pow(10,-3);
	if(isnan(newaverage))
		newaverage = 1.0;	
    
	//  LOG("%d- my new bucket %.10f for %p AVG REPEAT:%u\n", TID, newaverage, h, acc/counter);	
	return //FIXED_BW; //
	newaverage;
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
	if(head != tail)
	//printf("Try to migrate\n");
	while( (curr = head->next) != tail){
	//		printf("Moving first\n");
			curr_next = curr->next; 

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
					bucket_t *new 			= bucket_alloc();
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

			if(head->next != curr		) return ABORT; //abort
			if(ln->next   != rn  		) return ABORT; //abort
			if(curr->next != curr_next  ) return ABORT; //abort

			//atomic
			rtm_insertions++;
			ATOMIC2(&bckt->lock, &left->lock){
					if(head->next != curr		) TM_ABORT(0xf2); //abort
					if(ln->next   != rn  		) TM_ABORT(0xf3); //abort
					if(curr->next != curr_next 	) TM_ABORT(0xf4); //abort
				
					head->next = curr_next;
					ln->next   = curr;
					curr->next = rn;
					TM_COMMIT();
					__sync_fetch_and_add(&new_h->e_counter.count, 1);
				}
				FALLBACK2(&bckt->lock, &left->lock){
					return ABORT;
				}
			END_ATOMIC2(&bckt->lock, &left->lock);

			flush_current(new_h, new_index);
			curr = head->next;
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
	bucket_t *tail;
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
		tail 			= &h->tail;	
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
			
			assertf(get_unmarked(right_node) != tail, "Fail in line 972 %p %p %p %p\n",
			 bucket,
			  left_node,
			   right_node, 
			   tail); 
	
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
