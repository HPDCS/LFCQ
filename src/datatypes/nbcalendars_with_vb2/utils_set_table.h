#ifndef SET_TABLE_UTILS_H_
#define SET_TABLE_UTILS_H_

#include "vbucket.h"


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
		execute_operation(tmp);
		marked = is_marked(tmp_next, DEL);
		
		// Exit if tmp is a tail or its timestamp is > of the searched key
	} while (	tmp->type != TAIL && (marked ||  tmp_index <= index));
	
	// the virtual bucket is missing thus create a new one
	*old_left_next = left_next;
	*right_bucket = tmp;
	*distance = counter;
	return left;		
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
		new_h->cached_node			= NULL;
		new_h->b_tail.extractions 	= 0ULL;
		new_h->b_tail.epoch 		= 0U;
		new_h->b_tail.index 		= UINT_MAX;
		new_h->b_tail.type 			= TAIL;
		new_h->b_tail.next 			= NULL; 
		tail_node_init(&new_h->n_tail); 

		for (i = 0; i < new_size; i++)
		{
			new_h->array[i].next = &new_h->b_tail;
			new_h->array[i].tail = &new_h->n_tail;
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
		post_operation(bucket, SET_AS_MOV, 0ULL, NULL);
		execute_operation(bucket);


		// TODOOOOOOOO
		//Try to mark the first VALid node as MOV 
		do
		{
			search(bucket, &left_node_next, &right_node, &counter, 0);
			break;
			if(right_node->type != TAIL) {
				post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
				execute_operation(right_node);
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
				toskip = left->extractions;
				if(is_freezed(toskip)) toskip = get_cleaned_extractions(left->extractions);
			  	while(toskip > 0ULL && curr != left->tail){
			  		curr = curr->next;
			  		toskip--;
			  	}
 				if(curr != left->tail){
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


#endif