#ifndef _TABLE_UTILS_H_
#define _TABLE_UTILS_H_

#include "phynbcq.h"

#define MINIMUM_SIZE 1

#define ENABLE_EXPANSION 1
#define READTABLE_PERIOD 63

#define BASE 1000000ULL 
#ifndef RESIZE_PERIOD_FACTOR 
#define RESIZE_PERIOD_FACTOR 4ULL //2000ULL
#endif
#define RESIZE_PERIOD RESIZE_PERIOD_FACTOR*BASE

#define SAMPLE_SIZE 25

#define REMOVE_DEL	 	 0
#define REMOVE_DEL_INV	 1

extern __thread unsigned long long near;
extern __thread unsigned long long scan_list_length_en;

extern __thread unsigned int 	acc;
extern __thread unsigned int 	acc_counter;

extern __thread unsigned int 	read_table_count;

extern __thread struct drand48_data seedT;

static inline bool is_marked_for_search(void *pointer, int research_flag)
{
	unsigned long long mask_value = (UNION_CAST(pointer, unsigned long long) & MASK_MRK);
	
	return 
		(/*research_flag == REMOVE_DEL &&*/ mask_value == DEL) 
		|| (research_flag == REMOVE_DEL_INV && (mask_value == INV) );
}

static inline void flush_current(table_t* h, unsigned long long newIndex, node_t* node)
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
		// if the new item has been marked as MOV or DEL we can complete (INV cannot be reached here)
		|| is_marked(node->next)
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
		// if the item is still valid and
		&& is_marked(node->next, VAL)
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

// bucket search
static inline void bucket_search(bucket_t* head, unsigned int index, bucket_t **left_bkt, bucket_t** right_bkt, unsigned int *distance)
{
    assertf(head->type!=BCKT_HEAD, "Passing a non head to search%s\n","");

    bucket_t *left, *tmp, *left_next, *tmp_next, *right;
    unsigned long tmp_index;
    unsigned int counter;
    bool marked;

    do 
    {
        left = tmp = head;
        left_next = tmp_next = tmp->next;
        counter = 0;
        marked = is_marked(tmp_next, DEL); // Bucket cannot be INV

        do
        {
            //Find the first unmarked node that is <= index
            if (!marked)
            {
                left = tmp;
                left_next = tmp_next;
                counter = 0;
            }
            // increase the count of marked nodes met during scan
            counter += marked;

            // get an unmarked reference to the tmp node
		    tmp = get_unmarked(tmp_next);
            tmp_next = tmp->next;
			tmp_index = tmp->index;

			// @TODO check if bckt can be marked as del
            marked = is_marked(tmp_next, DEL);

            // Exit if tmp is a tail or its timestamp is > of the searched key
        } while (tmp->type != BCKT_TAIL && (marked ||  tmp_index <= index));
        
        // Set right node and copy the mark of left node
		right = get_marked(tmp,get_mark(left_next));
    
        if (!is_marked(left_next, DEL) && left_next != right)
        {
            if (!BOOL_CAS(&(left->next), left_next, right))
				continue;
            connect_to_be_freed_bucket_list(left_next, counter);
        }

        // at this point they are adjacent
		*left_bkt = left;
		*right_bkt = right;
		*distance = counter;

		return;

    } while (1);
    
}

// node search
static inline void node_search(bucket_t *head, pkey_t timestamp, unsigned int tie_breaker,
	node_t **left_node, node_t **right_node, int flag)
{
    node_t *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	unsigned int tmp_tie_breaker;
	pkey_t tmp_timestamp;
	bool marked, ts_equal, tie_lower, go_to_next;

    tail = head->tail;

	do
	{
		/// Fetch the head and its next node
		left = tmp = &(head->head);
		left_next = tmp_next = tmp->next;
		//assertf(head == NULL, "PANIC %s\n", "");
		//assertf(flag == REMOVE_DEL_INV && is_marked_for_search(left_next, flag), "PANIC2 %s\n", "");
		counter = 0;
		marked = is_marked_for_search(tmp_next, flag);
		
		do
		{

			//Find the first unmarked node that is <= timestamp
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				counter = 0;
			}
			counter+=marked;
			
			// Retrieve timestamp and next field from the current node (tmp)
			
			tmp = get_unmarked(tmp_next);
			tmp_next = tmp->next;
			tmp_timestamp = tmp->timestamp;
			tmp_tie_breaker = tmp->counter;
			
			// Check if the node is marked
			marked = is_marked_for_search(tmp_next, flag);
			// Check timestamp
			go_to_next = LESS(tmp_timestamp, timestamp);
			ts_equal = D_EQUAL(tmp_timestamp, timestamp);
			// Check tie breaker
			tie_lower = 		(
								tie_breaker == 0 || 
								(tie_breaker != 0 && tmp_tie_breaker <= tie_breaker)
							);
			go_to_next =  go_to_next || (ts_equal && tie_lower);

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (	tmp != tail &&
					(
						marked ||
						go_to_next
					)
				);

		// Set right node and copy the mark of left node
		right = get_marked(tmp,get_mark(left_next));
	
		//left node and right node have to be adjacent. If not try with CAS	
		if (!is_marked_for_search(left_next, flag) && left_next != right)
		//if (left_next != right)
		{
			// if CAS succeeds connect the removed nodes to to_be_freed_list
			if (!BOOL_CAS(&(left->next), left_next, right))
					continue;
			connect_to_be_freed_node_list(left_next, counter);
		}
		
		// at this point they are adjacent
		*left_node = left;
		*right_node = right;
		
		return;
		
	} while (1);
}

// search and insert
static inline int search_and_insert(bucket_t* head, unsigned long long newIndex,
	int flag, node_t *new_node_pointer, node_t **new_node)
{
	bucket_t *left, *left_next, *right;
	unsigned int distance;

	node_t *lnode, *rnode, *tail, *nleft, *tmp, *nleft_next, *tmp_next;
	pkey_t left_timestamp, tmp_timestamp;
	pkey_t timestamp = new_node_pointer->timestamp;
	unsigned int left_tie_breaker, tmp_tie_breaker;
	unsigned int len, counter;
	unsigned int tie_breaker = new_node_pointer->counter;
	bool marked, ts_equal, tie_lower, go_to_next;
	bool is_new_key = flag == REMOVE_DEL_INV;

	// the physical bucket head is MOV, a resize has started
	left_next = head->next;
	if (is_marked(left_next, MOV))
		return MOV_FOUND;

	do 
	{
		bucket_search(head, newIndex, &left, &right, &distance);
		left_next = left->next;

		scan_list_length_en += distance;
		// if the right or the left node is MOV signal this to the caller
		if (is_marked(left_next, MOV))
			return MOV_FOUND;

		// the virtual bucket is missing thus create a new one with the item
		if(left->index != newIndex || left->type == BCKT_HEAD)
		{
			// create new bucket -> alloca su nodo giusto
			bucket_t *newb = bucket_malloc(newIndex, BCKT_ITEM, NID);
			newb->next = get_unmarked(right);
			
			// add node
			newb->head.next = new_node_pointer;
			newb->head.next->next = newb->tail;

			if(!BOOL_CAS(&left->next, left_next, newb)){
				// free the bucket
				gc_free(ptst, newb->tail, gc_aid[GC_NODE]);
				gc_free(ptst, newb, gc_aid[GC_BUCKET]);
				continue;
			}

			return OK;
		}

		// the bucket exist - add the node
		node_search(left, -1.0, 0, &lnode, &rnode, flag); // can be removed

		tail = left->tail;
		do
		{
			len = 0;
			nleft = tmp = &(left->head);
			nleft_next = tmp_next = tmp->next;

			left_tie_breaker = tmp_tie_breaker = tmp->counter;
			left_timestamp = tmp_timestamp = tmp->timestamp;

			// init variables useful during iterations
			counter = 0;
			marked = is_marked_for_search(tmp_next, flag);

			do
			{
				len++;
				if (!marked)
				{
					nleft = tmp;
					nleft_next = tmp_next;
					left_tie_breaker = tmp_tie_breaker;
					left_timestamp = tmp_timestamp;
					counter = 0;
				}

				counter += marked;

				tmp = get_unmarked(tmp_next);

				tmp_next = tmp->next;
				tmp_timestamp = tmp->timestamp;
				tmp_tie_breaker = tmp->counter;
			
				marked = is_marked_for_search(tmp_next, flag);

				go_to_next = LESS(tmp_timestamp, timestamp);
				ts_equal = D_EQUAL(tmp_timestamp, timestamp);
				tie_lower = (
								is_new_key || 
								(!is_new_key && tmp_tie_breaker <= tie_breaker)
							);
				go_to_next =  go_to_next || (ts_equal && tie_lower);

			
			} while (tmp != tail &&
					(marked || go_to_next ));
			
			// if the right or the left node is MOV signal this to the caller
			if(is_marked(tmp, MOV) || is_marked(nleft_next, MOV) )
				return MOV_FOUND;
			
			// mark the to-be.inserted node as INV if flag == REMOVE_DEL
			new_node_pointer->next = get_marked(tmp, INV & (-(!is_new_key)) );

			// set the tie_breaker to:
			// 1+T1 		IF K1 == timestamp AND flag == REMOVE_DEL_INV 
			// 1			IF K1 != timestamp AND flag == REMOVE_DEL_INV
			// UNCHANGED 	IF flag != REMOVE_DEL_INV
			new_node_pointer->counter =  ( ((unsigned int)(-(is_new_key))) & (1 + ( ((unsigned int)-D_EQUAL(timestamp, left_timestamp )) & left_tie_breaker ))) +
									 	(~((unsigned int)(-(is_new_key))) & tie_breaker);


			// node already exists
			if(!is_new_key && D_EQUAL(timestamp, left_timestamp ) && left_tie_breaker == tie_breaker)
			{
				gc_free(ptst, new_node_pointer, gc_aid[GC_NODE]);
				*new_node = nleft;
				return OK;
			}
		
			#if KEY_TYPE != DOUBLE
				if(is_new_key && D_EQUAL(timestamp, left_timestamp ))
				{
					gc_free(ptst, new_node_pointer, gc_aid[GC_NODE]);
					*new_node = nleft;
					return PRESENT;
				}		
			#endif

			// copy left node mark			
			if (BOOL_CAS(&(nleft->next), nleft_next, get_marked(new_node_pointer,get_mark(nleft_next))))
			{	
				if(is_new_key)
				{
					scan_list_length_en += len;
				}
				if (counter > 0)
					connect_to_be_freed_node_list(nleft_next, counter);
				return OK;
			}
		
			// this could be avoided
			return ABORT;

		} while (1);
		
	} while(1);
}

// set new table
static inline void set_new_table(table_t* h, unsigned int threshold, double pub, unsigned int epb, unsigned int counter)
{
	table_t *new_h;
	bucket_t *tail;
	double log_size, current_num_items;
	unsigned int i, size, new_size, thp2;
	int res;

	new_h = NULL;
	tail = h->tail;
	size = h->size;
	
	current_num_items = pub*epb*size;
	log_size = 1.0;
	thp2 = 1;
	new_size = 0;

	i=size;
	while(i != 0)
	{ 
		log_size+=1.0;
		i>>=1;
	}
	while(thp2 < threshold *2)
		thp2 <<= 1;

	// check if resize is needed due to num of items 
	if(	size >= thp2 && counter > 2 * current_num_items)
		new_size = size << 1;
	else if(size > thp2 && counter < 0.5 * current_num_items)
		new_size = size >> 1;
	else if(size == 1 && counter > thp2)
		new_size = thp2;
	else if(size == thp2 && counter < threshold)
		new_size = 1;
	
	// is time for periodic resize?
	if(new_size == 0 && (h->e_counter.count + h->d_counter.count) > RESIZE_PERIOD && h->resize_count/log_size < 0.75)
		new_size = h->size;

	if(new_size != 0) 
	{
		// allocate new table
		res = posix_memalign((void**)&new_h, CACHE_LINE_SIZE, sizeof(table_t));
		if(res != 0) {
			printf("No enough memory to new table structure\n"); 
			return;
		}

		res = posix_memalign((void**)&new_h->array, CACHE_LINE_SIZE, new_size*sizeof(bucket_t));
		if(res != 0) {
			free(new_h); 
			printf("No enough memory to new table structure\n");
			return;
		}

		// "populate table"
		new_h->bucket_width  = -1.0;
		new_h->size 		 = new_size;
		new_h->new_table 	 = NULL;
		new_h->d_counter.count = 0;
		new_h->e_counter.count = 0;
		new_h->last_resize_count = counter;
		new_h->current 		 = ((unsigned long long)-1) << 32;
		new_h->read_table_period = h->read_table_period;
		new_h->resize_count = h->resize_count+1;
		new_h->tail = tail;

		for (i = 0; i < new_size; i++)
		{
			new_h->array[i].next = tail;
        	new_h->array[i].type = BCKT_HEAD;
        	new_h->array[i].index = i;
        	new_h->array[i].head.next = NULL;
		}

		// try to publish the table
		if(!BOOL_CAS(&(h->new_table), NULL,	new_h))
		{
			// attempt failed, thus release memory
			free(new_h->array);
			free(new_h);
		}
		else
			LOG("%u - CHANGE SIZE from %u to %u, items %u OLD_TABLE:%p NEW_TABLE:%p\n", TID, size, new_size, counter, h, new_h);
	}

}

// block_table
static inline void block_table(table_t *h)
{
	
	double rand = 0.0;
	drand48_r(&seedT, &rand);

	unsigned int i=0;
	unsigned int counter = 0;
	unsigned int size = h->size;
	bucket_t *array = h->array;
	bucket_t *bucket, *bucket_next;
	bucket_t *left_bckt, *right_bckt; 
	bucket_t *right_bckt_next;
	unsigned int start = 0;

	// start blocking table from a random physical bucket -> try to prefer a local bucket
	start = (unsigned int) rand * size;	

	for(i = 0; i < size; i++)
	{
		bucket = array + ((i + start) % size);

		// mark the head as MOV
		do {
			bucket_next = bucket->next;
		} while(!is_marked(bucket_next, MOV) &&
			!BOOL_CAS(&(bucket->next), bucket_next,	get_marked(bucket_next, MOV)));

		// try to mark the first VALID bucket as MOV

		// mark also all the elements?
		do 
		{
			bucket_search(bucket, 0, &left_bckt, &right_bckt, &counter);
			right_bckt = get_unmarked(right_bckt);
			right_bckt_next = right_bckt->next;
		}	while(
				right_bckt->type != BCKT_TAIL &&
				(
					is_marked(right_bckt_next, DEL) ||
					(is_marked(right_bckt_next, VAL) 
					&& !BOOL_CAS(&(right_bckt->next), right_bckt_next, get_marked(right_bckt_next, MOV))
					)
				)
		);
	}

}

// compute mst
static inline double compute_mean_separation_time(table_t* h,
		unsigned int new_size, unsigned int threashold, unsigned int elem_per_bucket)
{

	pkey_t sample_array[SAMPLE_SIZE+1];
	pkey_t tmp_ts;

	table_t *new_h;
	bucket_t *tmp, *array, *left_bckt, *right_bckt;
	node_t *tmp_node, *tmp_node_next, *tail;

	double new_bw, average, newaverage;
	
	unsigned int i, index, sample_size,
		counter, size, distance;

	int new_min_index = -1;

	array = h->array;

	acc_counter = 0;
	counter = 0;

	new_h = h->new_table;
	new_bw = new_h->bucket_width;

	average = 0.0;
	newaverage = 0.0;

	index = (unsigned int)(h->current >> 32);
	size = h->size;

	if(new_bw >= 0)
		return new_bw;

	sample_size = (new_size <= 5) ? (new_size/2) : (5 + (unsigned int)((double)new_size / 10));
	sample_size = (sample_size > SAMPLE_SIZE) ? SAMPLE_SIZE : sample_size;
    
	//read nodes until the total samples is reached or until someone else do it
	acc = 0;
	while(counter != sample_size && new_h->bucket_width == -1.0)
	{
		for (i = 0; i < size; i++)
		{
			tmp = array + (index+i) % size;
			
			bucket_search(tmp, index, &left_bckt, &right_bckt, &distance);
			right_bckt = get_unmarked(right_bckt);

			// the bucket is not empty
			if(left_bckt->index == index && left_bckt->type != BCKT_HEAD)
			{
				tmp_node = left_bckt->head.next;
				
				tail = left_bckt->tail;
				// scan bucket
				while (tmp_node != tail && counter < sample_size)
				{
					tmp_ts = tmp_node->timestamp;
					tmp_node_next = tmp_node->next;

					if(!is_marked(tmp_node_next, DEL) && !is_marked(tmp_node_next, INV))
					{
						if(D_EQUAL(tmp_ts, sample_array[counter]))
							acc++;
						else
							sample_array[++counter] = tmp_ts;
					}
					tmp_node = get_unmarked(tmp_node_next);
				}
				
			}

			if(right_bckt->type != BCKT_TAIL) 
				new_min_index = new_min_index < right_bckt->index ? new_min_index : right_bckt->index;
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
    
	int j=0;
	// Recalculate ignoring large separations
	for (i = 2; i <= sample_size; i++) {
		if ((sample_array[i] - sample_array[i - 1]) < (average * 2.0))
		{
			newaverage += (sample_array[i] - sample_array[i - 1]);
			j++;
		}
	}
    
	// Compute new width
	newaverage = (newaverage / j) * elem_per_bucket;	// this is the new width

	if(newaverage <= 0.0)
		newaverage = 1.0;
	if(isnan(newaverage))
		newaverage = 1.0;	
    
	return newaverage;

}

// migrate node
static inline void migrate_node(node_t* right_node, table_t *new_h)
{
	node_t **new_node;
	node_t *replica, *right_replica_field, *right_next,
		*new_node_ptr;

	bucket_t* bucket;
	
	pkey_t new_node_ts;
	
	unsigned int index;
	int res = 0;

	//Create a new node to be inserted in the new table as as INValid
	replica = node_malloc(right_node->payload, right_node->timestamp, right_node->counter, NID);
	
	new_node 			= &replica;
	new_node_ptr	 	= (*new_node);
	new_node_ts		 	= new_node_ptr->timestamp;
	
	index = hash(new_node_ts, new_h->bucket_width);
	// node to be added in the hashtable
	bucket = new_h->array + (index % new_h->size);

	do{
		right_replica_field = right_node->replica; 
		// try to insert the replica in the new table	
	} while(right_replica_field == NULL && (res = 
		search_and_insert(bucket, index, REMOVE_DEL, new_node_ptr, new_node)
		) == ABORT);
	// at this point we have at least one replica into the new table

	// try to decide which is the right replica and if I won the challenge increase the counter of enqueued items into the new set table
	if( right_replica_field == NULL && 
			BOOL_CAS(
				&(right_node->replica),
				NULL,
				replica
				)
		)
		ATOMIC_INC(&(new_h->e_counter));
             
	right_replica_field = right_node->replica;

	// make the replica being valid
	do
	{	
		right_next = right_replica_field->next;
	} while( 
		is_marked(right_next, INV) && 
		!BOOL_CAS
		(
			&(right_replica_field->next),
			right_next,
			get_unmarked(right_next)
		)
		);

	// now the insertion is completed so flush the current of the new table
	flush_current(new_h, index, right_replica_field);
	
	// invalidate the node MOV to DEL (11->01)
	right_next = FETCH_AND_AND(&(right_node->next), MASK_DEL);
	//
}

// migrate node
static inline void migrate_bucket(bucket_t* bucket, table_t *new_h)
{
	node_t *tail, *node, *node_next, *left_node, *right_node, *right_node_next;

	assertf(!is_marked(bucket->next), "MIGRATING A CLEAN BUCKET%s\n","");

	tail = bucket->tail;
	
	node = bucket->head.next;
	
	if (node == tail)
	{
		// the bucket is empty, we mark it as del
		FETCH_AND_AND(&(bucket->next), MASK_DEL);
		return;
	}
	
	do {
		node_next = node->next;
	} while(!is_marked(node_next, MOV) && // the node is already marked
		(
			is_marked(node_next, VAL)	// the node is valid 
			&& !BOOL_CAS(&(node->next), node_next, get_marked(node_next, MOV))
		)
	);

	do
	{
		if (node == tail)
			break;

		do 
		{
			node_search(bucket, node->timestamp, node->counter, &left_node, &right_node, REMOVE_DEL_INV);
			right_node = get_unmarked(right_node);
			right_node_next = right_node->next;
		} while(right_node != tail &&					// the successor of node is not a tail
			(
				is_marked(right_node_next, DEL) ||		// the successor of node is DEL (we need to check the next one)
				(
					is_marked(right_node_next, VAL) 	// the successor of node is VAL and the cas to mark it as MOV is failed
					&& !BOOL_CAS(&(right_node->next), right_node_next, get_marked(right_node_next, MOV))
				)
			));

		if(is_marked(node->next, MOV)) 
			migrate_node(node, new_h); // if node is marked as MOV we can migrate it 
		node = right_node;
	}while(1);

	// comapct the bucket 
	node_search(bucket, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
	
	// invalidate the node MOV to DEL (11->01)
	FETCH_AND_AND(&(bucket->next), MASK_DEL);
}

// read table
static inline table_t* read_table(table_t *volatile *curr_table_ptr, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket)
{
    #if ENABLE_EXPANSION == 0
  	    return *curr_table_ptr;
    #else
	
	table_t *h, *new_h;

	bucket_t *array, *bucket, *left_bckt, *right_bckt,
		*right_bckt_next;
	double new_bw,
		newaverage, rand;

	unsigned int i, size,
		counter, start, distance;
	int samples[2];
	int a, b, sample_a, sample_b,
		signed_counter;;

	h = *curr_table_ptr;
	size = h->size;

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
		sample_a = abs(samples[0] - ((int)(size*perc_used_bucket)));
		sample_b = abs(samples[1] - ((int)(size*perc_used_bucket)));

		// take the minimum of the samples		
		signed_counter =  (sample_a < sample_b) ? samples[0] : samples[1];

		// take the maximum between the signed_counter and ZERO
		counter = (unsigned int) ( (-(signed_counter >= 0)) & signed_counter);
		
		// call the set_new_table
		if( h->new_table == NULL )
			set_new_table(h, threshold, perc_used_bucket, elem_per_bucket, counter);
	}

	// if a reshuffle is started execute the protocol
	if(h->new_table != NULL)
	{
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;

		if (new_bw < 0)
		{
			block_table(h);																			// avoid that extraction can succeed after its completition
			newaverage = compute_mean_separation_time(h, new_h->size, threshold, elem_per_bucket);	// get the new bucket width
			// commit the new bucket width
			if 
			(
				BOOL_CAS(
					UNION_CAST(&(new_h->bucket_width), unsigned long long *),
					UNION_CAST(new_bw,unsigned long long),
					UNION_CAST(newaverage, unsigned long long)
				)
			)
				LOG("COMPUTE BW -  OLD:%.20f NEW:%.20f %u SAME TS:%u\n", new_bw, newaverage, new_h->size, acc_counter != 0 ? acc/acc_counter : 0);
		}

		//First speculative try: try to migrate the nodes, if a conflict occurs, continue to the next bucket
		drand48_r(&seedT, &rand);
		start = (unsigned int) rand * size;	// start to migrate from a random bucket -> try to start from local bucker

		for (i=0; i<size; i++)
		{
			bucket = array + ((i+size) % size); // get the head
			bucket_search(bucket, 0, &left_bckt, &right_bckt, &distance); // clean the heading of th ebucket
			left_bckt = get_unmarked(left_bckt->next);
			do
			{
				bucket_search(bucket, left_bckt->index, &left_bckt, &right_bckt, &distance);
				right_bckt = get_unmarked(right_bckt);
				right_bckt_next = right_bckt->next;

				if (right_bckt->type == BCKT_TAIL //the bucket is empty
					||is_marked(right_bckt_next) || 
					!BOOL_CAS(&(right_bckt->next), right_bckt_next, get_marked(right_bckt_next, MOV)))
					
					break;

				migrate_bucket(left_bckt, new_h);
				left_bckt = right_bckt;
			} while (1);
			

		}
		
		//Second conservative try: migrate the nodes and continue until each bucket is empty
		drand48_r(&seedT, &rand); 
		
		start = (unsigned int) rand + size;	
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// get the head
			bucket_search(bucket, 0, &left_bckt, &right_bckt, &distance); // clean the heading of th ebucket
			left_bckt = get_unmarked(left_bckt->next);
			do
			{
				if (left_bckt->type == BCKT_TAIL)
					break;
				
				do {
					bucket_search(bucket, left_bckt->index, &left_bckt, &right_bckt, &distance);
					right_bckt = get_unmarked(right_bckt);
					right_bckt_next = right_bckt->next;
				} while(right_bckt->type != BCKT_TAIL &&
					(
						is_marked(right_bckt_next, DEL) ||
						(
							is_marked(right_bckt_next, VAL) &&
							!BOOL_CAS(&(right_bckt->next), right_bckt_next, get_marked(right_bckt_next, MOV))
						)
					)
					);
				if (is_marked(left_bckt->next, MOV))
				{
					migrate_bucket(left_bckt, new_h);
				}
				left_bckt = right_bckt;
				// @TODO
			} while (1);
			
			bucket_search(bucket, 0, &left_bckt, &right_bckt, &distance); // compact to remove all DEL buckets (make head and tail adjacent again)

			assertf(((bucket_t*) get_unmarked(right_bckt))->type != BCKT_TAIL, "Fail in line %d %p %p %p\n",
			 __LINE__,
			bucket,
			left_bckt,
			right_bckt); 
			
		}

		//Try to replace the old table with the new one
		if( BOOL_CAS(curr_table_ptr, h, new_h) )
		{ 
		 	// I won the challenge thus I collect memory
		 	gc_add_ptr_to_hook_list(ptst, h, 		 gc_hid[0]);
			gc_add_ptr_to_hook_list(ptst, h->array,  gc_hid[0]);
		}

		
		h = new_h;

	}

	return h;

	#endif
}

#endif /* !_TABLE_UTILS_H */