/*****************************************************************************
*
*	This file is part of NBCQ, a lock-free O(1) priority queue.
*
*   Copyright (C) 2019, Romolo Marotta
*
*   This program is free software: you can redistribute it and/or modify
*   it under the terms of the GNU General Public License as published by
*   the Free Software Foundation, either version 3 of the License, or
*   (at your option) any later version.
*
*   This is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.
*
*   You should have received a copy of the GNU General Public License
*   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
******************************************************************************/
/*
 *  common_nb_calqueue.c
 *
 *  Author: Romolo Marotta
 */

#include <stdlib.h>
#include <limits.h>

#include "common_nb_calqueue.h"


/*************************************
 * GLOBAL VARIABLES					 *
 ************************************/

int gc_aid[2];
int gc_hid[1];

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



/**
 * This function commits a value in the current field of a queue. It retries until the timestamp
 * associated with current is strictly less than the value that has to be committed
 *
 * @param h: the interested set table
 * @param newIndex: index of the bucket where the new node belongs
 */
void flush_current(table* h, unsigned long long newIndex)
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

bucket_t* search(bucket_t *head, bucket_t **old_left_next, bucket_t **right_bucket, unsigned int *distance, unsigned int index)
{
	bucket_t *left, *left_next, *tmp, *tmp_next;
	unsigned int counter;
	unsigned int len;
	unsigned int tmp_index;
	bool marked = false;
	
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
		marked = is_marked(tmp_next, DEL);
		
		// Exit if tmp is a tail or its timestamp is > of the searched key
	} while (	tmp->type != TAIL && (marked ||  tmp_index <= index));
	
	// the virtual bucket is missing thus create a new one
	*old_left_next = left_next;
	*right_bucket = tmp;
	*distance = counter;
	return left;		
}


/**
 * This function implements the search of a node needed for inserting a new item.
 * 
 * It runs in two modes according to the value of the parameter 'flag':
 * 
 * REMOVE_DEL_INV:
 * 		With this value of 'flag', the routine searches for 2 subsequent (not necessarily adjacent)
 * 		key K1 and K2 such that: K1 <= timestamp < K2
 * 		The algorithm tries to insert the new key with a single cas on the next field of the node containing K1
 * 		If K1 == timestamp it sets the tie_breaker of the new node as T1+1, where T1 is the tie_breaker of K1
 * 		If the CAS succeeds it collects the disconnected nodes making the 3 nodes adjacent
 * 
 * REMOVE_DEL:
 * 		This value of flag is used during a resize operation. 
 * 		It searches for 2 subsequent (not necessarily adjacent)
 * 		key K1 and K2 such that: K1 <= timestamp < K2
 * 		and T1 <= tie_breaker < T2
 * 		If <K1, T1> != <timestamp, tie_breaker> 
 * 			insert the node (which was previously inserted in the previous set table> with a cas
 * 		If <K1, T1> == <timestamp, tie_breaker> 
 * 			it follows that a replica of the interested key has been already inserted, thus is exchanges the parameter node with the found one
 * 			 and frees the node passed as parameter 
 * 		
 *
 * Based on the code by Timothy L. Harris. For further information see:
 * Timothy L. Harris, "A Pragmatic Implementation of Non-Blocking Linked-Lists"
 * Proceedings of the 15th International Symposium on Distributed Computing, 2001
 *
 * @author Romolo Marotta
 *
 * @param head: the head of the list in which we have to perform the search
 * @param timestamp: the value to be found
 * @param tie_breaker: tie breaker of the key 
 * @param left_node a pointer to a pointer used to return the left node
 * @param left_node_next a pointer to a pointer used to return the next field of the left node 
 *
 */   
static int search_and_insert(bucket_t *head, bucket_t *tail, unsigned int index, pkey_t timestamp, unsigned int tie_breaker, unsigned long long epoch, void* payload)
{
	bucket_t *left, *left_next, *right;
	unsigned int distance;
	atomic128_t new_field;
	atomic128_t old_field;
	entry_t new_item;

	
	new_item.payload = payload;
	new_item.timestamp = timestamp;
	new_item.valid = 1; 
	new_item.counter = 0; 

	left = search(head, &left_next, &right, &distance, index);
	
	// if the right or the left node is MOV signal this to the caller
	if(is_marked(left_next, MOV) )
		return MOV_FOUND;
	
	// the virtual bucket is missing thus create a new one with the item
	if(left->index != index || left->type == HEAD){
		bucket_t *new = new_bucket_with_entry(epoch, index, new_item);
		new->next = right;
		if(!BOOL_CAS(&left->next, left_next, new)){
			bucket_unsafe_free(new);
			return ABORT;
		}
		connect_to_be_freed_node_list(left_next, distance);
		return OK;
	}

	// Here we have taken a bucket
  read_again:

	old_field = left->cas128_field; // TODO Non atomic read
	new_field = old_field;

	// The bucket is not freezed
	if(!is_freezed(old_field)){
		// The bucket can handle my event and it is not freezed --- try to connect
		if(!need_compact(old_field) && left->epoch >= epoch) 	return bucket_connect(left, new_field, new_item);

		// The bucket is is not compatible with my epoch --- try to increase it
		if(left->epoch < epoch) BOOL_CAS(&left->new_epoch, 0, epoch);

		// Bucket empty or with old epoch --- try to freeze in order to compact --- if fails restart
		if(!freeze_bucket_for_replacement(left, old_field)) goto read_again;
		// update the field as it were given by CAS2
		old_field.a.extractions_counter_old = old_field.a.extractions_counter;
	}

	// Cannot insert the item --- try to manage the situation
	
	// The bucket is freezed for a reshuffle --- signal a MOV
	if(is_freezed_for_mov(old_field))	return MOV_FOUND;

	// If I'm here it means that or the bucket is freezed for compact of someone is proposing a new epoch	
	if(need_compact(old_field)){
		// Go for test and set? TODO
		BOOL_CAS(&left->next, left_next, get_marked(left_next, DEL));
		return ABORT;
	}

	// Replace here for new epoch 
	// TODO insert here new item
	bucket_t *new = get_clone(left);

	new->epoch = new->epoch < epoch ? epoch : new->epoch;
	new->next = right;


	if(!BOOL_CAS(&left->next, left_next, get_marked(new, DEL))){
			bucket_unsafe_free(new);
	}
	else 
		connect_to_be_freed_node_list(left_next, distance);

	return ABORT;
	
		
}

static void set_new_table(table* h, unsigned int threshold, double pub, unsigned int epb, unsigned int counter, bucket_t *tail)
{
	table *new_h = NULL;
	double current_num_items = pub*epb*h->size;
	int res = 0;
	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int new_size = 0;
	unsigned int thp2	  = 1;
	double log_size = 1.0; 

	i=size;
	while(i != 0){ log_size+=1.0;i>>=1; }
	while(thp2 < threshold *2)
		thp2 <<= 1;
	

	// check if resize is needed due to num of items 
	if(		size >= thp2 && counter > 2   * current_num_items)
		new_size = size << 1;
	else if(size >  thp2 && counter < 0.5 * current_num_items)
		new_size = size >> 1;
	else if(size == 1    && counter > thp2)
		new_size = thp2;
	else if(size == thp2 && counter < threshold)
		new_size = 1;
	
	
	// is time for periodic resize?
	if(new_size == 0 && (h->e_counter.count + h->d_counter.count) > RESIZE_PERIOD && h->resize_count/log_size < 0.75)
		new_size = h->size;
	// the num of items is doubled/halved but it is not enough for change size
	//if(new_size == 0 && h->last_resize_count != 0 && (counter >  h->last_resize_count*2 || counter < h->last_resize_count/2 ) )
	//	new_size = h->size;

	if(new_size != 0) 
	{
		// allocate new table
		res = posix_memalign((void**)&new_h, CACHE_LINE_SIZE, sizeof(table));
		if(res != 0) {printf("No enough memory to new table structure\n"); return;}

		res = posix_memalign((void**)&new_h->array, CACHE_LINE_SIZE, new_size*sizeof(bucket_t));
		if(res != 0) {free(new_h); printf("No enough memory to new table structure\n"); return;}
		

		new_h->bucket_width  = -1.0;
		new_h->size 		 = new_size;
		new_h->new_table 	 = NULL;
		new_h->d_counter.count = 0;
		new_h->e_counter.count = 0;
		new_h->last_resize_count = counter;
		new_h->resize_count = h->resize_count+1;
		new_h->current 		 = ((unsigned long long)-1) << 32;
		new_h->read_table_period = h->read_table_period;


		for (i = 0; i < new_size; i++)
		{
			new_h->array[i].next = tail;
			new_h->array[i].type = HEAD;
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

void block_table(table* h)
{
	unsigned int i=0;
	unsigned int size = h->size;
	unsigned int counter = 0;
	bucket_t *array = h->array;
	bucket_t *bucket, *bucket_next;
	bucket_t *left_node, *right_node; 
	bucket_t *left_node_next;
	atomic128_t field;
	double rand = 0.0;			
	unsigned int start = 0;		
		
	drand48_r(&seedT, &rand); 
	// start blocking table from a random physical bucket
	start = (unsigned int) rand * size;	

	for(i = 0; i < size; i++)
	{
		bucket = array + ((i + start) % size);	
		
		//Try to mark the head as MOV
		do{ bucket_next = bucket->next;}
		while( !is_marked(bucket_next, MOV) &&
				!BOOL_CAS(&(bucket->next), bucket_next,	get_marked(bucket_next, MOV)) 
		);

		//Try to mark the first VALid node as MOV
		do
		{
			left_node = search(bucket, &left_node_next, &right_node, &counter, 0);
			//right_node = get_unmarked(left_node_next);
			//right_node_next = right_node->next;	
		}
		while(
				right_node->type == TAIL &&
				(
					is_marked(left_node_next, DEL) ||
					(
						is_marked(left_node_next, VAL) 
						&& !BOOL_CAS(&(left_node->next), left_node_next, get_marked(left_node_next, MOV))
					)
				)
		);

		do{
			field = left_node->cas128_field;
		}while(!is_freezed_for_mov(field) && !freeze_bucket_for_mov(left_node, field));
	}
}

double compute_mean_separation_time(table* h,
		unsigned int new_size, unsigned int threashold, unsigned int elem_per_bucket)
{

	unsigned int i = 0, index, j, distance;
	unsigned int size = h->size;
	unsigned int sample_size;
	unsigned int new_min_index = -1;
	unsigned int counter = 0;

	table *new_h = h->new_table;
	bucket_t *tmp, *left, *left_next, *right, *array = h->array;

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

				unrolled_node_t *current = left->cas128_field.a.entries;
				unsigned int extracted = left->cas128_field.a.extractions_counter_old;
				extracted = extracted & (~(1UL << 31));

				while(extracted > UNROLLED_FACTOR){
					current = current->next;
					extracted-=UNROLLED_FACTOR;
				}

				j = extracted;

				while(current != NULL && current->array[j].timestamp != INFTY){
					
					if(current->array[j].valid && current->array[j].timestamp != sample_array[counter]){
						sample_array[++counter] = current->array[j].timestamp; 
						if(counter == sample_size) break;
					}
					
					j++;
					if(j == UNROLLED_FACTOR){
						current = current->next;
						j = 0;
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
	return newaverage;
}

void migrate_node(bucket_t *bckt, table *new_h)
{
	bucket_t *left, *left_next, *right, *new;
	unsigned int new_index, i, distance;
	entry_t *new_item;
	unrolled_node_t *node = bckt->cas128_field.a.entries;
	unsigned int extracted = bckt->cas128_field.a.extractions_counter_old;
	extracted = extracted & ( ~(1UL << 31) );
	unsigned int res;
	unsigned int inserted;
	atomic128_t new_field;
	atomic128_t old_field;


	//printf("MIGRATING BUCKET %p %u HEAD %d skip extracted:%u num_item:%u Next:%p\n", bckt, bckt->index, bckt->type == HEAD, extracted, bckt->cas128_field.a.entries->count, bckt->next);
	if(!is_marked(bckt->next, MOV)) return;

	if(node->count == 0)
		return;



	do{
		//printf("Migrating items in node %p\n", node);
		for(i=0;i<UNROLLED_FACTOR;i++){
			new = NULL;
			inserted = 0;
			// ignore extracted items
			if(i<extracted || !node->array[i].valid) {
				//printf("Skip item\n");
				continue;}

			new_item = node->array +i;
			if(new_item->timestamp == INFTY) break;

			new_index = hash(new_item->timestamp, new_h->bucket_width);
			//printf("Inserting a new invalid key with index %u\n", new_index);
			
			if(node->array[i].replica == NULL){
			//	printf("Inserting a new invalid key\n");
				// get bucket
			  first_search:
				left = search(&new_h->array[new_index % new_h->size], &left_next, &right, &distance, new_index);
	
				// if the right or the left node is MOV signal this to the caller
				if(is_marked(left_next, MOV) ) return;
					
					// the virtual bucket is missing thus create a new one with the item
				if(left->index != new_index || left->type == HEAD){
					inserted = 1;
					new = new_bucket_with_entry(0, new_index, *new_item);
					new->next = right;
					new->cas128_field.a.entries->array[0].valid = 0;
					if(!BOOL_CAS(&left->next, left_next, new)){
						bucket_unsafe_free(new);
						goto first_search;
					}
					else
						connect_to_be_freed_node_list(left_next, distance);
					left = new;
				}
				
				// look for item in the bucket

				old_field = left->cas128_field; // TODO Non atomic read
				new_field = old_field;
				//printf("Inserting a new node\n");
				res = bucket_connect_invalid(left, new_field, *new_item);
				if(res == MOV_FOUND) return;
				if(res == ABORT) goto first_search;

				BOOL_CAS(&node->array[i].replica, NULL, new_item);

			}
			

			assertf(node->array[i].replica == NULL, "%s\n", "Strange thing happen");
			

			// validate item
			unsigned int repeatition = 0;
			//printf("IN\n");
			res = ABORT;
			do{
			//	printf("INA\n");
				left = search(&new_h->array[new_index % new_h->size], &left_next, &right, &distance, new_index);	
			//	printf("OUTA\n");
				if(left->type == ITEM && left->index == new_index){
					res = bucket_validate_item(left, *new_item);
					repeatition++;
					if(res == OK || res == PRESENT)
						BOOL_CAS(&node->array[i].replica, NULL, new_item);
				}
				else{
					assertf(node->array[i].replica == NULL, "%s\n", "Strange thing happen");
					if(node->array[i].replica != NULL) break;	
				}
			//	if(repeatition==1000)
			//		printf("HERE\n");
			//	printf("OUTB\n");
				// check if there is an extraction otherwise return;
			}while(res == ABORT);
			//printf("OUT\n");


			if(res == OK)
				ATOMIC_INC(&(new_h->e_counter));

			flush_current(new_h, new_index);
		}
		node = node->next;
	}while(node != NULL);


	left = FETCH_AND_AND(&(bckt->next), MASK_DEL);
	//printf("MIGRATING BUCKET DONE %p %u HEAD %d %p\n", bckt, bckt->index, bckt->type == HEAD, bckt->next);
	(void)left;
}

table* read_table(table *volatile *curr_table_ptr, unsigned int threshold, unsigned int elem_per_bucket, double perc_used_bucket, bucket_t *tail)
{
  #if ENABLE_EXPANSION == 0
  	return *curr_table_ptr;
  #else

	bucket_t *bucket, *array	;
	bucket_t *right_node, *left_node, *left_node_next, *right_node_next, *node;
	table 			*new_h 			;
	table 			*h = *curr_table_ptr		;
  	double 			 new_bw 		;
	double 			 newaverage		;
	double rand;			
	int a,b,signed_counter;
	int samples[2];
	int sample_a;
	int sample_b;
	unsigned int counter;
	unsigned int start;		
	unsigned int i, size = h->size	;
	atomic128_t old_field;
	
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
			set_new_table(h, threshold, perc_used_bucket, elem_per_bucket, counter, tail);
	}
	
	// if a reshuffle is started execute the protocol
	if(h->new_table != NULL)
	{
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;

		if(new_bw < 0)
		{
			block_table(h); 																		// avoid that extraction can succeed after its completition
			newaverage = compute_mean_separation_time(h, new_h->size, threshold, elem_per_bucket);	// get the new bucket width
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

		//First speculative try: try to migrate the nodes, if a conflict occurs, continue to the next bucket
		drand48_r(&seedT, &rand); 			
		start = (unsigned int) rand * size;	// start to migrate from a random bucket
		
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// get the head 

			node = get_unmarked(bucket->next);		// get the successor of the head (unmarked because heads are MOV)
			do
			{
				left_node = search(bucket, &left_node_next, &right_node, &counter, 0); // NEED TO COMPACT;
				if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
						connect_to_be_freed_node_list(left_node_next, counter);

				if(right_node->type == TAIL) 	break;			// the bucket is empty				
				
				right_node_next = right_node->next; 

				if(!is_marked(right_node_next) && !BOOL_CAS(&right_node->next, right_node_next, get_marked(right_node_next, MOV)))
					break;

				old_field = right_node->cas128_field;
				if(!is_freezed_for_mov(old_field) && !freeze_bucket_for_mov(right_node, old_field))
					break;

				//search(node, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV); // compact and get the successor of node
				left_node = search(bucket, &left_node_next, &right_node, &counter, right_node->index); // NEED TO COMPACT;
				if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
						connect_to_be_freed_node_list(left_node_next, counter);

				if(right_node->type != TAIL){
					right_node_next = right_node->next; 
					if(!is_marked(right_node_next) && !BOOL_CAS(&right_node->next, right_node_next, get_marked(right_node_next, MOV)))
						break;

					old_field = right_node->cas128_field;
					if(!is_freezed_for_mov(old_field) && !freeze_bucket_for_mov(right_node, old_field))
						break;
				}
				

				assertf(is_marked(left_node->next, VAL), "Not marked%s\n", "");
				//printf("Bucket next %p\n", left_node->next);
				if(left_node->type != HEAD)	migrate_node(left_node, new_h);				// migrate node
				
			}while(true);
		}
	
		//Second conservative try: migrate the nodes and continue until each bucket is empty
		drand48_r(&seedT, &rand); 
		
		start = (unsigned int) rand + size;	
		for(i = 0; i < size; i++)
		{
			bucket = array + ((i + start) % size);	// get the head 
			node = get_unmarked(bucket->next);		// get the successor of the head (unmarked because heads are MOV)
			do
			{
				left_node = search(bucket, &left_node_next, &right_node, &counter, 0); // NEED TO COMPACT;
				if(!is_marked(right_node_next) && !BOOL_CAS(&right_node->next, right_node_next, get_marked(right_node_next, MOV)))
						connect_to_be_freed_node_list(left_node_next, counter);

				if(right_node->type == TAIL) 	break;			// the bucket is empty				
				
				do{
					right_node_next = right_node->next; 
				}
				while(!is_marked(right_node_next) && !BOOL_CAS(&right_node->next, right_node_next, get_marked(right_node_next, MOV)));

				do{
					old_field = right_node->cas128_field;
				}while(!is_freezed_for_mov(old_field) && !freeze_bucket_for_mov(right_node, old_field));

								
				left_node = search(bucket, &left_node_next, &right_node, &counter, right_node->index); // NEED TO COMPACT;
				if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
						connect_to_be_freed_node_list(left_node_next, counter);

				if(right_node->type != TAIL){
					do{
						right_node_next = right_node->next; 
					}while(!is_marked(right_node_next) && !BOOL_CAS(&right_node->next, right_node_next, get_marked(right_node_next, MOV)));

					do{
						old_field = right_node->cas128_field;
					}while(!is_freezed_for_mov(old_field) && !freeze_bucket_for_mov(right_node, old_field));
				}
				
				if(is_marked(left_node->next, MOV) && left_node->type != HEAD) 
					migrate_node(left_node, new_h);				// migrate node

				
			
				
			}while(true);
	
			//search(bucket, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);  // perform a compact to remove all DEL nodes (make head and tail adjacents again)
			left_node = search(bucket, &left_node_next, &right_node, &counter, 0); // NEED TO COMPACT;
			
			if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, counter);

			assertf(get_unmarked(right_node) != tail, "Fail in line 972 %p %p %p %p %p\n",
			 bucket,
			  left_node,
			   right_node, 
			   ((bucket_t*)get_unmarked(right_node))->next, 
			   tail); 
	
		}
		
		
		if( BOOL_CAS(curr_table_ptr, h, new_h) ){ //Try to replace the old table with the new one
		 	// I won the challenge thus I collect memory
		 	gc_add_ptr_to_hook_list(ptst, h, 		 gc_hid[0]);
			gc_add_ptr_to_hook_list(ptst, h->array,  gc_hid[0]);
		 }

		
		h = new_h;
	}
	
	// return the current set table
	return h;
	#endif
}


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
	nb_calqueue* res = NULL;

	// init fraser garbage collector/allocator 
	_init_gc_subsystem();
	// add allocator of nbc_bucket_node
	init_bucket_subsystem();
	// add callback for set tables and array of nodes whene a grace period has been identified
	gc_hid[0] = gc_add_hook(std_free_hook);
	critical_enter();
	critical_exit();

	// allocate memory
	res_mem_posix = posix_memalign((void**)(&res), CACHE_LINE_SIZE, sizeof(nb_calqueue));
	if(res_mem_posix != 0) 	error("No enough memory to allocate queue\n");
	res_mem_posix = posix_memalign((void**)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table));
	if(res_mem_posix != 0)	error("No enough memory to allocate set table\n");
	res_mem_posix = posix_memalign((void**)(&res->hashtable->array), CACHE_LINE_SIZE, MINIMUM_SIZE*sizeof(bucket_t));
	if(res_mem_posix != 0)	error("No enough memory to allocate array of heads\n");
	

	
	res->threshold = threshold;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;
	res->read_table_period = READTABLE_PERIOD;
	res->tail = bucket_alloc();
	res->tail->next = NULL;
	res->tail->type = TAIL;

	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;
	res->hashtable->current = 0;
	res->hashtable->last_resize_count = 0;
	res->hashtable->resize_count = 0;
	res->hashtable->e_counter.count = 0;
	res->hashtable->d_counter.count = 0;
    res->hashtable->read_table_period = res->read_table_period;	
	
	for (i = 0; i < MINIMUM_SIZE; i++)
	{
		res->hashtable->array[i].next = res->tail;
		res->hashtable->array[i].type = HEAD;
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
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");

	nb_calqueue* queue = (nb_calqueue*) q; 	
	bucket_t *bucket;
	critical_enter();
	table * h = NULL;		
	unsigned int index, size, epoch;
	unsigned long long newIndex = 0;
	
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	int res, con_en = 0;
	

	//init the result
	res = MOV_FOUND;
	
	//repeat until a successful insert
	while(res != OK){
		
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){
			// check for a resize
			h = read_table(&queue->hashtable, th, epb, pub, queue->tail);
			// get actual size
			size = h->size;
	        // read the actual epoch
        	epoch = (h->current & MASK_EPOCH);
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			// compute the index of the physical bucket
			index = ((unsigned int) newIndex) % size;
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
	    res = search_and_insert(bucket, queue->tail, newIndex, timestamp, 0, epoch, payload);
	}


	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
	flush_current(h, newIndex);
	performed_enqueue++;
	res=1;
	
	// updates for statistics
	
	concurrent_enqueue += (unsigned long long) (__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);
	
	#if COMPACT_RANDOM_ENQUEUE == 1
	// clean a random bucket
	unsigned long long oldCur = h->current;
	unsigned long long oldIndex = oldCur >> 32;
	unsigned long long dist = 1;
	double rand;
	nbc_bucket_node *left_node, *right_node;
	drand48_r(&seedT, &rand);
	search(h->array+((oldIndex + dist + (unsigned int)( ( (double)(size-dist) )*rand )) % size), -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
	#endif

  #if KEY_TYPE != DOUBLE
  out:
  #endif
	critical_exit();
	return res;

}





void pq_report(int TID)
{
	
	printf("%d- "
	"Enqueue: %.10f LEN: %.10f ### "
	"Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ### "
	"NEAR: %llu "
	"RTC:%d,M:%lld\n",
			TID,
			((float)concurrent_enqueue) /((float)performed_enqueue),
			((float)scan_list_length_en)/((float)performed_enqueue),
			((float)concurrent_dequeue) /((float)performed_dequeue),
			((float)scan_list_length)   /((float)performed_dequeue),
			num_cas, num_cas_useful,
			near,
			read_table_count	  ,
			malloc_count);
}

void pq_reset_statistics(){
		near = 0;
		num_cas = 0;
		num_cas_useful = 0;	
}

unsigned int pq_num_malloc(){ return (unsigned int) malloc_count; }
