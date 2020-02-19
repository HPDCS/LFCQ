#include "common_nb_calqueue.h"

/**
 * This function implements the enqueue interface of the NBCQ.
 * Cost O(1) when succeeds
 *
 * @param q: pointer to the queueu
 * @param timestamp: the key associated with the value
 * @param payload: the value to be enqueued
 * @return true if the event is inserted in the set table else false
 */
__thread unsigned long long enq_failed = 0; 
__thread unsigned long long check_allocation = 0;
int pq_enqueue(void* q, pkey_t timestamp, void* payload)
{
	assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");

	nb_calqueue* queue = (nb_calqueue*) q; 	
	critical_enter();
	nbc_bucket_node *bucket, *new_node = node_malloc(payload, timestamp, 0);
	table * h = NULL;		
	unsigned int index, size;
	unsigned long long newIndex = 0;
	
	// get configuration of the queue
	double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;
	
	int res, con_en = 0;
	int dw_enqueue_result = -1;
	int iters = 0;
	
	//init the result
	res = MOV_FOUND;
	
	//repeat until a successful insert
	while(res != OK){
		iters++;
		if(timestamp == 8.7765882640457473) h->new_table->new_table = 0;
		// It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){

			// check for a resize
			h = read_table(&queue->hashtable, th, epb, pub);

			// get actual size
			size = h->size;
	        // read the actual epoch
        	new_node->epoch = (h->current & MASK_EPOCH);
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);

			last_bw = h->bucket_width;
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
/*
		if(newIndex <= h->current>>32)
			dw_enqueue_result = -2;
*/
		if(size > DIM_TH && dw_enqueue_result == -1){
			dw_enqueue_result = dw_enqueue(h, newIndex, new_node);

			if(dw_enqueue_result != OK){
				// search the two adjacent nodes that surround the new key and try to insert with a CAS 
		    	res = search_and_insert(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node);
		    	enq_failed += res==OK;
		    	//printf("ARGH\n");//exit(1);
		    }
		    else
		    	res = OK;
		}
		else
			res = search_and_insert(bucket, timestamp, 0, REMOVE_DEL_INV, new_node, &new_node);
	}				

	// the CAS succeeds, thus we want to ensure that the insertion becomes visible
			
	flush_current(h, newIndex, new_node);
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
	//if(rand < 0.2)
	{
	//drand48_r(&seedT, &rand);
	search(h->array+((oldIndex + dist + (unsigned int)( ( (double)(size-dist) )*rand )) % size), -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
	}
	#endif

  #if KEY_TYPE != DOUBLE
  out:
  #endif
	critical_exit();
	return res;

}
