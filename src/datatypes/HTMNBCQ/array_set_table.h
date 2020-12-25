#ifndef ARRAY_SET_TABLE_H_
#define ARRAY_SET_TABLE_H_

#include "array_utils_set_table.h"

static inline int copyOrderedArray(arrayNodes_t* ordered, unsigned long long extractions, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, double newHBW){
	
	// Obtain the number of elements to copy
	int newNodLen = getFixed(get_extractions_wtoutlk(ordered->indexWrite)) - 
		get_cleaned_extractions(get_extractions_wtoutlk(extractions));
	// printf("locked elements %d\n", newNodLen);
	// fflush(stdout);

	// Allocation of all needed arrays
	for(int i = 0; i < lenNewPtrs; i++){
		newPtrs[i] = initArray(newNodLen);
	}

	// Copy of all the elements putting it in the right array associated to a bucket
	unsigned int new_index = 0;
	unsigned int myIdx = 0;
	int idxWrite = getFixed(get_extractions_wtoutlk(ordered->indexWrite));
	int i = get_cleaned_extractions(get_extractions_wtoutlk(extractions));
	unsigned int counter = 0;
	void* ptrCopy = NULL;
	for(; i < idxWrite && i < ordered->length; i++){
		new_index = hash(ordered->nodes[i].timestamp, newHBW);
		myIdx = new_index % lenNewPtrs;
	
		assert(ordered->nodes[i].timestamp >= MIN || ordered->nodes[i].timestamp < INFTY);
		newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].ptr = ordered->nodes[i].ptr;
		newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].timestamp = ordered->nodes[i].timestamp;
		//newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].tie_breaker = ordered->nodes[i].tie_breaker;
		newPtrs[myIdx]->indexWrite++;
		counter++;
	}

	// Iteration to remove the unused arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(newPtrs[i]->indexWrite == 0){
			arrayNodes_unsafe_free_malloc(newPtrs[i]);
			newPtrs[i] = NULL;
		}
	}

	return counter;
}

static inline int copyFormList(node_t* head, node_t* tail, unsigned long long extractions, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, double newHBW){

	// Count the elements to copy
	node_t* current = head;
	int elmsToCopy = 0;
	while (current->next != tail){
		if(current->timestamp >= MIN && current->timestamp < INFTY)
			elmsToCopy++;
		current = current->next;
	}
	// printf("locked elements %d\n", elmsToCopy);
	// fflush(stdout);
	
	// Allocation of all needed arrays
	elmsToCopy -= get_cleaned_extractions(get_extractions_wtoutlk(extractions));
	for(int i = 0; i < lenNewPtrs; i++){
		newPtrs[i] = initArray(elmsToCopy);
	}

	// Copy of all the elements putting it in the right array associated to a bucket
	current = head;
	unsigned int new_index = 0;
	unsigned int myIdx = 0;
	while (current->next != tail){
		if(current->timestamp >= MIN && current->timestamp < INFTY){
			new_index = hash(current->timestamp, newHBW);
			myIdx = new_index % lenNewPtrs;

			assert(current->timestamp >= MIN || current->timestamp < INFTY);
			newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].ptr = current->payload;
			newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].timestamp = current->timestamp;
			//newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].tie_breaker = current->tie_breaker;
			newPtrs[myIdx]->indexWrite++;
		}
		
		current = current->next;
	}
	
	// Iteration to remove the unused arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(newPtrs[i]->indexWrite == 0){
			arrayNodes_unsafe_free_malloc(newPtrs[i]);
			newPtrs[i] = NULL;
		}
	}

	return elmsToCopy;
}

static inline int copyUnorderedArrays(arrayNodes_t** unordered, unsigned int lenUnord, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, double newHBW){
	
	// Count the elements to copy
	unsigned int newNodLen = 0;
	for(int i = 0; i < lenUnord; i++){
		if(unordered[i] == NULL) continue;
		newNodLen += getFixed(get_extractions_wtoutlk(unordered[i]->indexWrite));
	}

	// Considerare le estrazioni

	// Allocation of all needed arrays
	for(int i=0; i < lenNewPtrs; i++){
		newPtrs[i] = initArray(newNodLen);
	}

	// Copy of all the elements putting it in the right array associated to a bucket
	unsigned int new_index = 0;
	long long int idxWNods = 0;
	long long int idxRNods = 0;
	unsigned int myIdx = 0;
	unsigned int counter = 0;
	arrayNodes_t* consArray = NULL;
	for(int idxArray = 0; idxArray < lenUnord; idxArray++){

		consArray = unordered[idxArray];
		if(consArray == NULL) continue;
		idxRNods = 0;
		idxWNods = getFixed(get_extractions_wtoutlk(consArray->indexWrite));
		// printf("locked elements %lld\n", idxWNods);
		// fflush(stdout);

		while(idxRNods < idxWNods && idxRNods < consArray->length){
			new_index = hash(consArray->nodes[idxRNods].timestamp, newHBW);
			myIdx = new_index % lenNewPtrs;

			if(consArray->nodes[idxRNods].ptr != BLOCK_ENTRY){
				newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].ptr = consArray->nodes[idxRNods].ptr;
				newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].timestamp = consArray->nodes[idxRNods].timestamp;
				//newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].tie_breaker = consArray->nodes[idxRNods].tie_breaker;
				newPtrs[myIdx]->indexWrite++;
				counter++;
			}

			idxRNods++;
		}
	}

	// Iteration to remove the unused arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(newPtrs[i]->indexWrite == 0){
			arrayNodes_unsafe_free_malloc(newPtrs[i]);
			newPtrs[i] = NULL;
		}
	}

	return counter;
}

static inline int publishArray(table_t* new_h, bucket_t* src, bucket_t** destBckt, unsigned int numArrDest, unsigned int numBuckDest, 
	unsigned int new_index, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, int* i){
	bucket_t* dest = *destBckt;
	unsigned int countElemPub = 0;
	unsigned int myIdx = new_index % numBuckDest;
	BOOL_CAS(src->destBuckets+myIdx, NULL, dest);
	if(src->destBuckets[myIdx] != dest) return -1;
	assert(src->destBuckets[myIdx] == dest);

	int writes = 0;
	int idxDest = 0;
	while(*i < lenNewPtrs && newPtrs[*i] != NULL && 
		new_index == hash(newPtrs[*i]->nodes[0].timestamp, new_h->bucket_width)){

		writes = newPtrs[*i]->indexWrite;
		if(writes > 0){
			idxDest = (src->index % (numArrDest-dest->numaNodes)) + dest->numaNodes;
			assert(idxDest < numArrDest || idxDest > numArrDest);
			if(BOOL_CAS(dest->ptr_arrays+idxDest, NULL, newPtrs[*i])){
				// Controllare se va a buon fine oppure se somma zero
				assert(dest->ptr_arrays[idxDest] == newPtrs[*i]);
				__sync_fetch_and_add(&new_h->e_counter.count, writes);
				countElemPub += writes;
			}
		}
		(*i)++;
	}
	(*i)--;

	if(countElemPub) flush_current(new_h, new_index);

	return countElemPub;
}

__thread void *array_last_bckt = NULL;
__thread unsigned long long array_last_bckt_count = 0ULL;

int array_migrate_node(bucket_t *bckt, table_t *new_h, unsigned int numArrDest, unsigned int numBuckDest)
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
	unsigned blocked;
  
  begin:
  	blocked = 0;	

	if(array_last_bckt == bckt){
		if(++array_last_bckt_count == 1000000){
			blocked = 1;
			printf("HUSTON abbiamo un problema %p\n", bckt);
//			while(1);
		}
	}
	else
	
	array_last_bckt = bckt;
	extractions = bckt->extractions;
	assertf(!is_freezed(get_extractions_wtoutlk(extractions)), "Migrating bucket not freezed%s\n", "");

	// LUCKY:
	
	unsigned int bcktsLen = numBuckDest;
	if(bckt->destBuckets == NULL){
		bucket_t** buckets = (bucket_t**)malloc(sizeof(bucket_t*)*bcktsLen);
		bzero(buckets, sizeof(bucket_t*)*bcktsLen);
		while(bckt->destBuckets == NULL){
			BOOL_CAS(&bckt->destBuckets, NULL, buckets);
		}
	}

	unsigned int newPtrlen = numBuckDest;
	arrayNodes_t** newPtr_arrays = (arrayNodes_t**)malloc(sizeof(arrayNodes_t*)*newPtrlen);

	// La configurazone che ha ora l'algoritmo va sempre verso copyUnordered
	// Ora ritorna che pubblica 240 elementi, contollare le estrazioni
	// anche in questo caso, se un bucket ha extractions != 0 allora ordino e
	// tolgo gli elementi estratti

	int countElmsCopied = 0;
	if(is_freezed_for_lnk(bckt->extractions))
		countElmsCopied = copyFormList(bckt->head.next, bckt->tail, bckt->extractions, newPtr_arrays, newPtrlen, new_h->bucket_width);
	else if(!unordered(bckt))
		countElmsCopied = copyOrderedArray(bckt->arrayOrdered, bckt->extractions, newPtr_arrays, newPtrlen, new_h->bucket_width);
	else
		countElmsCopied = copyUnorderedArrays(bckt->ptr_arrays, bckt->tot_arrays, newPtr_arrays, newPtrlen, new_h->bucket_width);

	int contaElementi = 0;
	for(int idxArray = 0; idxArray < newPtrlen; idxArray++){
		if(newPtr_arrays[idxArray] != NULL){
			contaElementi += newPtr_arrays[idxArray]->indexWrite;
			// printArray(newPtr_arrays[idxArray]->nodes, newPtr_arrays[idxArray]->indexWrite, 0);
			// fflush(stdout);
		}
	}

	assert(countElmsCopied == contaElementi);
	int countElemPub = 0;
	int res = 0;

	// Cerco di inserire gli array costruiti nel bucket di destinazione
	for(int i=0; i < newPtrlen; i++){
		if(newPtr_arrays[i] != NULL) new_index = hash(newPtr_arrays[i]->nodes[0].timestamp, new_h->bucket_width);
		else continue;

		do{
			// first get bucket
			left = search(&new_h->array[virtual_to_physical(new_index, new_h->size)], &left_next, &right, &distance, new_index);

			//printf("A left: %p right:%p\n", left, right);
			extractions = left->extractions;
			toskip = extractions;
			// if the left node is freezed signal this to the caller
			if(is_freezed(get_extractions_wtoutlk(extractions))) 	return ABORT;
			// if left node has some extraction signal this to the caller
			if(toskip) return ABORT;
			
			assertf(left->type != HEAD && left->tail->timestamp != INFTY, "HUGE Problems....%s\n", "");

			// the virtual bucket is missing thus create a new one with the item
			if(left->index != new_index || left->type == HEAD){
				bucket_t *new 			= bucket_alloc_resize(&new_h->n_tail, numArrDest);
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

					res = publishArray(new_h, bckt, &left, numArrDest, numBuckDest, new_index, newPtr_arrays, newPtrlen, &i);
					if(res < 0) return ABORT;
					countElemPub += res;

					break;
				}
				bucket_unsafe_free(new);
				return ABORT;
				// Cercare di pubblicare gli array qui
			}else if(left->index == new_index && left->type == ITEM){
				// int myIdx = new_index % numBuckDest;
				// BOOL_CAS(bckt->destBuckets+myIdx, NULL, left);
				
				// int writes = 0;
				// int idxDest = 0;
				// while(i < newPtrlen && newPtr_arrays[i] != NULL && 
				// 	new_index == hash(newPtr_arrays[i]->nodes[0].timestamp, new_h->bucket_width)){

				// 	// Sbagliata la biunivocità
				// 	writes = newPtr_arrays[i]->indexWrite;
				// 	if(writes > 0){
				// 		idxDest = (bckt->index % (numArrDest-left->numaNodes)) + left->numaNodes;
				// 		if(BOOL_CAS(left->ptr_arrays+idxDest, NULL, newPtr_arrays[i])){
				// 			// Controllare se va a buon fine oppure se somma zero
				// 			__sync_fetch_and_add(&new_h->e_counter.count, writes);
				// 			countElemPub += writes;
				// 		}
				// 	}
				// 	i++;
				// }
				// i--;
				// flush_current(new_h, new_index);

				res = publishArray(new_h, bckt, &left, numArrDest, numBuckDest, new_index, newPtr_arrays, newPtrlen, &i);
				if(res < 0) return ABORT;
				countElemPub += res;

				break;
			}
			else
				break;
		}while(1);
	}
	// LUCKY: end
	//assert(contaElementi == countElemPub);
	// printf("bckt %p, sum = %d\n", bckt, new_h->e_counter.count);
	// fflush(stdout);
	finalize_set_as_mov(bckt);

	return OK;
}

static inline table_t* array_read_table(table_t * volatile *curr_table_ptr){
  #if ENABLE_EXPANSION == 0
  	return *curr_table_ptr;
  #else

	bucket_t *bucket, *array;
		#ifndef NDEBUG
	bucket_t *btail;
		#endif
	bucket_t *right_node, *left_node, *left_node_next;
	table_t *h = *curr_table_ptr;
	table_t *new_h;
	double new_bw;
	double newaverage;
	double rand;			
	int a,b,signed_counter;
	int samples[2];
	int sample_a;
	int sample_b;
	unsigned int counter;
	unsigned int distance;
	unsigned int start;		
	unsigned int i, size = h->size;
	init_cache();
	
	// this is used to break synchrony while invoking readtable
	read_table_count = 	( ((unsigned int)( -(read_table_count == UINT_MAX) )) & TID) + 
						( ((unsigned int)( -(read_table_count != UINT_MAX) )) & read_table_count);
	
	// after READTABLE_PERIOD iterations check if a new set table is required 
	if(read_table_count++ % h->read_table_period == 0){
		// make two reads of op. counters in order to reduce probability of a descheduling between each read
		for(i=0;i<2;i++){
			b = ATOMIC_READ( &h->d_counter );
			a = ATOMIC_READ( &h->e_counter );
			samples[i] = a-b;
		}
		
		// compute two samples
		sample_a = abs(samples[0] - ((int)(size*h->perc_used_bucket)));
		sample_b = abs(samples[1] - ((int)(size*h->perc_used_bucket)));
		
		// take the minimum of the samples		
		signed_counter = (sample_a < sample_b) ? samples[0] : samples[1];
		
		// take the maximum between the signed_counter and ZERO
		counter = (unsigned int)((-(signed_counter >= 0)) & signed_counter);
		
		// call the set_new_table
		if(h->new_table == NULL) 
			set_new_table(h, counter);
	}

	// if a reshuffle is started execute the protocol
	if(h->new_table != NULL){
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;
	    #ifndef NDEBUG 
		btail 			= &h->b_tail;	
	    #endif
		//		init_index(new_h);
		if(new_bw < 0){
			array_block_table(h);
																				// avoid that extraction can succeed after its completition
			newaverage = array_compute_mean_separation_time(h, new_h->size, new_h->threshold, new_h->elem_per_bucket);	// get the new bucket width
			if 																						// commit the new bucket width
			(BOOL_CAS(
						UNION_CAST(&(new_h->bucket_width), unsigned long long *),
						UNION_CAST(new_bw,unsigned long long),
						UNION_CAST(newaverage, unsigned long long)
					)
			)
			LOG("COMPUTE BW -  OLD:%.20f NEW:%.20f %u SAME TS:%u\n", new_bw, newaverage, new_h->size, acc_counter != 0 ? acc/acc_counter : 0);
		}
		
		unsigned int bucket_done = 0;
		unsigned int lenNewPtrs = 0;
		unsigned int lenDestBckt = 0;
		lenNewPtrs = h->array->numaNodes + 1 + ceil(new_h->bucket_width/h->bucket_width);
		lenDestBckt = ceil(h->bucket_width/new_h->bucket_width)+1;
		
		
		//for(retry_copy_phase = 0;retry_copy_phase<10;retry_copy_phase++){
		while(bucket_done != size){
			bucket_done = 0;
			//First speculative try: try to migrate the nodes, if a conflict occurs, continue to the next bucket
			drand48_r(&seedT, &rand); 			
			start = (unsigned int) (rand * size);	// start to migrate from a random bucket
			//		LOG("Start: %u\n", start);
			
			for(i = 0; i < size; i++){
				bucket = array + ((i + start) % size); // get the head 
				// get the successor of the head (unmarked because heads are MOV)
				do{
					bucket_t *left_node2 = get_next_valid(bucket);
					if(left_node2->type == TAIL) 	break;			// the bucket is empty	

					post_operation(left_node2, SET_AS_MOV, 0ULL, NULL);
					execute_operation(left_node2);
					// LUCKY: Try to block read and write from arrays in bucket
					// setUnvalidRead(left_node2);
					setUnvalidContent(left_node2);
					for(int i = 0; i < left_node2->tot_arrays; i++){
						if(left_node2->ptr_arrays[i] != NULL)
							blockArray(left_node2->ptr_arrays+i);
					}
					// LUCKY: end

					left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

					if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
						connect_to_be_freed_node_list(left_node_next, distance);
					
					if(left_node2 != left_node) break;
					assertf(!is_freezed(get_extractions_wtoutlk(left_node->extractions)), "%s\n", "NODE not FREEZED");
					if(right_node->type != TAIL){
						post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
						execute_operation(right_node);
						// LUCKY: Try to block read and write from arrays in bucket
						// setUnvalidRead(right_node);
						setUnvalidContent(right_node);
						for(int i = 0; i < left_node2->tot_arrays; i++){
							if(left_node2->ptr_arrays[i] != NULL)
								blockArray(left_node2->ptr_arrays+i);
						}
						// LUCKY: end
					}
					if(left_node->type != HEAD){
						int res = array_migrate_node(left_node, new_h, lenNewPtrs, lenDestBckt);
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
		
		for(i = 0; i < size; i++){
			bucket = array + ((i + start) % size); // get the head 
			// get the successor of the head (unmarked because heads are MOV)
			do{
				bucket_t *left_node2 = get_next_valid(bucket);
				if(left_node2->type == TAIL) 	break;			// the bucket is empty	

				post_operation(left_node2, SET_AS_MOV, 0ULL, NULL);
				execute_operation(left_node2);
				// LUCKY: Try to block read and write from arrays in bucket
				// setUnvalidRead(left_node2);
				setUnvalidContent(left_node2);
				for(int i = 0; i < left_node2->tot_arrays; i++){
					if(left_node2->ptr_arrays[i] != NULL)
						blockArray(left_node2->ptr_arrays+i);
				}
				// LUCKY: end

				left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

				if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, distance);
				
				if(left_node2 != left_node) continue;
				assertf(!is_freezed(get_extractions_wtoutlk(left_node->extractions)), "%s\n", "NODE not FREEZED");
				if(right_node->type != TAIL){
					post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
					execute_operation(right_node);
					// LUCKY: Try to block read and write from arrays in bucket
					// setUnvalidRead(right_node);
					setUnvalidContent(right_node);
					for(int i = 0; i < left_node2->tot_arrays; i++){
						if(left_node2->ptr_arrays[i] != NULL)
							blockArray(left_node2->ptr_arrays+i);
					}
					// LUCKY: end
				}
				if(left_node->type != HEAD) array_migrate_node(left_node, new_h, lenNewPtrs, lenDestBckt);
			}while(1);
	
			// perform a compact to remove all DEL nodes (make head and tail adjacents again)
			left_node = search(bucket, &left_node_next, &right_node, &distance, 0);
			if(left_node_next != right_node && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
				connect_to_be_freed_node_list(left_node_next, distance);
			
			assertf(get_unmarked(right_node) != btail, "Fail in line 972 %p %p %p %p\n",
				bucket, left_node, right_node, btail); 
		}
		
		unsigned long long a,b;
		a = ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter );
		b =  ATOMIC_READ( &new_h->e_counter ) - ATOMIC_READ( &new_h->d_counter );
		// TODO: Attenzione a questo assert
		assert(a == b || *curr_table_ptr != h);
		//LOG("OLD ELEM COUNT: %llu NEW ELEM_COUNT %llu\n", ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter ), ATOMIC_READ( &new_h->e_counter ) - ATOMIC_READ( &new_h->d_counter ));		
		if( BOOL_CAS(curr_table_ptr, h, new_h) ){ //Try to replace the old table with the new one
		 	// I won the challenge thus I collect memory
			//			LOG("OLD ELEM COUNT: %llu\n", ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter ));
			//LOG("OLD ELEM COUNT: %llu NEW ELEM_COUNT %llu\n",a,b);
		 	gc_add_ptr_to_hook_list(ptst, h, 		 gc_hid[0]);
		}
		h = new_h;
	}

	if(h != __cache_tblt){
		count_epoch_ops = 0ULL;
		bckt_connect_count = 0ULL;
		rq_epoch_ops = 0ULL;
		num_cas = 0ULL; 
		num_cas_useful = 0ULL;
		near = 0ULL;
		__cache_tblt = h;
		performed_enqueue=0;
		performed_dequeue=0;
		concurrent_enqueue = 0;
		scan_list_length_en = 0;
		concurrent_dequeue = 0;
		scan_list_length = 0;   
		search_en= 0;			
		search_de = 0;
		flush_cache();
	}

	//	acquire_node(&h->socket);
	return h;

	#endif
}

#endif