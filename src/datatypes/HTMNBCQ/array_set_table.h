#ifndef ARRAY_SET_TABLE_H_
#define ARRAY_SET_TABLE_H_

#include "array_utils_set_table.h"

int barrierArray3 = 0;

static inline int copyFormList(bucket_t* bckt, node_t* head, node_t* tail, unsigned long long extractions, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, double newHBW){

	// Count the elements to copy
	node_t* current = head;
	node_t* start = NULL;
	unsigned int alreadyExt = get_cleaned_extractions(get_extractions_wtoutlk(bckt->extractions))+1;

	skip_extracted(tail, &current, alreadyExt);	
	start = current;

	unsigned int new_index = 0;
	unsigned int myIdx = 0;
	int *numElmsPerArray = malloc(lenNewPtrs * sizeof(int));
	assert(numElmsPerArray != NULL);
	bzero(numElmsPerArray, lenNewPtrs * sizeof(int));
	int elmsToCopy = 0;
	while (current != tail){
		if(current->payload != BLOCK_ENTRY && current->timestamp >= MIN && current->timestamp < INFTY){
			new_index = hash(current->timestamp, newHBW);
			myIdx = new_index % lenNewPtrs;
			numElmsPerArray[myIdx] += 1;
			elmsToCopy++;
		}
		current = current->next;
	}
	
	// Allocation of all needed arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(numElmsPerArray[i] > 0){
			newPtrs[i] = initArray(numElmsPerArray[i]);
		}else{
			newPtrs[i] = NULL;
		}
	}

	// Copy of all the elements putting it in the right array associated to a bucket
	current = start;

	unsigned int counter = 0;
	while(current != tail){
		if(current->payload != BLOCK_ENTRY && current->timestamp >= MIN && current->timestamp < INFTY){
			new_index = hash(current->timestamp, newHBW);
			myIdx = new_index % lenNewPtrs;

			assert(get_cleaned_extractions(bckt->extractions) == get_cleaned_extractions(extractions));
			
			newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].ptr = current->payload;
			newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].timestamp = current->timestamp;
			newPtrs[myIdx]->indexWrite++;
			counter++;
		}
		
		current = current->next;
	}
	
	// Iteration to remove the unused arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(newPtrs[i] != NULL && newPtrs[i]->indexWrite == 0){
			arrayNodes_unsafe_free_malloc(newPtrs[i]);
			newPtrs[i] = NULL;
		}
	}
	// printf("%u In copyFormList, head %p, copied = %u, alreadyEx = %u daCopiare %d\n", syscall(SYS_gettid), bckt, counter, alreadyExt,elmsToCopy);
	// fflush(stdout);
	assert(counter == elmsToCopy);
	free(numElmsPerArray);
	return counter;
}

static inline int copyOrderedArray(bucket_t* bckt, arrayNodes_t* ordered, unsigned long long extractions, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, double newHBW){
	
	// Obtain the number of elements to copy
	int newNodLen = getFixed(get_extractions_wtoutlk(ordered->indexWrite)) - 
		get_cleaned_extractions(get_extractions_wtoutlk(extractions));
	// printf("locked elements %d\n", newNodLen);
	// fflush(stdout);
	if(newNodLen == 0) return 0;
	if(newNodLen < 0){
		return copyFormList(bckt, &bckt->head, bckt->tail, bckt->extractions, newPtrs, lenNewPtrs, newHBW);
	}

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
	for(; i < ordered->length; i++){
		assert(get_cleaned_extractions(bckt->extractions) == get_cleaned_extractions(extractions) && !is_freezed_for_lnk(bckt->extractions));
		if(ordered->nodes[i].ptr != BLOCK_ENTRY && ordered->nodes[i].timestamp >= MIN && ordered->nodes[i].timestamp < INFTY){
			new_index = hash(ordered->nodes[i].timestamp, newHBW);
			myIdx = new_index % lenNewPtrs;
		
			assert(ordered->nodes[i].ptr != NULL && ordered->nodes[i].timestamp >= MIN || ordered->nodes[i].timestamp < INFTY);
			newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].ptr = ordered->nodes[i].ptr;
			newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].timestamp = ordered->nodes[i].timestamp;
			//newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].tie_breaker = ordered->nodes[i].tie_breaker;
			newPtrs[myIdx]->indexWrite++;
			counter++;
		}
	}

	// Iteration to remove the unused arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(newPtrs[i]->indexWrite == 0){
			arrayNodes_unsafe_free_malloc(newPtrs[i]);
			newPtrs[i] = NULL;
		}
	}
	
	assert(counter == newNodLen);

	// printf("%u In copyOrderedArray, head %p, copied = %u\n", syscall(SYS_gettid), bckt, counter);
	// fflush(stdout);
	return counter;
}

static inline int copyUnorderedArrays(bucket_t* bckt, arrayNodes_t** unordered, unsigned int lenUnord, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, double newHBW){
	
	// Count the elements to copy
	unsigned int newNodLen = 0;
	for(int i = 0; i < lenUnord; i++){
		if(unordered[i] == NULL) continue;
		newNodLen += getFixed(get_extractions_wtoutlk(unordered[i]->indexWrite));
	}

	unsigned int new_index = 0;
	unsigned int myIdx = 0;
	int *numElmsPerArray = malloc(lenNewPtrs * sizeof(int));
	assert(numElmsPerArray != NULL);
	bzero(numElmsPerArray, lenNewPtrs * sizeof(int));
	for(int j = 0; j < lenUnord; j++){
		for(int k = 0; unordered[j] != NULL && k < getFixed(get_extractions_wtoutlk(unordered[j]->indexWrite)); k++){
			if(unordered[j]->nodes[k].timestamp != BLOCK_ENTRY && unordered[j]->nodes[k].timestamp >= MIN && unordered[j]->nodes[k].timestamp < INFTY){
				new_index = hash(unordered[j]->nodes[k].timestamp, newHBW);
				myIdx = new_index % lenNewPtrs;
				numElmsPerArray[myIdx]+=1;
			}
		}
	}

	// Allocation of all needed arrays
	for(int i=0; i < lenNewPtrs; i++){
		if(numElmsPerArray[i] > 0){
			newPtrs[i] = initArray(numElmsPerArray[i]);
		}else{
			newPtrs[i] = NULL;
		}
	}

	// Copy of all the elements putting it in the right array associated to a bucket
	long long int idxWNods = 0;
	long long int idxRNods = 0;
	unsigned int counter = 0;
	arrayNodes_t* consArray = NULL;
	for(int idxArray = 0; idxArray < lenUnord; idxArray++){
		consArray = unordered[idxArray];
		if(consArray == NULL) continue;
		idxRNods = 0;
		// printf("locked elements %lld\n", idxWNods);
		// fflush(stdout);

		while(idxRNods < consArray->length){
			//assert(!is_freezed_for_lnk(bckt->extractions) && bckt->arrayOrdered == NULL);
			if(consArray->nodes[idxRNods].ptr != BLOCK_ENTRY && consArray->nodes[idxRNods].timestamp >= MIN && consArray->nodes[idxRNods].timestamp < INFTY){
				new_index = hash(consArray->nodes[idxRNods].timestamp, newHBW);
				myIdx = new_index % lenNewPtrs;
					assert(consArray->nodes[idxRNods].ptr != NULL);
					newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].ptr = consArray->nodes[idxRNods].ptr;
					newPtrs[myIdx]->nodes[newPtrs[myIdx]->indexWrite].timestamp = consArray->nodes[idxRNods].timestamp;
					newPtrs[myIdx]->indexWrite++;
					counter++;
			}

			idxRNods++;
		}
	}

	// Iteration to remove the unused arrays
	for(int i = 0; i < lenNewPtrs; i++){
		if(newPtrs[i] != NULL && newPtrs[i]->indexWrite == 0){
			arrayNodes_unsafe_free_malloc(newPtrs[i]);
			newPtrs[i] = NULL;
		}
	}
	free(numElmsPerArray);

	// int contaElementi = 0;
	// for(int idxArray = 0; idxArray < bckt->tot_arrays; idxArray++){
	// 	for(int i = 0; bckt->ptr_arrays[idxArray] != NULL && i < bckt->ptr_arrays[idxArray]->length; i++){
	// 		if(bckt->ptr_arrays[idxArray]->nodes[i].ptr != BLOCK_ENTRY && bckt->ptr_arrays[idxArray]->nodes[i].timestamp != INFTY){
	// 			contaElementi++;
	// 		}
	// 	}
	// }
	// assert(contaElementi == counter);

	// printf("%u In copyUnorderedArrays, head %p, copied = %u\n", syscall(SYS_gettid), bckt, counter);
	// fflush(stdout);
	return counter;
}

static inline int publishArray(table_t* new_h, bucket_t* src, bucket_t** destBckt, unsigned int numArrDest, unsigned int numBuckDest, 
	unsigned int new_index, arrayNodes_t** newPtrs, unsigned int lenNewPtrs, int* i){
	bucket_t* dest = *destBckt;
	unsigned int countElemPub = 0;
	unsigned int myIdx = new_index % numBuckDest;

	BOOL_CAS(src->destBuckets+myIdx, NULL, dest);
	//assert(src->destBuckets[myIdx]->index == dest->index);
	//assert(src->destBuckets[myIdx]->type == dest->type && dest->type == ITEM);
	if(src->destBuckets[myIdx] != dest) return -1;

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
		assert(buckets != NULL);
		bzero(buckets, sizeof(bucket_t*)*bcktsLen);
		while(bckt->destBuckets == NULL){
			BOOL_CAS(&bckt->destBuckets, NULL, buckets);
		}
	}

	unsigned int newPtrlen = numBuckDest;
	arrayNodes_t** newPtr_arrays = (arrayNodes_t**)malloc(sizeof(arrayNodes_t*)*newPtrlen);
	assert(newPtr_arrays != NULL);

	// La configurazone che ha ora l'algoritmo va sempre verso copyUnordered
	// Ora ritorna che pubblica 240 elementi, contollare le estrazioni
	// anche in questo caso, se un bucket ha extractions != 0 allora ordino e
	// tolgo gli elementi estratti

	arrayNodes_t* ordered = bckt->arrayOrdered;

	int countElmsCopied = 0;
	if(is_freezed_for_lnk(bckt->extractions) && !unordered(bckt))
		countElmsCopied = copyFormList(bckt, &bckt->head, tail, extractions, newPtr_arrays, newPtrlen, new_h->bucket_width);
	else if(!is_freezed_for_lnk(bckt->extractions) && !unordered(bckt))
		countElmsCopied = copyOrderedArray(bckt, ordered, extractions, newPtr_arrays, newPtrlen, new_h->bucket_width);
	else if(!is_freezed_for_lnk(bckt->extractions) && unordered(bckt))
		countElmsCopied = copyUnorderedArrays(bckt, bckt->ptr_arrays, bckt->tot_arrays, newPtr_arrays, newPtrlen, new_h->bucket_width);
	else
		assert(0);

	//assert(countElmsCopied == contaElementi);
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
			}else if(left->index == new_index && left->type == ITEM){
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
	//if(read_table_count++ % h->read_table_period == 0)
	{
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
		
		// int old = 0;
		// if(barrierArray3 = 0) old = BOOL_CAS(&barrierArray3, 0, BLOCK_ENTRY);
		// else old = VAL_FAA(&barrierArray3, 1);
		// if(old == 11) BOOL_CAS(&barrierArray3, old, 0);
		// else while(!barrierArray3);

		// dumpTable(h, "Original");
		
		// old = 0;
		// if(barrierArray3 = 0) old = BOOL_CAS(&barrierArray3, 0, BLOCK_ENTRY);
		// else old = VAL_FAA(&barrierArray3, 1);
		// if(old == 11) BOOL_CAS(&barrierArray3, old, 0);
		// else while(!barrierArray3);

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

					left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

					if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
						connect_to_be_freed_node_list(left_node_next, distance);
					
					if(left_node2 != left_node) break;
					assertf(!is_freezed(get_extractions_wtoutlk(left_node->extractions)), "%s\n", "NODE not FREEZED");

					// LUCKY: Try to block read and write from arrays in bucket
					// setUnvalidRead(left_node2);
					setUnvalidContent(left_node2);
					for(int i = 0; i < left_node2->tot_arrays; i++){
						if(left_node2->ptr_arrays[i] != NULL)
							blockArray(left_node2->ptr_arrays+i);
					}
					// LUCKY: end

					if(right_node->type != TAIL){
						post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
						execute_operation(right_node);
						// LUCKY: Try to block read and write from arrays in bucket
						// setUnvalidRead(right_node);
						setUnvalidContent(right_node);
						for(int i = 0; i < right_node->tot_arrays; i++){
							if(right_node->ptr_arrays[i] != NULL)
								blockArray(right_node->ptr_arrays+i);
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

				left_node = search(bucket, &left_node_next, &right_node, &distance, left_node2->index);

				if(distance && BOOL_CAS(&left_node->next, left_node_next, get_marked(right_node, MOV)))
					connect_to_be_freed_node_list(left_node_next, distance);
				
				if(left_node2 != left_node) continue;
				assertf(!is_freezed(get_extractions_wtoutlk(left_node->extractions)), "%s\n", "NODE not FREEZED");

				// LUCKY: Try to block read and write from arrays in bucket
				// setUnvalidRead(left_node2);
				setUnvalidContent(left_node2);
				for(int i = 0; i < left_node2->tot_arrays; i++){
					if(left_node2->ptr_arrays[i] != NULL)
						blockArray(left_node2->ptr_arrays+i);
				}
				// LUCKY: end

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
		//if(a != b && *curr_table_ptr != h) printf("Pre a = %llu, a_ex = %d, b = %llu\n", a, h->d_counter, b);
		//fflush(stdout);
		
		// old = 0;
		// if(barrierArray3 = 0) old = BOOL_CAS(&barrierArray3, 0, BLOCK_ENTRY);
		// else old = VAL_FAA(&barrierArray3, 1);
		// if(old == 11) BOOL_CAS(&barrierArray3, old, 0);
		// else while(!barrierArray3);

		// dumpTable(new_h, "New");

		// old = 0;
		// if(barrierArray3 = 0) old = BOOL_CAS(&barrierArray3, 0, BLOCK_ENTRY);
		// else old = VAL_FAA(&barrierArray3, 1);
		// if(old == 11) BOOL_CAS(&barrierArray3, old, 0);
		// else while(!barrierArray3);

		assert(a == b || *curr_table_ptr != h);
		//LOG("OLD ELEM COUNT: %llu NEW ELEM_COUNT %llu\n", ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter ), ATOMIC_READ( &new_h->e_counter ) - ATOMIC_READ( &new_h->d_counter ));		
		if( BOOL_CAS(curr_table_ptr, h, new_h) ){ //Try to replace the old table with the new one
		 	// I won the challenge thus I collect memory
			//			LOG("OLD ELEM COUNT: %llu\n", ATOMIC_READ( &h->e_counter ) - ATOMIC_READ( &h->d_counter ));
			//LOG("OLD ELEM COUNT: %llu NEW ELEM_COUNT %llu\n",a,b);
			//if(a != b) printf("a = %llu, a_ex = %d, b = %llu\n", a, h->d_counter, b);
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

void dumpTable(table_t* h, char** string){
	char str[50];
	snprintf( str, 50, "./Tables/%d_%p.txt", syscall(SYS_gettid), h);
	FILE* fp;
	fp = fopen(str, "w+");
	bucket_t left_node_next;
	bucket_t right_node;
	bucket_t* bucket;
	bucket_t* prec = NULL;
	unsigned int counter = 0;
	node_t* current;
	unsigned long long ext = 0;
	for(int i = 0; i < h->size; i++){
		bucket = h->array+i;
		while(bucket->type != TAIL){
			if(bucket->type != HEAD){
				ext = get_cleaned_extractions(bucket->extractions);
				if(is_freezed_for_lnk(bucket->extractions) && bucket != NULL){
					current = bucket->head.next;
					if(ext > 0) skip_extracted(bucket->tail, &current, ext);
					else skip_extracted(bucket->tail, &current, getDynamic(get_extractions_wtoutlk(bucket->extractions)));
					//if(current != bucket->tail) current = current->next;
					while (current != bucket->tail){	
						//if(current->payload != NULL && current->timestamp >= MIN && current->timestamp <= INFTY){
							fprintf(fp, "%p, %u, %p, %f, Linked\n", bucket, bucket->index, current->payload, current->timestamp);
							fflush(fp);
							counter++;
						//}
						current = current->next;
					}
				}
				else if(!is_freezed_for_lnk(bucket->extractions) && bucket->arrayOrdered != NULL){
					int j = ext > 0 ? ext : 0;
					for(; j < bucket->arrayOrdered->length; j++){
						if(bucket->arrayOrdered->nodes[j].ptr != NULL && bucket->arrayOrdered->nodes[j].timestamp >= MIN && bucket->arrayOrdered->nodes[j].timestamp <= INFTY){
							fprintf(fp, "%p, %u, %p, %f, Ordered\n", bucket, bucket->index, bucket->arrayOrdered->nodes[j].ptr, bucket->arrayOrdered->nodes[j].timestamp);
							fflush(fp);
							counter++;
						}
					}
				}else{
					for (int i = 0; i < bucket->tot_arrays; i++){
						for(int k = 0; bucket->ptr_arrays[i] != NULL && k < bucket->ptr_arrays[i]->length; k++){
							if(bucket->ptr_arrays[i]->nodes[k].ptr != NULL && bucket->ptr_arrays[i]->nodes[k].timestamp >= MIN && bucket->ptr_arrays[i]->nodes[k].timestamp <= INFTY){
								fprintf(fp, "%p, %u, %p, %f, Unordered\n", bucket, bucket->index, bucket->ptr_arrays[i]->nodes[k].ptr, bucket->ptr_arrays[i]->nodes[k].timestamp);
								fflush(fp);
								counter++;
							}
						}
					}
				}
				if(bucket->pending_insert != NULL && bucket->pending_insert != 0x1){
					node_t* node = (node_t*)bucket->pending_insert;
					fprintf(fp, "%p, %u, %p, %f, Posted\n", bucket, bucket->index, node->payload, node->timestamp);
					fflush(fp);
					counter++;
				}
			}
			bucket = get_next_valid(bucket);
		}
	}
	fprintf(fp, "%s e_counter = %d, d_counter = %d difference = %d\n", string, h->e_counter, h->d_counter, h->e_counter.count -h->d_counter.count);
	fprintf(fp, "Fine %p elems %d\n", h, counter);
	fflush(fp);
	fclose(fp);
	printf("Printed table %p\n", h);
	fflush(stdout);
}

void checkSameArray(arrayNodes_t* arr1, arrayNodes_t* arr2){
	int minLen = arr1->length > arr2->length ? arr2->length : arr1->length;
	int trovato = 0;
	int k = 0;
	for(int i = 0; i < minLen; i++){
		for(k = 0; k < minLen; k++){
			if(arr1->nodes[i].timestamp == arr2->nodes[k].timestamp &&
				arr1->nodes[i].ptr == arr2->nodes[k].ptr){
					trovato++;
					break;
				}
		}
		if(trovato == 0 && k == minLen-1){
			printf("Elemento non trovato (%p, %f)\n", arr1->nodes[i].ptr, arr1->nodes[i].timestamp);
			printArray(arr1->nodes, arr1->indexWrite-1, 0);
			printArray(arr2->nodes, arr2->indexWrite-1, 0);
			fflush(stdout);
			assert(0);
			//break;
		}
		trovato = 0;		
	}
}
#endif