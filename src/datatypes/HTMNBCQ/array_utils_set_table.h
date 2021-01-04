#ifndef ARRAY_UTILS_SET_TABLE_H_
#define ARRAY_UTILS_SET_TABLE_H_

#include "set_table.h"

static inline void array_block_table(table_t *h)
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

		// LUCKY: Try to block read and write from arrays in bucket
		//setUnvalidRead(bucket);
		// setUnvalidContent(bucket);
		// for(unsigned int i = 0; i < bucket->tot_arrays; i++){
		// 	blockArray(bucket->ptr_arrays+i);
		// }
		// LUCKY: end


		// TODOOOOOOOO
		//Try to mark the first VALid node as MOV 
		do
		{
			search(bucket, &left_node_next, &right_node, &counter, 0);
			break;
			if(right_node->type != TAIL) {
				post_operation(right_node, SET_AS_MOV, 0ULL, NULL);
				execute_operation(right_node);
				// LUCKY: Try to block read and write from arrays in bucket
				//setUnvalidRead(right_node);
				setUnvalidContent(right_node);
				for(unsigned int i = 0; i < right_node->tot_arrays; i++){
					blockArray(right_node->ptr_arrays+i);
				}
				// LUCKY: end
			}
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
 

double array_compute_mean_separation_time(table_t *h,
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
	// TODO: Chiedere perché questo counter = 1 a Romolo
	counter = 1;    
    //read nodes until the total samples is reached or until someone else do it
	acc = 0;

	// LUCKY:
	unsigned int idxPtr = 0;
	long long int start = 0;
	unsigned int stopIter = 1;
	long long int idxWrite = 0;
	arrayNodes_t* consArr = NULL;
	arrayNodes_t* selected = initArray(SAMPLE_SIZE);
	// LUCKY: end

// printf("sample_size %d\n", sample_size);
// fflush(stdout);

	while(counter != sample_size && new_h->bucket_width == -1.0)
	{
		for (i = 0; i < (size << BUCKET_ASSOCIATIVITY); i++)
		{
			tmp = array + virtual_to_physical(index, size); //get the head of the bucket
			//tmp = get_unmarked(tmp->next);		//pointer to first node
//			printf("Searching index %u into %u bucket\n", index, virtual_to_physical(index + i, size));
			left = search(tmp, &left_next, &right, &distance, index);

			// the bucket is not empty
			if(left->index == index  && left->type != HEAD){
//				if(tid == 1)	LOG("%d- INDEX: %u COUNTER %u SAMPLESIZE %u\n",tid, index, counter, sample_size);
				// LUCKY:
				for(int i = 0; i < left->tot_arrays; i++){
					if(left->ptr_arrays[i] != NULL){
						blockArray(left->ptr_arrays+i);
					}
				}

				stopIter = 1;
				unsigned long long elmToOrder = 0;
				int actNuma;
				int writes = 0;
				for (actNuma = 0; actNuma < left->tot_arrays; actNuma++){
					consArr = left->ptr_arrays[actNuma];
					if(consArr == NULL) continue;
					writes = get_cleaned_extractions(consArr->indexWrite);
					if(writes > consArr->length)
						elmToOrder += consArr->length;
					else 
						elmToOrder += writes;
				}
				actNuma = 0;
				arrayNodes_t* newArray = NULL;
				if(elmToOrder > 0){
					newArray = initArray(elmToOrder);
					for (actNuma = 0; actNuma < left->tot_arrays; actNuma++){
						consArr = left->ptr_arrays[actNuma];
						if(consArr == NULL) continue;
						writes = get_cleaned_extractions(consArr->indexWrite);
						if(writes > 0){
							if(writes > consArr->length)
								copyArray(consArr, consArr->length, newArray);
							else
								copyArray(consArr, writes, newArray);
						}
					}

					// Sort elements
					//printArray(newArray->nodes, newArray->indexWrite, 0);
					quickSort(newArray->nodes, 0, newArray->indexWrite-1);
					//printf("Dopo\n");
					//printArray(newArray->nodes, newArray->indexWrite, 0);
				}
				for(start = 0; newArray != NULL && start < newArray->length && stopIter; start++){
					if(newArray->nodes[start].timestamp == INFTY){
						printf("\n\t!ERROR! INFINITE TIMESTAMP IN THE QUEUE! ABORT!\n");
						fflush(stdout);
						_exit(1);
					}
					// if(ordered->nodes[start].timestamp != sample_array[counter-1]){
					// 	sample_array[++counter] = ordered->nodes[start].timestamp; 
					// }
					assert(selected->indexWrite < selected->length);
					if(selected->indexWrite > 0){
						if(newArray->nodes[start].timestamp != selected->nodes[selected->indexWrite-1].timestamp){
							// printf("Selected elem %f\n", ordered->nodes[start].timestamp);
							selected->nodes[selected->indexWrite].timestamp = newArray->nodes[start].timestamp;
						}
					}else{
						// printf("Selected elem %f\n", ordered->nodes[start].timestamp);
						selected->nodes[selected->indexWrite].timestamp = newArray->nodes[start].timestamp;
					}
					selected->indexWrite++;
					counter++;
					if(h->new_table->bucket_width != -1.0) return h->new_table->bucket_width;
					if(counter == sample_size) stopIter = 0;
				}
				arrayNodes_safe_free_malloc(newArray);
				// LUCKY: end
			}

			if(index == new_min_index) new_min_index = -1;
			if(right->type != TAIL) new_min_index = new_min_index < right->index ? new_min_index : right->index;
			index++;
			if(counter == sample_size) break;
		}
//		printf("COUNTER %u SAMPLE %u\n", counter, sample_size);
		//if the calendar has no more elements I will go out
		if(new_min_index == -1 || counter == sample_size)
			break;
		//otherwise I will restart from the next good bucket
		index = new_min_index;
		new_min_index = -1;
	}


	counter = 1;
	//printArray(selected->nodes, selected->indexWrite, 0);
	if(selected->indexWrite > 1)
		quickSort(selected->nodes, 0, selected->indexWrite-1);
	for(int i = 0; i < sample_size; i++){
		sample_array[++counter] = selected->nodes[i].timestamp;
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

	if(newaverage <= 0.0){
//		newaverage = 1.0;
	printf("NEW AVG:.%10f AVG:%.20f 2*AVG:%.20f SAMPLE SIZE:%u:\n" , newaverage, average, 2.0*average, sample_size);
newaverage=1.0;

        for(i = 1; i<sample_size;i++)
                printf("%u: %.20f\n", i, sample_array[i]);

        for(i = 2; i<=sample_size;i++)
                printf("%u: %.20f %u %.20f\n", i, sample_array[i]-sample_array[i-1], sample_array[i]==sample_array[i-1], 2.0*average);
	}
	//if(newaverage <  pow(10,-4))
	//	newaverage = pow(10,-3);
	if(isnan(newaverage)){
	//	newaverage = 1.0;
        printf("NEW AVG:.%10f AVG:%.10f SAMPLE SIZE:%u\n" , newaverage, average, sample_size);
newaverage=1.0;       
 }	
    
	//  LOG("%d- my new bucket %.10f for %p AVG REPEAT:%u\n", TID, newaverage, h, acc/counter);	
	return //FIXED_BW; //
	newaverage;
}

#endif
