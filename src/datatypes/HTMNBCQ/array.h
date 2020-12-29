#ifndef ARRAY_H
#define ARRAY_H

// Maybe already included by others .h files

#include <stdbool.h>
#include <strings.h>
#include <assert.h>
#include <immintrin.h>
#include "./vbucket.h"
#include "./common_f.h"

#define GC_ARRAYNODES 2
#define GC_NODEELEMS 3

#define SM_DEQUEUE 1ULL
#define SM_ENQUEUE 2ULL
#define SM_DEQUEUE_ARRAY_FINISHED 3ULL
#define DEQUEUE 1UL
#define ENQUEUE 0UL

#define MYARRAY_ERROR -1
#define MYARRAY_INSERT 0
#define MYARRAY_EXTRACT 1

#define BLOCK_ENTRY 0xFFFFFFFFFFFFFFFF

// Data for expBackoffTime, should be thread local
unsigned int testSleep = 1;
unsigned int maxSleep = 0;

/**
 * Structure that implements my idea of an element of the array nodes
*/
typedef struct __nodeElem_t {
	// Event payload and its replica
	void* ptr;
	pkey_t timestamp;
	// unsigned int tie_breaker;
	// void* replica;
} nodeElem_t;

/**
 * Structure that implements the array nodes
*/
typedef struct __arrayNodes_t {
	// Array of nodeElem
	nodeElem_t* nodes;

	// Metadata
	long long int length;
	// Important for insertion ed extraction
	unsigned int epoch;
	//long long int indexRead;
	long long int indexWrite;
} arrayNodes_t;

#include "./myQuickSort.h"
#include "./mm_array.h"

/**
 * Method that initializes the array structure
*/
arrayNodes_t* initArray(unsigned int length){

	arrayNodes_t* array;
	//array = arrayNodes_alloc(length);

	array = gc_alloc(ptst, gc_aid[GC_ARRAYNODES]);
	assert(array != NULL);
	bzero(array,  sizeof(arrayNodes_t));

	array->epoch = 0;
	//array->indexRead = 0;
	array->indexWrite = 0;
	array->length = length;

	assert(sizeof(nodeElem_t)*array->length > 0);
	array->nodes = (nodeElem_t*)malloc(sizeof(nodeElem_t)*array->length);
	assert(array->nodes != NULL);
	bzero(array->nodes, sizeof(nodeElem_t)*array->length);

	return array;
}

/**
 * Function return the low part of a 64 bit number
*/
static inline long long int getDynamic(unsigned long long idx){
	return ((idx << 32) >> 32);
}

/**
 * Function return the high part of a 64 bit number
*/
static inline long long int getFixed(unsigned long long idx){
	//idx = (idx & (1ULL << 63));
	idx = idx & ~(1ULL << LNK_BIT_POS);
	idx = idx >> 32;
	return idx;
}

/**
 * Function create a copy of an array
*/
static inline void copyArray(arrayNodes_t* array, unsigned long long limit, arrayNodes_t* newArray){
	int found = 0;
	for (unsigned long i = 0; i < limit; i++){
		if(array->nodes[i].ptr != BLOCK_ENTRY && array->nodes[i].timestamp >= MIN && array->nodes[i].timestamp < INFTY){
			// found = binarySearch(newArray->nodes, 0, newArray->indexWrite-1, array->nodes[i].timestamp);
			// if(found == -1){
				newArray->nodes[newArray->indexWrite].ptr = array->nodes[i].ptr;
				//newArray->nodes[newArray->indexWrite].replica = array->nodes[i].replica;
				newArray->nodes[newArray->indexWrite].timestamp = array->nodes[i].timestamp;
				//newArray->nodes[newArray->indexWrite].tie_breaker = 0;
				newArray->indexWrite++;
			// }else
			// 	newArray->nodes[found].tie_breaker++;
		}else
			newArray->length--;
	}
}

/**
 * Function create a copy of an array
*/
static inline void toList(arrayNodes_t* array, unsigned long long limit, node_t** tail){
	node_t* prec = node_alloc();
	node_t* succ = NULL;
	for(unsigned long i = 0; i < limit; i++){
		prec->payload = array->nodes[i].ptr;
		prec->timestamp = array->nodes[i].timestamp;
		//prec->tie_breaker = array->nodes[i].tie_breaker;
		prec->next = *tail;
		array->nodes[i].ptr = prec;
		if(i < limit-1){
			succ = node_alloc();
			prec->next = succ;
			prec = succ;
		}
	}
}

/**
 * Function that checks if the an ordered array is published
*/
static inline int unordered(bucket_t* bckt){
	return bckt->arrayOrdered == NULL;
}

/**
 * Function that publish the ordered array of nodes
*/
static inline void setOrdered(bucket_t* bckt, arrayNodes_t* newArray){
	do{
		BOOL_CAS(&bckt->arrayOrdered, NULL, newArray);
	}while(unordered(bckt));
}

/**
 * To read from the array return nodeElem_t
*/
nodeElem_t* getNodeElem(arrayNodes_t* array, unsigned long long position){
	return array->nodes+position;
}

static inline void blockArray(arrayNodes_t** array){
	long long int idx = 0;
	long long int length = (*array)->length;
	nodeElem_t* tuples = (*array)->nodes;
	while(idx < length){
		while(tuples[idx].timestamp == MIN){
			BOOL_CAS(UNION_CAST(&tuples[idx].timestamp, unsigned long long *),
						UNION_CAST(MIN,unsigned long long),
						UNION_CAST(INFTY, unsigned long long));
			BOOL_CAS(&tuples[idx].ptr, NULL, BLOCK_ENTRY);
		}
		idx++;
	}
}

/**
 * Function that implements the logic to ordering the array and build the list
*/
arrayNodes_t* vectorOrderingAndBuild(bucket_t* bckt){
	unsigned long long elmToOrder = 0;
	int actNuma;
	arrayNodes_t* array = NULL;
	for (actNuma = 0; actNuma < bckt->tot_arrays; actNuma++){
		array = bckt->ptr_arrays[actNuma];
		if(array == NULL) continue;
		if(getFixed(array->indexWrite) > array->length)
			elmToOrder += array->length;
		else 
			elmToOrder += getFixed(array->indexWrite);
	}
	actNuma = 0;
	arrayNodes_t* newArray = NULL;
	// printf("newArray->nodes %p, elemToOrder: %u\n", newArray->nodes, elmToOrder);
	if(elmToOrder > 0){
		newArray = initArray(elmToOrder);
		for (actNuma = 0; actNuma < bckt->tot_arrays; actNuma++){
			array = bckt->ptr_arrays[actNuma];
			if(array == NULL) continue;
			if(getFixed(array->indexWrite) > 0){
				if(getFixed(array->indexWrite) > array->length)
					copyArray(array, array->length, newArray);
				else
					copyArray(array, getFixed(array->indexWrite), newArray);
			}
			if(!unordered(bckt)){
				arrayNodes_safe_free(newArray);
				return NULL;
			}
		}

		if(!unordered(bckt)) return NULL;

		void* addr = NULL;
		for (int i = 0; i < newArray->length; i++){
			addr = newArray->nodes[i].ptr;
			//assert(addr != NULL);
		}

		//  printArray(newArray->nodes, newArray->length, 0);
		// Sort elements
		quickSort(newArray->nodes, 0, newArray->length-1);

		// toList also allocate the node elem
		toList(newArray, newArray->length, &bckt->tail);
		
		//  printList(newArray->nodes[0].ptr);
	}
	return newArray;
}

/**
 * Function that checks if the index passed indicates that the array insert is still valid
*/
static inline unsigned long long validContent(unsigned long long index){
	return (index >> 32) == 0;
}

/**
 * Function that blocks the insert in the array setting the high part equal to the low part in indexWrite
*/
static inline void setUnvalidContent(bucket_t* bckt){
	long long int nValCont = -1;
	long long int index = -1;
	int numRetry = 0;
	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];

	while(validContent(array->indexWrite)){
		index = array->indexWrite;
		if(numRetry > MAX_ATTEMPTS && index == 0) 
			nValCont = index | (1ULL << LNK_BIT_POS);
		else{
			nValCont =  index | (index << 32);
		}
		BOOL_CAS(&array->indexWrite, index, nValCont);
		numRetry++;
	}
	
	numRetry = 0;
	int actNuma;
	// printf("Array da bloccare = %u\n", bckt->tot_arrays);
	for (actNuma = 0; actNuma < bckt->tot_arrays; actNuma++){
		array = bckt->ptr_arrays[actNuma];
		if(array == NULL) continue;
		while(validContent(array->indexWrite)){
			index = array->indexWrite;
			if(numRetry > MAX_ATTEMPTS && index == 0)
				nValCont = index | (1ULL << LNK_BIT_POS);
			else{
				nValCont =  index | (index << 32);
			}
			BOOL_CAS(&array->indexWrite, index, nValCont);
			numRetry++;
		}
		// printf("%p, bloccato Array %d idxWrite = %lld\n", bckt, actNuma, getFixed(get_extractions_wtoutlk(array->indexWrite)));
		// fflush(stdout);
	}
}

// /**
//  * Function that checks if the dequeue is blocked
// */
// static inline unsigned long long validRead(unsigned long long index){
// 	// index &= ~(1ULL << 63);
// 	// return (index >> 32) == 0;
// 	index = ((index << 1) >> 1);
// 	index = index >> 32;
// 	return index == 0;
// }

/**
 * Function that blocks the dequeue in the array setting the high part equal to the low part in indexWrite
*/
// static inline void setUnvalidRead(bucket_t* bckt){
// 	unsigned long long  nValCont = 0;
// 	unsigned long long  index = 0;
// 	int numRetry = 0;
// 	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
// 	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
// 	// TODO: Valutare se mettere un if prima di fare questo while
// 	do{
// 		index = array->indexRead;
// 		if(numRetry > MAX_ATTEMPTS && index == 0) 
// 			nValCont = index | (1ULL << 63);
// 		else{
// 			nValCont =  index | (index << 32);
// 		}
// 		BOOL_CAS(&array->indexRead, index, nValCont);
// 		numRetry++;
// 	}while(validContent(array->indexRead));

// 	numRetry = 0;
// 	for (int actNuma = 0; actNuma < bckt->tot_arrays; actNuma++){
// 		array = bckt->ptr_arrays[actNuma];
// 		if(array == NULL) continue;
// 		if(validContent(array->indexRead)){
// 			do{
// 				index = array->indexRead;
// 				if(numRetry > MAX_ATTEMPTS && index == 0)
// 					nValCont = index | (1ULL << 63);
// 				else{
// 					nValCont =  index | (index << 32);
// 				}
// 				BOOL_CAS(&array->indexRead, index, nValCont);
// 				numRetry++;
// 			}while(validContent(array->indexRead));
// 		}
// 	}
// }

/**
 * To write in the array
*/
static inline int set(arrayNodes_t* array, int position, void* payload, pkey_t timestamp){
	int retVal = MYARRAY_ERROR;
	if(array->nodes[position].ptr == NULL){
		array->nodes[position].ptr = payload;
		array->nodes[position].timestamp = timestamp;
		retVal = MYARRAY_INSERT;
	}
	return retVal;
}

/**
 * To write in the array using CAS
*/
static inline int setCAS(arrayNodes_t* array, int position, void* payload, pkey_t timestamp){
	void* res = 0x1;
	int retVal = MYARRAY_ERROR;
	assert(array->nodes[position].ptr == NULL || array->nodes[position].ptr == BLOCK_ENTRY);
	while(array->nodes[position].ptr == NULL){
		res = VAL_CAS(&array->nodes[position].ptr, NULL, payload);
		if(res == NULL){
			array->nodes[position].timestamp = timestamp;
			retVal = MYARRAY_INSERT;
		}
	}

	return retVal;
}

/**
 * To read from the array
*/
void get(arrayNodes_t* array, int position, void** payload, pkey_t* timestamp){
	void* app1 = (void*)array->nodes[position].ptr;
	assert(app1 != NULL && app1 != BLOCK_ENTRY);
	node_t* app = (node_t*)app1;
	*payload = app->payload;
	*timestamp = app->timestamp;
}

/**
 * Function that implements the logic of flag changes
*/
int stateMachine(bucket_t* bckt, unsigned long dequeueStop){
	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
	int public = 0;
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	if(validContent(array->indexWrite)){
		if(getDynamic(array->indexWrite) > 0 && getDynamic(array->indexWrite)-1 >= array->length){
			setUnvalidContent(bckt);
			assert(validContent(array->indexWrite) == false);
		}
	}
	if(!validContent(array->indexWrite) && unordered(bckt)){
		// First Phase: Block all used entries
		blockArray(bckt->ptr_arrays+numaNode);

		for (int actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
			blockArray(bckt->ptr_arrays+actNuma);
		}
		// Second Phase: Order elements and the public the array

		// for (int i = 0; i < bckt->tot_arrays && bckt->index == 24; i++){
		// 	if(bckt->ptr_arrays[i] != NULL){
		// 		printArray(bckt->ptr_arrays[i]->nodes, bckt->ptr_arrays[i]->indexWrite, 0);
		// 	}
		// }

		arrayNodes_t*  newArray = NULL;
		if(unordered(bckt))
			newArray = vectorOrderingAndBuild(bckt);

		if(newArray != NULL){
			// Lock the write on the array ordered
			newArray->indexWrite = (newArray->indexWrite << 32) | newArray->indexWrite;
			if(unordered(bckt)) 
				setOrdered(bckt, newArray);
			if(bckt->arrayOrdered != newArray)
				arrayNodes_unsafe_free_malloc(newArray);
			else
				public = 1;
			assert(unordered(bckt) == false);
			// printArray(bckt->arrayOrdered->nodes, bckt->arrayOrdered->length, 0);
		}

		// Code for checking if the arrayOrdered elements are composed as I expect
		// for(int i = 0; i < getFixed(bckt->arrayOrdered->indexWrite); i++){
		// 	node_t* app = ((node_t*)bckt->arrayOrdered->nodes[i].ptr);
		// 	assert(app->payload != NULL && app->timestamp > MIN && app->timestamp < INFTY);
		// }
	}

	if(dequeueStop) return public;

	if(!unordered(bckt) && !is_freezed_for_lnk(bckt->extractions)){
		int attempts = MAX_ATTEMPTS;
		unsigned int idx;
		unsigned int __status;
		unsigned long long newExt = 0;
		array = bckt->arrayOrdered;
		while(bckt->head.next == bckt->tail){
			if(attempts > 0){
				// START TRANSACTION
				if((__status = _xbegin ()) == _XBEGIN_STARTED){
					idx = get_extractions_wtoutlk(bckt->extractions);
					if(is_freezed_for_lnk(bckt->extractions) == false){
						if(idx < array->length && array->nodes[idx].ptr != NULL){
							bckt->head.next = (node_t*)array->nodes[idx].ptr;
							newExt = bckt->extractions >> 60;
							newExt = (0ULL | (newExt << 60));
							bckt->extractions = newExt;
							TM_COMMIT();
						}
					}else
						TM_ABORT(0x1);
				}else
					expBackoffTime(&testSleep, &maxSleep);
			}else{
				if(array->nodes[0].ptr != NULL){
					BOOL_CAS(&bckt->head.next, bckt->tail, array->nodes[0].ptr);
				}
				break;
			}
			attempts--;
		}
		assert(bckt->head.next != NULL);
		atomic_bts_x64(&bckt->extractions, LNK_BIT_POS);
		assert(bckt->extractions & FREEZE_FOR_LNK);
		// printList(bckt->head.next);
	}
	return public;
}

/**
 * Function that insert a new node in the array
*/
int nodesInsert(arrayNodes_t* array, int idxWrite, void* payload, pkey_t timestamp){
	//printf("Insert in Array: %p %f\n", payload, timestamp);
	int attempts = MAX_ATTEMPTS;
	int resRet = MYARRAY_ERROR;
	int __status = 0;
	while(attempts > 0 && array->nodes[idxWrite].ptr == NULL && resRet == MYARRAY_ERROR){
		// START TRANSACTION
		__status = _xbegin();
		if(__status == _XBEGIN_STARTED){
			if(array->nodes[idxWrite].ptr == NULL){
				array->nodes[idxWrite].ptr = payload;
				array->nodes[idxWrite].timestamp = timestamp;
				resRet = MYARRAY_INSERT;
				TM_COMMIT();
				//resRet =  set(array, idxWrite, payload, timestamp);
			}else{
				TM_ABORT(XABORT_CODE_RET);
			}
		}else{
			// I can no longer use the array
			if(__status & _XABORT_EXPLICIT ) break;
			expBackoffTime(&testSleep, &maxSleep);
		}
		attempts--;
	}

	// Fallback to CAS
	if(resRet == MYARRAY_ERROR){
		resRet = setCAS(array, idxWrite, payload, timestamp);
	}
	return resRet;
}

/**
 * Function that extract a new node in the array
*/
int nodesDequeue(arrayNodes_t* array, int idxRead, void** payload, pkey_t* timestamp){
	if(idxRead < array->length){
		get(array, idxRead, payload, timestamp);
		//printf("ArrayDequeue: result %p, timestamp %f\n", *payload, *timestamp);
		return MYARRAY_EXTRACT;
	} else{
		return MYARRAY_ERROR;
	}
}

#endif