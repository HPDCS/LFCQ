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

// Data for expBackoffTime, should be thread local
unsigned int testSleep = 1;
unsigned int maxSleep = 0;

/**
 * Structure that implements my idea of an element of the array nodes
*/
typedef struct __nodeElem_t {
	// Event payload and its replica
	void* ptr;
	void* replica;
	pkey_t timestamp;
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
	unsigned long long indexRead;
	unsigned long long indexWrite;
} arrayNodes_t;

#include "./myQuickSort.h"
#include "./mm_array.h"

/**
 * Method that initializes the array structure
*/
arrayNodes_t* initArray(unsigned int length){

	arrayNodes_t* array;
	array = arrayNodes_alloc(length);

	//array = gc_alloc(ptst, gc_aid[GC_INTERNALS]);
	//array = (arrayNodes_t*)malloc(sizeof(arrayNodes_t));
	//assert(array != NULL);
	//bzero(array,  sizeof(arrayNodes_t));

	//array->nodes = gc_alloc(ptst, gc_aid[GC_INTERNALS]);
	// array->nodes = (nodeElem_t*)malloc(sizeof(nodeElem_t)*array->length);
	// assert(array->nodes != NULL);
	// bzero(array->nodes, sizeof(nodeElem_t)*array->length);

	return array;
}

/**
 * Function return the low part of a 64 bit number
*/
static inline unsigned long long getDynamic(unsigned long long idx){
	return ((idx << 32) >> 32);
}

/**
 * Function return the high part of a 64 bit number
*/
static inline unsigned long long getFixed(unsigned long long idx){
	//idx = (idx & (1ULL << 63));
	idx = ((idx << 1) >> 1);
	idx = idx >> 32;
	return idx;
}

/**
 * Function create a copy of an array
*/
static inline void copyArray(arrayNodes_t* array, unsigned long long limit, arrayNodes_t* newArray){
	for (unsigned long i = 0; i < limit; i++){
		if(array->nodes[i].timestamp > 0.0){
			newArray->nodes[i].ptr = array->nodes[i].ptr;
			newArray->nodes[i].replica = array->nodes[i].replica;
			newArray->nodes[i].timestamp = array->nodes[i].timestamp;
			newArray->indexWrite++;
		}
	}
}

/**
 * Function create a copy of an array
*/
static inline void toList(arrayNodes_t* array, unsigned long long limit, node_t* tail){
	node_t* prec = node_alloc();
	node_t* succ = NULL;
	for (unsigned long i = 0; i < limit; i++){
		if(array->nodes[i].timestamp > 0.0){
			prec->payload = array->nodes[i].ptr;
			prec->timestamp = array->nodes[i].timestamp;
			prec->next = tail;
			array->nodes[i].ptr = prec;
			if(i != limit){
				succ = node_alloc();
				prec->next = succ;
				prec = succ;
			}
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

/**
 * Function that implements the logic to ordering the array and build the list
*/
arrayNodes_t* vectorOrderingAndBuild(bucket_t* bckt){
	unsigned long long elmToOrder = 0;
	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(bckt->ptr_arrays[actNuma]->length < getFixed(bckt->ptr_arrays[actNuma]->indexWrite))
			elmToOrder += bckt->ptr_arrays[actNuma]->length;
		else
			elmToOrder += getFixed(bckt->ptr_arrays[actNuma]->indexWrite);
	}
	actNuma = 0;
	//arrayNodes_t* newArray = initArray(elmToOrder);
	arrayNodes_t* newArray = bckt->app;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(getFixed(bckt->ptr_arrays[actNuma]->indexWrite) > 0)
			copyArray(bckt->ptr_arrays[actNuma], getFixed(bckt->ptr_arrays[actNuma]->indexWrite), newArray);
		if(!unordered(bckt)){
			free(newArray);
			return NULL;
		}
	}

	// Sort elements
	if(newArray->indexWrite > newArray->length)
		quickSort(newArray->nodes, 0, newArray->length-1);
	else
		quickSort(newArray->nodes, 0, newArray->indexWrite-1);

	// toList also allocate the node elem
	toList(newArray, elmToOrder, bckt->tail);
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
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];

	do{
		index = array->indexWrite;
		if(numRetry > MAX_ATTEMPTS && index == 0) 
			nValCont = index | (1ULL << 63);
		else{
			nValCont =  index | (index << 32);
		}
		BOOL_CAS(&array->indexWrite, index, nValCont);
		numRetry++;
	}while(validContent(array->indexWrite));

	numRetry = 0;
	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(validContent(array->indexWrite)){
			do{
				index = array->indexWrite;
				if(numRetry > MAX_ATTEMPTS && index == 0)
					nValCont = index | (1ULL << 63);
				else{
					nValCont =  index | (index << 32);
				}
				BOOL_CAS(&array->indexWrite, index, nValCont);
				numRetry++;
			}while(validContent(array->indexWrite));
		}
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
static inline void setUnvalidRead(bucket_t* bckt){
	unsigned long long  nValCont = 0;
	unsigned long long  index = 0;
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	do{
		index = array->indexRead;
		nValCont =  index | (index << 32);
		BOOL_CAS(&array->indexRead, index, nValCont);
	}while(!is_freezed(get_cleaned_extractions(array->indexRead)));

	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(!is_freezed(get_cleaned_extractions(array->indexRead))){
			do{
				index = array->indexRead;
				nValCont =  index | (index << 32);
				BOOL_CAS(&array->indexRead, index, nValCont);
			}while(!is_freezed(get_cleaned_extractions(array->indexRead)));
		}
	}
}

// /**
//  * Function that checks if the list is enabled or not
// */
// static inline int unlinked(unsigned long long index){
// 	//return (index >> 63) == 0;
// 	index =is_freezed_for_lnk(index);
// 	return index;
// }

// /**
//  * Function that publish the ordered array of nodes
// */
// static inline void setLinked(bucket_t* bckt){
// 	unsigned long long nValCont = -1;
// 	unsigned long long index = -1;
// 	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
// 	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
// 	do{
// 		index = array->indexRead;
// 		nValCont = index | 1ULL << 63;
// 		BOOL_CAS(&array->indexRead, index, nValCont);
// 	}while(!is_freezed_for_lnk(array->indexRead));

// 	int actNuma;
// 	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
// 		array = bckt->ptr_arrays[actNuma];
// 		if(validRead(array->indexRead)){
// 			do{
// 				index = array->indexRead;
// 				nValCont = index | 1ULL << 63;
// 				BOOL_CAS(&array->indexRead, index, nValCont);
// 			}while(!is_freezed_for_lnk(array->indexRead));
// 		}
// 	}
// }

/**
 * To write in the array
*/
int set(arrayNodes_t* array, int position, void* payload, pkey_t timestamp){
		array->nodes[position].ptr = payload;
		array->nodes[position].timestamp = timestamp;
		if(array->nodes[position].ptr == payload)
			return MYARRAY_INSERT;
		else
			return MYARRAY_ERROR;
}

/**
 * To write in the array using CAS
*/
int setCAS(arrayNodes_t* array, int position, void* payload, pkey_t timestamp){
	int res = -1;
	printArray(array->nodes, array->length-1, 0);
	assert(&array->nodes[position] != NULL);
	assert(array->nodes[position].ptr == NULL);
	while(array->nodes[position].ptr == NULL){
		res = BOOL_CAS(&array->nodes[position].ptr, NULL, payload);
	}
	if(res){
		array->nodes[position].timestamp = timestamp;
		return MYARRAY_INSERT;
	}
	else return MYARRAY_ERROR;
}

/**
 * To read from the array
*/
void get(arrayNodes_t* array, int position, void** payload, pkey_t* timestamp){
	assert(array->nodes[position].ptr != NULL);
	node_t* app = (node_t*)array->nodes[position].ptr;
	*payload = app->payload;
	*timestamp = app->timestamp;
}

static inline void blockEntriesArray(arrayNodes_t* array, unsigned long long limit, unsigned long long start){
	nodeElem_t* app = NULL;
	unsigned long long new_app = 0;
	while(start <= limit){
		app = getNodeElem(array, start);
		if((((unsigned long long)array->nodes[start].ptr) & BLOCK) == 0){
			new_app = (((unsigned long long)app->ptr) | BLOCK);
			BOOL_CAS(&array->nodes[start].ptr, app, new_app);
		}
		
		start++;
	}
}

static inline int isBlocked(arrayNodes_t* array){
	return ((unsigned long long)array->nodes[0].ptr) & BLOCK;
}

/**
 * Function that implements the logic of flag changes
*/
void stateMachine(bucket_t* bckt, unsigned long dequeueStop){
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	if(validContent(array->indexWrite)){
		if(getDynamic(array->indexWrite)-1 >= array->length){
			setUnvalidContent(bckt);
			assert(validContent(array->indexWrite) == false);
		}
	}
	if(!validContent(array->indexWrite) && unordered(bckt)){
		// First Phase: Block all used entries
		// FIXME: Per il momento non blocca
		if(!isBlocked(array))
			blockEntriesArray(array, getFixed(array->indexWrite), 0);

		for (int actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
			array = bckt->ptr_arrays[actNuma];
			if(!isBlocked(array)){
				blockEntriesArray(array, getFixed(array->indexWrite), 0);
			}
		}
		// Second Phase: Order elements and the public the array
		arrayNodes_t*  newArray = vectorOrderingAndBuild(bckt);
		if(newArray != NULL){
			setOrdered(bckt, newArray);
		}
		if(bckt->arrayOrdered != newArray)
			free(newArray);
		assert(unordered(bckt) == false);
	}

	if(dequeueStop) return;

	assert(validContent(array->indexWrite) == false || unordered(bckt) == true || is_freezed_for_lnk(bckt->extractions) == false);
	int isnotLinked = !is_freezed_for_lnk(bckt->extractions);
	int isOrdered = !unordered(bckt);
	if(isOrdered && isnotLinked){
		unsigned int attempts = MAX_ATTEMPTS;
		unsigned int idx;
		unsigned int __status;
		unsigned long long newExt = 0;
		array = bckt->arrayOrdered;
		assert(bckt->head.next == bckt->tail);
		while(bckt->head.next == bckt->tail){
			if(attempts > 0){
				// START TRANSACTION
				if((__status = _xbegin ()) == _XBEGIN_STARTED){
					idx = get_extractions_wtoutlk(bckt->extractions);
					bckt->head.next = (node_t*)array->nodes[idx].ptr;
					newExt = bckt->extractions >> 60;
					newExt = (0ULL | (newExt << 60));
					bckt->extractions = newExt;
					TM_COMMIT();
				}else
					expBackoffTime(&testSleep, &maxSleep);
			}else{
				if(BOOL_CAS(&bckt->head.next, NULL, (node_t*)array->nodes[0].ptr))
					VAL_FAA(&bckt->extractions, 0ULL);
			}
			attempts--;
		}
		//setLinked(bckt);
		atomic_bts_x64(&bckt->extractions, LNK_BIT_POS);
	}
}

//#define array_safe_free(ptr) 			gc_free(ptst, ptr, gc_aid[GC_INTERNALS])
//#define array_unsafe_free(ptr) 			gc_unsafe_free(ptst, ptr, gc_aid[GC_INTERNALS])
//void array_safe_free(myArray_t* array);
//void array_unsafe_free(myArray_t* array);

/**
 * Function that insert a new node in the array
*/
int nodesInsert(arrayNodes_t* array, int idxWrite, void* payload, pkey_t timestamp){
	printf("Insert in Array: %p %f\n", payload, timestamp);
	int attempts = MAX_ATTEMPTS;
	int resRet = MYARRAY_ERROR;
	int __status = 0;
	while(attempts > 0 && resRet == MYARRAY_ERROR){
		// START TRANSACTION
		__status = _xbegin();
		if(__status == _XBEGIN_STARTED){
			if(validContent(idxWrite)){
				resRet =  set(array, idxWrite, payload, timestamp);
				TM_COMMIT();
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
	if(attempts <= 0){
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
		printf("ArrayDequeue: result %p, timestamp %f\n", *payload, *timestamp);
		return MYARRAY_EXTRACT;
	} else{
		return MYARRAY_ERROR;
	}
}

#endif