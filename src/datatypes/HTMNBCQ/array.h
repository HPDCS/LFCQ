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
	//array = arrayNodes_alloc(length);

	array = gc_alloc(ptst, gc_aid[GC_ARRAYNODES]);
	assert(array != NULL);
	bzero(array,  sizeof(arrayNodes_t));

	array->epoch = 0;
	array->indexRead = 0;
	array->indexWrite = 0;
	array->length = length;

	array->nodes = (nodeElem_t*)malloc(sizeof(nodeElem_t)*array->length);
	assert(array->nodes != NULL);
	bzero(array->nodes, sizeof(nodeElem_t)*array->length);

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
		if(array->nodes[i].ptr != NULL && array->nodes[i].timestamp > MIN){
			//printf("%ld, %p, %f\n", syscall(SYS_gettid), array->nodes[i].ptr, array->nodes[i].timestamp);
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

static inline void blockEntriesArray(arrayNodes_t** array){
	unsigned long long new_array = 0;
	if((((unsigned long long)array) & BLOCK) == 0){
		new_array = (((unsigned long long)(*array)) | BLOCK);
		BOOL_CAS(array, *array, new_array);
	}
}

static inline int isBlocked(arrayNodes_t* array){
	return ((unsigned long long)array) & BLOCK;
}

static inline arrayNodes_t* unBlock(arrayNodes_t* array){
	unsigned long long app = (unsigned long long)array;
	app = app & ~BLOCK;
	return (arrayNodes_t*)app;
}

/**
 * Function that implements the logic to ordering the array and build the list
*/
arrayNodes_t* vectorOrderingAndBuild(bucket_t* bckt){
	unsigned long long elmToOrder = 0;
	int actNuma;
	arrayNodes_t* array = NULL;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		array = unBlock(bckt->ptr_arrays[actNuma]);
		if(array->length < getFixed(array->indexWrite)){
			elmToOrder += array->length;
		}else{
			elmToOrder += getFixed(array->indexWrite);
		}
	}
	actNuma = 0;
	arrayNodes_t* newArray = initArray(elmToOrder);
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		array = unBlock(bckt->ptr_arrays[actNuma]);
		if(getFixed(array->indexWrite) > 0)
			copyArray(array, getFixed(array->indexWrite), newArray);
		if(!unordered(bckt)){
			arrayNodes_safe_free(newArray);
			return NULL;
		}
	}
	// Sort elements
	if(newArray->indexWrite > newArray->length){
		quickSort(newArray->nodes, 0, newArray->length-1);
	}else{
		quickSort(newArray->nodes, 0, newArray->indexWrite-1);
	}

	// printf("Fine %ld\n", syscall(SYS_gettid));
	// printArray(newArray->nodes, newArray->indexWrite, 0);
	// fflush(stdout);
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
	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
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
		array = bckt->ptr_arrays[actNuma];
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
	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
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
// 	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
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
	if(array->nodes[position].ptr == NULL && array->nodes[position].timestamp == MIN){
		*payload = array->nodes[position].ptr;
		*timestamp = array->nodes[position].timestamp;
	}else{
		void* app1 = (void*)array->nodes[position].ptr;
		assert(app1 != NULL);
		node_t* app = (node_t*)app1;
		*payload = app->payload;
		*timestamp = app->timestamp;
	}

}

/**
 * Function that implements the logic of flag changes
*/
void stateMachine(bucket_t* bckt, unsigned long dequeueStop){
	int numaNode = getNumaNode(syscall(SYS_gettid), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	if(validContent(array->indexWrite)){
		if(getDynamic(array->indexWrite)-1 >= array->length){
			setUnvalidContent(bckt);
			assert(validContent(array->indexWrite) == false);
		}
	}
	if(!validContent(array->indexWrite) && unordered(bckt)){
		// First Phase: Block all used entries
		if(!isBlocked(bckt->ptr_arrays[numaNode]))
			blockEntriesArray(bckt->ptr_arrays+numaNode);

		for (int actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
			if(!isBlocked(bckt->ptr_arrays[actNuma])){
				blockEntriesArray(bckt->ptr_arrays+actNuma);
			}
		}
		// Second Phase: Order elements and the public the array
		arrayNodes_t*  newArray = vectorOrderingAndBuild(bckt);
		if(newArray != NULL){
			setOrdered(bckt, newArray);
		}
		// if(bckt->arrayOrdered != newArray)
		// 	arrayNodes_unsafe_free_malloc(newArray);
		assert(unordered(bckt) == false);
		//assert(validContent(array->indexWrite) == false && unordered(bckt) == false && is_freezed_for_lnk(bckt->extractions) == false);
	}

	if(dequeueStop) return;

	if(!unordered(bckt) && !is_freezed_for_lnk(bckt->extractions)){
		//assert(validContent(array->indexWrite) == false && unordered(bckt) == false && is_freezed_for_lnk(bckt->extractions) == false);
		int attempts = MAX_ATTEMPTS;
		unsigned int idx;
		unsigned int __status;
		unsigned long long newExt = 0;
		node_t* tmp = node_alloc();
		array = bckt->arrayOrdered;
		while(bckt->head.next == bckt->tail){
			if(attempts > 0){
				// START TRANSACTION
				if((__status = _xbegin ()) == _XBEGIN_STARTED){
					idx = get_extractions_wtoutlk(bckt->extractions);
					if(is_freezed_for_lnk(bckt->extractions) == false){
						if(array->nodes[idx].ptr != NULL){
							bckt->head.next = (node_t*)array->nodes[idx].ptr;
							newExt = bckt->extractions >> 60;
							newExt = (0ULL | (newExt << 60));
							bckt->extractions = newExt;
						}else{
							tmp->payload = NULL;
							tmp->timestamp = MIN;
							tmp->next = bckt->head.next;
							bckt->head.next = tmp;
						}
						TM_COMMIT();
					}else
						TM_ABORT(0x1);
				}else
					expBackoffTime(&testSleep, &maxSleep);
			}else{
				BOOL_CAS(&bckt->head.next, bckt->tail, array->nodes[0].ptr);
			}
			attempts--;
		}
		if(tmp->timestamp == INFTY) node_safe_free(tmp);
		assert(bckt->head.next != NULL);
		atomic_bts_x64(&bckt->extractions, LNK_BIT_POS);
		//FIXME: Triggera
		assert(is_freezed_for_lnk(bckt->extractions) == true);
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
	//printf("Insert in Array: %p %f\n", payload, timestamp);
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
		//printf("ArrayDequeue: result %p, timestamp %f\n", *payload, *timestamp);
		return MYARRAY_EXTRACT;
	} else{
		return MYARRAY_ERROR;
	}
}

#endif