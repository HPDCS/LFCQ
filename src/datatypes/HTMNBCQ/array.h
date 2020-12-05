#ifndef ARRAY_H
#define ARRAY_H

// Maybe already included by others .h files

#include <stdbool.h>
#include <strings.h>
#include <assert.h>
#include <immintrin.h>
#include "./vbucket.h"
#include "./common_f.h"


#define SM_DEQUEUE 1ULL
#define SM_ENQUEUE 2ULL
#define SM_DEQUEUE_ARRAY_FINISHED 3ULL
#define DEQUEUE 0UL
#define ENQUEUE 1UL

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
	unsigned int length;
	// Important for insertion ed extraction
	unsigned int epoch;
	unsigned long long indexRead;
	unsigned long long indexWrite;
} arrayNodes_t;

/**
 * Method that initializes the array structure
*/
arrayNodes_t* initArray(int length){
	arrayNodes_t* array;
	//array = gc_alloc(ptst, gc_aid[GC_INTERNALS]);
	array = (arrayNodes_t*)malloc(sizeof(arrayNodes_t));
	assert(array != NULL);
	bzero(array,  sizeof(arrayNodes_t));

	// Set the metadata values
	array->length = length;
	array->epoch = -1;
	array->indexRead = 0;
	array->indexWrite = 0;

	//array->nodes = gc_alloc(ptst, gc_aid[GC_INTERNALS]);
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
	return idx >> 32;
}

/**
 * Function create a copy of an array
*/
static inline void copyArray(arrayNodes_t* array, unsigned long long limit, arrayNodes_t* newArray){
	for (unsigned long i = 0; i < limit; i++){
		newArray->nodes[i].ptr = array->nodes[i].ptr;
		newArray->nodes[i].timestamp = array->nodes[i].timestamp;
	}
}

/**
 * Function create a copy of an array
*/
static inline void toList(arrayNodes_t* array, unsigned long long limit){
	node_t* prec = node_alloc();
	node_t* succ = node_alloc();
	for (unsigned long i = 0; i < limit; i++){
		prec->payload = array->nodes[i].ptr;
		prec->timestamp = array->nodes[i].timestamp;
		prec->next = succ;
		array->nodes[i].ptr = prec;
		prec = succ;
		succ = node_alloc();
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
	unsigned long long elmToOrder = -1;
	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		elmToOrder += getFixed(bckt->ptr_arrays[actNuma]->indexWrite);
	}
	actNuma = 0;
	arrayNodes_t* newArray = initArray(elmToOrder);
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		copyArray(bckt->ptr_arrays[actNuma], getFixed(bckt->ptr_arrays[actNuma]->indexWrite), newArray);
		if(!unordered(bckt)){
			free(newArray);
			return NULL;
		}
	}

	// TODO: Manca l'implementazione di sort
	//sort(newArray, elmToOrder);
	
	// toList also allocate the node elem
	toList(newArray, elmToOrder);

	return newArray;
}

/**
 * Function that checks if the index passed indicates that the array insert is still valid
*/
static inline unsigned long long validContent(unsigned long long index){
	return !((index >> 32) & 0);
}

/**
 * Function that blocks the insert in the array setting the high part equal to the low part in indexWrite
*/
static inline void setUnvalidContent(bucket_t* bckt){
	unsigned long long nValCont = -1;
	unsigned long long index = -1;
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];

	do{
		index = array->indexWrite;
		nValCont =  index | (index << 32);
		BOOL_CAS(&array->indexWrite, index, nValCont);
	}while(validContent(array->indexWrite));

	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(validContent(array->indexWrite)){
			do{
				index = array->indexWrite;
				nValCont =  index | (index << 32);
				BOOL_CAS(&array->indexWrite, index, nValCont);
			}while(validContent(array->indexWrite));
		}
	}
}

/**
 * Function that checks if the dequeue is blocked
*/
static inline unsigned long long validRead(unsigned long long index){
	index &= ~(1UL << 63);
	return !((index >> 32) & 0);
}

/**
 * Function that blocks the dequeue in the array setting the high part equal to the low part in indexWrite
*/
static inline void setUnvalidRead(bucket_t* bckt){
	unsigned long long  nValCont = -1;
	unsigned long long  index = -1;
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	do{
		index = array->indexRead;
		nValCont =  index | (index << 32);
		BOOL_CAS(&array->indexRead, index, nValCont);
	}while(validRead(array->indexRead));

	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(validRead(array->indexRead)){
			do{
				index = array->indexRead;
				nValCont =  index | (index << 32);
				BOOL_CAS(&array->indexRead, index, nValCont);
			}while(validRead(array->indexRead));
		}
	}
}

/**arrayNodes_t
 * Function that checks if the list is enabled or not
*/
static inline int unlinked(unsigned long long index){
	return !((index >> 63) & 0);
}

/**
 * Function that publish the ordered array of nodes
*/
static inline void setLinked(bucket_t* bckt){
	unsigned long long nValCont = -1;
	unsigned long long index = -1;
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	do{
		index = array->indexRead;
		nValCont = index | 1ULL << 63;
		BOOL_CAS(&array->indexRead, index, nValCont);
	}while(validRead(array->indexWrite));

	int actNuma;
	for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
		if(validRead(array->indexRead)){
			do{
				index = array->indexRead;
				nValCont = index | 1ULL << 63;
				BOOL_CAS(&array->indexRead, index, nValCont);
			}while(validRead(array->indexRead));
		}
	}
}

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
	*payload = array->nodes[position].ptr;
	*timestamp = array->nodes[position].timestamp;
}

static inline void blockEntriesArray(arrayNodes_t* array, unsigned int limit, unsigned int start){
	nodeElem_t* app = NULL;
	while(start <= limit){
		app = getNodeElem(array, start);
		if((((unsigned long long)array->nodes[start].ptr) & BLOCK) == 0)
			BOOL_CAS(&array->nodes[start].ptr, app, (((unsigned long long)app->ptr) & BLOCK));
		
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
	if(validContent(array->indexRead)){
		if(array->indexWrite >= array->length){
			setUnvalidContent(bckt);
		}
	}
	if(!validContent(array->indexRead) && unordered(bckt)){
		// First Phase: Block all used entries
		if(!isBlocked(array))
			blockEntriesArray(array, getFixed(array->indexWrite), 0);

		int actNuma;
		for (actNuma = 0; actNuma < bckt->numaNodes; actNuma++){
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
	}

	if(dequeueStop) return;

	if(!unordered(bckt) && unlinked(array->indexRead)){
		int attempts = MAX_ATTEMPTS;
		unsigned long long idx = -1;
		unsigned long long __status = -1;
		array = bckt->arrayOrdered;
		while(bckt->head.next == NULL){
			if(attempts > 0){
				// START TRANSACTION
				if((__status = _xbegin ()) == _XBEGIN_STARTED){
					idx = getDynamic(array->indexRead);
					bckt->head.next = (node_t*)array->nodes[idx].ptr;
					bckt->extractions = 0;
					TM_COMMIT();
				}else
					expBackoffTime(&testSleep, &maxSleep);
			}else{
				BOOL_CAS(&bckt->head.next, NULL, (node_t*)array->nodes[0].ptr);
			}
			attempts--;
		}
		setLinked(bckt);
	}
}

//#define array_safe_free(ptr) 			gc_free(ptst, ptr, gc_aid[GC_INTERNALS])
//#define array_unsafe_free(ptr) 			gc_unsafe_free(ptst, ptr, gc_aid[GC_INTERNALS])
//void array_safe_free(myArray_t* array);
//void array_unsafe_free(myArray_t* array);

/**
 * Function that insert a new node in the array
*/
int nodesInsert(bucket_t* bckt, int idxWrite, void* payload, pkey_t timestamp){
	int numaNode = getNumaNode(pthread_self(), bckt->numaNodes);
	arrayNodes_t* array = bckt->ptr_arrays[numaNode];
	int attempts = MAX_ATTEMPTS;
	int resRet = MYARRAY_ERROR;
	int __status = 0;
	while(attempts > 0 && resRet == MYARRAY_ERROR){
		// START TRANSACTION
		if((__status = _xbegin ()) == _XBEGIN_STARTED){
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
	if(attempts < 0){
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
		return MYARRAY_EXTRACT;
	} else{
		return MYARRAY_ERROR;
	}
}

#endif