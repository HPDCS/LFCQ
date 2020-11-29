#ifndef MYARRAY_H
#define MYARRAY_H

// Maybe already included by others .h files
#include <stdbool.h>
#include <strings.h>
#include <assert.h>
#include <immintrin.h>
#include "./common_f.h"
#include "./vbucket_array.h"


#define SM_DEQUEUE 1ULL
#define SM_ENQUEUE 2ULL
#define SM_DEQUEUE_ARRAY_FINISHED 3ULL

#define MYARRAY_ERROR -1;
#define MYARRAY_EXTRACT 0;
#define MYARRAY_INSERT 1;

#define MAX-ATTEMPTS 5;

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
} arrayNodes_t;

/**
 * Function that implements the logic of flag changes
*/
void stateMachine(bucket_t* bckt, unsigned long dequeueStop){
	if(validContent(bckt->indexRead){
		if(bckt->indexWrite >= bckt->array->length){
			setUnvalidContent(bckt);
		}
	}
	if(!validContent(bckt->indexRead) && unordered(bckt->indexRead)){
		unsigned int idxWrite = 0;
		unsigned int limit = bckt->indexWrite >> 32;
		nodeElem_t* app = NULL;
		while(idxWrite <= limit){
			app = getNodeElem(bckt->array, idxWrite);
			if(bckt->array->nodes[idxWrite]->ptr & BLOCK == 0){
				setCAS(bckt->array->nodes[idxWrite].ptr, app, app->ptr & BLOCK);
			}
			idxWrite++;
		}
		arrayNodes_t*  newArray = vectorOrderingAndBuild(bucket);
		if(newArray != NULL){
			setOrdered(bckt, newArray);
		}
		if(bckt->arrayOrdered != newArray)
			free(newArray);
	}

	if(dequeueStop) return;

	if(!unordered(bckt) && unlined(bckt->indexRead)){
		int attempts = MAX-ATTEMPTS;
		unsigned long long idx = -1;
		while(bckt->head == NULL){
			if(attempts > 0){
				idx = bckt->indexRead;
				// START TRANSACTION
				if((__status = _xbegin ()) == _XBEGIN_STARTED){
					bckt->head = bckt->array->nodes[idx];
					bckt->indexRead = bckt->indexRead - idx;
					TM_COMMIT();
				}else
					expBackoffTime(&testSleep, &maxSleep);
			}else{
				BOOL_CAS(bckt->head, NULL, bckt->array->nodes[0].ptr);
			}
			attempts--;
		}
		setLinked(bckt);
	}
}

/**
 * Function that implements the logic to ordering the array and build the list
*/
arrayNodes_t* vectorOrderingAndBuild(bucket_t* bckt){
	unsigned long long elmToOrder = bckt->indexWrite >> 32;
	arrayNodes_t* newArray = initArray(bckt->array->length);
	copyArray(bckt->array->nodes, elmToOrder, newArray);
	if(!unordered(bckt)){
		free(newArray);
		return NULL;
	}
	// TODO: Manca l'implementazione di sort
	sort(newArray, elmToOrder);
	// toList also allocate the node elem
	toList(newArray, memNodes, elmToOrder);

	return newArray;
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
static inline void toList(arrayNodes_t* array, unsigned long long limit, arrayNodes_t* newArray){
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
 * Function that checks if the index passed indicates that the array insert is still valid
*/
static inline int validContent(unsigned long long index){
	return !((index >> 32) & 0);
}

/**
 * Function that blocks the insert in the array setting the high part equal to the low part in indexWrite
*/
static inline int setUnvalidContent(bucket_t* bckt){
	unsigned long long nValCont = -1;
	unsigned long long index = -1;
	do{
		index = bckt->indexWrite;
		nValCont =  index | (index << 32);
		BOOL_CAS(&bckt->indexWrite, index, nValCont);
	}while(validContent(bckt->indexWrite));
}

/**
 * Function that checks if the dequeue is blocked
*/
static inline int validRead(unsigned long long index){
	index &= ~(1UL << 63);
	return !((index >> 32) & 0);
}

/**
 * Function that blocks the dequeue in the array setting the high part equal to the low part in indexWrite
*/
static inline int setUnvalidRead(bucket_t* bckt){
	unsigned long long  nValCont = -1;
	unsigned long long  index = -1;
	do{
		index = bckt->indexRead;
		nValCont =  index | (index << 32);
		BOOL_CAS(&bckt->indexRead, index, nValCont);
	}while(validRead(bckt->indexWrite));
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
static inline int setOrdered(bucket_t* bckt, arrayNodes_t* newArray){
	do{
		BOOL_CAS(&bckt->arrayOrdered, NULL, newArray);
	}while(unordered(bckt));
}

/**
 * Function that checks if the list is enabled or not
*/
static inline int unlined(unsigned long long index){
	return !((index >> 63) & 0);
}

/**
 * Function that publish the ordered array of nodes
*/
static inline int setLinked(bucket_t* bckt){
	unsigned long long nValCont = -1;
	unsigned long long index = -1;
	do{
		index = bckt->indexRead;
		nValCont = index | 1ULL << 63;
		BOOL_CAS(&bckt->indexRead, index, nValCont);
	}while(validRead(bckt->indexWrite));
}

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

	//array->nodes = gc_alloc(ptst, gc_aid[GC_INTERNALS]);
	array->nodes = (node_t**)malloc(sizeof(node_t*)*array->length);
	assert(array->nodes != NULL);
	bzero(array->nodes, sizeof(node_t)*array->length);

	return array;
}

//#define array_safe_free(ptr) 			gc_free(ptst, ptr, gc_aid[GC_INTERNALS])
//#define array_unsafe_free(ptr) 			gc_unsafe_free(ptst, ptr, gc_aid[GC_INTERNALS])
//void array_safe_free(myArray_t* array);
//void array_unsafe_free(myArray_t* array);

/**
 * To write in the array
*/
int set(arrayNodes_t* array, int position, void* payload, pkey_t timestamp){
		array->nodes[position].ptr = payload;
		array->nodes[position].timestamp = timestamp;
		if(array->nodes[position].ptr == payload)
			return MYARRAY_INSERT;
		else return MYARRAY_ERROR;
}

/**
 * To write in the array using CAS
*/
int setCAS(arrayNodes_t* array, int position, void* payload, pkey_t timestamp){
	int res = BOOL_CAS(&array->nodes[position].ptr, NULL, payload);
	if(res){
		array->nodes[position].timestamp = timestamp;
		return MYARRAY_INSERT;
	}
	else return MYARRAY_ERROR;
}

/**
 * To read from the array
*/
void get(arrayNodes_t* array, int position, void* payload, pkey_t* timestamp){
	*payload = array->nodes[position].ptr;
	*timestamp = array->nodes[position].timestamp;
}

/**
 * To read from the array return nodeElem_t
*/
nodeElem_t* getNodeElem(arrayNodes_t* array, int position){
	return array->nodes[position];
}

/**
 * Function that insert a new node in the array
*/
int nodesInsert(arrayNodes_t* array, int idxWrite, void* payload, pkey_t timestamp){
	int attempts = MAX-ATTEMPTS;
	int resRet = MYARRAY_ERROR;
	while(attempts > 0 && resRet = MYARRAY_ERROR){
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
		while(array->nodes[position].ptr == NULL){
			resRet = setCAS(&array->nodes[position], NULL, payload, timestamp);
		}
	}
	return resRet;
}

/**
 * Function that extract a new node in the array
*/
int nodesDequeue(arrayNodes_t* array, int idxRead, void* payload, pkey_t* timestamp){
	if(idxRead < array->length){
		get(array, idxRead, payload, timestamp);
	}
}

#endif
