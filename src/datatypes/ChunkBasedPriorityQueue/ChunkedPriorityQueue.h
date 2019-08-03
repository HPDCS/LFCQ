//
//  ChunkedPriorityQueue.h
//  ChunkedSkipList
//
//  Authors: Nachshon Cohen, Anastasia Braginsky.
//  Chunked Priority Queue, the description appears in the paper:
//  "CBPQ: High Performance Lock-Free Priority Queue",
//  Anastasia Braginsky, Nachshon Cohen and Erez Petrank, EuroPar'16
//
//  The Software is provided WITHOUT ANY WARRANTY, EXPRESS OR IMPLIED.

#ifndef __ChunkedSkipList__ChunkedPriorityQueue__
#define __ChunkedSkipList__ChunkedPriorityQueue__

#include <iostream>
#include <assert.h>
#include <limits.h>

#include "Chunk.h"
#include "skipList.h"


extern "C" {
#include "../../gc/gc.h"
#include "../../gc/ptst.h"
}


extern __thread  ptst_t *ptst; 
extern int gc_ck;

using namespace std;

struct privateThreadInfo;                   // forward declaration
typedef struct privateThreadInfo ThrInf;

//__thread Chunk local_arr[ALLOCCACHE/20];
//__thread int local_cache_usage = 0;
/*************************************** CONSTANT DEFINITIONS **********************************************/
#define ALLOCCACHE 		30000000             // static allocation of 3,000,000 chunks
#define REDUCE_CACHE		250
                                                                                               
extern __thread Chunk __local_arr[];                                                       
extern __thread int local_cache_usage;

#ifdef FREEZE_64
#define DIRTY_EXIST 	0x8000000000000000ull
#define SET_WORD	0xFFFFFFFFFFFFFFFFULL
#else
#define DIRTY_EXIST 	0x100000000ull	// for 32 bits word
#define SET_WORD	0xFFFFFFFF
#endif	// FREEZE_64

#if KEY_TYPE == INT || KEY_TYPE == FLOAT
#define ELM_TAKEN           0x80000000ull	// MSB set for 32 bits word, key must take less then 31 bit
#define NO_ELM_TAKEN        0x7fffffffull   // MSB reset, all the rest set
#endif


#if KEY_TYPE == LONG || KEY_TYPE == DOUBLE
#define ELM_TAKEN           0x8000000000000000ull	// MSB set for 32 bits word, key must take less then 31 bit
#define NO_ELM_TAKEN        0x7fffffffffffffffull   // MSB reset, all the rest set
#endif


const int SPLIT_CHUNKS_NUM	= 2;            // number of new chunks upon a split of a chunk

const int SKIP_ENTRIES_IN_BUFFER = 20;      // skip entries in buffer chunk to avoid false sharing
/************************************************************************************************/


typedef enum FreezeTrigger_t 		
{								
	INSERT_FULL 			= 1,		
	INSERT_FIRST_HEAD 		= 2, 		
	INSERT_FIRST_BUFFER 	= 3, 		
	INSERT_FIRST_HEAD_HELP	= 4, 		
	DELETE_MIN 				= 5,
	FREEZE_RECOVERY 		= 6,
	FIRST_RECOVERY_BUFFER	= 7,
	FIRST_RECOVERY_MORE		= 8
} FreezeTrigger;


/************************************************************************************************/
/* ------ CLASS: ChunkedPriorityQueue defines the entire PQ, which manages a linked list of ------ */
/* ------ chunks and their memory management. Deletions are allowed only from first chunk.  ------ */
class ChunkedPriorityQueue
{
#ifdef DEBUG
public:
#endif

	/************************************************************************************************/
#ifdef FLAG_RELEASE
	volatile int flag ; // 0 for insert, 1 for delete
	volatile int count;
#endif //FLAG_RELEASE
	volatile Chunk *head;	// pointer to the first chunk
	volatile SkipList sl;	// pointer to the skip list

	/* --------------- Memory Management structures and methods - start --------------- */
	Chunk arr[ALLOCCACHE];
	int freecnt; 			// from where to start next allocation (initialized to zero by constructor)

	Chunk* alloc(){
		//return (Chunk*) gc_alloc(ptst, gc_ck);

		int val = local_cache_usage;
		if(val >= REDUCE_CACHE){
			int val = atomicInc(&freecnt, 1);
			if(val>=ALLOCCACHE){
				if(val==ALLOCCACHE) printf("Cache finita %llu\n", sizeof(Chunk));
				Chunk* res = (Chunk*) 
				//	malloc(sizeof(Chunk));// 
					gc_alloc(ptst, gc_ck);
				if(!res){
					printf(" Statically allocated %d chunks were not enough :-(\n", ALLOCCACHE);
					assert(0);
				}
				memset(res, 0, sizeof(Chunk));
				return res;
			}
//			memset(&arr[val], 0, sizeof(Chunk));	// zero the space for the chunks (long operation)
		
			return &arr[val];
		}
//		memset(&__local_arr[val], 0, sizeof(Chunk));
		local_cache_usage++;
		return &__local_arr[val];
	}
	void free(Chunk *c) {
		gc_free(ptst, c, gc_ck);
	}
	/* --------------- Memory Management structures and methods - end ----------------- */
	/************************************************************************************************/

	//------- ChunkedPriorityQueue object methods -------
	void freezeChunk(Chunk *curr, pkey_t *sortedval, int *len,		// freezes chunk, returns the sorted values of
			FreezeTrigger ft, ThrInf* t);              // the chunk
	void readFrozenChunk(Chunk *curr, pkey_t *sortedval,			// creates a local sorted array of the frozen
			int *len);                             // entries from already frozen chunk
	void readAndFreezeValues(Chunk *curr, int frozenIdx,        // freeze the values of the chunk, and return them
			pkey_t *sortedval, int *len,          // sorted in a local array
			bool isFirstChunk);
	Chunk* split(Chunk *curr, Chunk *prev, pkey_t *svals, int len,	// splits a chunk into SPLIT_CHUNKS_NUM, gets the
			Chunk** nextCreated);                          // list of sorted value in the chunk
	void freezeRecovery(ThrInf* t, Chunk *curr, Chunk *prev,    // recovers any frozen chunk from its existence
			pkey_t *svals, int len,                    // in the PQ list
			FreezeTrigger ft);
	Chunk* merge(Chunk *next, pkey_t svals[], int len,             // takes merged values of the frozen chunks and
			Chunk** nextCreated);                          // creates a first chunk, and an internal chunk
	Chunk* recoverFirstChunk(Chunk *curr, pkey_t svals[], int len,	// merges frozen first chunk with buffer and
			Chunk** nextFrozen,                // second chunk if needed
			Chunk** nextNextFrozen,
			Chunk** nextCreated, ThrInf* t);
	bool insert_to_first_chunk( ThrInf* t, pkey_t key,             // inserts a key into the buffer instead of first
			Chunk *currhead);
	bool create_insert_buffer(pkey_t key, Chunk *currhead,         // buffer creation and attachment
			Chunk **curbuf);
	void fill_chunk(pkey_t *svals, int len, int start,             // updates chunks values from the given array
			int pivotIdx, Chunk *c);                    // on split

	bool recursiveRecovery(ThrInf* t, Chunk** prev, Chunk* curr);
	void debugPrintLoopInInsert( ThrInf* t, Chunk* prevPrev, Chunk* prevCurr,
			pkey_t key, Chunk* prev, Chunk* curr, int iter, int r);
 
	/*******************************************************************************************************/
	int getChunk (Chunk** cur, Chunk** prev, pkey_t key){         // search the needed (according to the key) chunk
		//Chunk* frozen   = NULL;
		*cur = head; *prev = NULL;

#ifdef SKIP_LIST
		int res = skipListContains(sl, key, (intptr_t*)cur);    // first try to get the chunk through the skip-list
		if ( res == 0 ) {                                       // the key wasn't explicitly found in the skip-list,
			if (*cur == NULL) {                                 // continue linear search from cur.
				*cur = head; *prev = NULL;                      // But if the pointer to the chunk was nullified
			}                                                   // it is maybe about a key in the first chunk
			// Some chunk was found in the skip-list with the closest (higher) key, and if this chunk is frozen,
			else if (  (*cur)->meta.status.isFrozen()  ) {      // remove it in order not to follow a frozen chain,
				skipListRemove(sl, (*cur)->meta.max);
				*cur = head; *prev = NULL;                      // reset the cur to head
			}
		} else {                                                // the key was explicitly found;
			if ((*cur) && (*cur)->meta.status.isFrozen())
				skipListRemove(sl, (*cur)->meta.max);
			*cur = head; *prev = NULL;                          // search from the head in order to find the prev
		}                                                       // (should be rare case)

#endif //SKIP_LIST

		DEB(int iter=0);

		while(key > (*cur)->meta.max){
			DEB(assert(iter++<1000000));
			*prev = *cur;
			*cur = (*cur)->meta.next;

			intptr_t tmp = (intptr_t)(*cur);                    // code for removing the deleted bit from pointer
			if (tmp & DELETED_PTR)	{                           // if deleted bit is set just clear it
				tmp = (tmp & ~(DELETED_PTR) );
				*cur = (Chunk*)tmp;
			}

		}

		_assert( (*cur)!=NULL );
		return res;
	}
	/*******************************************************************************************************/

	Chunk* getChunk (Chunk** cur, Chunk** prev){			// search the chunk previous to the needed chunk
		Chunk* c = head; 									// the needed chunk is given by cur pointing to
		*prev = NULL;										// it
		DEB(int iter=0);

		while( c && c!=*cur ){
			_assert(iter++<100000);

			*prev = c;
			c = c->clearDelNext();							// if deleted bit is set just clear it
		}
		return c;
	}
	/*******************************************************************************************************/
	void getChunkDebug (Chunk** cur, Chunk** prev, pkey_t key){         // search the needed (according to the key) chunk
		int iter = 0;
		*cur = head; *prev = NULL;

		while(key > (*cur)->meta.max){
			assert(iter++<1000000);
			*prev = *cur;
			*cur = (*cur)->meta.next;

			intptr_t tmp = (intptr_t)(*cur);                    // code for removing the deleted bit from pointer
			if (tmp & DELETED_PTR)	{                           // if deleted bit is set just clear it
				tmp = (tmp & ~(DELETED_PTR) );
				*cur = (Chunk*)tmp;
			}
		}   // end of the while
	}

	/*******************************************************************************************************/

public:
	ChunkedPriorityQueue(){								// ChunkedPriorityQueue constructor
		freecnt = 0;									// allocator counter initialization
		//memset(this->arr, 0, sizeof(Chunk)*ALLOCCACHE);	// zero the space for the chunks (long operation)
		//flag=0; count=0;
		Chunk *second = alloc(), *first=alloc();		// local pointers to first and second chunks
		first->init(MIN_VAL, second, DELETE, MAX_IDX);	// initialize PQ so that first deletion immediately
		second->init(MAX_VAL, NULL, INSERT, MIN_IDX);	// causes freeze and all inserts go to second chunk
		head = first;									// make local pointers global
		sl = skipListInit();							// initialize the skip list and make it point on 
		DEB(int res1 =) skipListAdd(sl, MIN_VAL, (intptr_t)first); // two newly created chunks
		DEB(int res2 =) skipListAdd(sl, MAX_VAL, (intptr_t)second);
		PRINT2("*************** From constructor first chunk:%p, second chunk: %p (MAX_IDX=%d, MAX_VAL=%d, "
				"CHUNK_SIZE=%d) ***************\n",
				first, second, MAX_IDX, MAX_VAL, CHUNK_SIZE);
		_assert(res1 && res2);
	}

	~ChunkedPriorityQueue(){							// ChunkedPriorityQueue destructor	
		//skipListDestroy(sl);
	}

	void insert(pkey_t key, ThrInf* t);				// the interface for inserting a key
	pkey_t  delmin(ThrInf* t);							// the interface for deleting the min value
	void print();

	void assertStructure(){
		DEB(
				Chunk *first=head;
		int max = first->meta.max;
		Chunk *cur = first->clearDelNext();
		int iter=0;
		while(cur!=NULL){
			int curmax = cur->meta.max;

			if (curmax < max) {
				printf("In iteration %d previous max is %d and current max is %d\n", iter, max, curmax);
			}

			_assert(curmax>=max);
			_assert(curmax!=0);
			max = curmax;
			_assert(max>0);
			iter++;
			cur=cur->clearDelNext();
		}
		)
	}

	void compareStructure(int* values, int length){
		DEB(

				// PRINT("Printing the debugging structure:\n");
				// for (int i = 0; i<length; i++) {
				// if (values[i] == 0) continue;
				// PRINT("k:%d,cnt:%d; ", i, values[i]);
				// }
				// PRINT("\n");

				for (int key=0; key<length; ++key) {

					if (values[key] == 0) continue;
					Chunk *cur = head;
					int cnt = 0;

					while ((cur!=NULL) && (key > cur->meta.max))
						cur=cur->clearDelNext();

					cnt = cur->countForDebug(key);

					if (cur->clearDelNext())
						cnt += cur->clearDelNext()->countForDebug(key);

					if (cnt != values[key]) {
						PRINT(	"\nInconsistency between PQ (%d of %d) and debug structure (%d of %d). Number"
								" of times found in appropriate chunk: %d, in next chunk: %d\n",
								cnt, key, values[key], key, cur->countForDebug(key),
								cur->clearDelNext()->countForDebug(key));
					}

					_assert(cnt == values[key]);

				} // end of for every value
		) // end of DEBUG
	}
};



#endif /* defined(__ChunkedSkipList__ChunkedPriorityQueue__) */
