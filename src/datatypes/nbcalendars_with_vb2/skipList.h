/*
 * skipList.h
 * Author: Elad Gidron
 */

#ifndef SKIPLIST_H_
#define SKIPLIST_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include <time.h>

#include "../../key_type.h"
#include "../../gc/gc.h"
#include "../../gc/ptst.h"



#define REF_MASK (~(0x1))
#define TRUE_MARK 0x1
#define FALSE_MARK 0x0
#define LSB 0x1

typedef size_t markable_ref;

//#define CAS(ptr, oldval, newval) __sync_val_compare_and_swap(ptr, oldval, newval)

#define NEW_MARKED_REFERENCE(addr, mark) ((markable_ref)addr | mark)

#define GET_MARK(m_ref) ((int)(m_ref & LSB))
#define GET_REF(m_ref) ((void*)(m_ref & REF_MASK))

static inline void* get_mark_and_ref(markable_ref m_ref, int* mark)
{
	*mark = GET_MARK(m_ref);
	return GET_REF(m_ref);
}

//adds mark to a pointer
#define ADD_MARK(addr, mark) ((markable_ref)(GET_REF((markable_ref)addr)) | mark)

//atomic actions:
#define SET_ATOMIC_REF(ptr, newAddr, newMark) 	(*ptr = ADD_MARK(newAddr, newMark))

#define REF_CAS(ptr,oldaddr,newaddr,oldmark,newmark) (__sync_val_compare_and_swap(ptr, ADD_MARK(oldaddr, oldmark), ADD_MARK(newaddr, newmark)))


#ifndef	FALSE
#define	FALSE	(0)
#endif

#ifndef	TRUE
#define	TRUE	(!FALSE)
#endif

typedef unsigned int sl_key_t;

typedef struct listNode_t {
	sl_key_t key;
	intptr_t value;
	int topLevel;
	markable_ref next[];
} ListNode;

typedef struct skipList_t {
	ListNode *head;
	ListNode *tail;
} SkipList;


//__thread unsigned long nextr = 1;
__thread unsigned long nextr;
extern __thread  ptst_t *ptst; 

inline static long simRandom(void) {
    nextr = nextr * 1103515245 + 12345;
    return((unsigned)(nextr/65536));
}


//Max skiplist level
#define NUM_LEVELS 28
#define MAX_LEVEL  (NUM_LEVELS-1)
#define MIN_LEVEL  0

int gc_id[NUM_LEVELS];

/********** AUX FUNCTIONS *************/

inline static int randomLevel() {	
	unsigned val = simRandom();
	int ctr=0; 

	//not accurate because the probability of level==MAX_LEVEL is equal to level==MAX_LEVEL-1. Still reasonable.
	while( (val&1) && ctr<MAX_LEVEL){ //__builtin_ffs work slower.
		ctr++;
		val=val/2;
	}

	assert(ctr>=0 && ctr<=MAX_LEVEL);
	return ctr;
}

ListNode* makeNormalNode(sl_key_t key, int height, intptr_t value) {
	int i;
	ListNode *newNode = (ListNode*) 
	gc_alloc(ptst, gc_id[height]); 
	//malloc(sizeof (struct listNode_t) + (sizeof(markable_ref) * (height+1)));
	assert(newNode != NULL);
	newNode->key = key;
	newNode->value = value;

	for (i = 0; i <= height; i++)
		newNode->next[i] = (markable_ref)NULL;
	newNode->topLevel = height;

	return newNode;
}


/* creates a new skipList*/
void __init_skipList_subsystem() {

	// init fraser garbage collector/allocator 
	int i; 
	for(i=0;i<NUM_LEVELS;i++)  
		gc_id[i] = gc_add_allocator(sizeof (struct listNode_t) + (sizeof(markable_ref) * (i+1)));

	critical_enter();
	critical_exit();
	
	srand(time(NULL));
}

/* creates a new skipList*/
static SkipList* skipListInit() {
	int i;
	SkipList *newSkipList = (SkipList*) malloc(sizeof(SkipList));
	assert(newSkipList != NULL);
	newSkipList->head = makeNormalNode(0  , MAX_LEVEL, 0);   //makeSentinelNode(MIN);
	newSkipList->tail = makeNormalNode(UINT_MAX, MAX_LEVEL, 0); //makeSentinelNode(INFTY);

	for (i = 0; i <= MAX_LEVEL; i++) {
		newSkipList->head->next[i]
					= NEW_MARKED_REFERENCE(newSkipList->tail, FALSE_MARK);

	}
	return newSkipList;
}

/*
 * finds an element in the skiplist
 * returns all its predecessors and successors in the preds & succs arrarys.
 * Also makes sure the the first MAX_LEVEL*2 hazard pointers point to the elemesnts in preds & succs
 */
int skipListFind(SkipList *skipList, sl_key_t key, ListNode *preds[], ListNode *succs[]) {
	int marked, snip, level, retry;
	sl_key_t currkey;
	ListNode *pred = NULL, *curr = NULL, *succ = NULL;
	while (TRUE) {
		//retry:
		retry = FALSE;
		pred = skipList->head;
		for (level = MAX_LEVEL; level >= MIN_LEVEL; level--) {
			curr = (ListNode*)GET_REF(pred->next[level]);

			while (TRUE) {
				succ = (ListNode*)get_mark_and_ref(curr->next[level], &marked);
				while (marked) {

					snip
					= REF_CAS(&(pred->next[level]),curr,succ,FALSE_MARK,FALSE_MARK);

					if (!snip) {
						//goto retry
						retry = TRUE;
						break;
					}
					curr = (ListNode*)GET_REF(pred->next[level]);
					succ = (ListNode*)get_mark_and_ref(curr->next[level], &marked);
				}
				if (retry)
					break;
				//correction from Hazard pointers paper (fig. 9. line 21)
				currkey = curr->key;
				if (curr != GET_REF(pred->next[level])
						|| GET_MARK(pred->next[level])) {
					//goto retry
					retry = TRUE;
					break;
				}
				if (currkey < key) {
					pred = curr;
					curr = succ;
				} else {
					break;
				}
			}
			if (retry)
				break;
			preds[level] = pred;
			succs[level] = curr;
		}
		if (retry)
			continue;
		return (currkey == key);
	}
}

/*
 * adds key to the skiplist
 * returns 1 on success or 0 if the key was already in the skiplist.
 */
int skipListAdd(SkipList *skipList, sl_key_t key, intptr_t value) {
	int topLevel = randomLevel();
	int level;

	ListNode *preds[MAX_LEVEL + 1];
	ListNode *succs[MAX_LEVEL + 1]; 

	while (TRUE) {
		int found = skipListFind(skipList, key, preds, succs);

		if (found) return FALSE;
		else {
			ListNode *newNode = makeNormalNode(key, topLevel, value);
			for (level = MIN_LEVEL; level <= topLevel; level++) {
				ListNode *succ = succs[level];
				SET_ATOMIC_REF(&(newNode->next[level]), succ, FALSE_MARK);
			}
			ListNode *pred = preds[MIN_LEVEL];
			ListNode *succ = succs[MIN_LEVEL];
			if (  !(REF_CAS(&(pred->next[MIN_LEVEL]), succ, newNode, FALSE_MARK, FALSE_MARK))  ) {
				gc_free(ptst, newNode, topLevel);
				continue;
			}
			for (level = MIN_LEVEL + 1; level <= topLevel; level++) {
				while (TRUE) {
					pred = preds[level];
					succ = succs[level];
					//BUGFIX (Elad): For the case that succ changes when calling find in this loop
					if (succ != GET_REF(newNode->next[level])) {
						int currMark;
						ListNode *newNodeSucc = (ListNode*)get_mark_and_ref(newNode->next[level], &currMark);
						if (currMark)
							break;
						if (!REF_CAS(&(newNode->next[level]), newNodeSucc, succ, FALSE_MARK, FALSE_MARK)) {
							continue;
						}
					}

					if (REF_CAS(&(pred->next[level]),succ,newNode,FALSE_MARK,FALSE_MARK)) {
						break;
					}
					skipListFind(skipList, key, preds, succs);
				}
			}
			return TRUE;
		}
	}
}

/*
 * removes a key from the skiplist
 * returns 1 on success or 0 if the key wasn't in the skiplist.
 */
int skipListRemove(SkipList *skipList, sl_key_t key) {
	int level;
	
	ListNode *preds[MAX_LEVEL + 1];
	ListNode *succs[MAX_LEVEL + 1]; 
	ListNode *succ;

	while (TRUE) {
		int found = skipListFind(skipList, key, preds, succs);
	
		if (!found) return FALSE;
		else {
			ListNode *nodeToRemove = succs[MIN_LEVEL];

			for (level = nodeToRemove->topLevel; level >= MIN_LEVEL + 1; level--) {
				int marked = FALSE;
				succ = (ListNode*)get_mark_and_ref(nodeToRemove->next[level], &marked);
				while (!marked) {
					REF_CAS(&(nodeToRemove->next[level]), succ, succ, FALSE_MARK, TRUE_MARK);
					succ = (ListNode*)get_mark_and_ref(nodeToRemove->next[level], &marked);
				}
			}
			int marked = FALSE;
			succ = (ListNode*)get_mark_and_ref(nodeToRemove->next[MIN_LEVEL], &marked);
			while (TRUE) {
				int
				iMarkedIt =
						REF_CAS(&(nodeToRemove->next[MIN_LEVEL]), succ, succ, FALSE_MARK, TRUE_MARK);
				succ = (ListNode*)get_mark_and_ref(succs[MIN_LEVEL]->next[MIN_LEVEL], &marked);
				if (iMarkedIt) {
					skipListFind(skipList, nodeToRemove->key, preds, succs);
					return TRUE;
				} 
				else if (marked) return FALSE;
			}
		}
	}
}

/*
 * finds a key in the skiplist.
 * returns 1 if the key was in the skiplist or 0 if it wasn't.
 * in any case pValue is holding this or previous key value or NULL
 */
int skipListContains(SkipList *skipList, sl_key_t key, intptr_t* pValue) {
	int marked = 0, level;
	*pValue = 0;		// initialize for the case the key is the non-existing minimal 

	while (TRUE) {
		ListNode *pred = skipList->head, *curr = NULL, *succ = NULL;

		for (level = MAX_LEVEL; level >= MIN_LEVEL; level--) {
			curr = (ListNode*)GET_REF(pred->next[level]);
			while (TRUE) {
				succ = (ListNode*)get_mark_and_ref(curr->next[level], &marked);
				while (marked) {
					curr = (ListNode*)GET_REF(curr->next[level]);
					succ = (ListNode*)get_mark_and_ref(curr->next[level], &marked);
				}
				if (curr->key < key) {
					*pValue = curr->value;
					pred = curr;
					curr = succ;
				} else {
					break;
				}
			}

		} // end of the for loop going over the levels

		return (curr->key == key);
	} // end of the while loop

}

/*Destroy the skiplist*/
void skipListDestroy(SkipList *sl) {
	return;
	/*
	ListNode curr = sl->head;
	ListNode next;

	while (curr != NULL) {
		next = (ListNode)GET_REF(curr->next[MIN_LEVEL]);
		//freeListNode(curr);
		free(node);

		curr = next;
	}
	free(sl);
	*/
}

#endif /* SKIPLIST_H_ */

