#ifndef DEFERRED_WORK
#define DEFERRED_WORK

#include <stddef.h>
#include <string.h>
#include "linked_list.h"

#define DW_USAGE 1
#define DW_AUDIT if(0)

// stati dw
#define INS (0ULL)
#define ORD (1ULL)
#define EXT (2ULL)
#define BLK (3ULL)
//#define END (4ULL)

// stati nodo in dw
#define DELN (1ULL)
#define BLKN (2ULL)

#define DW_PTR				(0xfffffffffffffff0)    // vengono utilizzati gli ultimi 4 bit per contenere delle info(3bit di stato array | 1 stato del nodo nella lista)
#define DW_PTR_LIST_STATE	(0xfffffffffffffff1)	// per pulire completamente lo stato DW senza toccare un eventuale marcatura
#define DW_STATE			(0x000000000000000e)	// per prendere solo lo stato riferito a DW

#define DW_NODE_PTR			(0xfffffffffffffffc)	// per togliere solo gli ultimi due bit, quindi BLK o DEL

#define VEC_SIZE 10

#define FETCH_AND_ADD 				__sync_fetch_and_add
#define ADD_AND_FETCH				__sync_add_and_fetch
#define SUB_AND_FETCH				__sync_sub_and_fetch
#define FETCH_AND_SUB				__sync_fetch_and_sub

#define DW_SET_STATE(dwnp, state) \
		(((unsigned long long)dwnp & DW_PTR_LIST_STATE) | (state << 1))	// aggiunge lo stato passato come parametro al puntatore senza sovrascrivere un eventuale bit di marcatura

#define DW_GET_STATE(dwnp) (((unsigned long long)dwnp & DW_STATE) >> 1)	// ritorna uno tra INS, ORD, EXT, BLK


#define DW_GET_PTR(dwnp) ((dwn*)((unsigned long long)dwnp & DW_PTR))
#define DW_GET_NODE_PTR(node)	((nbnc*)((unsigned long long)node & DW_NODE_PTR))

#define DW_NODE_IS_BLK(node) 	((bool)((unsigned long long)node & BLKN))	// verifica se un elemeno dell'array dw è bloccato	
#define DW_NODE_IS_DEL(node) 	((bool)((unsigned long long)node & DELN))	// verifica se un elemeno dell'array dw è stato estratto
		

extern int cmp_node(const void *, const void *);
extern dwn* dw_dequeue(void*, unsigned long long);
extern int dw_enqueue(void*, nbc_bucket_node*, unsigned long long);
extern void dw_block_table(void*, unsigned int);
extern void insertionSort(nbnc**, int);

#endif // DEFERRED_WORK