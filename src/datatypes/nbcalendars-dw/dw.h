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
#define VALN (0ULL)
#define DELN (1ULL)
#define BLKN (2ULL)

#define DW_PTR				(0xfffffffffffffff0)// vengono utilizzati gli ultimi 4 bit per contenere delle info(3bit di stato array | 1 stato del nodo nella lista)
#define DW_PTR_LIST_STATE	(0xfffffffffffffff1)
#define DW_STATE			(0x000000000000000e)

#define DW_NODE_PTR			(0xfffffffffffffffc)
#define DW_NODE_DEL			(0x0000000000000001)
#define DW_NODE_BLK	    	(0x0000000000000002)
#define DW_NODE_BLK_MASK 	(0xfffffffffffffffd)

#define VEC_SIZE 10

#define FETCH_AND_ADD 				__sync_fetch_and_add
#define ADD_AND_FETCH				__sync_add_and_fetch
#define SUB_AND_FETCH				__sync_sub_and_fetch
#define FETCH_AND_SUB				__sync_fetch_and_sub

#define DW_SET_STATE(dwnp, state) \
		(((unsigned long long)dwnp & DW_PTR_LIST_STATE) | (state << 1))

#define DW_GET_STATE(dwnp) (((unsigned long long)dwnp & DW_STATE) >> 1)
#define DW_GET_PTR(dwnp) ((dwn*)((unsigned long long)dwnp & DW_PTR))

#define DW_GET_NODE_PTR(node)	((nbc_bucket_node*)((unsigned long long)node & DW_NODE_PTR))
#define DW_NODE_IS_BLK(node) 	((nbc_bucket_node*)((unsigned long long)node & DW_NODE_BLK))		
#define DW_NODE_IS_DEL(node) 	((nbc_bucket_node*)((unsigned long long)node & DW_NODE_DEL))
		

extern int cmp_node(const void *, const void *);
extern dwn* dw_dequeue(void*, unsigned long long);
extern int dw_enqueue(void*, nbc_bucket_node*, unsigned long long);
extern void dw_block_table(void*, unsigned int);

#endif // DEFERRED_WORK