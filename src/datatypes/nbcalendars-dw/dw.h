#ifndef DEFERRED_WORK
#define DEFERRED_WORK

#include <string.h>
#include "linked_list.h"

// stati bucket virtuale
#define INS (0ULL)
#define ORD (1ULL)
#define EXT (2ULL)
#define BLK (3ULL)

// stati di un nodo in dw
#define DELN (1ULL)
#define BLKN (2ULL)
#define MOVN (4ULL)

#define INV_TS (-1.0)

#define VEC_SIZE 	128

//#define BUCKET_STATE_MASK	 (0xeULL) 	// per lo stato del bucket(LSB 2,3,4)
#define BUCKET_STATE_MASK	 (6ULL) 	// per lo stato del bucket(LSB 2,3)
#define BUCKET_PTR_MASK 	~(0x7ULL)	// per il puntatore senza nessun stato o marcatura del bucket
#define BUCKET_PTR_MASK_WM	~(6ULL)		// per il puntatore del bucket mantenendo la marcatura

#define NODE_STATE_MASK		 (7ULL) 	// per lo stato del nodo
#define NODE_PTR_MASK 		~(7ULL)		// per il puntatore senza nessun stato del nodo

#define ENQ_BIT_SHIFT	16					// numero di bit da shiftare a destra per ottenere l'indice di inserimento
#define DEQ_IND_MASK (0x0000ffff)

#if NUMA_DW
int dw_enqueue(void*, unsigned long long, nbc_bucket_node*, int);
#else
int dw_enqueue(void*, unsigned long long, nbc_bucket_node*);
#endif
dwb* dw_dequeue(void*, unsigned long long);
void dw_block_table(void*, unsigned int);

#endif // DEFERRED_WORK
