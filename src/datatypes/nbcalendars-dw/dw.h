#ifndef DEFERRED_WORK
#define DEFERRED_WORK

//#include <stddef.h>
#include <string.h>
#include "linked_list.h"

// stati dw
#define INS (0ULL)
#define ORD (1ULL)
#define EXT (2ULL)
#define BLK (3ULL)

// stati di un nodo in dw
#define DELN (1ULL)
#define BLKN (2ULL)

#define VEC_SIZE 	10
#define DIM_TH		16000

#define BUCKET_STATE_MASK	(6ULL) 				// per lo stato del bucket
#define BUCKET_MARK_MASK	(1ULL) 				// per la marcatura del bucket
#define BUCKET_PTR_MASK	(0xfffffffffffffff8)	// per il puntatore del bucket
#define BUCKET_PTR_MASK_WM	(0xfffffffffffffff9)	// per il puntatore del bucket

#define NODE_PTR_MASK	(0xfffffffffffffffc)
#define NODE_STATE_MASK	(3ULL) 					// per lo stato del nodo

#define ENQ_BIT_SHIFT	16					// numero di bit da shiftare a destra per ottenere l'indice di inserimento
#define DEQ_IND_MASK (0x0000ffff)

int dw_enqueue(void*, unsigned long long, nbc_bucket_node*);
dwb* dw_dequeue(void*, unsigned long long);

#endif // DEFERRED_WORK