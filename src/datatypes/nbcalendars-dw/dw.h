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
#define NONE (0ULL)
#define DELN (1ULL)
#define BLKN (2ULL)
#define MVGN (4ULL)
#define MVDN (8ULL)

// Stati testa di una lista
#define DELH (0x1ULL)	// segnala possibile eliminazione(marcatura, ultimo bit)
#define MOVH (0x1ULL)	// applicato alla testa per segnalare l'inizio di resize(stato, com se fosse stato del bucket, penultimo bit)

#define INV_TS (-1.0)

#define VEC_SIZE 	128

//#define BUCKET_STATE_MASK	 (0xeULL) 	// per lo stato del bucket(LSB 2,3,4)
#define BUCKET_STATE_MASK	 (6ULL) 	// per lo stato del bucket(LSB 2,3)
#define BUCKET_PTR_MASK 	~(0x7ULL)	// per il puntatore senza nessun stato o marcatura del bucket
#define BUCKET_PTR_MASK_WM	~(6ULL)		// per il puntatore del bucket mantenendo la marcatura

#define NODE_STATE_MASK		 (0xfULL) 	// per lo stato del nodo
#define NODE_PTR_MASK 		~(0xfULL)		// per il puntatore senza nessun stato del nodo

#define ENQ_BIT_SHIFT	16					// numero di bit da shiftare a destra per ottenere l'indice di inserimento
#define DEQ_IND_MASK (0x0000ffff)

// Valori ritornati dalla funzione di estrazione
#define GOTO		-1.0
#define CONTINUE	-2.0 
#define EMPTY		-3.0

#if NUMA_DW
int dw_enqueue(void*, unsigned long long, nbc_bucket_node*, int);
#else
int dw_enqueue(void*, unsigned long long, nbc_bucket_node*);
#endif
dwb* dw_dequeue(void*, unsigned long long);
void dw_block_table(void*, unsigned int);
pkey_t dw_extraction(dwb*, void**, pkey_t, bool, bool);

#endif // DEFERRED_WORK
