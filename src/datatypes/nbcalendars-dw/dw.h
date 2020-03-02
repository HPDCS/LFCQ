#ifndef DEFERRED_WORK
#define DEFERRED_WORK

#include <string.h>
#include "linked_list.h"

// Stati bucket virtuale
#define INS (0ULL)
#define ORD (1ULL)
#define EXT (2ULL)
#define BLK (3ULL)

// Marcature di un nodo in dw
#define NONE (0ULL)
#define DELN (1ULL)
#define BLKN (2ULL)
#define MVGN (4ULL)
#define MVDN (8ULL)

// Marcature di un bucket
#define DELB (1ULL)	// possibile eliminazione
#define MOVB (2ULL)	// bucket in movimento

#define INV_TS 		(-1.0)
#define VEC_SIZE 	128

#define BUCKET_STATE_MASK	 (0xcULL) 	// per lo stato del bucket(LSB 3, 4)
#define BUCKET_PTR_MASK 	~(0xfULL)	// per il puntatore senza nessuno stato o marcatura del bucket
#define BUCKET_PTR_MASK_WM	~(0xcULL)	// per il puntatore del bucket mantenendo la marcatura

#define NODE_PTR_MASK 		~(0xfULL)	// per il puntatore senza nessuno stato del nodo

#define BUCKET_STATE_SHIFT	2			// numero bit di shift dello stato 

#define ENQ_BIT_SHIFT	16				// numero di bit da shiftare a destra per ottenere l'indice di inserimento
#define DEQ_IND_MASK (0x0000ffff)		// per ottenere solo l'indice di estrazione
#define MAXIMUM_NUMBER_OF_THREADS 1024

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
