#ifndef DEFERRED_WORK
#define DEFERRED_WORK

#include <string.h>

// configuration
#define VEC_SIZE 	128
#define DISABLE_EXTRACTION_FROM_DW	1
#define ENABLE_SORTING 				0
#define DW_USAGE_TH					0
#define ENABLE_PROACTIVE_FLUSH		0
#define ENABLE_BLOCKING_FLUSH		0
#define NUMA_DW 					1	// se 1 allora utilizza allocatore NUMA aware
#define SEL_DW						0	// se 1 allora lavoro differito solo se la destinazione si trova su un nodo numa remoto
#define NODE_HASH(bucket_id) (bucket_id % _NUMA_NODES)	// per bucket fisico
#define NID nid
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

#if NUMA_DW
#include "./gc/ptst.h"
#else
#include "../../gc/ptst.h"
#endif

#define BUCKET_STATE_MASK	 (0xcULL) 	// per lo stato del bucket(LSB 3, 4)
#define BUCKET_PTR_MASK 	~(0xfULL)	// per il puntatore senza nessuno stato o marcatura del bucket
#define BUCKET_PTR_MASK_WM	~(0xcULL)	// per il puntatore del bucket mantenendo la marcatura

#define NODE_PTR_MASK 		~(0xfULL)	// per il puntatore senza nessuno stato del nodo

#define BUCKET_STATE_SHIFT	2			// numero bit di shift dello stato 

#define ENQ_BIT_SHIFT	16				// numero di bit da shiftare a destra per ottenere l'indice di inserimento
#define DEQ_IND_MASK (0x0000ffff)		// per ottenere solo l'indice di estrazione

// Valori ritornati dalla funzione di estrazione
#define GOTO		-1.0
#define CONTINUE	-2.0 
#define EMPTY		-3.0

int dw_enqueue(
 void*, 
 unsigned long long, 
 nbc_bucket_node* 
#if NUMA_DW
 ,int
#endif
);

dwb* dw_dequeue(void *tb, unsigned long long index_vb);


void dw_block_table(void*, unsigned int);
pkey_t dw_extraction(dwb*, void**, pkey_t, bool, bool);

#endif // DEFERRED_WORK
