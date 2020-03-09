#ifndef DEFERRED_WORK
#define DEFERRED_WORK

#include <string.h>

#ifndef NUMA_DW
#define NUMA_DW 					0	// se 1 allora utilizza allocatore NUMA aware
#endif

// configuration
#define VEC_SIZE                    128
#define DISABLE_EXTRACTION_FROM_DW  1	// disabilita le estrazioni dirette dall dwq
#define ENABLE_SORTING              0   // abilita il sorting per le dwq
#define DW_USAGE_TH                 0	// setta il numero di elementi minimo per abilitare le dwq
#define ENABLE_PROACTIVE_FLUSH      0   // abilita il flush proattivo
#define ENABLE_BLOCKING_FLUSH       0	// abilita il lock per flushare elementi della dwq sui bucket della cq
#define SEL_DW                      0	// se 1 allora lavoro differito solo se la destinazione si trova su un nodo numa remoto
#define NODE_HASH(bucket_id)        ((bucket_id) % _NUMA_NODES)	// per bucket fisico
#define NID                         nid


// Stati bucket virtuale
#define INS 	(0ULL)
#define ORD 	(1ULL)
#define EXT 	(2ULL)
#define BLK 	(3ULL)

// Marcature di un nodo in dw
#define NONE 	(0ULL)
#define DELN 	(1ULL)
#define BLKN 	(2ULL)
#define MVGN 	(4ULL)
#define MVDN 	(8ULL)

// Marcature di un bucket
#define DELB 	(1ULL)	// possibile eliminazione
#define MOVB 	(2ULL)	// bucket in movimento

#define INV_TS 	(-1.0)

#if NUMA_DW
#include "../nbcalendars-dw-numa/gc/ptst.h"
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

// container contnente un evento da gestire
typedef struct nbc_bucket_node_container nbnc;
struct nbc_bucket_node_container{
	nbc_bucket_node* node;	// puntatore al nodo
	pkey_t timestamp;  		// relativo timestamp			
};

// virtual bucket
typedef struct deferred_work_bucket dwb;
struct deferred_work_bucket{	
	nbnc* volatile dwv;             // 8  // array di eventi deferred
	nbnc* volatile dwv_sorted;      // 16 // array di eventi sorted e deferred
	dwb* volatile next;             // 24 // puntatore al prossimo elemento
	long long index_vb;             // 32 //
	long long epoch;                // 40 //
	int volatile cicle_limit;       // 44 //
	int volatile valid_elem;        // 48 //
	int volatile indexes;           // 52 // inserimento|estrazione
	int volatile lock;              // 56 //
	long long pad;                  // 64 //
	//char pad[32];
};

// struttura di deferred work 
typedef struct deferred_work_structure dwstr;
struct deferred_work_structure{
	dwb* heads; 
	dwb* list_tail;
	int vec_size;
};

dwb* list_add(
 dwb*, 
 long long,
#if NUMA_DW
 int,
#endif
 dwb*
);

dwb* list_remove(
 dwb*, 
 long long, 
 dwb*
);

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

static inline nbc_bucket_node* get_node_pointer(nbc_bucket_node* node){return (nbc_bucket_node*)((unsigned long long)node & NODE_PTR_MASK);}
static inline nbc_bucket_node* get_marked_node(nbc_bucket_node* node, unsigned long long state){return ((nbc_bucket_node*)((unsigned long long)node | state));}

static inline bool is_deleted(nbc_bucket_node* node){return (bool)((unsigned long long)node & DELN);}
static inline bool is_blocked(nbc_bucket_node* node){return (bool)((unsigned long long)node & BLKN);}
static inline bool is_moving(nbc_bucket_node* node){return (bool)((unsigned long long)node & MVGN);}
static inline bool is_moved(nbc_bucket_node* node){return (bool)((unsigned long long)node & MVDN);}
static inline bool is_none(nbc_bucket_node* node){return (bool)(((unsigned long long)node & 0xfULL) == 0ULL);}

static inline dwb* get_bucket_pointer(dwb* bucket){return (dwb*)((unsigned long long)bucket & BUCKET_PTR_MASK);}
static inline unsigned long long get_bucket_state(dwb* bucket){return (((unsigned long long)bucket & BUCKET_STATE_MASK) >> BUCKET_STATE_SHIFT);}
static inline dwb* set_bucket_state(dwb* bucket, unsigned long long state){return (dwb*)(((unsigned long long)bucket & BUCKET_PTR_MASK_WM) | (state << BUCKET_STATE_SHIFT));}

static inline int get_enq_ind(int indexes){return indexes >> ENQ_BIT_SHIFT;}
static inline int get_deq_ind(int indexes){return indexes & DEQ_IND_MASK;}
static inline int add_enq_ind(int indexes, int num){return indexes + (num << ENQ_BIT_SHIFT);}

static inline bool is_marked_ref(dwb* bucket, unsigned long long mark){return (bool)((unsigned long long)bucket & mark);}
static inline dwb* get_marked_ref(dwb* bucket, unsigned long long mark){return (dwb*)((unsigned long long)bucket | mark);}


#endif // DEFERRED_WORK
