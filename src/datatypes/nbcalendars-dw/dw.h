#ifndef DEFERRED_WORK
#define DEFERRED_WORK

#include <string.h>

#ifndef NUMA_DW
#define NUMA_DW 					0	// se 1 allora utilizza allocatore NUMA aware
#endif

extern unsigned int TOTAL_OPS1;
extern unsigned int THREADS;
extern unsigned int MAX_THREAD_NUM;

#define DISTRIBUTED_PIN		0	// numero minimo dei thread su ogni nodo	
#define GRADUAL_PIN			1	// prima il nodo 0, poi 1, 2 e cosi via
#define GRAD_PIN DISTRIBUTED_PIN 

#define DISTINCT_THREAD_TYPES	0	// il ruolo dei thread è suddiviso



#define CPU_PER_NODE (MAX_THREAD_NUM / _NUMA_NODES)	

#define TH0 						(int)(THREADS == 1)
#define TH1 						(int)(THREADS >1 && THREADS <= 4)
#define TH2 						(int)(THREADS > 4 && THREADS <= 8)
#define TH3 						(int)(THREADS > 8 && THREADS <= 16)
#define TH4 						(int)(THREADS > 16)

// configuration
//#define START_EPB					65
//#define INC_EPB_PER_THREAD			(THREADS < (/*MAX_THREAD_NUM*/0 / 2) ? 42 : ((800 - START_EPB) / (THREADS - 1)))//24
#define EPB 						(unsigned int)(TH0 * 65 + TH1 * 140 + TH2 * 300 + TH3 * 620 + TH4 * 800)
#define VEC_SIZE 					(unsigned int)(EPB * 1.27)
//#define VEC_SIZE                    (unsigned int)((START_EPB + (THREADS - 1) * INC_EPB_PER_THREAD) * 1.27)//1020
//#define VEC_SIZE 					255
#define DW_ENQUEUE_USAGE_TH			0	// minima distanza tra current e virtual bucket di inserimento per utilizzare DWQ

#define DEQUEUE_WAIT_CICLES			5000	// numero di cicli di attesa per un thread remoto prima di provare a fare la dequeue

#define ENABLE_PROACTIVE_FLUSH      1   // abilita il flush proattivo
#define DEQUEUE_NUM_TH				0	// dopo aver fatto questo numero di dequeue provo a fare flush proattivo di un bucket
#define PRO_FLUSH_BUCKET_NUM		THREADS * 2	 
#define PRO_FLUSH_BUCKET_NUM_MIN	1	// distanza dal bucket attuale in numero di bucket che posso considerare per flush proattivo

#define DISABLE_EXTRACTION_FROM_DW  ENABLE_PROACTIVE_FLUSH	// disabilita le estrazioni dirette dall dwq
#define ENABLE_SORTING              0//!DISABLE_EXTRACTION_FROM_DW   // abilita il sorting per le dwq
//#define DW_USAGE_TH                 TOTAL_OPS1*0.39//190000	// setta il numero di elementi minimo per abilitare le dwq
#define DW_USAGE_TH                 (int)(TOTAL_OPS1*0.39/((float)MAX_THREAD_NUM / THREADS))

#define ENABLE_BLOCKING_FLUSH       0	// abilita il lock per flushare elementi della dwq sui bucket della cq
#define SEL_DW                      0	// se 1 allora lavoro differito solo se la destinazione si trova su un nodo numa remoto
#define ENABLE_ENQUEUE_WORK			0   // abilita eventuale ulteriore lavoro svolto da un thread che esegue enqueue in DWQ
#if GRAD_PIN
#define NUMA_NODES_IN_USE		(((THREADS-1)/CPU_PER_NODE)+1)
//#define NODE_HASH(bucket_id)        ((bucket_id) % (((THREADS-1)/CPU_PER_NODE)+1)/*_NUMA_NODES*/)	// per bucket fisico
#else
	#if DISTINCT_THREAD_TYPES
	#define NUMA_NODES_IN_USE	((THREADS < 4) ? 1 : 2)
//	#define NODE_HASH(bucket_id)        ((bucket_id) % ((THREADS < 4) ? 1 : 2))// per il caso dei thread distinti
	#else
	#define NUMA_NODES_IN_USE	((THREADS < _NUMA_NODES) ? THREADS : _NUMA_NODES)
//	#define NODE_HASH(bucket_id)        ((bucket_id) % ((THREADS < _NUMA_NODES) ? THREADS : _NUMA_NODES))
	#endif
#endif
#define NODE_HASH(bucket_id)	(bucket_id % NUMA_NODES_IN_USE)
#define NID                         nid
#define HEADS_ARRAY_SCALE			1

#define PRO_CACHE					1		// utilizzo della cache in flush proattivo
#define PRO_THREADS 				1		// utilizzo dei thread appositi
#define PRO_THREAD_US				1		// attesa dei thread che si occupano del flush proattivo


#define ENQ_FAILS_STAT				1

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
#define ANYB	(3ULL)  // una delle precedenti

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
	int volatile pro;				// 60 //
	int volatile pad;						// 64 // 
	//long long pad;					               
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
dwb*
#if NUMA_DW
 ,int
#endif
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
void dw_proactive_flush(void*, unsigned long long);
void dw_block_table(void*, unsigned int);
int cmp_node(const void*, const void*);

static inline nbc_bucket_node* get_node_pointer(nbc_bucket_node* node){return (nbc_bucket_node*)((unsigned long long)node & NODE_PTR_MASK);}
static inline nbc_bucket_node* get_marked_node(nbc_bucket_node* node, unsigned long long state){return ((nbc_bucket_node*)((unsigned long long)node | state));}

static inline bool is_deleted(nbc_bucket_node* node){return (bool)((unsigned long long)node & DELN);}
static inline bool is_blocked(nbc_bucket_node* node){return (bool)((unsigned long long)node & BLKN);}
static inline bool is_moving(nbc_bucket_node* node){return (bool)((unsigned long long)node & MVGN);}
static inline bool is_moved(nbc_bucket_node* node){return (bool)((unsigned long long)node & MVDN);}
static inline bool is_none(nbc_bucket_node* node){return (bool)(((unsigned long long)node & 0xfULL) == NONE);}

static inline dwb* get_bucket_pointer(dwb* bucket){return (dwb*)((unsigned long long)bucket & BUCKET_PTR_MASK);}
static inline unsigned long long get_bucket_state(dwb* bucket){return (((unsigned long long)bucket & BUCKET_STATE_MASK) >> BUCKET_STATE_SHIFT);}
static inline dwb* set_bucket_state(dwb* bucket, unsigned long long state){return (dwb*)(((unsigned long long)bucket & BUCKET_PTR_MASK_WM) | (state << BUCKET_STATE_SHIFT));}

static inline int get_enq_ind(int indexes){return indexes >> ENQ_BIT_SHIFT;}
static inline int get_deq_ind(int indexes){return indexes & DEQ_IND_MASK;}
static inline int add_enq_ind(int indexes, int num){return indexes + (num << ENQ_BIT_SHIFT);}

static inline bool is_marked_ref(dwb* bucket, unsigned long long mark){return (bool)((unsigned long long)bucket & mark);}
static inline dwb* get_marked_ref(dwb* bucket, unsigned long long mark){return (dwb*)((unsigned long long)bucket | mark);}

/*
+static inline int NODE_HASH(unsigned int bucket_id){
+       static int nodes[]={0,2,4,7,1,3,5,6};
+       int interm = bucket_id % THREADS;
+//     printf("interm %d, %d\n",interm, nodes[(((int)(interm/16))*4+(interm%4))]);
+       return nodes[(((int)(interm / 16))*4 + (interm % 4))]; 
+}*/


static inline unsigned int hash_dw(unsigned long long index_vb, unsigned int size){
	unsigned int scale = HEADS_ARRAY_SCALE;

	if(size < HEADS_ARRAY_SCALE)	scale = 1;

	return (unsigned int) index_vb % (size / scale);
}

extern __thread long estr;
extern __thread long conflitti_estr;

static inline pkey_t dw_extraction(dwb* bucket_p, void** result, pkey_t left_ts){
	nbc_bucket_node *dw_node;
	pkey_t dw_node_ts;
	int indexes_enq, deq_cn;
	unsigned int old_v;
								 
	indexes_enq = get_enq_ind(bucket_p->indexes) << ENQ_BIT_SHIFT;// solo la parte inserimento di indexes
	dw_retry:
					 
	old_v = bucket_p->indexes;
	deq_cn = get_deq_ind(old_v);

	// cerco un possibile indice
	while(	deq_cn < bucket_p->valid_elem && is_deleted(bucket_p->dwv_sorted[deq_cn].node) && 
			!is_moving(bucket_p->dwv_sorted[deq_cn].node) && !is_marked_ref(bucket_p->next, DELB))
		deq_cn++;

	old_v = bucket_p->indexes;
	if(get_deq_ind(old_v) == 0 || get_deq_ind(old_v) < deq_cn)
		BOOL_CAS(&bucket_p->indexes, old_v, (indexes_enq + deq_cn));	// aggiorno deq

	// controllo se sono uscito perchè è iniziata la resize
	if(deq_cn < bucket_p->valid_elem && is_moving(bucket_p->dwv_sorted[deq_cn].node))
		return GOTO;

	if(deq_cn >= bucket_p->valid_elem || is_marked_ref(bucket_p->next, DELB)){

		//no_dw_node_curr_vb = true;
		if(!is_marked_ref(bucket_p->next, DELB))
			BOOL_CAS(&(bucket_p->next), bucket_p->next, get_marked_ref(bucket_p->next, DELB));
								
			// TODO
		//}else if(bucket_p->dwv[deq_cn].node->epoch > epoch){
		//	goto begin;

		return EMPTY;
	}else{


		assertf(is_blocked(bucket_p->dwv_sorted[deq_cn].node), 
		"pq_dequeue(): si cerca di estrarre un nodo bloccato. stato nodo %p, stato bucket %llu\n", 
		bucket_p->dwv_sorted[deq_cn].node, get_bucket_state(bucket_p->next));

		assertf((bucket_p->dwv_sorted[deq_cn].node == NULL), 
			"pq_dequeue(): si cerca di estrarre un nodo nullo." 
			"\nnumero bucket: %llu"
			"\nstato bucket virtuale: %llu"
			"\nbucket marcato: %d"
			"\nindice estrazione letto: %d"
			"\nindice estrazione nella struttura: %d"
			"\nindice inserimento: %d"
			"\nlimite ciclo: %d"
			"\nelementi validi: %d"
			"\ntimestamp: %f\n",
			bucket_p->index_vb, get_bucket_state(bucket_p->next), is_marked_ref(bucket_p->next, DELB), deq_cn, get_deq_ind(bucket_p->indexes), 
			get_enq_ind(indexes_enq), bucket_p->cicle_limit, bucket_p->valid_elem, bucket_p->dwv_sorted[deq_cn].timestamp);
		
		// provo a fare l'estrazione se il nodo della dw viene prima 
					
		// TODO
		assertf(bucket_p->dwv_sorted[deq_cn].timestamp == INV_TS, "INV_TS in extractions%s\n", ""); 
		if((dw_node_ts = bucket_p->dwv_sorted[deq_cn].timestamp) <= left_ts){
		
			dw_node = get_node_pointer(bucket_p->dwv_sorted[deq_cn].node);	// per impedire l'estrazione di uno già estratto o in movimento
			assertf((dw_node == NULL), "pq_dequeue(): dw_node è nullo %s\n", "");

			if(BOOL_CAS(&bucket_p->dwv_sorted[deq_cn].node, dw_node, get_marked_node(dw_node, DELN))){// se estrazione riuscita
							
				assertf(deq_cn >= VEC_SIZE, "pq_dequeue(): indice di estrazione fuori range %s\n", "");	
				assertf(get_enq_ind(bucket_p->indexes) != get_enq_ind(indexes_enq), "pq_dequeue(): indice di inserimento non costante %s\n", "");
							
				BOOL_CAS(&bucket_p->indexes, (indexes_enq + deq_cn), (indexes_enq + deq_cn + 1));

				estr++;
				*result = dw_node->payload;

				return dw_node_ts;
			}else{// provo a vedere se ci sono altri nodi in dw
				conflitti_estr++;

				// controllo se non ci sono riuscito perchè stiamo nella fase di resize
				if(is_moving(bucket_p->dwv_sorted[deq_cn].node)) 
					return GOTO;

				goto dw_retry;
			}
		}

		return CONTINUE;
	}
}

#endif // DEFERRED_WORK