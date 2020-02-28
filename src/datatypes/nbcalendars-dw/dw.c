#include "common_nb_calqueue.h"

extern __thread unsigned long long no_empty_vb;
extern __thread long ins;
extern __thread long conflitti_ins;

extern __thread int blocked;
extern __thread bool from_block_table;
extern __thread long estr;
extern __thread long conflitti_estr;

extern bool is_marked_ref(dwb*);
extern dwb* get_marked_ref(dwb*);

// Bucket
dwb* getBucketPointer(dwb*);
unsigned long long getBucketState(dwb*);
dwb* setBucketState(dwb*, unsigned long long);
bool isBlocked(nbc_bucket_node*);
bool isDeleted(nbc_bucket_node*);
bool isMoving(nbc_bucket_node*);
bool isMoved(nbc_bucket_node*);
bool isNone(nbc_bucket_node*);

// Nodo
nbc_bucket_node* getNodePointer(nbc_bucket_node*);
unsigned long long getNodeState(nbc_bucket_node*);
nbc_bucket_node* setNodeState(nbc_bucket_node*, unsigned long long);

// Indici
int getEnqInd(int);
int getDeqInd(int);
int addEnqInd(int, int);

// Ausiliarie
void blockIns(dwb*);
#if NUMA_DW
void doOrd(dwb*, int);
#else
void doOrd(dwb*);
#endif
int cmp_node(const void*, const void*);
void printDWV(int, nbnc*);

nbc_bucket_node* getNodePointer(nbc_bucket_node* node){return (nbc_bucket_node*)((unsigned long long)node & NODE_PTR_MASK);}
unsigned long long getNodeState(nbc_bucket_node* node){return ((unsigned long long)node & NODE_STATE_MASK);}
nbc_bucket_node* setNodeState(nbc_bucket_node* node, unsigned long long state){return ((nbc_bucket_node*)((unsigned long long)node | state));}

bool isDeleted(nbc_bucket_node* node){return (bool)((unsigned long long)node & DELN);}
bool isBlocked(nbc_bucket_node* node){return (bool)((unsigned long long)node & BLKN);}
bool isMoving(nbc_bucket_node* node){return (bool)((unsigned long long)node & MVGN);}
bool isMoved(nbc_bucket_node* node){return (bool)((unsigned long long)node & MVDN);}
bool isNone(nbc_bucket_node* node){return (bool)(((unsigned long long)node & 0xfULL) == 0ULL);}

dwb* getBucketPointer(dwb* bucket){return (dwb*)((unsigned long long)bucket & BUCKET_PTR_MASK);}
unsigned long long getBucketState(dwb* bucket){return (((unsigned long long)bucket & BUCKET_STATE_MASK) >> 1);}
dwb* setBucketState(dwb* bucket, unsigned long long state){return (dwb*)(((unsigned long long)bucket & BUCKET_PTR_MASK_WM) | (state << 1));}

int getEnqInd(int indexes){return indexes >> ENQ_BIT_SHIFT;}
int getDeqInd(int indexes){return indexes & DEQ_IND_MASK;}
int addEnqInd(int indexes, int num){return indexes + (num << ENQ_BIT_SHIFT);}

void printDWV(int size, nbnc *vect){
	int j;

	for(j = 0; j < size; j++){
					
		if(vect[j].node != NULL)
			printf("%p %f ", vect[j].node, vect[j].timestamp);
		else
			printf("%p ", vect[j].node);
	}	

	fflush(stdout);	
}

void dw_block_table(void* tb, unsigned int start){
	
	nbc_bucket_node *dw_node;
	table* h = (table*)tb;
	dwb* bucket_p = NULL;
	int index_pb, i, j;

	for(i = 0; i < h->size; i++){	// per ogni bucket fisico
		index_pb = ((i + start) % h->size); // calcolo l'indice del bucket fisico
		
		// blocco gli inserimenti
		BOOL_CAS(&h->deferred_work->heads[index_pb].next, 
			getBucketPointer(h->deferred_work->heads[index_pb].next),
			setBucketState(h->deferred_work->heads[index_pb].next, MOVH));

		assertf(getBucketState(h->deferred_work->heads[index_pb].next) != MOVH, 
			"dw_block_table(): head non marcata in movimento %p\n", 
			h->deferred_work->heads[index_pb].next);

		bucket_p = getBucketPointer(h->deferred_work->heads[index_pb].next);

		// prima passata sulla lista, blocco gli elementi dei bucket virtuali non ancora estratti
		for(;bucket_p != h->deferred_work->list_tail; bucket_p = getBucketPointer(bucket_p->next)){
			
			if(is_marked_ref(bucket_p->next)) // bucket virtuale marcato, lo salto
				continue;	

			// porto il bucket in estrazione
			while(getBucketState(bucket_p->next) != BLK && getBucketState(bucket_p->next) != EXT){
				if(getBucketState(bucket_p->next) == INS) 	// può darsi che ci fosse qualcuno che stava inserendo in questo momento
					blockIns(bucket_p);						// impedisco gli inserimenti iniziati

				if(getBucketState(bucket_p->next) == ORD){	// eventuale ordinamento
					#if NUMA_DW
					doOrd(bucket_p, NODE_HASH(index_pb));
					#else
					doOrd(bucket_p);
					#endif
				}
			}

			assertf(getBucketState(bucket_p->next) != EXT && getBucketState(bucket_p->next) != BLK, "dw_block_table(): bucket non in estrazione %s\n", "");

			// TODO: forse non più necessario
			/*
			if(getDeqInd(bucket_p->indexes) < VEC_SIZE)
				FETCH_AND_ADD(&bucket_p->indexes, 2 * VEC_SIZE); // impedisco le estrazioni ed inserimenti
			*/ 

			for(j = 0; j < bucket_p->valid_elem && !is_marked_ref(bucket_p->next); j++){	// controllo solo se marcato(bloccaggio avviene contestualmente)
				if(isNone(bucket_p->dwv_sorted[j].node)){	// se non è ancora stato marcato in nessun modo(qui possibile DELN, MVG, MOVD)
					dw_node = getNodePointer(bucket_p->dwv_sorted[j].node);
					BOOL_CAS(&bucket_p->dwv_sorted[j].node, dw_node, setNodeState(dw_node, MVGN));// metto in trasferimento
				}
			}
		}

		bucket_p = getBucketPointer(h->deferred_work->heads[index_pb].next);

		from_block_table = true;// necessario per migrate_node()

		for(;bucket_p != h->deferred_work->list_tail; bucket_p = getBucketPointer(bucket_p->next)){

			// scorro tutto il bucket migrando i nodi contrassegnati in movimento(MVGN)
			for(j = 0; j < bucket_p->valid_elem && !is_marked_ref(bucket_p->next); j++){
				// se non è già stato trasferito o eliminato cerco di contribuire
				if(isMoving(bucket_p->dwv_sorted[j].node) && !isMoved(bucket_p->dwv_sorted[j].node)){
					dw_node = getNodePointer(bucket_p->dwv_sorted[j].node);	 // prendo soltanto il puntatore
					migrate_node(dw_node, h);	// migro
					dw_node = setNodeState(dw_node, MVGN);
					BOOL_CAS(&bucket_p->dwv_sorted[j].node, dw_node, setNodeState(dw_node, MVDN));// marco come trasferito(potrebbe fallire se già stato marcato MVDN)
					// alla fine sarà marcato sia come in trasferimento(MVGN) che trasferito(MVDN), cosi in estrazione controllo solo il primo
					assertf(!isMoved(bucket_p->dwv_sorted[j].node) || !isMoving(bucket_p->dwv_sorted[j].node), "dw_block_table(): nodo non marcato come trasferito %p\n", bucket_p->dwv_sorted[j].node);
				}
			}

			// marco il bucket come nello stato di BLK e possibile da eliminare
			BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, EXT), get_marked_ref(setBucketState(bucket_p->next, BLK)));
			assertf(!is_marked_ref(bucket_p->next) || getBucketState(bucket_p->next) != BLK, 
				"dw_block_table(): bucket non marcato %d %llu\n", is_marked_ref(bucket_p->next), 
				getBucketState(bucket_p->next));
		}

		from_block_table = false;

		// compatto una volta rilasciando la memoria
		bucket_p = list_remove(&h->deferred_work->heads[index_pb], LLONG_MAX-1, h->deferred_work->list_tail);
	}
}

#if NUMA_DW
int dw_enqueue(void *tb, unsigned long long index_vb, nbc_bucket_node *new_node, int numa_node)
#else
int dw_enqueue(void *tb, unsigned long long index_vb, nbc_bucket_node *new_node)
#endif
{
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb* bucket_p;
	int result = ABORT;
	int indexes, enq_cn; 

	assertf(from_block_table, "dw_enqueue(): thread proviene dalla block table %s\n", "");

	// controllo se la testa è stata marcata per l'inizio resize
	if(getBucketState(str->heads[index_vb % h->size].next) == MOVH) 
		return MOV_FOUND;

	#if NUMA_DW
	bucket_p = getBucketPointer(list_add(&str->heads[index_vb % h->size], index_vb, numa_node, str->list_tail));
	#else
	bucket_p = getBucketPointer(list_add(&str->heads[index_vb % h->size], index_vb, str->list_tail));
	#endif

	// potrebbe essere NULL se ho trovato la testa marcata durante la ricerca
	if(bucket_p == NULL || getBucketState(bucket_p->next) == BLK)
		return MOV_FOUND;

	assertf(bucket_p == NULL, "dw_enqueue(): bucket NULL dove non dovrebbe %p\n", bucket_p);

	if(getBucketState(bucket_p->next) == EXT)
		return ABORT;// bucket già pieno o è iniziata l'estrazione

	while((enq_cn = getEnqInd(bucket_p->indexes)) < bucket_p->cicle_limit && getBucketState(bucket_p->next) == INS){

		indexes = enq_cn << ENQ_BIT_SHIFT;	// per escludere la parte estrazione

		// provo ad aggiornare
		if(BOOL_CAS(&bucket_p->indexes, indexes, addEnqInd(indexes, 1))){// se presenta la parte deq diversa da zero non ci riesce

			// se sto qui enq_cn deve essere quello giusto
			assertf(enq_cn < 0 || enq_cn > bucket_p->cicle_limit, "dw_enqueue(): indice enqueue fuori range consentito %s\n", "");

			if(BOOL_CAS(&bucket_p->dwv[enq_cn].node, NULL, new_node) && BOOL_CAS_DOUBLE((unsigned long long*)&bucket_p->dwv[enq_cn].timestamp, INV_TS, new_node->timestamp) ){
				assertf(new_node == NULL, "dw_enqueue(): nuovo nodo nullo %s\n", "");
				assertf(bucket_p->dwv[enq_cn].node == NULL, "dw_enqueue(): nodo inserito nullo %s\n", "");

				ins++;
				//bucket_p->dwv[enq_cn].timestamp = new_node->timestamp;
				result = OK;
				break;
			}else{
				conflitti_ins++;
				if(getNodeState(bucket_p->dwv[enq_cn].node) > 0)// il nodo è stato marcato in qualche modo
					break;
			}
		}else
			conflitti_ins++;
	}

	// bucket riempito
	if(getBucketState(bucket_p->next) == INS && getEnqInd(bucket_p->indexes) >= VEC_SIZE)
		blockIns(bucket_p);

	if(getBucketState(bucket_p->next) == ORD){
		#if NUMA_DW
		doOrd(bucket_p, numa_node);
		#else
		doOrd(bucket_p);
		#endif
	}

	//assertf(getEnqInd(bucket_p->indexes) >= VEC_SIZE && getBucketState(bucket_p->next) != EXT, "dw_enqueue(): bucket pieno e non in estrazione. stato %llu, enq_cn %d, enq_cn salvato %d\n", getBucketState(bucket_p->next), enq_cn, getEnqInd(bucket_p->indexes));
	//assertf((result != OK && getBucketState(bucket_p->next) != EXT), "dw_enqueue(): condizione di uscita sbagliata. state %llu, result %d\n", getBucketState(bucket_p->next), result);
	return result;
}

dwb* dw_dequeue(void *tb, unsigned long long index_vb){
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb* bucket_p = NULL;

	bucket_p = list_remove(&str->heads[index_vb % h->size], index_vb, str->list_tail);
	if(bucket_p == NULL || is_marked_ref(bucket_p->next))
		return NULL;

	//assertf(is_marked_ref(bucket_p->next) == 1, "dw_dequeue(): nodo marcato %p\n", bucket_p->next);

	do{		
		if(getBucketState(bucket_p->next) == EXT || getBucketState(bucket_p->next) == BLK){ 
			if(is_marked_ref(bucket_p->next))
				return NULL;
			else
			//assertf(is_marked_ref(bucket_p->next) == 1, "dw_dequeue(): nodo marcato %p\n", bucket_p->next);
				return bucket_p;
		}

		/*
		assertf(getEnqInd(bucket_p->indexes) >= VEC_SIZE && getBucketState(bucket_p->next) == EXT,
		 "dw_dequeue():indice estrazione = %d, stato bucket %llu\n", getEnqInd(bucket_p->indexes), getBucketState(bucket_p->next));
		*/
		if(getBucketState(bucket_p->next) == INS)	
			blockIns(bucket_p);

		if(getBucketState(bucket_p->next) == ORD){
			#if NUMA_DW
				#if SEL_DW
				doOrd(bucket_p, NODE_HASH(index_vb % h->size));// sta su un nodo remoto
				#else
				doOrd(bucket_p, NID);// sta sul mio nodo
				#endif
			#else
			doOrd(bucket_p);
			#endif 
		}

	}while(1);

}

void blockIns(dwb* bucket_p){
	int i, limit;
	int enq_cn;
	int valid_elem = 0;

	//assertf(getDeqInd(bucket_p->indexes) > 0, "blockIns(): deq_cn non nullo %d %d %p\n", getDeqInd(bucket_p->indexes), bucket_p->indexes, bucket_p->next);
	//assertf((getEnqInd(bucket_p->indexes) == VEC_SIZE && from == 0), "blockIns(): bucket pieno da dw_dequeue. ultimo %p \n", bucket_p->dwv[VEC_SIZE - 1].node);
	//assertf((bucket_p->dwv[VEC_SIZE - 1].node == NULL && getEnqInd(bucket_p->indexes) == VEC_SIZE && bucket_p->dwv[VEC_SIZE - 1].node == NULL), "blockIns(): falso bucket virtuale pieno. index = %llu %p %d\n", bucket_p->index_vb, bucket_p->dwv[VEC_SIZE - 1].node, from);
	
	while((enq_cn = getEnqInd(bucket_p->indexes)) < VEC_SIZE){// se non l'ha fatto ancora nessuno
		assertf(enq_cn == 0, "blockIns(): non ci sono elementi nel bucket. enq_cn = %d\n", enq_cn);
		limit = getEnqInd(VAL_CAS(&bucket_p->indexes, enq_cn << ENQ_BIT_SHIFT, ((enq_cn + VEC_SIZE) << ENQ_BIT_SHIFT)));	// aggiungo il massimo
		assertf(limit == 0, "blockIns(): non ci sono elementi nel bucket. limit = %d\n", enq_cn);
		if(limit < VEC_SIZE){	// se ci sono riuscito
			//assertf((!BOOL_CAS(&bucket_p->cicle_limit, VEC_SIZE, limit)), "blockIns(): settaggio di cicle_limit non riuscito, limit %d %d %d\n", limit, bucket_p->cicle_limit, enq_cn);// imposto quello del bucket
			BOOL_CAS(&bucket_p->cicle_limit, VEC_SIZE, limit);
		}
	}

	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit < 0, "blockIns(): limite ciclo sbagliato %d\n", enq_cn);
	assertf(enq_cn < VEC_SIZE, "blockIns(): enq_cn non è stato incrementato %d\n", enq_cn);

	for(i = 0; i < bucket_p->cicle_limit; i++){

		if(bucket_p->dwv[i].node == NULL) BOOL_CAS(&bucket_p->dwv[i].node, NULL, (nbc_bucket_node*)BLKN);
		if(bucket_p->dwv[i].timestamp == INV_TS) BOOL_CAS_DOUBLE(&bucket_p->dwv[i].timestamp,INV_TS,INFTY);
		
		if(!isBlocked(bucket_p->dwv[i].node) && bucket_p->dwv[i].timestamp != INFTY)
			valid_elem++;

		assertf(bucket_p->dwv[i].node == NULL, "blockIns(): nodo non marcato come bloccato %p %f\n", bucket_p->dwv[i].node, bucket_p->dwv[i].timestamp);
	}

	if(BOOL_CAS(&bucket_p->valid_elem, VEC_SIZE, valid_elem)){
		blocked += bucket_p->cicle_limit - bucket_p->valid_elem;// statistica
	}

	BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, INS), setBucketState(bucket_p->next, ORD));
}

#if NUMA_DW
void doOrd(dwb* bucket_p, int numa_node)
#else
void doOrd(dwb* bucket_p)
#endif
{
	nbnc *old_dwv, *aus_ord_array;
	int i, j;

	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit <= 0, "doOrd(): limite del ciclo fuori dal range %s\n", "");
	assertf((bucket_p->cicle_limit == VEC_SIZE && bucket_p->dwv[VEC_SIZE - 1].node == NULL), "doOrd(): falso bucket virtuale pieno. index = %llu\n", bucket_p->index_vb);
	assertf(bucket_p->cicle_limit == 0, "doOrd(): nulla da ordinare %s\n", "");

	old_dwv = bucket_p->dwv;
	if(bucket_p->dwv_sorted != NULL){
		BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, ORD), setBucketState(bucket_p->next, EXT));
		return;
	} 
	
	#if NUMA_DW
	aus_ord_array = gc_alloc_node(ptst, gc_aid[2], numa_node);
	#else
	aus_ord_array = gc_alloc(ptst, gc_aid[2]);
	#endif
	if(aus_ord_array == NULL)	error("Non abbastanza memoria per allocare un array dwv in ORD\n");
	else{
		/*
		if(bucket_p->cicle_limit == VEC_SIZE){
			printDWV(VEC_SIZE, old_dwv);
			printf(" prima dell'ordinamento\n");
		}
		*/
		
		// memcopy selettiva
		for(i = 0, j = 0; i < bucket_p->cicle_limit; i++){
			if(!isBlocked(old_dwv[i].node) && bucket_p->dwv[i].timestamp != INFTY){
				memcpy(aus_ord_array + j, old_dwv + i, sizeof(nbnc));
				j++;
			}
		}

		assertf(j != bucket_p->valid_elem, "doOrd(): numero di elementi copiati per essere ordinati non corrisponde %d  %d\n", j, bucket_p->valid_elem);
		qsort(aus_ord_array, bucket_p->valid_elem, sizeof(nbnc), cmp_node);		// ordino solo elementi inseriti effettivamente nella fase INS
	
		/*
		if(bucket_p->cicle_limit == VEC_SIZE){
				printDWV(VEC_SIZE, aus_ord_array);
				printf(" dopo l'ordinamento\n");
		}
		*/

		// cerco di inserirlo
		if(BOOL_CAS(&bucket_p->dwv_sorted, NULL, aus_ord_array))
			BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, ORD), setBucketState(bucket_p->next, EXT));	
		else
			gc_free(ptst, aus_ord_array, gc_aid[2]);	
	}	
}

int cmp_node(const void *p1, const void *p2){
	nbnc *node_1 = (nbnc*)p1;
	nbnc *node_2 = (nbnc*)p2;
	pkey_t tmp;

	assertf(!isNone(node_1->node), "cmp_node(): nodo marcato %p %f %f\n", node_1->node, getNodePointer(node_1->node)->timestamp, node_1->timestamp);
	assertf(!isNone(node_2->node), "cmp_node(): nodo marcato %p %f %f\n", node_2->node, getNodePointer(node_2->node)->timestamp, node_2->timestamp);

	tmp = (node_1->timestamp - node_2->timestamp);
	return (tmp > 0.0) - (tmp < 0.0);
}
/*
pkey_t dw_extraction(dwb* bucket_p, void** result, pkey_t left_ts, bool remote, bool no_cq_node){
	nbc_bucket_node *dw_node;
	pkey_t dw_node_ts;
	int indexes_enq, deq_cn;

	indexes_enq = getEnqInd(bucket_p->indexes) << ENQ_BIT_SHIFT;// solo la parte inserimento di indexes
	dw_retry:
					 
	deq_cn = getDeqInd(bucket_p->indexes); 

	// cerco un possibile indice
	while(	deq_cn < bucket_p->valid_elem && isDeleted(bucket_p->dwv_sorted[deq_cn].node) && 
			!isMoving(bucket_p->dwv_sorted[deq_cn].node) && !is_marked_ref(bucket_p->next))
		{
			BOOL_CAS(&bucket_p->indexes, (indexes_enq + deq_cn), (indexes_enq + deq_cn + 1));	// aggiorno deq
			deq_cn = getDeqInd(bucket_p->indexes);
		}

	// controllo se sono uscito perchè è iniziata la resize
	if(isMoving(bucket_p->dwv_sorted[deq_cn].node))
		return GOTO;

	if(deq_cn >= bucket_p->valid_elem || is_marked_ref(bucket_p->next)){

		//no_dw_node_curr_vb = true;
		if(!is_marked_ref(bucket_p->next))
			BOOL_CAS(&(bucket_p->next), bucket_p->next, get_marked_ref(bucket_p->next));
					
		// TODO
		//}else if(bucket_p->dwv[deq_cn].node->epoch > epoch){
		//	goto begin;
	}else{
		assertf((getNodeState(bucket_p->dwv_sorted[deq_cn].node) & BLKN), 
			"pq_dequeue(): si cerca di estrarre un nodo bloccato. stato nodo %llu, stato bucket %llu\n", 
			getNodeState(bucket_p->dwv_sorted[deq_cn].node), 
			getBucketState(bucket_p->next));

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
			bucket_p->index_vb, getBucketState(bucket_p->next), is_marked_ref(bucket_p->next), deq_cn, getDeqInd(bucket_p->indexes), 
			getEnqInd(indexes_enq), bucket_p->cicle_limit, bucket_p->valid_elem, bucket_p->dwv_sorted[deq_cn].timestamp);
		
		if(!no_cq_node) no_empty_vb++;
		
		// provo a fare l'estrazione se il nodo della dw viene prima 
					
		// TODO
		assertf(bucket_p->dwv_sorted[deq_cn].timestamp == INV_TS, "INV_TS in extractions%s\n", ""); 
		if((dw_node_ts = bucket_p->dwv_sorted[deq_cn].timestamp) <= left_ts){
		
			dw_node = getNodePointer(bucket_p->dwv_sorted[deq_cn].node);	// per impedire l'estrazione di uno già estratto o in movimento
			assertf((dw_node == NULL), "pq_dequeue(): dw_node è nullo %s\n", "");

			if(BOOL_CAS(&bucket_p->dwv_sorted[deq_cn].node, dw_node, setNodeState(dw_node, DELN))){// se estrazione riuscita
							
				assertf(deq_cn >= VEC_SIZE, "pq_dequeue(): indice di estrazione fuori range %s\n", "");	
				assertf(getEnqInd(bucket_p->indexes) != getEnqInd(indexes_enq), "pq_dequeue(): indice di inserimento non costante %s\n", "");
							
				BOOL_CAS(&bucket_p->indexes, (indexes_enq + deq_cn), (indexes_enq + deq_cn + 1));
				//concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

				estr++;
				*result = dw_node->payload;

				//critical_exit();
				#if NUMA_DW
				if (!remote)
					local_deq++;
				else
					remote_deq++;
				#endif

				return dw_node_ts;
			}else{// provo a vedere se ci sono altri nodi in dw
				conflitti_estr++;

				// controllo se non ci sono riuscito perchè stiamo nella fase di resize
				if(isMoving(bucket_p->dwv_sorted[deq_cn].node)) 
					return GOTO;

				goto dw_retry;
			}
		}
	}

	return EMPTY;
}
*/