#include "common_nb_calqueue.h"

extern __thread unsigned long long no_empty_vb;
extern __thread long ins;
extern __thread long conflitti_ins;
extern __thread int blocked;
extern __thread bool from_block_table;
extern __thread long estr;
extern __thread long conflitti_estr;
extern __thread long cache_hit;
__thread unsigned int dequeue_num = 0;
__thread dwb* last_bucket_p = NULL;

extern unsigned int THREADS;


void block_ins(dwb* bucket_p){
	int i, limit;
	int enq_cn;
	int valid_elem = 0;

	//assertf(get_deq_ind(bucket_p->indexes) > 0, "block_ins(): deq_cn non nullo %d %d %p\n", get_deq_ind(bucket_p->indexes), bucket_p->indexes, bucket_p->next);
	//assertf((get_enq_ind(bucket_p->indexes) == VEC_SIZE && from == 0), "block_ins(): bucket pieno da dw_dequeue. ultimo %p \n", bucket_p->dwv[VEC_SIZE - 1].node);
	//assertf((bucket_p->dwv[VEC_SIZE - 1].node == NULL && get_enq_ind(bucket_p->indexes) == VEC_SIZE && bucket_p->dwv[VEC_SIZE - 1].node == NULL), "block_ins(): falso bucket virtuale pieno. index = %llu %p %d\n", bucket_p->index_vb, bucket_p->dwv[VEC_SIZE - 1].node, from);
	
	while((enq_cn = get_enq_ind(bucket_p->indexes)) < VEC_SIZE){// se non l'ha fatto ancora nessuno
		//assertf(enq_cn == 0, "block_ins(): non ci sono elementi nel bucket. enq_cn = %d\n", enq_cn);
		limit = get_enq_ind(VAL_CAS(&bucket_p->indexes, enq_cn << ENQ_BIT_SHIFT, ((enq_cn + VEC_SIZE) << ENQ_BIT_SHIFT)));	// aggiungo il massimo
		//assertf(limit == 0, "block_ins(): non ci sono elementi nel bucket. limit = %d\n", enq_cn);
		if(limit < VEC_SIZE){	// se ci sono riuscito
			//assertf((!BOOL_CAS(&bucket_p->cicle_limit, VEC_SIZE, limit)), "block_ins(): settaggio di cicle_limit non riuscito, limit %d %d %d\n", limit, bucket_p->cicle_limit, enq_cn);// imposto quello del bucket
			BOOL_CAS(&bucket_p->cicle_limit, VEC_SIZE, limit);
		}
	}

	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit < 0, "block_ins(): limite ciclo sbagliato %d\n", enq_cn);
	assertf(enq_cn < VEC_SIZE, "block_ins(): enq_cn non è stato incrementato %d\n", enq_cn);

	for(i = 0; i < bucket_p->cicle_limit; i++){

		if(bucket_p->dwv[i].node == NULL) BOOL_CAS(&bucket_p->dwv[i].node, NULL, (nbc_bucket_node*)BLKN);
		if(bucket_p->dwv[i].timestamp == INV_TS) BOOL_CAS_DOUBLE(&bucket_p->dwv[i].timestamp,INV_TS,INFTY);
		
		if(!is_blocked(bucket_p->dwv[i].node) && bucket_p->dwv[i].timestamp != INFTY)
			valid_elem++;

		assertf(bucket_p->dwv[i].node == NULL, "block_ins(): nodo non marcato come bloccato %p %f\n", bucket_p->dwv[i].node, bucket_p->dwv[i].timestamp);
	}

	if(BOOL_CAS(&bucket_p->valid_elem, VEC_SIZE, valid_elem)){
		blocked += bucket_p->cicle_limit - bucket_p->valid_elem;// statistica
	}

	BOOL_CAS(&bucket_p->next, set_bucket_state(bucket_p->next, INS), set_bucket_state(bucket_p->next, ORD));
}


void apply_transition_ORD_to_EXT(dwb *bucket_p
#if NUMA_DW
, int numa_node
#endif
){
	nbnc *old_dwv, *aus_ord_array;
	int i, j;
	
	if(get_bucket_state(bucket_p->next) == ORD){
		//potrebbe capitare che in fase di inserimento il bucket è stato creato ma non èstato aggiornato nessun indice ne inserito nessun elemento
		//assertf( bucket_p->cicle_limit == 0, "do_ord(): nulla da ordinare %s\n", "");
		assertf( bucket_p->cicle_limit  > VEC_SIZE || bucket_p->cicle_limit < 0, "do_ord(): limite del ciclo fuori dal range %d\n", bucket_p->cicle_limit);
		assertf((bucket_p->cicle_limit == VEC_SIZE && bucket_p->dwv[VEC_SIZE - 1].node == NULL), "do_ord(): falso bucket virtuale pieno. index = %llu\n", bucket_p->index_vb);
		
		if(bucket_p->dwv_sorted != NULL || bucket_p->valid_elem == 0){
			BOOL_CAS(&bucket_p->next, set_bucket_state(bucket_p->next, ORD), set_bucket_state(bucket_p->next, EXT));
			return;
		} 
		
	#if NUMA_DW			
		aus_ord_array = gc_alloc_node(ptst, gc_aid[2], numa_node);
	#else
		aus_ord_array = gc_alloc(ptst, gc_aid[2]);
	#endif
	
		if(aus_ord_array == NULL)	error("Non abbastanza memoria per allocare un array dwv in ORD\n");

		//if(bucket_p->cicle_limit == VEC_SIZE){ print_dwv(VEC_SIZE, old_dwv); printf(" prima dell'ordinamento\n"); }

		// memcopy selettiva
		old_dwv = bucket_p->dwv;
		for(i = 0, j = 0; i < bucket_p->cicle_limit; i++){
			if(!is_blocked(old_dwv[i].node) && bucket_p->dwv[i].timestamp != INFTY){
				memcpy(aus_ord_array + j, old_dwv + i, sizeof(nbnc));
				j++;
			}
		}

		assertf(j != bucket_p->valid_elem, "do_ord(): numero di elementi copiati per essere ordinati non corrisponde %d  %d\n", j, bucket_p->valid_elem);
	
	#if ENABLE_SORTING 
		qsort(aus_ord_array, bucket_p->valid_elem, sizeof(nbnc), cmp_node);		// ordino solo elementi inseriti effettivamente nella fase INS
	#endif
		//if(bucket_p->cicle_limit == VEC_SIZE){ print_dwv(VEC_SIZE, old_dwv); printf("dopo l'ordinamento\n"); }
		
		// cerco di inserirlo
		if(!BOOL_CAS(&bucket_p->dwv_sorted, NULL, aus_ord_array)) gc_free(ptst, aus_ord_array, gc_aid[2]);	
		else BOOL_CAS(&bucket_p->next, set_bucket_state(bucket_p->next, ORD), set_bucket_state(bucket_p->next, EXT)); 
	}
}


void print_dwv(int size, nbnc *vect){
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
	dwb* bucket_p, *prev_bucket_p;
	int index_pb, i, j;

	for(i = 0; i < h->size; i++){	// per ogni bucket fisico
		index_pb = ((i + start) % h->size); // calcolo l'indice del bucket fisico
		
		// blocco gli inserimenti dopo la testa
		BOOL_CAS(&h->deferred_work->heads[index_pb].next, 
			get_bucket_pointer(h->deferred_work->heads[index_pb].next),
			get_marked_ref(h->deferred_work->heads[index_pb].next, MOVB));

		assertf(!is_marked_ref(h->deferred_work->heads[index_pb].next, MOVB), 
			"dw_block_table(): head non marcata in movimento %p\n", 
			h->deferred_work->heads[index_pb].next);
		
		// blocco i bucket per impedire inserimenti, blocco nodi per impedire estrazioni.
		// in seguito migro bucket in mezzo a due bloccati.
		for(bucket_p = get_bucket_pointer(h->deferred_work->heads[index_pb].next), prev_bucket_p = NULL;
			bucket_p != NULL; 
			prev_bucket_p = bucket_p, bucket_p = get_bucket_pointer(bucket_p->next)){
			
			if(bucket_p != h->deferred_work->list_tail){	// evito di marcare la tail
			
				// se il bucket è già marcato per eliminazione lo salto
				if(is_marked_ref(bucket_p->next, DELB)) continue;	

				// porto il bucket in estrazione
				while(get_bucket_state(bucket_p->next) != BLK && get_bucket_state(bucket_p->next) != EXT){
					if(get_bucket_state(bucket_p->next) == INS) 	// può darsi che ci fosse qualcuno che stava inserendo in questo momento
						block_ins(bucket_p);						// impedisco gli inserimenti iniziati

					if(get_bucket_state(bucket_p->next) == ORD){	// eventuale ordinamento
						apply_transition_ORD_to_EXT(bucket_p
						#if NUMA_DW
						, NODE_HASH(index_pb)
						#endif
						);
					}
				}

				assertf(get_bucket_state(bucket_p->next) != EXT && get_bucket_state(bucket_p->next) != BLK, 
					"dw_block_table(): bucket non in estrazione %s\n", "");

				// blocco il bucket virtuale per impedire gli inserimenti
				BOOL_CAS(&bucket_p->next, set_bucket_state(bucket_p->next, EXT), get_marked_ref(set_bucket_state(bucket_p->next, EXT), MOVB));

				assertf(!is_marked_ref(bucket_p->next, MOVB), 
				"dw_block_table(): bucket non marcato in movimento %p\n", bucket_p->next);
			}

			// TODO: forse non più necessario
			/*
			if(get_deq_ind(bucket_p->indexes) < VEC_SIZE)
				FETCH_AND_ADD(&bucket_p->indexes, 2 * VEC_SIZE); // impedisco le estrazioni ed inserimenti
			*/ 

			if(prev_bucket_p != NULL){ // se ho un bucket in mezzo
				
				for(j = 0; j < prev_bucket_p->valid_elem && !is_marked_ref(prev_bucket_p->next, DELB); j++){	// controllo solo se il bucket è marcato(bloccaggio avviene contestualmente)
					if(is_none(prev_bucket_p->dwv_sorted[j].node)){	// se non è ancora stato marcato in nessun modo lo metto in trasferimento(qui possibile DELN, MVG, MOVD)
						dw_node = get_node_pointer(prev_bucket_p->dwv_sorted[j].node);
						BOOL_CAS(&prev_bucket_p->dwv_sorted[j].node, dw_node, get_marked_node(dw_node, MVGN));// metto in trasferimento
					}
				}

				from_block_table = true;	// necessario per migrate_node()

				// scorro tutto il bucket migrando i nodi contrassegnati in movimento(MVGN)
				for(j = 0; j < prev_bucket_p->valid_elem && !is_marked_ref(prev_bucket_p->next, DELB); j++){
					// se non è già stato trasferito o eliminato cerco di contribuire
					if(is_moving(prev_bucket_p->dwv_sorted[j].node) && !is_moved(prev_bucket_p->dwv_sorted[j].node)){
						dw_node = get_node_pointer(prev_bucket_p->dwv_sorted[j].node);	 // prendo soltanto il puntatore
						flush_node(NULL, dw_node, h);	// migro
						dw_node = get_marked_node(dw_node, MVGN);
						BOOL_CAS(&prev_bucket_p->dwv_sorted[j].node, dw_node, get_marked_node(dw_node, MVDN));// marco come trasferito(potrebbe fallire se già stato marcato MVDN)
						// alla fine sarà marcato sia come in trasferimento(MVGN) che trasferito(MVDN), cosi in estrazione controllo solo il primo
						assertf(!is_moved(prev_bucket_p->dwv_sorted[j].node) || !is_moving(prev_bucket_p->dwv_sorted[j].node), "dw_block_table(): nodo non marcato come trasferito %p\n", prev_bucket_p->dwv_sorted[j].node);
					}
				}

				// marco il bucket come BLK e possibile da eliminare
				// potrebbe non essere bloccato esplicitamente per gli inserimenti se era marcato come DELN prima della resize
				BOOL_CAS(&prev_bucket_p->next, set_bucket_state(prev_bucket_p->next, EXT), get_marked_ref(set_bucket_state(prev_bucket_p->next, BLK), DELB));
				assertf(!is_marked_ref(prev_bucket_p->next, DELB) || get_bucket_state(prev_bucket_p->next) != BLK, 
					"dw_block_table(): bucket non marcato %d %llu\n", is_marked_ref(prev_bucket_p->next, DELB),
					get_bucket_state(prev_bucket_p->next));
			

				from_block_table = false;
			}
		}
		// compatto una volta rilasciando la memoria
		bucket_p = list_remove(&h->deferred_work->heads[index_pb], LLONG_MAX-1, h->deferred_work->list_tail);
	}
}


int dw_enqueue(void *tb, unsigned long long index_vb, nbc_bucket_node *new_node
#if NUMA_DW
, int numa_node
#endif
)
{
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb* bucket_p;
	int result = ABORT;
	int indexes, enq_cn; 

	assertf(from_block_table, "dw_enqueue(): thread proviene dalla block table %s\n", "");

	// controllo se la testa è stata marcata per l'inizio resize 
	if(is_marked_ref(str->heads[index_vb % h->size].next, MOVB))
		return MOV_FOUND;

	bucket_p = get_bucket_pointer(
		list_add(
			&str->heads[index_vb % h->size], 
			index_vb, 
	#if NUMA_DW
			numa_node, 
	#endif
			str->list_tail
		)
	);

	// potrebbe essere NULL se ho trovato la testa marcata durante la ricerca
	if(bucket_p == NULL || get_bucket_state(bucket_p->next) == BLK)
		return MOV_FOUND;

	assertf(bucket_p == NULL, "dw_enqueue(): bucket NULL dove non dovrebbe %p\n", bucket_p);

	if(get_bucket_state(bucket_p->next) == EXT)
		return ABORT;// bucket già pieno o è iniziata l'estrazione

	while((enq_cn = get_enq_ind(bucket_p->indexes)) < bucket_p->cicle_limit && get_bucket_state(bucket_p->next) == INS){

		indexes = enq_cn << ENQ_BIT_SHIFT;	// per escludere la parte estrazione

		// provo ad aggiornare
		if(BOOL_CAS(&bucket_p->indexes, indexes, add_enq_ind(indexes, 1))){// se presenta la parte deq diversa da zero non ci riesce

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
				if(!is_none(bucket_p->dwv[enq_cn].node))// il nodo è stato marcato in qualche modo
					break;
			}
		}else
			conflitti_ins++;
	}

/*
	if(get_bucket_state(bucket_p->next) == EXT)
		return result;// bucket già pieno o è iniziata l'estrazione
*/
	#if ENABLE_ENQUEUE_WORK

		// bucket riempito
		if(get_bucket_state(bucket_p->next) == INS && get_enq_ind(bucket_p->indexes) >= VEC_SIZE)
			block_ins(bucket_p);

		if(get_bucket_state(bucket_p->next) == ORD){	// eventuale ordinamento
			apply_transition_ORD_to_EXT(bucket_p
		#if NUMA_DW
			, numa_node 
		#endif
		);
		}

	#endif

	//assertf(get_enq_ind(bucket_p->indexes) >= VEC_SIZE && get_bucket_state(bucket_p->next) != EXT, "dw_enqueue(): bucket pieno e non in estrazione. stato %llu, enq_cn %d, enq_cn salvato %d\n", get_bucket_state(bucket_p->next), enq_cn, get_enq_ind(bucket_p->indexes));
	//assertf((result != OK && get_bucket_state(bucket_p->next) != EXT), "dw_enqueue(): condizione di uscita sbagliata. state %llu, result %d\n", get_bucket_state(bucket_p->next), result);
	return result;
}

void dw_flush(void *tb, dwb *bucket_p, bool mark_bucket){
	table* h = (table*)tb;
	nbc_bucket_node* dw_node, *suggestion, *next_sug;
	int j, step = THREADS;

#if ENABLE_BLOCKING_FLUSH == 1
	if(BOOL_CAS(&bucket_p->lock, 0, 1)){
		step = 1;	
#endif
		//if(step == 0)
		//	step = (int)TID * 2 + 1;

		next_sug = NULL;

		while(1){

			if(step > 1)
				j = (int)TID;
			else
				j = 0;
			
			suggestion = next_sug;
			next_sug = NULL;
			
			// scorro tutto il bucket flushando i nodi
			for(/*j = 0*/; j < bucket_p->valid_elem && !is_marked_ref(bucket_p, DELB) && !bucket_p->pro; j += step){

				// se già cancellato oppure bloccato ma il mio step non è 1 vado avanti
				if(is_deleted(bucket_p->dwv_sorted[j].node) 
					|| (step != 1 && is_blocked(bucket_p->dwv_sorted[j].node))
				)
					continue;

				//altrimenti marco come bloccato
				dw_node = get_node_pointer(bucket_p->dwv_sorted[j].node);	 // prendo soltanto il puntatore
				if(BOOL_CAS(&bucket_p->dwv_sorted[j].node, dw_node, get_marked_node(dw_node, BLKN))){
					// se ci riesco faccio il flush
					suggestion = flush_node(suggestion, dw_node, h);	// migro

					if(next_sug == NULL)
						next_sug = suggestion;

					dw_node = get_marked_node(get_node_pointer(bucket_p->dwv_sorted[j].node), BLKN);	 // prendo soltanto il puntatore marcato come bloccato
					BOOL_CAS(&bucket_p->dwv_sorted[j].node, dw_node, get_marked_node(dw_node, DELN));
				}
			}

			//step >>= 1;

			if(step == 1/*0*/ || is_marked_ref(bucket_p, DELB) || bucket_p->pro)
				break;

			step = 1;
		}

		if(mark_bucket){
			while(
				!is_marked_ref(bucket_p->next, DELB) 
				&& !BOOL_CAS(&bucket_p->next, set_bucket_state(bucket_p->next, EXT), get_marked_ref(set_bucket_state(bucket_p->next, EXT), DELB))
			);
		}
		else if(!bucket_p->pro)
			BOOL_CAS(&bucket_p->pro, 0, 1);	

#if ENABLE_BLOCKING_FLUSH == 1
	}else {
		while(!is_marked_ref(bucket_p->next, DELB) && mark_bucket && !bucket_p->pro);	

		while(
			bucket_p->pro
			&& !BOOL_CAS(&bucket_p->next, set_bucket_state(bucket_p->next, EXT), get_marked_ref(set_bucket_state(bucket_p->next, EXT), DELB))
		);
	}	
#endif

	if(mark_bucket){
		/*
		if(!is_marked_ref(bucket_p->next, DELB)){
			p = true;
			printf("pippo\n");
			while(1);
		}
		*/
		assertf(
			!is_marked_ref(bucket_p->next, DELB), 
			"dw_flush(): bucket non marcato %p %llu\n", bucket_p->next, get_bucket_state(bucket_p->next));
	}
}

void dw_proactive_flush(void *tb, unsigned long long index_vb){
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb *bucket_p = NULL;
	unsigned long long index;
	long int rand;

	lrand48_r(&seedP, &rand);
	index = index_vb + rand % PRO_FLUSH_BUCKET_NUM + PRO_FLUSH_BUCKET_NUM_MIN;

	// calcolo il numero del bucket in modo che sia locale a me
	for(;NODE_HASH(index % h->size) != NID; index++);

	bucket_p = list_remove(&str->heads[index % h->size], index, str->list_tail);

	if(bucket_p == NULL || is_marked_ref(bucket_p->next, DELB) || get_bucket_state(bucket_p->next) != INS)
		return;

	do{	

		if(get_bucket_state(bucket_p->next) == INS)	
			block_ins(bucket_p);

		if(get_bucket_state(bucket_p->next) == ORD)
			apply_transition_ORD_to_EXT(bucket_p
	#if NUMA_DW
			, NODE_HASH(index % h->size) 
	#endif
	);

		if(get_bucket_state(bucket_p->next) == EXT || get_bucket_state(bucket_p->next) == BLK){

			if(get_bucket_state(bucket_p->next) == EXT && !is_marked_ref(bucket_p->next, DELB)){
				//printf("%d\n", bucket_p->valid_elem);
				dw_flush(h, bucket_p, false);
			}

			return;
		}
			
	}while(1);
}

dwb* dw_dequeue(void *tb, unsigned long long index_vb){
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb *bucket_p = NULL;
	int i;

	#if ENABLE_PROACTIVE_FLUSH
		dequeue_num++;
	#endif
// controllare bucket_p->index_vb
	if(last_bucket_p != NULL && last_bucket_p->index_vb == index_vb){
		bucket_p = last_bucket_p;
		cache_hit++;
	}
	else
		bucket_p = list_remove(&str->heads[index_vb % h->size], index_vb, str->list_tail);

	if(bucket_p == NULL || is_marked_ref(bucket_p->next, DELB)){
		return NULL;
	}

	last_bucket_p = bucket_p;

	//assertf(is_marked_ref(bucket_p->next, DELB) == 1, "dw_dequeue(): nodo marcato %p\n", bucket_p->next);

	do{	
		#if ENABLE_PROACTIVE_FLUSH			
			if(dequeue_num > DEQUEUE_NUM_TH){
				dequeue_num = 0;
				dw_proactive_flush(tb, index_vb);
			}
		#endif

		if(NID != NODE_HASH(index_vb % h->size)){// se cerco di estrarre da un nodo non locale per me
			/*
			if(ENABLE_PROACTIVE_FLUSH){
				if(dequeue_num > DEQUEUE_NUM_TH){
					dequeue_num = 0;
					dw_proactive_flush(tb, index_vb);
				}
			}
			*/

			for(i = 0; i < DEQUEUE_WAIT_CICLES && !is_marked_ref(bucket_p->next, DELB); i++);
		}

		/*
		assertf(get_enq_ind(bucket_p->indexes) >= VEC_SIZE && get_bucket_state(bucket_p->next) == EXT,
		 "dw_dequeue():indice estrazione = %d, stato bucket %llu\n", get_enq_ind(bucket_p->indexes), get_bucket_state(bucket_p->next));
		*/
		if(get_bucket_state(bucket_p->next) == INS)	
			block_ins(bucket_p);

		if(get_bucket_state(bucket_p->next) == ORD)
			apply_transition_ORD_to_EXT(bucket_p
	#if NUMA_DW
			, NODE_HASH(index_vb % h->size) 
	#endif
	);

		if(get_bucket_state(bucket_p->next) == EXT || get_bucket_state(bucket_p->next) == BLK){

			if(get_bucket_state(bucket_p->next) == EXT && DISABLE_EXTRACTION_FROM_DW && !is_marked_ref(bucket_p->next, DELB)){
				dw_flush(h, bucket_p, true);
				//return NULL;
			}

			if(is_marked_ref(bucket_p->next, DELB) || bucket_p->valid_elem == 0)
				return NULL;
			else
			//assertf(is_marked_ref(bucket_p->next, DELB) == 1, "dw_dequeue(): nodo marcato %p\n", bucket_p->next);
				return bucket_p;
		}
			
	}while(1);

}

int cmp_node(const void *p1, const void *p2){
	nbnc *node_1 = (nbnc*)p1;
	nbnc *node_2 = (nbnc*)p2;
	pkey_t tmp;

	assertf(!is_none(node_1->node), "cmp_node(): nodo marcato %p %f %f\n", node_1->node, get_node_pointer(node_1->node)->timestamp, node_1->timestamp);
	assertf(!is_none(node_2->node), "cmp_node(): nodo marcato %p %f %f\n", node_2->node, get_node_pointer(node_2->node)->timestamp, node_2->timestamp);

	tmp = (node_1->timestamp - node_2->timestamp);
	return (tmp > 0.0) - (tmp < 0.0);
}






/*
pkey_t dw_extraction(dwb* bucket_p, void** result, pkey_t left_ts, bool remote, bool no_cq_node){
	nbc_bucket_node *dw_node;
	pkey_t dw_node_ts;
	int indexes_enq, deq_cn;

	indexes_enq = get_enq_ind(bucket_p->indexes) << ENQ_BIT_SHIFT;// solo la parte inserimento di indexes
	dw_retry:
					 
	deq_cn = get_deq_ind(bucket_p->indexes); 

	// cerco un possibile indice
	while(	deq_cn < bucket_p->valid_elem && is_deleted(bucket_p->dwv_sorted[deq_cn].node) && 
			!is_moving(bucket_p->dwv_sorted[deq_cn].node) && !is_marked_ref(bucket_p->next, DELB))
		{
			BOOL_CAS(&bucket_p->indexes, (indexes_enq + deq_cn), (indexes_enq + deq_cn + 1));	// aggiorno deq
			deq_cn = get_deq_ind(bucket_p->indexes);
		}

	// controllo se sono uscito perchè è iniziata la resize
	if(is_moving(bucket_p->dwv_sorted[deq_cn].node))
		return GOTO;

	if(deq_cn >= bucket_p->valid_elem || is_marked_ref(bucket_p->next, DELB)){

		//no_dw_node_curr_vb = true;
		if(!is_marked_ref(bucket_p->next, DELB))
			BOOL_CAS(&(bucket_p->next), bucket_p->next, get_marked_ref(bucket_p->next, DELB));
					
		// TODO
		//}else if(bucket_p->dwv[deq_cn].node->epoch > epoch){
		//	goto begin;
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
		
		if(!no_cq_node) no_empty_vb++;
		
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
				//concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

				estr++;
				*result = dw_node->payload;

				//critical_exit();
				#if NUMA_DW || SEL_DW
				if (!remote)
					local_deq++;
				else
					remote_deq++;
				#endif

				return dw_node_ts;
			}else{// provo a vedere se ci sono altri nodi in dw
				conflitti_estr++;

				// controllo se non ci sono riuscito perchè stiamo nella fase di resize
				if(is_moving(bucket_p->dwv_sorted[deq_cn].node)) 
					return GOTO;

				goto dw_retry;
			}
		}
	}

	return EMPTY;
}
*/
