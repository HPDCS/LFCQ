#include "common_nb_calqueue.h"

extern __thread long ins;
extern __thread long conflitti_ins;

extern bool is_marked_ref(dwb*);

// stato
unsigned long long getBucketState(dwb*);
unsigned long long getNodeState(nbc_bucket_node*);
dwb* setBucketState(dwb*, unsigned long long);
// indici
int getEnqInd(int);
int getDeqInd(int);
int addEnqInd(int, int);
// puntatori
dwb* getBucketPointer(dwb*);
nbc_bucket_node* getNodePointer(nbc_bucket_node*);
// ausiliarie
void blockIns(dwb*, int);
void doOrd(dwb*);
int cmp_node(const void*, const void*);
void printDWV(int, nbnc*);

int inc=0;

unsigned long long getBucketState(dwb* bucket){return (((unsigned long long)bucket & BUCKET_STATE_MASK) >> 1);}
unsigned long long getNodeState(nbc_bucket_node* node){return ((unsigned long long)node & NODE_STATE_MASK);}
dwb* setBucketState(dwb* bucket, unsigned long long state){return (dwb*)(((unsigned long long)bucket & BUCKET_PTR_MASK_WM) | (state << 1));}
int getEnqInd(int indexes){return indexes >> ENQ_BIT_SHIFT;}
int getDeqInd(int indexes){return indexes & DEQ_IND_MASK;}
int addEnqInd(int indexes, int num){return indexes + (num << ENQ_BIT_SHIFT);}
dwb* getBucketPointer(dwb* bucket){return (dwb*)((unsigned long long)bucket & BUCKET_PTR_MASK);}
nbc_bucket_node* getNodePointer(nbc_bucket_node* node){return (nbc_bucket_node*)((unsigned long long)node & NODE_PTR_MASK);}

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

int dw_enqueue(void *tb, unsigned long long index_vb, nbc_bucket_node *new_node){
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb* bucket_p;
	int result = ABORT;
	int indexes, enq_cn; 

	bucket_p = getBucketPointer(list_add(&str->heads[index_vb % h->size], index_vb, str->list_tail));
	assertf(bucket_p == NULL, "dw_enqueue(): bucket inesistente %s\n", "");

	//printf("prova %llu\n", bucket_p->index_vb);
	if(getBucketState(bucket_p->next) == EXT)
	{
		//while(1);
		return result;
	}
	//assertf(is_marked_ref(bucket_p->next) == 1, "dw_eqnqueue(): bucket bucket marcato %s\n", "");
	//printf("prova %u, state %llu , indexes %d\n", index_vb, getBucketState(bucket_p->next), bucket_p->indexes);
	//printf("prova %llu\n", bucket_p->index_vb);
	while((enq_cn = getEnqInd(bucket_p->indexes)) < bucket_p->cicle_limit && getBucketState(bucket_p->next) == INS){

		indexes = enq_cn << ENQ_BIT_SHIFT;	// per escludere la parte inserimento

		// provo ad aggiornare
		if(BOOL_CAS(&bucket_p->indexes, indexes, addEnqInd(indexes, 1))){// se presenta la parte deq diversa da zero non ci riesce

			// se sto qui enq_cn deve essere quello giusto
			assertf(enq_cn < 0 || enq_cn > bucket_p->cicle_limit, "dw_enqueue(): indice enqueue fuori range consentito %s\n", "");

			if(BOOL_CAS(&bucket_p->dwv[enq_cn].node, NULL, new_node) && BOOL_CAS_DOUBLE((unsigned long long*)&bucket_p->dwv[enq_cn].timestamp, INV_TS, new_node->timestamp) ){
				assertf(new_node == NULL, "dw_enqueue(): nuovo nodo nullo %s\n", "");
				assertf(bucket_p->dwv[enq_cn].node == NULL, "dw_enqueue(): nodo inserito nullo %s\n", "");
				//while(1);
				ins++;
				bucket_p->dwv[enq_cn].timestamp = new_node->timestamp;
				result = OK;
				break;
			}else{
				conflitti_ins++;
				if(getNodeState(bucket_p->dwv[enq_cn].node) > 0)
					break;
					//return result;
			}
		}else
			conflitti_ins++;
	}

	// bucket riempito
	if(getBucketState(bucket_p->next) == INS && getEnqInd(bucket_p->indexes) >= VEC_SIZE)
		blockIns(bucket_p, 1);

	if(getBucketState(bucket_p->next) == ORD)
		doOrd(bucket_p);

//	assertf(getEnqInd(bucket_p->indexes) >= VEC_SIZE && getBucketState(bucket_p->next) != EXT, "dw_enqueue(): bucket pieno e non in estrazione. stato %llu, enq_cn %d, enq_cn salvato %d\n", getBucketState(bucket_p->next), enq_cn, getEnqInd(bucket_p->indexes));

	//assertf((result != OK && getBucketState(bucket_p->next) != EXT), "dw_enqueue(): condizione di uscita sbagliata. state %llu, result %d\n", getBucketState(bucket_p->next), result);
	return result;
}

dwb* dw_dequeue(void *tb, unsigned long long index_vb){
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb* bucket_p = NULL;

	bucket_p = getBucketPointer(list_remove(&str->heads[index_vb % h->size], index_vb, str->list_tail));
	if(bucket_p == NULL || is_marked_ref(bucket_p->next))
		return NULL;

	//assertf(is_marked_ref(bucket_p->next) == 1, "dw_dequeue(): nodo marcato %p\n", bucket_p->next);
	//printf("DQ:TID %d index_pb %llu\n", TID, index_vb);
	do{
//		printf("DQ: index_pb %llu\n", index_vb);
		
		if(getBucketState(bucket_p->next) == EXT){ 
			if(is_marked_ref(bucket_p->next))
				return NULL;
			else
			//assertf(is_marked_ref(bucket_p->next) == 1, "dw_dequeue(): nodo marcato %p\n", bucket_p->next);
				return bucket_p;
		}

		/*assertf(getEnqInd(bucket_p->indexes) >= VEC_SIZE && getBucketState(bucket_p->next) == EXT,
		 "dw_dequeue():indice estrazione = %d, stato bucket %llu\n", getEnqInd(bucket_p->indexes), getBucketState(bucket_p->next));
*/
		if(getBucketState(bucket_p->next) == INS){
		//printf("%llu %llu\n", getBucketState(bucket_p->next), index_vb);	
			blockIns(bucket_p, 0);
			//BOOL_CAS(&bucket_p->indexes, bucket_p->indexes, addEnqInd(bucket_p->indexes, VEC_SIZE));
			//BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, INS), setBucketState(bucket_p->next, ORD));
			//while(1);
		}

		if(getBucketState(bucket_p->next) == ORD)
			doOrd(bucket_p); 

	}while(1);

}

void blockIns(dwb* bucket_p, int from){
	int i, limit;
	int enq_cn;

	//assertf(getDeqInd(bucket_p->indexes) > 0, "blockIns(): deq_cn non nullo %d %p\n", getDeqInd(bucket_p->indexes), bucket_p->next);
	//if(bucket_p->indexes << ENQ_BIT_SHIFT > 0){
		//printf("bloccato in blockIns\n");
		//while(1);
	//	return;
	//}

	//if(getEnqInd(bucket_p->indexes) == VEC_SIZE)
	//	printf("numero bucket %llu %p da dw_dequeue %d\n", bucket_p->index_vb, bucket_p->dwv[VEC_SIZE - 1].node, from);
	//assertf((getEnqInd(bucket_p->indexes) == VEC_SIZE && from == 0), "blockIns(): bucket pieno da dw_dequeue. ultimo %p \n", bucket_p->dwv[VEC_SIZE - 1].node);
	/*
	if(getEnqInd(bucket_p->indexes) == VEC_SIZE && from == 0)
		printf("blockIns(): bucket pieno da dw_dequeue. ultimo %p , index_vb %llu \n", bucket_p->dwv[VEC_SIZE - 1].node, bucket_p->index_vb);
	if(getEnqInd(bucket_p->indexes) == VEC_SIZE && from == 1)
		printf("blockIns(): bucket pieno da dw_enqueue. ultimo %p , index_vb %llu \n", bucket_p->dwv[VEC_SIZE - 1].node, bucket_p->index_vb);
	*/
	if(getEnqInd(bucket_p->indexes) == VEC_SIZE)
		BOOL_CAS(&bucket_p->from_enq, -1, from);

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
//printf("%llu %d\n",bucket_p->index_vb, bucket_p->cicle_limit);
	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit < 0, "blockIns(): limite ciclo sbagliato %d\n", enq_cn);
	assertf(enq_cn < VEC_SIZE, "blockIns(): enq_cn non è stato incrementato %d\n", enq_cn);
	for(i = 0; i < bucket_p->cicle_limit; i++){
		if(bucket_p->dwv[i].node == NULL) BOOL_CAS(&bucket_p->dwv[i].node, NULL, (nbc_bucket_node*)BLKN);
		if(bucket_p->dwv[i].timestamp == INV_TS) BOOL_CAS_DOUBLE(&bucket_p->dwv[i].timestamp,INV_TS,INFTY);
		
		assertf(bucket_p->dwv[i].node == NULL, "blockIns(): nodo non marcato come bloccato %p %f\n", bucket_p->dwv[i].node, bucket_p->dwv[i].timestamp);
	}

	BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, INS), setBucketState(bucket_p->next, ORD));
}

void doOrd(dwb* bucket_p){
	nbnc *old_dwv, *aus_ord_array;

	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit < 0, "doOrd(): limite del ciclo fuori dal range %s\n", "");
	assertf((bucket_p->cicle_limit == VEC_SIZE && bucket_p->dwv[VEC_SIZE - 1].node == NULL), "doOrd(): falso bucket virtuale pieno. index = %llu\n", bucket_p->index_vb);
	if(bucket_p->cicle_limit == 0){// nulla da ordinare, ritorno subito
		BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, ORD), setBucketState(bucket_p->next, EXT));
		return;
	}

	old_dwv = bucket_p->dwv;
	if(bucket_p->dwv_sorted != NULL) return;
	
	aus_ord_array = gc_alloc(ptst, gc_aid[2]);
	if(aus_ord_array == NULL)	error("Non abbastanza memoria per allocare un array dwv in ORD\n");
	else{
	/*if(bucket_p->cicle_limit == VEC_SIZE){
			printDWV(VEC_SIZE, old_dwv);
			printf(" prima dell'ordinamento\n");
		}
	*/
		//memcpy(aus_ord_array, bucket_p->dwv, VEC_SIZE * sizeof(nbnc));		// copia dell'array
		memcpy(aus_ord_array, old_dwv, VEC_SIZE * sizeof(nbnc));
		qsort(aus_ord_array, bucket_p->cicle_limit, sizeof(nbnc), cmp_node);					// ordino
	/*if(bucket_p->cicle_limit == VEC_SIZE){
			printDWV(VEC_SIZE, aus_ord_array);
			printf(" dopo l'ordinamento\n");
	}*/
		// cerco di sostituirlo all'originale
		if(BOOL_CAS(&bucket_p->dwv_sorted, NULL, aus_ord_array)){
			BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, ORD), setBucketState(bucket_p->next, EXT));	
			//gc_free(ptst, old_dwv, gc_aid[2]);
		}
		else
			gc_free(ptst, aus_ord_array, gc_aid[2]);	
	}	
}

int cmp_node(const void *p1, const void *p2){
	nbnc *node_1 = (nbnc*)p1;
	nbnc *node_2 = (nbnc*)p2;
	pkey_t tmp;

	assertf(getNodeState(node_1->node) == DELN, "cmp_node(): nodo marcato come eliminato %p %f %f\n", node_1->node, getNodePointer(node_1->node)->timestamp, node_1->timestamp);
	assertf(getNodeState(node_2->node) == DELN, "cmp_node(): nodo marcato come eliminato %p %f %f\n", node_2->node, getNodePointer(node_2->node)->timestamp, node_2->timestamp);

/*
	if(getNodeState(node_1->node) == DELN || getNodeState(node_2->node) == DELN){
//		printf("SORTING-NOTO\n");
		return 0;
	}

	if(getNodeState(node_1->node) == BLKN || getNodeState(node_2->node) == BLKN){
	//	printf("SORTING\n");
		return ((getNodeState(node_1->node) == BLKN) - (getNodeState(node_2->node) == BLKN));
	}
	else
	*/ 
	{
		tmp = (node_1->timestamp - node_2->timestamp);
		return (tmp > 0.0) - (tmp < 0.0);
	}
}
