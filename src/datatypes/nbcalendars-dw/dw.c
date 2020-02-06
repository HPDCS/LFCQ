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
void blockIns(dwb*);
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

	bucket_p = getBucketPointer(list_add(str->dwls[index_vb % h->size], index_vb));
	assertf(bucket_p == NULL, "dw_eqnqueue(): bucket inesistente %s\n", "");

	//printf("prova %llu\n", bucket_p->index_vb);
	if(getBucketState(bucket_p->next) == EXT){
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

			if(BOOL_CAS(&bucket_p->dwv[enq_cn].node, NULL, new_node)){
				//while(1);
				ins++;
				bucket_p->dwv[enq_cn].timestamp = new_node->timestamp;
				result = OK;
				break;
			}else
				conflitti_ins++;
		}else
			conflitti_ins++;
	}

	// bucket riempito
	if(getBucketState(bucket_p->next) == INS && getEnqInd(bucket_p->indexes) >= VEC_SIZE)
		blockIns(bucket_p);

	if(getBucketState(bucket_p->next) == ORD)
		doOrd(bucket_p);

	//assertf((result != OK && getBucketState(bucket_p->next) != EXT), "dw_enqueue(): condizione di uscita sbagliata. state %llu, result %d\n", getBucketState(bucket_p->next), result);
	return result;
}

dwb* dw_dequeue(void *tb, unsigned long long index_vb){
	table *h = (table*)tb;
	dwstr *str = h->deferred_work;
	dwb* bucket_p = NULL;

	bucket_p = getBucketPointer(list_remove(str->dwls[index_vb % h->size], index_vb));
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

		if(getBucketState(bucket_p->next) == INS){
		//printf("%llu %llu\n", getBucketState(bucket_p->next), index_vb);	
			blockIns(bucket_p);
			//BOOL_CAS(&bucket_p->indexes, bucket_p->indexes, addEnqInd(bucket_p->indexes, VEC_SIZE));
			//BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, INS), setBucketState(bucket_p->next, ORD));
			//while(1);
		}

		if(getBucketState(bucket_p->next) == ORD)
			doOrd(bucket_p); 

	}while(1);

}

void blockIns(dwb* bucket_p){
	int i, limit;
	int enq_cn;

	//assertf(getDeqInd(bucket_p->indexes) > 0, "blockIns(): deq_cn non nullo %d %p\n", getDeqInd(bucket_p->indexes), bucket_p->next);
	//if(bucket_p->indexes << ENQ_BIT_SHIFT > 0){
		//printf("bloccato in blockIns\n");
		//while(1);
	//	return;
	//}
	
	while((enq_cn = getEnqInd(bucket_p->indexes)) <= VEC_SIZE){// se non l'ha fatto ancora nessuno
		limit = getEnqInd(VAL_CAS(&bucket_p->indexes, enq_cn << ENQ_BIT_SHIFT, ((enq_cn + VEC_SIZE) << ENQ_BIT_SHIFT)));	// aggiungo il massimo
		if(limit <= VEC_SIZE)	// se ci sono riuscito
			BOOL_CAS(&bucket_p->cicle_limit, VEC_SIZE, limit);// imposto quello del bucket
	}
//printf("%llu %d\n",bucket_p->index_vb, bucket_p->cicle_limit);
	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit < 0, "blockIns(): limite ciclo sbagliato %d\n", enq_cn);
	
	for(i = 0; i < bucket_p->cicle_limit; i++)
		BOOL_CAS(&bucket_p->dwv[i].node, NULL, (nbc_bucket_node*)BLKN);	

	BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, INS), setBucketState(bucket_p->next, ORD));
}

void doOrd(dwb* bucket_p){
	nbnc *old_dwv, *aus_ord_array;

	assertf(bucket_p->cicle_limit > VEC_SIZE || bucket_p->cicle_limit < 0, "doOrd(): limite del ciclo fuori dal range %s\n", "");

	if(bucket_p->cicle_limit == 0){// nulla da ordinare, ritorno subito
		BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, ORD), setBucketState(bucket_p->next, EXT));
		return;
	}

	old_dwv = bucket_p->dwv;

	aus_ord_array = gc_alloc(ptst, gc_aid[2]);
	if(aus_ord_array == NULL)	error("Non abbastanza memoria per allocare un array dwv in ORD\n");
	else{
		//printDWV(VEC_SIZE, old_dwv);
		//printf(" prima dell'ordinamento\n");

		memcpy(aus_ord_array, bucket_p->dwv, VEC_SIZE * sizeof(nbnc));		// copia dell'array
		qsort(aus_ord_array, bucket_p->cicle_limit, sizeof(nbnc), cmp_node);					// ordino

		//printDWV(VEC_SIZE, aus_ord_array);
		//printf(" dopo l'ordinamento\n");

		// cerco di sostituirlo all'originale
		if(BOOL_CAS(&bucket_p->dwv, old_dwv, aus_ord_array)){
			BOOL_CAS(&bucket_p->next, setBucketState(bucket_p->next, ORD), setBucketState(bucket_p->next, EXT));	
			gc_free(ptst, old_dwv, gc_aid[2]);
		}
		else
			gc_free(ptst, aus_ord_array, gc_aid[2]);	
	}	
}

int cmp_node(const void *p1, const void *p2){
	nbnc *node_1 = (nbnc*)p1;
	nbnc *node_2 = (nbnc*)p2;
	pkey_t tmp;

	if(getNodeState(node_1->node) == BLKN || getNodeState(node_2->node) == BLKN)
		return ((getNodeState(node_1->node) == BLKN) - (getNodeState(node_2->node) == BLKN));
	else{
		tmp = (node_1->timestamp - node_2->timestamp);
		return (tmp > 0.0) - (tmp < 0.0);
	}
}