#include "common_nb_calqueue.h"
#include <time.h>

void blockIns(dwn*, int);
void doOrd(dwn*, int);
void insertionSort(nbnc**, int);
void printDWV(int, nbnc*);

extern __thread bool from_block_table;
//extern __thread int 		estratti;

#if DW_USAGE
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

void dw_block_table(void* q, unsigned int start){
	
	nbnc node_cont;
	nb_calqueue* queue;
	table* h;
	dwn* dw_list_node = NULL;
	dwl** dw_list_arr = NULL; 
	unsigned long long prev_state, curr_state;	// stato precedente e corrente
	unsigned int size, cn;
	int vec_size, index_pb, i;
	bool another_thread;	// indica se il bucket virtuale in esame l'ha bloccato qualcun altro

	queue = (nb_calqueue*)q;
	h = queue->hashtable;
	size = h->size;
	dw_list_arr = h->deferred_work->dwls;
	vec_size = h->deferred_work->vec_size;

	for(i = 0; i < size; i++){
		index_pb = ((i + start) % size); // calcolo l'indice del bucket fisico

		while(1){ // riprovo finche il bucket fisico non ha più nodi dei bucket virtuali
			
			// ottengo il nodo che mi rappresenta dw del bucket virtuale
			dw_list_node = list_remove(dw_list_arr[index_pb], -1); // con -1 dovrebbe ritornare il primo disponibile

			if(dw_list_node == NULL)// non ci sono elementi
				break;				// prendero il bucket fisico successivo

			DW_AUDIT{
				printf("BLOCK_TABLE: TID %d, size = %d , blocco la coda %d, stato = %llu , elementi %d \n\n", TID, size, index_pb, DW_GET_STATE(dw_list_node->next), dw_list_node->enq_cn);
				fflush(stdout);
			}

			if(DW_GET_STATE(dw_list_node->next) != BLK){ // se non è già stato contrassegnato come bloccato

				another_thread = false;

				do{						
					if((curr_state = DW_GET_STATE(dw_list_node->next)) == BLK){
						another_thread = true; // qualcuno ci è riuscito
						break;
					}
				}while((prev_state = DW_GET_STATE(VAL_CAS(&dw_list_node->next, DW_SET_STATE(dw_list_node->next, curr_state), DW_SET_STATE(dw_list_node->next, BLK)))) != curr_state);
					
				if(!another_thread){  // se sono stato io a cambiare lo stato devo proseguire il lavoro					
					
					if(prev_state == INS){ // può darsi che ci fosse qualcuno che stava inserendo in questo momento

						// impedisco gli inserimenti
						//blockIns(dw_list_node, vec_size);

						// eseguo un ordinamento diretto, nessun'altro poteva entrare in concorrenza a me
						qsort(dw_list_node->dwv, dw_list_node->enq_cn, sizeof(nbnc), cmp_node);

					}else if(prev_state == ORD)
						doOrd(dw_list_node, vec_size);

					if(dw_list_node->enq_cn - dw_list_node->deq_cn > 0){ // se ci sono elementi a disposizione allora posso estrarre(potrebbe essere che alcuni non sono finalizzati)
						printf("flush\n");
						from_block_table = true;

						do{
							cn = FETCH_AND_ADD(&dw_list_node->deq_cn, 1);

							DW_AUDIT{
								printf("BLOCK_TABLE: cn corrente %d\n", cn);
								fflush(stdout);
							}

							if(cn > dw_list_node->enq_cn) // DEBUG
								printf("cn di dw_block_table vale %d\n", cn);
							else{
								node_cont = dw_list_node->dwv[cn];

								if(node_cont.node != NULL/* && !DW_NODE_IS_DEL(node_cont) && !DW_NODE_IS_BLK(node_cont)*/){
									pq_enqueue(queue, -1.0, node_cont.node);	// chiamo la funzione per l'inserimento effettivo nella coda finale

									//__sync_fetch_and_add(&h->e_counter.count, 1); // aggiorno il contatore degli inserimenti
								}
							}

						}while(cn < dw_list_node->enq_cn - 1);

						from_block_table = false;
					}					
				}
			}
		}
	}
}

int dw_enqueue(void* q, nbc_bucket_node* new_node, unsigned long long new_node_vb){

	dwn* dw_list_node = NULL;	// nodo di deferred work in cui verrà inserito l'evento
	nb_calqueue *queue = (nb_calqueue*)q;
	table *h;
	unsigned int cn;
	int vec_size, result = ABORT;	// di default non riuscito
	//nbnc new_node_cont;
				
	h = queue->hashtable;
	vec_size = h->deferred_work->vec_size;

	// ottengo il nodo che mi rappresenta dw del bucket virtuale(se non esiste viene creato)
	dw_list_node = list_add(h->deferred_work->dwls[((unsigned int) new_node_vb) % h->size], (long)new_node_vb, vec_size);

	if(DW_GET_STATE(dw_list_node->next) >= EXT) // se EXT o maggiore ritorno subito
		return result;

	//if((cn = dw_list_node->enq_cn) < vec_size && DW_GET_STATE(dw_list_node->next) == INS){// se è in inserimento proviamo ad inserire
	while((cn = dw_list_node->enq_cn) < vec_size && DW_GET_STATE(dw_list_node->next) == INS){// se è in inserimento proviamo ad inserire

		//if((cn = FETCH_AND_ADD(&dw_list_node->enq_cn, 1)) < vec_size){// cerco di prendere un indice per eseguire l'inserimento
		if(BOOL_CAS(&dw_list_node->enq_cn, cn, cn + 1)){// cerco di aggiornare l'indice

			DW_AUDIT{
				printf("INSERIMENTO:TID %d, bucket %ld, numero elementi %d, stato = %llu\n",TID, (long)new_node_vb % h->size, cn+1, DW_GET_STATE(dw_list_node->next));
				fflush(stdout);
			}

			//new_node_cont = gc_alloc(ptst, gc_aid[3]);
			//new_node_cont.node = new_node;
			//new_node_cont.timestamp = new_node->timestamp;

			dw_list_node->dwv[cn].node = new_node;
			dw_list_node->dwv[cn].timestamp = new_node->timestamp;

			//if(BOOL_CAS(&dw_list_node->dwv[cn], NULL, new_node_cont)){ // cerco di inserire l'elemento
				result = OK; 
				break;
			//}
			/*else
				gc_free(ptst, new_node_cont, gc_aid[3]);
				*/
		}

		// DEBUG
		if(cn >= vec_size)
			printf("DW_ENQUEUE: cn di enqueue vale %d\n", cn);
			
	}

	// se la coda è piena cerco di bloccarla
	if(dw_list_node->enq_cn >= vec_size && DW_GET_STATE(dw_list_node->next) == INS){ // se dw è piena e sto ancora in INS
		//printf("la coda è piena\n");
		//fflush(stdout);
		//blockIns(dw_list_node, vec_size); // impedisco gli inserimenti

		BOOL_CAS(&dw_list_node->next, DW_SET_STATE(dw_list_node->next, INS), DW_SET_STATE(dw_list_node->next, ORD));	// imposto a ORD
	}

	if(DW_GET_STATE(dw_list_node->next) == ORD){
		//printf("chiamo ord enq\n");
		doOrd(dw_list_node, vec_size);
	}

	return result;
}

dwn* dw_dequeue(void* q, unsigned long long index_vb){
	
	dwn* dw_list_node;
	nb_calqueue *queue;
	table* h;	
	int vec_size;

	queue = (nb_calqueue*)q;
	h = queue->hashtable;
	vec_size = h->deferred_work->vec_size;

	//printf("dw_dequeue %llu \n", index_vb);
	dw_list_node = list_remove(h->deferred_work->dwls[index_vb % (h->size)], (long)index_vb);

	if(dw_list_node == NULL){ // non ci sono elementi
		//printf("Returning NULL\n");
		return dw_list_node;	
	}

	if(index_vb != dw_list_node->index_vb) {printf("Returning a bucket %ld less than current %llu\n", dw_list_node->index_vb, index_vb);}
	
	DW_AUDIT{
		printf("ESTRAZIONE: TID %d: current %d , index_pb %ld\n", TID, dw_list_node->enq_cn, (long)(index_vb % (h->size)));	
		fflush(stdout);
	}

	while(1){

		if(DW_GET_STATE(dw_list_node->next) >= EXT){
			//printf("stato deq EXT\n");
			//fflush(stdout);
			break;
		}

		if(DW_GET_STATE(dw_list_node->next) == INS){ // se sto in INS
			//printf("chiamo blockIns deq %lld\n",index_vb);
			//blockIns(dw_list_node, vec_size);// impedisco gli inserimenti
			BOOL_CAS(&dw_list_node->next, DW_SET_STATE(dw_list_node->next, INS), DW_SET_STATE(dw_list_node->next, ORD));	// imposto a ORD
		}

		if(DW_GET_STATE(dw_list_node->next) == ORD){
			//printf("chiamo ord deq\n");
			/*
			printf("prima di doOrd()\n");
			printDWV(vec_size, dw_list_node->dwv);
			printf("\n");
			*/
			doOrd(dw_list_node, vec_size);
		}
	}

	return dw_list_node;
}

void doOrd(dwn* dw_list_node, int size){
	nbnc *old_dwv, *aus_ord_array;
	int j;

	old_dwv = dw_list_node->dwv;

	// cerco di costruirmi una copia
	//res_mem_posix = posix_memalign((void**)(&aus_ord_array), CACHE_LINE_SIZE, size * sizeof(nbc_bucket_node*));
	aus_ord_array = gc_alloc(ptst, gc_aid[2]);
	if(aus_ord_array == NULL)	error("Non abbastanza memoria per allocare un array dwv in ORD\n");
	else{
	/*
		printf("index: %ld, numero elementi: %d\n", dw_list_node->index_vb, dw_list_node->enq_cn);
		printDWV(size, dw_list_node->dwv);
		printf(" prima di memcpy\n");
*/
		memcpy(aus_ord_array, dw_list_node->dwv, size * sizeof(nbnc));
/*
		printDWV(size, aus_ord_array);
		printf(" dopo memcpy, prima di ordinare\n");
*/		
		qsort(aus_ord_array, dw_list_node->enq_cn/*size*/, sizeof(nbnc), cmp_node);	// ordino
		//insertionSort(aus_ord_array, dw_list_node->enq_cn);
/*
		printDWV(size, aus_ord_array);
		printf(" dopo l'ordinamento\n");
*/
		// cerco di sostituirlo all'originale
		if(BOOL_CAS(&dw_list_node->dwv, old_dwv, aus_ord_array)){
			BOOL_CAS(&dw_list_node->next, DW_SET_STATE(dw_list_node->next, ORD), DW_SET_STATE(dw_list_node->next, EXT));	// imposto a EXT(se ordino da block_table non funzionerà)
			gc_free(ptst, old_dwv, gc_aid[2]);
		}
		else
			gc_free(ptst, aus_ord_array, gc_aid[2]);
			//free(aus_ord_array);		
	}	
}

void blockIns(dwn* dw_list_node, int size){
	int j;
	
	/*
	for(j = 0;j < dwgp->vec_size;j++)
		printf("%p ", dwbp->dwv[j]);
			
	printf(" prima di blocco\n");	
	*/
	
	for(j = 0; j < dw_list_node->enq_cn/*size*/; j++){ 
		//BOOL_CAS(&dw_list_node->dwv[j], NULL, (nbnc*)((unsigned long long)dw_list_node->dwv[j] | BLKN));
	}
	
	/*
	for(j = 0;j < dwgp->vec_size;j++)
		printf("%p ", dwbp->dwv[j]);
		
	printf(" dopo il blocco\n");
	*/
}

int chiamate = 0;
// ordinamento in ordine decrescente
int cmp_node(const void *p1, const void *p2){
	//nbnc **node_1, **node_2;
	pkey_t tmp;
	//unsigned long long t;
	nbnc *node_1 = (nbnc*)p1;
	nbnc *node_2 = (nbnc*)p2;
	//node_2 = (nbnc**)p2;
	chiamate++;

	/*if(DW_NODE_IS_BLK(*node_1) && DW_NODE_IS_BLK(*node_2))
		return 0;
	else if(DW_NODE_IS_BLK(*node_1))
		return 1;
	else if(DW_NODE_IS_BLK(*node_2))
		return -1;
	else*/
	//if(DW_NODE_IS_BLK(node_1) || DW_NODE_IS_BLK(node_2))
	//	return (DW_NODE_IS_BLK(node_1) - DW_NODE_IS_BLK(node_2));
	//if(node_1->node == NULL || node_2->node == NULL)
	//	return ((node_1->node == NULL) - (node_2->node == NULL));
	//else{
		tmp = (node_1->timestamp - node_2->timestamp);
  		//return (node_1->timestamp - node_2->timestamp) > 0.0 ? 1 : -1; // crescente
  		//while(1);
		return (tmp > 0.0) - (tmp < 0.0);
	//}
}

/*
void insertionSort(nbnc **number, int count){
int i, j;
nbnc* temp = NULL;

   for(i=1;i<count;i++){
      temp=number[i];
      j=i-1;
      while((j>=0) && (temp->timestamp < number[j]->timestamp)){
         number[j+1]=number[j];
         j=j-1;
      }
      number[j+1]=temp;
   }
}*/

#endif
