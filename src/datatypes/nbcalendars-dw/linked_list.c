#include "common_nb_calqueue.h"
#include "linked_list.h"

#if NUMA_DW
dwb* new_node(long long, dwb*, unsigned int);
#else
dwb* new_node(long long, dwb*);
#endif
dwb* list_search(dwb*, long long, dwb**, int, dwb*);
bool is_marked_ref(dwb*);
dwb* get_marked_ref(dwb*);

extern unsigned long long getBucketState(dwb*);
extern dwb* setBucketState(dwb*, unsigned long long);
extern dwb* getBucketPointer(dwb*);
extern nbc_bucket_node* getNodePointer(nbc_bucket_node*);
extern int getEnqInd(int);
extern int getDeqInd(int);
extern bool isDeleted(nbc_bucket_node*);
extern bool isMoved(nbc_bucket_node*);

bool is_marked_ref(dwb* bucket){return (bool)((unsigned long long)bucket & 0x1ULL);}
dwb* get_marked_ref(dwb* bucket){return (dwb*)((unsigned long long)bucket | 0x1ULL);}

dwb* list_search(dwb *head, long long index_vb, dwb** left_node, int mode, dwb* list_tail) {
 	int i;
 	dwb *left_node_next, *right_node;
  	left_node_next = right_node = NULL;

  if(mode){	              
	  dwb *t = head->next;

	  printf("%d: valore %llu | ",	TID, index_vb);
	  while(t != list_tail){
	    printf("%p %llu %d %lld %d %d| ", t, t->index_vb, (int)is_marked_ref(t->next), getBucketState(t->next), t->cicle_limit, getDeqInd(t->indexes));  
	    t = getBucketPointer(t->next);
	  }
	  printf("\n");
	  fflush(stdout);
	}

  
  	while(1) {

    	dwb *t = head;
    	dwb *t_next = head->next;
    
    	while (is_marked_ref(t_next) || (t->index_vb < index_vb)) {
      	
      		if (!is_marked_ref(t_next)) {
        		(*left_node) = t;
        		left_node_next = t_next;
      		}
      
      		t = getBucketPointer(t_next);
      		
      		if (t == list_tail) break;
      		t_next = t->next;
    	}
    	
    	right_node = t;

    	if(getBucketPointer(left_node_next) == right_node){
    		if(!is_marked_ref(right_node->next)){
   // 			assertf(is_marked_ref(right_node->next), "list_search(): nodo marcato %s\n", "");
         		return right_node;
         	}
    	}
    	else{

	      	if(BOOL_CAS(&((*left_node)->next), left_node_next, setBucketState(right_node, getBucketState(left_node_next)))) {

	      		while((left_node_next = getBucketPointer(left_node_next)) != right_node){

			    	for(i = 0; i < left_node_next->valid_elem; i++){

			    		assertf(left_node_next->dwv_sorted[i].timestamp == INV_TS, "INV_TS while releasing memory%s\n", "");
                        assertf(!isDeleted(left_node_next->dwv_sorted[i].node) && !isMoved(left_node_next->dwv_sorted[i].node), "nodo non marcato come eliminato o trasferito%s\n", ""); 
                        assertf(getNodePointer(left_node_next->dwv_sorted[i].node) == NULL || left_node_next->dwv_sorted[i].timestamp == INFTY, "nodo non valido per rilascio%s\n", ""); 

            			// if(getNodePointer(left_node_next->dwv_sorted[i].node) != NULL && left_node_next->dwv_sorted[i].timestamp != INFTY)
                      	if(isDeleted(left_node_next->dwv_sorted[i].node))
                            node_free(getNodePointer(left_node_next->dwv_sorted[i].node));
                    }

			      	gc_free(ptst, left_node_next->dwv, gc_aid[2]);
			      	gc_free(ptst, left_node_next->dwv_sorted, gc_aid[2]);
			      	gc_free(ptst, left_node_next, gc_aid[1]);			      	
			      	
			      	left_node_next = left_node_next->next;
			    }

	        	if (!is_marked_ref(right_node->next))
	          		return right_node;
	      	}
    	}
  	}
}

#if NUMA_DW
dwb* new_node(long long index_vb, dwb *next, unsigned int numa_node)
#else
dwb* new_node(long long index_vb, dwb *next)
#endif
{	
	int i;

	#if NUMA_DW
	dwb* node = gc_alloc_node(ptst, gc_aid[1], numa_node);
	#else
  	dwb* node = gc_alloc(ptst, gc_aid[1]);
  	#endif

  	if(node != NULL){

		#if NUMA_DW
		node->dwv = gc_alloc_node(ptst, gc_aid[2], numa_node);
		#else
		node->dwv = gc_alloc(ptst, gc_aid[2]);
		#endif
        
  		if(node->dwv == NULL){
  			gc_free(ptst, node, gc_aid[1]);
  			return NULL;
  		}

        // inizializzazione dell'array allocato
    	for(i = 0; i < VEC_SIZE; i++){
  			node->dwv[i].node = NULL;
  			node->dwv[i].timestamp = INV_TS;	
  		}

        node->indexes = 0;
        node->cicle_limit = VEC_SIZE;
        node->valid_elem = VEC_SIZE;
  		node->index_vb = index_vb;
  		node->next = next;
        node->dwv_sorted = NULL; 
	 }
  	
  	return node;
}

/*
int new_list(dwl* list){
	int result = 0;

  	// inizializzazione
  	list->head = new_node(0, NULL, false);
  	list->tail = new_node(ULLONG_MAX, NULL, false);
  	
  	if(list->head == NULL || list->tail == NULL)
  		result = 1;
  	else
  		list->head->next = list->tail;
  	
  	return result;
}
*/

#if NUMA_DW
dwb* list_add(dwb *head, long long index_vb, int numa_node, dwb* list_tail)
#else
dwb* list_add(dwb *head, long long index_vb, dwb* list_tail)
#endif
{

	unsigned long long state;
	dwb *right, *left, *new_elem;
  	right = left = new_elem = NULL;

  	while(1){
    	right = list_search(head, index_vb, &left, 0, list_tail);
    	if (right != list_tail && right->index_vb == index_vb){
      		return right;
    	}

    	if(new_elem == NULL){
    		#if NUMA_DW
    		new_elem = new_node(index_vb, NULL, numa_node);
    		#else
    		new_elem = new_node(index_vb, NULL);
    		#endif
    	}

    	new_elem->next = right;

    	state = getBucketState(left->next);

    	if (BOOL_CAS(&(left->next), setBucketState(right, state) , setBucketState(new_elem, state))){
      		return new_elem;
    	}
  	}
}

dwb* list_remove(dwb *head, long long index_vb, dwb* list_tail){

  	dwb* right, *left, *right_succ;
  	right = left = right_succ = NULL;

  	while(1){
    	right = list_search(head, index_vb, &left, 0, list_tail);
    
    	// check if we found our node
        if (right == list_tail || (index_vb >= 0 && right->index_vb != index_vb)){// se vuota o non c'è il nodo cercato
    	//if (right == list_tail || right->index_vb != index_vb){
      		return NULL;
    	}
    
    	right_succ = right->next;
    	//assertf(is_marked_ref(right_succ), "list_remove(): ritornato un nodo marcato %s\n", "");
    	//assertf(getEnqInd(right->indexes) > VEC_SIZE && ((getEnqInd(right->indexes) - VEC_SIZE) < getDeqInd(right->indexes)), 
    	//	"TID %d:list_remove(): indice di estrazione troppo grande: estrazione %d, inserimento %d\n", TID, getDeqInd(right->indexes), (getEnqInd(right->indexes) - VEC_SIZE));
    	if (!is_marked_ref(right_succ) && (right->valid_elem == getDeqInd(right->indexes))){
    		//printf("prima di marcare\n");
      		if (BOOL_CAS(&(right->next), right_succ, get_marked_ref(right_succ))){
      		//	printf("marcato %llu\n", index_vb);
                if(index_vb >= 0) // non posso prendere il successivo, cerco un vb in particolare
            		return NULL;
	      	}
    	}else if(!is_marked_ref(right_succ))
	      	return right;
  	}
}