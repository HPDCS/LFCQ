#include "common_nb_calqueue.h"
#include "linked_list.h"

#if NUMA_DW
dwb* new_node(long long, dwb*, unsigned int);
#else
dwb* new_node(long long, dwb*);
#endif
dwb* list_search(dwb*, long long, dwb**, int, dwb*);
bool is_marked_ref(dwb*, unsigned long long);
dwb* get_marked_ref(dwb*, unsigned long long);

extern unsigned long long get_bucket_state(dwb*);
extern dwb* set_bucket_state(dwb*, unsigned long long);
extern dwb* get_bucket_pointer(dwb*);
extern nbc_bucket_node* get_node_pointer(nbc_bucket_node*);
extern int get_deq_ind(int);
extern bool is_deleted(nbc_bucket_node*);
extern bool is_moved(nbc_bucket_node*);
extern bool is_none(nbc_bucket_node*);
extern bool is_blocked(nbc_bucket_node*);

bool is_marked_ref(dwb* bucket, unsigned long long mark){return (bool)((unsigned long long)bucket & mark);}
dwb* get_marked_ref(dwb* bucket, unsigned long long mark){return (dwb*)((unsigned long long)bucket | mark);}

dwb* list_search(dwb *head, long long index_vb, dwb** left_node, int mode, dwb* list_tail) {
 	int i;
 	dwb *left_node_next, *right_node;
  	left_node_next = right_node = NULL;

  if(mode){	              
	  dwb *t = head->next;

	  printf("%d: valore %llu | ",	TID, index_vb);
	  while(t != list_tail){
	    printf("%p %llu %d %lld %d %d| ", t, t->index_vb, (int)is_marked_ref(t->next, DELB), get_bucket_state(t->next), t->cicle_limit, get_deq_ind(t->indexes));  
	    t = get_bucket_pointer(t->next);
	  }
	  printf("\n");
	  fflush(stdout);
	}

  
  	while(1) {

    	dwb *t = head;
    	dwb *t_next = head->next;
    
    	while (is_marked_ref(t_next, DELB) || (t->index_vb < index_vb)) {
      	
      		if (!is_marked_ref(t_next, DELB)) {
        		(*left_node) = t;
        		left_node_next = t_next;
      		}
      
      		t = get_bucket_pointer(t_next);
      		
      		if (t == list_tail) break;
      		t_next = t->next;
    	}
    	
    	right_node = t;

    	if(get_bucket_pointer(left_node_next) == right_node){
    		if(!is_marked_ref(right_node->next, DELB)){
   // 			assertf(is_marked_ref(right_node->next, DELB), "list_search(): nodo marcato %s\n", "");
         		return right_node;
         	}
    	}
    	else{
	      	if(BOOL_CAS(&((*left_node)->next), left_node_next, 
                get_marked_ref(set_bucket_state(right_node, get_bucket_state(left_node_next)), is_marked_ref(left_node_next, MOVB) * MOVB))) {

	      		while((left_node_next = get_bucket_pointer(left_node_next)) != right_node){

			    	for(i = 0; i < left_node_next->valid_elem; i++){

			    		assertf(left_node_next->dwv_sorted[i].timestamp == INV_TS, "INV_TS while releasing memory%s\n", "");
                        //assertf(!is_deleted(left_node_next->dwv_sorted[i].node) && !is_moved(left_node_next->dwv_sorted[i].node), "\n\nnodo non marcato come eliminato o trasferito %p\n", left_node_next->dwv_sorted[i].node); 
                        assertf(get_node_pointer(left_node_next->dwv_sorted[i].node) == NULL || left_node_next->dwv_sorted[i].timestamp == INFTY, "nodo non valido per rilascio%s\n", ""); 

            			// if(get_node_pointer(left_node_next->dwv_sorted[i].node) != NULL && left_node_next->dwv_sorted[i].timestamp != INFTY)
                      	//if(!is_blocked(left_node_next->dwv_sorted[i].node))
                            node_free(get_node_pointer(left_node_next->dwv_sorted[i].node));
                    }

			      	gc_free(ptst, left_node_next->dwv, gc_aid[2]);
			      	gc_free(ptst, left_node_next->dwv_sorted, gc_aid[2]);
			      	gc_free(ptst, left_node_next, gc_aid[1]);			      	
			      	
			      	left_node_next = left_node_next->next;
			    }

	        	if (!is_marked_ref(right_node->next, DELB))
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

  	if(node == NULL)
  		return NULL;

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
    node->lock = 0;
	  	
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

        state = get_bucket_state(left->next);

    	if (BOOL_CAS(&(left->next), set_bucket_state(right, state), set_bucket_state(new_elem, state))){
      		return new_elem;
    	}else{ // inserimento fallito
            if(is_marked_ref(head->next, MOVB)){   // se il nodo di sinistra è marcato in movimento
                gc_free(ptst, new_elem, gc_aid[1]);// rilascio il nodo allocato
                return NULL;
            }
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
    	//assertf(is_marked_ref(right_succ, DELB), "list_remove(): ritornato un nodo marcato %s\n", "");
    	//assertf(getEnqInd(right->indexes) > VEC_SIZE && ((getEnqInd(right->indexes) - VEC_SIZE) < get_deq_ind(right->indexes)), 
    	//	"TID %d:list_remove(): indice di estrazione troppo grande: estrazione %d, inserimento %d\n", TID, get_deq_ind(right->indexes), (getEnqInd(right->indexes) - VEC_SIZE));
    	if (!is_marked_ref(right_succ, DELB) && (right->valid_elem == get_deq_ind(right->indexes))){
    		//printf("prima di marcare\n");
            assertf(is_marked_ref(right_succ, MOVB), "list_remove(): bucket marcato in movimento%s\n", "");
      		if (BOOL_CAS(&(right->next), right_succ, get_marked_ref(right_succ, DELB))){
      		//	printf("marcato %llu\n", index_vb);
                if(index_vb >= 0) // non posso prendere il successivo, cerco un vb in particolare
            		return NULL;
	      	}
    	}else if(!is_marked_ref(right_succ, DELB))
	      	return right;
  	}
}
