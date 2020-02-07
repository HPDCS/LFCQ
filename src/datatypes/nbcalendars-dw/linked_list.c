#include "common_nb_calqueue.h"
#include "linked_list.h"

dwb* new_node(unsigned long long, dwb*, bool);
dwb* list_search(dwb*, unsigned long long, dwb**, int, dwb*);
bool is_marked_ref(dwb*);
dwb* get_marked_ref(dwb*);

extern unsigned long long getBucketState(dwb*);
extern dwb* setBucketState(dwb*, unsigned long long);
extern dwb* getBucketPointer(dwb*);
extern nbc_bucket_node* getNodePointer(nbc_bucket_node*);
extern int getEnqInd(int);
extern int getDeqInd(int);

bool is_marked_ref(dwb* bucket){return (bool)((unsigned long long)bucket & 0x1ULL);}
dwb* get_marked_ref(dwb* bucket){return (dwb*)((unsigned long long)bucket | 0x1ULL);}

dwb* list_search(dwb *head, unsigned long long index_vb, dwb** left_node, int mode, dwb* list_tail) {
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
  		//printf("%llu\n", index_vb);
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
    	//		printf("%llu\n", index_vb);
         		return right_node;
         	}
    	}
    	else{
    		//printf("pippo1");
	      	if(BOOL_CAS(&((*left_node)->next), left_node_next, setBucketState(right_node, getBucketState(left_node_next)))) {
	      		//int limit;
				//printf("%p\n", left_node_next);
	      		while((left_node_next = getBucketPointer(left_node_next)) != right_node){
	      			//printf("pippo2");
      				//limit = getEnqInd(left_node_next->indexes) - VEC_SIZE;
      				//if(limit < 0)
      				//	while(1);
      				//assertf(limit < 0 || limit > VEC_SIZE, "list_remove(): indice di estrazione fuori range %d\n", limit);

			    	for(i = 0; i < left_node_next->cicle_limit; i++){
			    		//printf("nodi\n");
			        	if(getNodePointer(left_node_next->dwv[i].node) != NULL)
			          		node_free(getNodePointer(left_node_next->dwv[i].node));
			      	}

			      	gc_free(ptst, left_node_next->dwv, gc_aid[2]);
			      	gc_free(ptst, left_node_next, gc_aid[1]);			      	
			      	
			      	left_node_next = left_node_next->next;
			    }

	        	if (!is_marked_ref(right_node->next))
	          		return right_node;
	      	}
    	}
  	}
}

dwb* new_node(unsigned long long index_vb, dwb *next, bool allocate_dwv){
	int i;
	//printf("TID: %d, index_vb = %llu\n", TID, index_vb);
  	dwb* node = gc_alloc(ptst, gc_aid[1]);
  	
  	if(node != NULL){
  		if(allocate_dwv){
  			node->dwv = gc_alloc(ptst, gc_aid[2]);

  			if(node->dwv == NULL){
  				gc_free(ptst, node, gc_aid[1]);
  				return NULL;
  			}

  			// inizializzazione dell'array allocato
			node->indexes = 0;
			node->cicle_limit = VEC_SIZE;

			for(i = 0; i < VEC_SIZE; i++){
				node->dwv[i].node = NULL;
				node->dwv[i].timestamp = INFTY;	
			}
  		}

  		node->index_vb = index_vb;
  		node->next = next;
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

dwb* list_add(dwb *head, unsigned long long index_vb, dwb* list_tail){

	unsigned long long state;
	dwb *right, *left, *new_elem;
  	right = left = new_elem = NULL;

  	while(1){
    	right = list_search(head, index_vb, &left, 0, list_tail);
    	if (right != list_tail && right->index_vb == index_vb){
      		return right;
    	}

    	if(new_elem == NULL)
    		new_elem = new_node(index_vb, NULL, true);

    	new_elem->next = right;

    	state = getBucketState(left->next);

    	if (BOOL_CAS(&(left->next), setBucketState(right, state) , setBucketState(new_elem, state))){
    		//printf("eseguito l'inserimento\n");
      		return new_elem;
    	}
  	}
}

dwb* list_remove(dwb *head, unsigned long long index_vb, dwb* list_tail){

  	dwb* right, *left, *right_succ;
  	right = left = right_succ = NULL;

  	while(1){
    	right = list_search(head, index_vb, &left, 0, list_tail);
    
    	// check if we found our node
    	if (right == list_tail || right->index_vb != index_vb){
      		return NULL;
    	}
    
    	right_succ = right->next;
    	//assertf(is_marked_ref(right_succ), "list_remove(): ritornato un nodo marcato %s\n", "");

    	//assertf(getEnqInd(right->indexes) > VEC_SIZE && ((getEnqInd(right->indexes) - VEC_SIZE) < getDeqInd(right->indexes)), 
    	//	"TID %d:list_remove(): indice di estrazione troppo grande: estrazione %d, inserimento %d\n", TID, getDeqInd(right->indexes), (getEnqInd(right->indexes) - VEC_SIZE));
		//if(right->cicle_limit == getDeqInd(right->indexes))
		//	printf("TID: %d , index_vb = %llu\n", TID, index_vb);
		//if(right->cicle_limit == getDeqInd(right->indexes) || (right->cicle_limit - 1) == getDeqInd(right->indexes))
		//if(getDeqInd(right->indexes) == 0 && right->cicle_limit != VEC_SIZE)
		//printf("%d %d %llu\n", right->cicle_limit, getDeqInd(right->indexes), index_vb);
    	if (!is_marked_ref(right_succ) && (right->cicle_limit == getDeqInd(right->indexes))){
    		//printf("prima di marcare\n");
      		if (BOOL_CAS(&(right->next), right_succ, get_marked_ref(right_succ))){
      		//	printf("marcato %llu\n", index_vb);
        		return NULL;
	      	}
    	}else if(!is_marked_ref(right_succ))
	      	return right;
  	}
}