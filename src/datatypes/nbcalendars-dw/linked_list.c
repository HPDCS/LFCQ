/*
 *  linkedlist.c
 *
 *  Description:
 *   Lock-free linkedlist implementation of Harris' algorithm
 *   "A Pragmatic Implementation of Non-Blocking Linked Lists" 
 *   T. Harris, p. 300-314, DISC 2001.
 */

//#include "linked_list.h"
#include "common_nb_calqueue.h"
/*
 * The five following functions handle the low-order mark bit that indicates
 * whether a node is logically deleted (1) or not (0).
 *  - is_marked_ref returns whether it is marked, 
 *  - (un)set_marked changes the mark,
 *  - get_(un)marked_ref sets the mark before returning the node.
 */
bool is_marked_ref(dwn* node_p){
  return (bool) ((unsigned long long)(node_p) & 0x1ULL);
}

dwn* get_unmarked_ref(dwn* node_p) {
  return (dwn*) ((unsigned long long)(node_p) & 0xfffffffffffffffe);
}

dwn* get_marked_ref(dwn* node_p) {
  return (dwn*) ((unsigned long long)(node_p) | 0x1ULL);
}

/*
 * list_search looks for value val, it
 *  - returns right_node owning val (if present) or its immediately higher 
 *    value present in the list (otherwise) and 
 *  - sets the left_node to the node owning the value immediately lower than val. 
 * Encountered nodes that are marked as logically deleted are physically removed
 * from the list, yet not garbage collected.
 */
// lo stato di un nodo è mantenuto nel puntatore a next


dwn* list_search_2(dwn* left_node_next, dwn* right_node, dwn** left_node){
  dwn* result = NULL;

  if (DW_GET_PTR(left_node_next) == right_node){   // se tra sinistro e destro non ci sono altri nodi(dovrebbero essere marcati) allora non compatto
         result = right_node;
  }
  else{ // compatta
    if (BOOL_CAS(&((*left_node)->next), left_node_next, DW_SET_STATE(right_node, DW_GET_STATE(left_node_next)))) {
        if (!is_marked_ref(right_node->next)){
          result = right_node;
        }
    }
  }

  return result;
}


dwn* list_search_rm(dwl* set, long val, dwn** left_node) {
  dwn *left_node_next, *right_node;
  left_node_next = right_node = NULL;
  dwn* result;

/*              
  dwn *t = set->head->next;

  printf("valore %ld | ", val);
  while(t != set->tail){
    printf("%p %ld %d %lld | ", t, t->index_vb, (int)is_marked_ref(t->next), DW_GET_STATE(t->next));  
    t = DW_GET_PTR(get_unmarked_ref(t->next));
  }
*/

  while(1) {
    dwn *t = set->head;             
    dwn *t_next = set->head->next; 

    while (is_marked_ref(t_next) || (t->index_vb < val)) {  // se il nodo attuale è marcato o il valore è minore di quello che cerco proseguo
      if (!is_marked_ref(t_next)) {                         // se non marcato salvo i valori del nodo di sinistra(il nodo sinistro si sposta solo se non marcato)
        (*left_node) = t;
        left_node_next = t_next;
      }

      t = DW_GET_PTR(get_unmarked_ref(t_next));             // tolgo la marcatura e anche lo stato dw eventualmente
      if (t == set->tail) break;                            // se il successivo è la coda mi fermo(potrebbe anche ritornarla come risultato)
      t_next = t->next;                                     // alrimenti valuto il nodo successivo
    }

    right_node = t;

    result = list_search_2(left_node_next, right_node, left_node);
    if(result != NULL)
      return result;
  }
}

dwn* list_search_add(dwl* set, long val, dwn** left_node) {
  dwn *left_node_next, *right_node;
  left_node_next = right_node = NULL;
  dwn* result;

/*              
  dwn *t = set->head->next;

  printf("valore %ld | ", val);
  while(t != set->tail){
    printf("%p %ld %d %lld | ", t, t->index_vb, (int)is_marked_ref(t->next), DW_GET_STATE(t->next));  
    t = DW_GET_PTR(get_unmarked_ref(t->next));
  }
*/
  while(1) {
    dwn *t = set->head;             
    dwn *t_next = set->head->next; 

    while (is_marked_ref(t_next) || (t->index_vb < val)) {  // se il nodo attuale è marcato o il valore è minore di quello che cerco proseguo
      if (!is_marked_ref(t_next)) {                         // se non marcato salvo i valori del nodo di sinistra(il nodo sinistro si sposta solo se non marcato)
        (*left_node) = t;
        left_node_next = t_next;
      }

      t = DW_GET_PTR(get_unmarked_ref(t_next));             // tolgo la marcatura e anche lo stato dw eventualmente
      if (t == set->tail) break;                            // se il successivo è la coda mi fermo(potrebbe anche ritornarla come risultato)
      t_next = t->next;                                     // alrimenti valuto il nodo successivo
    }

    right_node = t;

    result = list_search_2(left_node_next, right_node, left_node);
    if(result != NULL)
      return result;
  }
}

/*
 * list_contains returns a value different from 0 whether there is a node in the list owning value val.
 */
/*
dwn* list_contains(dwl* the_list, long index_vb){
//printf("%d\n",the_list->size);
  dwn* iterator = DW_GET_PTR(get_unmarked_ref(the_list->head->next)); 
  while(iterator != the_list->tail){ 
    if (!is_marked_ref(iterator->next) && iterator->index_vb >= index_vb){ 
      // either we found it, or found the first larger element
      if (iterator->index_vb == index_vb) return iterator;
      else return NULL;
    }

    // always get unmarked pointer
    iterator = DW_GET_PTR(get_unmarked_ref(iterator->next));
  }  
  return NULL; 
}
*/


dwn* new_node(long index_vb, dwn* next, int vec_size){

    dwn* node = NULL;
    int i, res = 0;

    res = posix_memalign((void**)(&node), CACHE_LINE_SIZE, sizeof(dwn));
    if(res != 0){
        printf("Non abbastanza memoria per allocare un nodo dwn della lista\n"); 
        return NULL; 
    }

    if(vec_size != 0){
      res = posix_memalign((void**)(&node->dwv), CACHE_LINE_SIZE, vec_size * sizeof(nbc_bucket_node*)); // TODO: da vedere se si può inizializzare subito a zero
      if(res != 0){
          printf("Non abbastanza memoria per allocare l'array di un nodo dwn\n"); 
          free(node); // rilascio il nodo
          return NULL;
      }
    }
    
    for(i = 0; i < vec_size; i++)
        node->dwv[i] = NULL; // inizializzo le entry del vettore

    node->deq_cn = 0;
    node->enq_cn = 0;
    node->index_vb = index_vb;
    node->next = next;

    return node;
}

dwl* list_new(int vec_size){

    int res = 0;
    dwl* the_list = NULL;
    
    // allocate list
    res = posix_memalign((void**)(&the_list), CACHE_LINE_SIZE, sizeof(dwl));

    if(res != 0)
        printf("Non abbastanza memoria per allocare una lista\n"); 
    else{
        // now need to create the sentinel node
        the_list->head = new_node(LONG_MIN, NULL, 0);
        the_list->tail = new_node(LONG_MAX, NULL, 0);
        the_list->head->next = the_list->tail;
        //the_list->size = 0; // head e tail non contano per la dimensione
    }

    return the_list;
}
/*
int list_size(dwl* the_list) { 
  return the_list->size; 
} */

/*
 * list_add inserts a new node with the given value val in the list
 * (if the value was absent) or does nothing (if the value is already present).
 */

dwn* list_add(dwl *the_list, long index_vb, int vec_size){

  dwn *right, *left, *new_elem;
  unsigned long long state;
  right = left = new_elem = NULL;


  //right = list_search(the_list, -1, &left);

  //dwn* new_elem = new_node(index_vb, NULL, vec_size);// TODO: verificare se deve per forza stare qui(non credo)
  while(1){
  //  printf("Add ");
    right = list_search_add(the_list, index_vb, &left);
    if (right != the_list->tail && right->index_vb == index_vb){ // trovato, già esistente
      return right;
    }

    //printf("next precedente %p\n", left->next);
    if(new_elem == NULL)
      new_elem = new_node(index_vb, NULL, vec_size);

    new_elem->next = right;   
    state = DW_GET_STATE(left->next);  

    // cerco di inserire quello nuovo
    if (BOOL_CAS(&(left->next), DW_SET_STATE(right, state), DW_SET_STATE(new_elem, state))/* == right*/){
      //printf("aggiunto un nodo, next precedente %p\n", left->next);
      //FETCH_AND_ADD(&(the_list->size), 1);
      return new_elem;
    }
  }
}

/*
 * list_remove deletes a node with the given value val (if the value is present) 
 * or does nothing (if the value is already present).
 * The deletion is logical and consists of setting the node mark bit to 1.
 */
dwn* list_remove(dwl *the_list, long index_vb){

  dwn* right, *left, *right_succ;
  right = left = right_succ = NULL;

  //printf("dw_dequeue %ld \n", index_vb);
  while(1){
    //printf("Remove ");
    right = list_search_rm(the_list, index_vb, &left);
    // check if we found our node
    if (right == the_list->tail || (index_vb >= 0 && right->index_vb != index_vb)){// se vuota o non c'è il nodo cercato
      //return 0;
      //printf("ritorno prima\n");
    //fflush(stdout);
      return NULL;
    }

    right_succ = right->next;
    // se l'indice dell'estrazione arriva all'indice dell'inserimento e non ancora marcato allora marcalo
    //printf("%d, %d, %ld \n", right->enq_cn, right->deq_cn, index_vb);
    //fflush(stdout);
    if (!is_marked_ref(right_succ) && right->enq_cn <= right->deq_cn){

      //printf("Ho chiesto %ld, provo a marcare %ld", index_vb, right->index_vb);
      //fflush(stdout);
      if (VAL_CAS(&(right->next), right_succ, get_marked_ref(right_succ)) == right_succ){// dopo averlo marcato non ritorno 
        //FETCH_AND_SUB(&(the_list->size), 1);
        //return 1;
        //printf(" , marcato %ld\n", right->index_vb);
        //fflush(stdout);
        if(index_vb >= 0) // non posso prendere il successivo, cerco un vb in particolare
          return NULL;
      }
    }else if(!is_marked_ref(right_succ))
      return right;// ritorno il nodo corrente
  }
  // we just logically delete it, someone else will invoke search and delete it
}