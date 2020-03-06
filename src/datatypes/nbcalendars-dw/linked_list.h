/*
 *  linkedlist.h
 *  interface for the list
 *
 */
#ifndef LLIST_H_ 
#define LLIST_H_

#include "common_nb_calqueue.h"

//#include <stddef.h>
// container contnente un evento da gestire
typedef struct nbc_bucket_node_container nbnc;
struct nbc_bucket_node_container{
	nbc_bucket_node* node;	// puntatore al nodo
	pkey_t timestamp;  		// relativo timestamp			
};

// virtual bucket
typedef struct deferred_work_bucket dwb;
struct deferred_work_bucket{	
	nbnc* volatile dwv;		// array di eventi deferred
	nbnc* volatile dwv_sorted;		// array di eventi deferred
	dwb* volatile next;		// puntatore al prossimo elemento
	long long index_vb;
	int volatile cicle_limit;
	int volatile valid_elem;
	int volatile indexes;	// inserimento|estrazione
	//char pad[32];
};

// struttura di deferred work 
typedef struct deferred_work_structure dwstr;
struct deferred_work_structure{
	dwb* heads; 
	dwb* list_tail;
	int vec_size;
};

#define FETCH_AND_ADD 				__sync_fetch_and_add
/*
#define container_of(ptr, type, member) ({ \
const typeof(((type *)0)->member) *__mptr = (ptr); \
(type *)((char *)__mptr - offsetof(type, member)); })
*/
//int new_list(dwl*);

dwb* list_add(
 dwb*, 
 long long,
#if NUMA_DW
 int,
#endif
 dwb*
);
dwb* list_remove(dwb*, long long, dwb*);

#endif
