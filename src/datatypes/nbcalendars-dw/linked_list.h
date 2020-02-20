/*
 *  linkedlist.h
 *  interface for the list
 *
 */
#ifndef LLIST_H_ 
#define LLIST_H_


#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>

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
	int volatile indexes;	// inserimento|estrazione
	int volatile num_extractable_items;
	int from_enq;
	//char pad[32];
};

// lista dei virtual bucket di un physical bucket
/*
typedef struct deferred_work_list dwl;
struct deferred_work_list{	
	dwb head;
//	dwb tail;		
};
*/

// struttura di deferred work 
typedef struct deferred_work_structure dwstr;
struct deferred_work_structure{
	dwb* heads; 
	dwb* list_tail;
	int vec_size;
};

//int new_list(dwl*);
dwb* list_add(dwb*, long long, dwb*);
dwb* list_remove(dwb*, long long, dwb*);

#endif
