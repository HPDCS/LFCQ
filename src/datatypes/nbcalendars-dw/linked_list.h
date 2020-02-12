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
	int volatile indexes;	// inserimento|estrazione
	nbnc* volatile dwv;		// array di eventi deferred
	dwb* volatile next;		// puntatore al prossimo elemento
	int volatile cicle_limit;
	unsigned long long index_vb;
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
dwb* list_add(dwb*, unsigned long long, int, dwb*);
dwb* list_remove(dwb*, unsigned long long, dwb*);

#endif