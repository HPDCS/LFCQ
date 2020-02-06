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
typedef struct deferred_work_node dwb;
struct deferred_work_node{	
	int volatile indexes;	// inserimento|estrazione
	nbnc* volatile dwv;		// array di eventi deferred
	dwb* volatile next;		// puntatore al prossimo elemento
	int volatile cicle_limit;
	/*
	int indexes;	// inserimento|estrazione
	nbnc* dwv;		// array di eventi deferred
	dwb* next;		// puntatore al prossimo elemento
	int cicle_limit;
	*/
	unsigned long long index_vb;
	//char pad[32];
};

// lista dei virtual bucket di un physical bucket
typedef struct deferred_work_list dwl;
struct deferred_work_list{	
	dwb* head;
	dwb* tail;		
};

// struttura di deferred work 
typedef struct deferred_work_structure dwstr;
struct deferred_work_structure{
	dwl* dwls; 
	int vec_size;
};

int new_list(dwl*);
dwb* list_add(dwl, unsigned long long);
dwb* list_remove(dwl, unsigned long long);

#endif