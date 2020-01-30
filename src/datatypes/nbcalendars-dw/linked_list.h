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


typedef struct nbc_bucket_node_container nbnc;
struct nbc_bucket_node_container{
	nbc_bucket_node* node;	// puntatore al nodo
	pkey_t timestamp;  		// relativo timestamp			
};

typedef struct deferred_work_node dwn;
struct deferred_work_node{
	long index_vb;			// 8
	nbnc** dwv;	// 16
	dwn* next;				// 24
//	char c1[40];
	int deq_cn;			
//	char c2[56];
	int enq_cn;
	
};


typedef struct deferred_work_list dwl;
struct deferred_work_list{
	dwn *head;
	dwn *tail;
	//int size;
};

typedef struct deferred_work_structure dwstr;
struct deferred_work_structure{
	dwl** dwls; 
	int vec_size;
};

bool is_marked_ref(dwn*);
dwn* get_unmarked_ref(dwn*);
dwn* get_marked_ref(dwn*);

dwl* list_new(int);
//return 0 if not found, positive number otherwise
//dwn* list_contains(dwl*, long);
//return 0 if value already in the list, positive number otherwise
dwn* list_add(dwl*, long, int);
//return 0 if value already in the list, positive number otherwise
dwn* list_remove(dwl*, long);
void list_delete(dwl*);
//int list_size(dwl*);


dwn* new_node(long, dwn*, int);
//dwn* list_search(dwl*, long, dwn**);

dwn* list_search_2(dwn*, dwn*, dwn**);
dwn* list_search_rm(dwl*, long, dwn**);
dwn* list_search_add(dwl*, long, dwn**);


#endif