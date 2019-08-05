#ifndef __H_INDEX
#define __H_INDEX

#include "skipList.h"

typedef struct __index index_t;

struct __index
{
	unsigned int length;
	SkipList ** volatile array;
};


static index_t* alloc_index(unsigned int len){
	index_t *res 	= (index_t*) 	malloc(sizeof(index_t));
	res->length 	= len;
	res->array		= (SkipList**) 	malloc(sizeof(void*)*len);
	return res;
}

static void init_index(index_t *index){
	int i = 0;
	SkipList *curr = NULL;

	for(i=0;i<index->length;i++){
		if(index->array[i] == NULL){
			if(!curr) curr = skipListInit();
			if(__sync_bool_compare_and_swap(index->array + i, NULL, curr)) curr = NULL;
		}
	}
}

#endif
