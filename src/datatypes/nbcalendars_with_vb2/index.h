#ifndef __H_INDEX
#define __H_INDEX


static index_t* alloc_index(unsigned int len){
	index_t *res 	= (index_t*) 	malloc(sizeof(index_t));
	res->length 	= len;
	res->array		= (SkipList**) 	malloc(sizeof(void*)*len);
	bzero(res->array, sizeof(void*)*len);
	return res;
}

static void init_index(table_t *table){
	int i = 0;
	SkipList *curr = NULL;
//	printf("INIT SKIP LIST\n");
	for(i=0;i<table->index->length;i++){
		if(table->index->array[i] == NULL){
			if(curr == NULL) curr = skipListInit();
			curr->head->bottom_level = table->array[i];
			curr->tail->bottom_level = &table->b_tail;
			if(__sync_bool_compare_and_swap(table->index->array + i, NULL, curr)){
//				printf("Setting skip list %d %p\n", i, curr);
				 curr = NULL;
			}
		}
	}
}

#endif
