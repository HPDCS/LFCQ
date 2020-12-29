#ifndef __MM_ARRAY_H
#define __MM_ARRAY_H

static inline void init_array_subsystem(){
	gc_aid[GC_ARRAYNODES] = gc_add_allocator(sizeof(arrayNodes_t));
	gc_aid[GC_NODEELEMS] 	=  gc_add_allocator(sizeof(nodeElem_t	));	
}

/* initialization of a nodeElem */
static inline nodeElem_t* nodeElem_init(nodeElem_t* nodes, int length){
	nodeElem_t* res;
	int i = 0;
	while(i < length){
		res = nodes+i;
		bzero(res, sizeof(nodeElem_t));
		res->ptr = NULL;
		//res->replica = NULL;
		res->timestamp = MIN;
	}
	return res;
}

static inline nodeElem_t* nodeElem_alloc(int length){
	nodeElem_t* res;
	do{
		res = gc_alloc(ptst, gc_aid[GC_NODEELEMS]);
	    #ifdef ENABLE_CACHE_PARTITION
	    	unsigned long long index = CACHE_INDEX(res);
			assert(index <= CACHE_INDEX_MASK);
	    	if(index >= CACHE_LIMIT  )
	    #endif
	    		{break;}
  }while(1);
	return res;
}


/* allocate a arraynodes */
static inline arrayNodes_t* arrayNodes_alloc(int length){
	arrayNodes_t* res;
	do{
		res = gc_alloc(ptst, gc_aid[GC_ARRAYNODES]);
	    #ifdef ENABLE_CACHE_PARTITION
	    	unsigned long long index = CACHE_INDEX(res);
			assert(index <= CACHE_INDEX_MASK);
	    	if(index >= CACHE_LIMIT  )
	    #endif
	    		{break;}
    }while(1);
    bzero(res, sizeof(arrayNodes_t));
	res->epoch = 0;
	//res->indexRead = 0;
	res->indexWrite = 0;
	res->length = length;
	//res->nodes = nodeElem_alloc(length);
	res->nodes = (nodeElem_t*)malloc(sizeof(nodeElem_t)*length);
	assert(res->nodes != NULL);
	bzero(res->nodes, sizeof(nodeElem_t)*length);
	return res;
}

/* Safe free nodeElem memory */
static inline void nodeElem_safe_free(nodeElem_t* ptr){
	gc_free(ptst, ptr, gc_aid[GC_NODEELEMS]);
}

/* Unsafe free nodeElem memory */
static inline void nodeElem_unsafe_free(nodeElem_t* ptr){
	gc_unsafe_free(ptst, ptr, gc_aid[GC_NODEELEMS]);
}

/* Safe free arrayNodes memory */
static inline void arrayNodes_safe_free(arrayNodes_t *ptr){
	for(int i = 0; i < ptr->length; i++){
		if(ptr->nodes+i != NULL) 
			nodeElem_safe_free(ptr->nodes+i);
	}
	gc_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);	
}

/* Safe free arrayNodes memory */
static inline void arrayNodesOrdered_safe_free(arrayNodes_t *ptr){
	for(int i = 0; i < ptr->length; i++){
		if(ptr->nodes+i != NULL){
			//if(ptr->nodes[i].ptr != NULL) 
				//node_safe_free(ptr->nodes[i].ptr);
			nodeElem_safe_free(ptr->nodes+i);
		}
	}
	gc_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);	
}

/* Unsafe free arrayNodes memory */
static inline void arrayNodes_unsafe_free(arrayNodes_t *ptr){
	for(int i = 0; i < ptr->length; i++){
		if(ptr->nodes+i != NULL)
			nodeElem_unsafe_free(ptr->nodes+i);
	}
	gc_unsafe_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);	
}

static inline void arrayNodes_safe_free_malloc(arrayNodes_t *ptr){
	if(ptr->nodes != NULL) 
		gc_add_ptr_to_hook_list(ptst, ptr->nodes, gc_hid[0]);
	gc_unsafe_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);	
}

static inline void arrayNodes_unsafe_free_malloc(arrayNodes_t *ptr){
	// if(ptr->nodes != NULL) 
	// 	free(ptr->nodes);
	gc_unsafe_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);	
}

#endif