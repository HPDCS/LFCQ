#ifndef __MM_ARRAY_H
#define __MM_ARRAY_H

static inline void init_array_subsystem(){
	gc_aid[GC_ARRAYNODES] = gc_add_allocator(sizeof(arrayNodes_t));
	gc_aid[GC_NODEELEMS] 	=  gc_add_allocator(sizeof(nodeElem_t)*NODES_LENGTH);
}


static inline nodeElem_t* nodeElem_alloc(int length){
	nodeElem_t* res = NULL;
	int status = 0;

	// allocate new array of tuples
	assert(length*sizeof(nodeElem_t) > 0);
	status = posix_memalign((void**)&res, CACHE_LINE_SIZE, length*sizeof(nodeElem_t));
	if(status != 0) {printf("No enough memory to array of tuples structure\n"); _exit(0);}
	bzero(res, length*sizeof(nodeElem_t));
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
	res->nodes = nodeElem_alloc(length);
	//res->nodes = (nodeElem_t*)malloc(sizeof(nodeElem_t)*length);
	assert(res->nodes != NULL);
	bzero(res->nodes, sizeof(nodeElem_t)*length);
	return res;
}

/* Safe free nodeElem memory */
static inline void nodeElemStaticAlloc_safe_free(nodeElem_t* ptr){
	//gc_free(ptst, ptr, gc_aid[GC_NODEELEMS]);
}

/* Unsafe free nodeElem memory */
static inline void nodeElemStaticAlloc_unsafe_free(nodeElem_t* ptr){
	//gc_unsafe_free(ptst, ptr, gc_aid[GC_NODEELEMS]);
}

// In realtà libera i nodi
static inline void arrayNodes_safe_free_malloc(arrayNodes_t *ptr){
	if(ptr != NULL){
		if(ptr->nodes != NULL){
			if(ptr->length == NODES_LENGTH){
				gc_free(ptst, ptr->nodes, gc_aid[GC_NODEELEMS]);
			}else{
				free(ptr->nodes);
			}
		}
	}
}

// In realtà libera i nodi
static inline void arrayNodes_unsafe_free_malloc(arrayNodes_t *ptr){
	if(ptr != NULL) {
		if(ptr->nodes != NULL){
			if(ptr->length == NODES_LENGTH){
				gc_unsafe_free(ptst, ptr->nodes, gc_aid[GC_NODEELEMS]);
			}else{
				free(ptr->nodes);
			}
		}
	}
}

/* Safe free arrayNodes memory */
static inline void arrayNodes_safe_free(arrayNodes_t *ptr){
	if(ptr != NULL){
			gc_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);
	}
}

/* UnSafe free arrayNodes memory */
static inline void arrayNodes_unsafe_free(arrayNodes_t *ptr){
	if(ptr != NULL){
			gc_unsafe_free(ptst, ptr, gc_aid[GC_ARRAYNODES]);
	}
}

#endif