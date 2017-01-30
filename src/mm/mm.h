#ifndef __HPDCS_MM__
#define __HPDCS_MM__

typedef struct _linked_pointer linked_pointer;

struct _linked_pointer{
	linked_pointer *next;
};

typedef struct hpdcs_gc_status
{
	linked_pointer *free_nodes_lists 	;
	void *to_free_nodes 				;
	void *to_free_nodes_old 			;
	unsigned int block_size 			;
	long long to_remove_nodes_count 	;
}
hpdcs_gc_status;


void* mm_node_malloc(hpdcs_gc_status *status);
void  mm_node_free(hpdcs_gc_status *status, void* pointer);

#endif
