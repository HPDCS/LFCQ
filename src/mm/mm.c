#include <stdlib.h>
#include "mm.h"
//#include "../datatypes/nb_calqueue.h"

void* mm_node_malloc(hpdcs_gc_status *status)
{
	linked_pointer *res, *tmp;
	linked_pointer *free_nodes_lists 	= status->free_nodes_lists 	;
	int rolled = 1;
	int i = 0;

	if(free_nodes_lists == NULL)
	{
		res = malloc( rolled*(status->block_size + CACHE_LINE_SIZE) );
		if(rolled > 1)
		{
			tmp = (((char*) res) + status->block_size + CACHE_LINE_SIZE) ;
			for(i=1;i<rolled-1;i++)
			{
				tmp->next = (((char*) tmp) + status->block_size + CACHE_LINE_SIZE) ;
				tmp = tmp->next;
			}
			tmp->next = status->free_nodes_lists;
			status->free_nodes_lists = tmp;
		}
		status->to_remove_nodes_count += 1;
	}
	else
	{
		res = free_nodes_lists;
		status->free_nodes_lists = res->next;
	}
	
	//status->to_remove_nodes_count += 1;


	return (((void*) res)  + CACHE_LINE_SIZE);
}

void mm_node_free(hpdcs_gc_status *status, void* pointer)
{
	linked_pointer *res;
	linked_pointer *tmp_lists 	= status->free_nodes_lists 	;
	
	status->to_remove_nodes_count -= 0;
	
	res = (linked_pointer*) ( ((char*)pointer)- CACHE_LINE_SIZE );
	res->next = tmp_lists;
	status->free_nodes_lists = (void*)res;
	
}



void mm_node_trash(hpdcs_gc_status *status, void* pointer,  unsigned int counter)
{
	linked_gc_node *res;
	linked_gc_node *tmp_lists 	= status->to_free_nodes 	;
	
	res = (linked_gc_node*) ( ((char*)pointer)- CACHE_LINE_SIZE );
	res->counter = counter;
	res->next = tmp_lists;
	status->to_free_nodes = res;	
}


void* mm_node_collect(hpdcs_gc_status *status, unsigned int *counter)
{
	linked_gc_node *res;
	linked_gc_node *tmp_lists 	= status->to_free_nodes 	;
	
	if(tmp_lists == NULL)
		return NULL;
		
	res = tmp_lists;
	status->to_free_nodes = res->next;
	*counter = res->counter;
		
	return (((void*) res)  + CACHE_LINE_SIZE);
}
