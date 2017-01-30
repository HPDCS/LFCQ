#include <stdlib.h>
#include "mm.h"
//#include "../datatypes/nb_calqueue.h"

void* mm_node_malloc(hpdcs_gc_status *status)
{
	linked_pointer *res, *tmp;
	linked_pointer *free_nodes_lists 	= status->free_nodes_lists 	;
	int rolled = 1;
	
	if(free_nodes_lists == NULL)
	{
		res = calloc( rolled,(status->block_size + CACHE_LINE_SIZE) );
		if(rolled > 1)
		{
			int i = 0;
			free_nodes_lists = (((char*) res) + status->block_size + CACHE_LINE_SIZE) ;
			tmp = free_nodes_lists;
			for(i=1;i<rolled-1;i++)
			{
				tmp->next = (((char*) tmp) + status->block_size + CACHE_LINE_SIZE) ;
				tmp = tmp->next;
			}
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
	//free(pointer);
	//return;
	linked_pointer *res;
	linked_pointer *tmp_lists 	= status->free_nodes_lists 	;
	
	status->to_remove_nodes_count -= 0;
	
	res = (linked_pointer*) ( ((char*)pointer)- CACHE_LINE_SIZE );
	//res = pointer;
	res->next = tmp_lists;
	status->free_nodes_lists = (void*)res;
	
}
