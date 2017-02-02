#include <stdlib.h>
#include <stdio.h>
#include "mm.h"
#include "../utils/hpdcs_utils.h"

//__thread unsigned int mio = 0;
//static linked_pointer * volatile global_free_list = NULL;


#define MASK_EPOCH	

void* mm_node_malloc(hpdcs_gc_status *status)
{
	linked_pointer *res;
	//linked_pointer *tmp;
	linked_pointer *free_nodes_lists 	= status->free_nodes_lists 	;
	//unsigned long long res_tmp;
	unsigned int res_pos_mem = 0;
	int rolled = 1;
	//int i = 0;
	//int step = 200;

	
	if(free_nodes_lists == NULL)
	{	
		//res = global_free_list;
		//
		//while( res != NULL)
		//{
		//	if(__sync_bool_compare_and_swap(&global_free_list, res, res->next))
		//		return (void*) (((char*) res)  + CACHE_LINE_SIZE);
		//	
		//res = global_free_list;
		//}
		
		{
			//res = malloc( rolled*(status->block_size + 2*CACHE_LINE_SIZE) );
			res_pos_mem = posix_memalign((void**) &res, CACHE_LINE_SIZE, rolled*(status->block_size + CACHE_LINE_SIZE) );
			if(res_pos_mem != 0)
			{
				printf("No enough memory to allocate a new node\n");
				exit(1);
				
			}
			//res_tmp  = 	UNION_CAST(res, unsigned long long);
			//res_tmp +=	CACHE_LINE_SIZE;
			//res_tmp &=	~((unsigned long long) CACHE_LINE_SIZE-1);
			//
			//tmp = UNION_CAST(res_tmp,  linked_pointer* );
			//tmp->base = res;
			//tmp->next = NULL;
			//res = tmp;
			
			
			//if(rolled > 1)
			//{
			//	tmp = (linked_pointer*) (((char*) res) + status->block_size + CACHE_LINE_SIZE) ;
			//	for(i=1;i<rolled-1;i++)
			//	{
			//		tmp->next = (linked_pointer*) (((char*) tmp) + status->block_size + CACHE_LINE_SIZE) ;
			//		tmp = tmp->next;
			//	}
			//	tmp->next = status->free_nodes_lists;
			//	status->free_nodes_lists = tmp;
			//}
			status->to_remove_nodes_count += 1;
		}
	}
	else
	{
		res = free_nodes_lists;
		status->free_nodes_lists = res->next;
	//	mio--;
	}
	
	//status->to_remove_nodes_count += 1;


	return (void*) (((char*) res)  + CACHE_LINE_SIZE);
}

void mm_node_free(hpdcs_gc_status *status, void* pointer)
{
	linked_pointer *res;
	//linked_pointer *tmp_global;
	linked_pointer *tmp_lists 	= status->free_nodes_lists 	;
	
	//status->to_remove_nodes_count -= 0;
	
	res = (linked_pointer*) ( ((char*)pointer)- CACHE_LINE_SIZE );
	
	//if(mio > 10)
	//{
	//	do
	//	{
	//	tmp_global = global_free_list;
	//	res->next = tmp_global;
	//	}
	//	while(!__sync_bool_compare_and_swap(&global_free_list, tmp_global, res));
	//		return;
	//	
	//}
	//mio++;
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
	linked_gc_node *tmp_lists 	= status->to_free_nodes_old	;
	
	if(tmp_lists == NULL)
		return NULL;
		
	res = tmp_lists;
	status->to_free_nodes_old = res->next;
	*counter = res->counter;
		
	return (void*) (((char*) res) + CACHE_LINE_SIZE);
}


void mm_new_era(hpdcs_gc_status *status)
{
	status->to_free_nodes_old = status->to_free_nodes ;
	status->to_free_nodes = NULL;
}
