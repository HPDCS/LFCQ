

#include <stdlib.h>
#include <stddef.h>


#define ARRAY_CHUNK_SIZE 32768
#define MAXIMUM_CHUNKS 32
#define MAXIMUM_ITEMS 1048576

struct _chunked_array
{
	size_t item_size;
	size_t low_limit;
	size_t high_limit;
	char *chunks[MAXIMUM_CHUNKS];
};

typedef struct _chunked_array chunked_array;


void* get_entry_pointer(chunked_array* array, size_t index)
{	
	size_t first_level_index;
	size_t second_level_index;
	
	if(index >= array->high_limit - array->low_limit)
		return NULL;
		
	first_level_index  = index >> 15;
	second_level_index = index & (~(ARRAY_CHUNK_SIZE-1));
	
	return array->chunks[first_level_index] + second_level_index*array->item_size;
	
		
}


void destroy_chucked_array( chunked_array* array)
{
	int i;
	char **chunks = array->chunks;
	
	for(i=0;i<MAXIMUM_CHUNKS;i++)
	{
		char *tmp = chunks[i];
		if(tmp != NULL)
			free(tmp);
	}
	
	free(array);
}



chunked_array* create_chunked_array(size_t size, size_t item_size)
{
	int i;
	int num_chunks;
	chunked_array* res;
	
	if(size > MAXIMUM_ITEMS)
		return NULL;
	
	res = (chunked_array*) malloc(sizeof(chunked_array));
	if(res == NULL)
		return NULL;
	
	res->item_size = item_size;
	res->low_limit = 0;
	res->high_limit = size;
	
	num_chunks = size/ARRAY_CHUNK_SIZE + (size%ARRAY_CHUNK_SIZE != 0);
	
	for(i=0;i<num_chunks;i++)
	{
		res->chunks[i] = (char*) malloc(item_size * ARRAY_CHUNK_SIZE);
		if(res->chunks[i] == NULL)
		{
				destroy_chucked_array(res);
				return NULL;
		}
	}
	return res;
}
