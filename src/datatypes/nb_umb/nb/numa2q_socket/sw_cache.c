#include "sw_cache.h"

__thread table *last_table;
__thread unsigned long long last_current;
__thread nbc_bucket_node *last_head 	= NULL; //next dell'ultima testa
__thread nbc_bucket_node *last_min 		= NULL; //ultimo nodo estratto


// se il current è cambiato azzera tutto
void validate_cache(table *h, unsigned long long current)
{
    #ifdef DCACHE_ON
    if (h != last_table || last_current != current)
    {
        last_table = h;
        last_current = current;
        last_head = NULL;
        last_min = NULL;
    }
    #else
    return;
    #endif
}

// passa la head corrente - se è diversa aggiorna e ritorna, se è uguale torna l'ultimo min
nbc_bucket_node* read_last_min(nbc_bucket_node *left_node)
{
    #ifdef DCACHE_ON
    if (left_node != last_head)
    {
        last_head = left_node;
        last_min = NULL;
    }
    else if (last_min)
        return last_min;
    #endif

    return left_node;
    
}

// setta l'ultimo min letto
void update_last_min(nbc_bucket_node* last_node)
{
    #ifdef DCACHE_ON
    last_min = last_node;
    #else
    return;
    #endif
} 