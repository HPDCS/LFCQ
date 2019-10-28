#include "sw_cache.h"

__thread table *last_table;
__thread unsigned long long last_current;
__thread nbc_bucket_node *last_head 	= NULL; //next dell'ultima testa
__thread nbc_bucket_node *last_min 		= NULL; //ultimo nodo estratto


// se il current è cambiato azzera tutto
void validate_cache(table *h, unsigned long long current)
{
    if (h != last_table || last_current != current)
    {
        last_table = h;
        last_current = current;
        last_head = NULL;
        last_min = NULL;
    }
}

// passa la head corrente - se è diversa aggiorna e ritorna, se è uguale torna l'ultimo min
nbc_bucket_node* read_last_min(nbc_bucket_node *left_node)
{
    if (left_node != last_head)
    {
        last_head = left_node;
        last_min = NULL;
    }
    else if (last_min)
        return last_min;
    
    return left_node;
}

// setta l'ultimo min letto
void update_last_min(nbc_bucket_node* last_node)
{
    last_min = last_node;
} 