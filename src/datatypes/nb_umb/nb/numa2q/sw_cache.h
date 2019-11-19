#include "common_nb_calqueue.h"

//#define DCACHE_ON

void validate_cache(table *h, unsigned long long current);
nbc_bucket_node* read_last_min(nbc_bucket_node *left_node);
void update_last_min(nbc_bucket_node* last_node);