#include <stdlib.h>

#include "table_utils.h"

__thread unsigned long long concurrent_enqueue;
__thread unsigned long long performed_enqueue;
__thread unsigned long long concurrent_dequeue;
__thread unsigned long long performed_dequeue;

__thread unsigned long long num_cas = 0ULL;
__thread unsigned long long num_cas_useful = 0ULL;

__thread unsigned long long near = 0;
__thread unsigned int 		acc = 0;
__thread unsigned int 		acc_counter = 0;

__thread unsigned long long scan_list_length_en = 0ull;
__thread unsigned long long scan_list_length = 0ull;

__thread unsigned int 		read_table_count	 = UINT_MAX;

int gc_aid[32];
int gc_hid[4];

void std_free_hook(ptst_t *p, void *ptr)
{
    free(ptr);
}

/**
 * This function create an instance of a NBCQ.
 *
 * @param threshold: ----------------
 * @param perc_used_bucket: set the percentage of occupied physical buckets 
 * @param elem_per_bucket: set the expected number of items for each virtual bucket
 * @return a pointer a new queue
 */
void* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{    
    phynbcq_t * res = NULL;
    unsigned int i = 0;
	int res_mem_posix = 0;

    // init fraser garbage collector/allocator 
	_init_gc_subsystem();
	// add allocator of nbc_bucket_node
    _init_queue_gc_subsystem();
    
    gc_hid[0] = gc_add_hook(std_free_hook);
    //
    critical_enter();
	critical_exit();

    // allocate memory
	res_mem_posix = 
        posix_memalign((void**)(&res), CACHE_LINE_SIZE, sizeof(phynbcq_t));
	if(res_mem_posix != 0) 	
        error("No enough memory to allocate queue\n");
	
    res_mem_posix = 
        posix_memalign((void**)(&res->hashtable), CACHE_LINE_SIZE, sizeof(table_t));
	if(res_mem_posix != 0)
        error("No enough memory to allocate set table\n");
	res_mem_posix = 
        posix_memalign((void**)(&res->hashtable->array), CACHE_LINE_SIZE, MINIMUM_SIZE*sizeof(bucket_t));
	if(res_mem_posix != 0)
        error("No enough memory to allocate array of heads\n");
	
    // @TODO alloca table
    res->threshold = threshold;
	res->perc_used_bucket = perc_used_bucket;
	res->elem_per_bucket = elem_per_bucket;
	res->pub_per_epb = perc_used_bucket * elem_per_bucket;
    res->read_table_period = READTABLE_PERIOD;
    res->tail = bucket_malloc(UINT_MAX, BCKT_TAIL, NID);

    res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;
	res->hashtable->current = 0;
	res->hashtable->last_resize_count = 0;
	res->hashtable->resize_count = 0;
	res->hashtable->e_counter.count = 0;
	res->hashtable->d_counter.count = 0;
    res->hashtable->read_table_period = res->read_table_period;
	res->hashtable->tail = res->tail;
	
    for (i = 0; i < MINIMUM_SIZE; i++)
	{
        res->hashtable->array[i].next = res->tail;
        res->hashtable->array[i].type = BCKT_HEAD;
        res->hashtable->array[i].index = i;
        res->hashtable->array[i].head.next = NULL;
    }

    return res;
}

/**
 * This function implements the enqueue interface of the NBCQ.
 * Cost O(1) when succeeds
 *
 * @param q: pointer to the queueu
 * @param timestamp: the key associated with the value
 * @param payload: the value to be enqueued
 * @return true if the event is inserted in the set table else false
 */
int pq_enqueue(void* q, pkey_t timestamp, void* payload)
{
    assertf(timestamp < MIN || timestamp >= INFTY, "Key out of range %s\n", "");
    
    phynbcq_t* queue;
    table_t* h;
    bucket_t *bucket;
    node_t *new_node;
    
    double pub;
    
    unsigned long long newIndex;

    unsigned int epb;
    unsigned int th;
    unsigned int size, index, epoch;

    int res, con_en;

    queue = (phynbcq_t*) q;

    pub = queue->perc_used_bucket;
	epb = queue->elem_per_bucket;
	th = queue->threshold;

    con_en = 0;

    critical_enter();

    res = MOV_FOUND;

    while(res != OK) 
    {
        // It is the first iteration or a node marked as MOV has been met (a resize is occurring)
		if(res == MOV_FOUND){
			// check for a resize
			h = read_table(&queue->hashtable, th, epb, pub);
			// get actual size
			size = h->size;
	        // read the actual epoch
        	epoch = (h->current & MASK_EPOCH);
			// compute the index of the virtual bucket
			newIndex = hash(timestamp, h->bucket_width);
			// compute the index of the physical bucket
			index = ((unsigned int) newIndex) % size;

            // @TODO compute dest node for allocation
            new_node = node_malloc(payload, timestamp, 0, NID);
            new_node->epoch = epoch;
            
			// get the bucket
			bucket = h->array + index;
			// read the number of executed enqueues for statistics purposes
			con_en = h->e_counter.count;
		}

		#if KEY_TYPE != DOUBLE
		if(res == PRESENT){
			res = 0;
			goto out;
		}
		#endif

		// search the two adjacent nodes that surround the new key and try to insert with a CAS 
	    res = search_and_insert(bucket, newIndex, REMOVE_DEL_INV, new_node, &new_node);

    }

    flush_current(h, newIndex, new_node);
    performed_enqueue++;
    res = 1;

    concurrent_enqueue += (unsigned long long) (__sync_fetch_and_add(&h->e_counter.count, 1) - con_en);

    #if KEY_TYPE != DOUBLE
    out:
    #endif
	critical_exit();
	return res;
}

/**
 * This function extract the minimum item from the NBCQ. 
 * The cost of this operation when succeeds should be O(1)
 *
 * @param  q: pointer to the interested queue
 * @param  result: location to save payload address
 * @return the highest priority 
 *
 */
pkey_t pq_dequeue(void *q, void** result)
{
    phynbcq_t* queue;
    table_t *h = NULL;

    bucket_t *min, *min_next, 
					*left_bucket, 
					*array, *right_bucket;

    node_t *left_node, *left_node_next, *tail;

    pkey_t left_ts;
  
    unsigned long long current, old_current, new_current;
    unsigned long long index;
	unsigned long long epoch;

    int con_de = 0;

    queue = (phynbcq_t*) q;

    unsigned int size, attempts, counter;

    double pub = queue->perc_used_bucket;
	unsigned int epb = queue->elem_per_bucket;
	unsigned int th = queue->threshold;

    performed_dequeue++;

    critical_enter();
begin:
    // Get the current set table
	h = read_table(&queue->hashtable, th, epb, pub);

    // Get data from the table
	size = h->size;
	array = h->array;
	current = h->current;
	con_de = h->d_counter.count;
    attempts = 0;

    do 
    {
        // To many attempts: there is some problem? recheck the table
		if( h->read_table_period == attempts){
			goto begin;
		}
		attempts++;

        // get fields from current
		index = current >> 32;
		epoch = current & MASK_EPOCH;

        // get the physical bucket
		min = array + (index % (size));
		min_next = min->next;

        // a reshuffle has been detected => restart
		if(is_marked(min_next, MOV))
            goto begin;

        bucket_search(min, index, &left_bucket, &right_bucket, &counter);
        right_bucket = get_unmarked(right_bucket);

        // if i'm here it means that the physical bucket was empty. Check for queue emptyness
		if(left_bucket->type == BCKT_HEAD  && right_bucket->type == BCKT_TAIL && size == 1 && !is_marked(min_next, MOV)) 
		{
			critical_exit();
			*result = NULL;
			return INFTY;
		}

        // Bucket present
		if(left_bucket->index == index && left_bucket->type != BCKT_HEAD)
        {
            left_node = &left_bucket->head;
            tail = left_bucket->tail;
            do 
            {
                // get data from the current node	
			    left_node_next = left_node->next;
			    left_ts = left_node->timestamp;

                // increase count of traversed deleted items
			    counter++;

                // Skip marked nodes, invalid nodes and nodes with timestamp out of range (Head of the bucket)
			    if(is_marked(left_node_next, DEL) || is_marked(left_node_next, INV) || (left_ts == -1 && left_node != tail)) 
                    continue;

                // Abort the operation since there is a resize or a possible insert in the past
			    if(is_marked(left_node_next, MOV) || left_node->epoch > epoch)
                    goto begin;
			
			    // The virtual bucket is empty
			    if(left_node == tail)
                    break;

                // the node is a good candidate for extraction! lets try for it
			    int res = atomic_test_and_set_x64(UNION_CAST(&left_node->next, unsigned long long*));

			    // the extraction is failed
			    if(!res) left_node_next = left_node->next;

			    // the node cannot be extracted && is marked as MOV	=> restart
			    if(is_marked(left_node_next, MOV))	goto begin;

			    // the node cannot be extracted && is marked as DEL => skip
			    if(is_marked(left_node_next, DEL))	continue;
			
			    // the node has been extracted

			    // use it for count the average number of traversed node per dequeue
			    scan_list_length += counter;
			    // use it for count the average of completed extractions
			    concurrent_dequeue += (unsigned long long) (__sync_fetch_and_add(&h->d_counter.count, 1) - con_de);

			    *result = left_node->payload;
				
			    critical_exit();

                return left_ts;
            } while((left_node = get_unmarked(left_node_next)));
        }
        
        // bucket empty or absent
		new_current = h->current;
		if(new_current == current)
        {
            if(index == MASK_EPOCH && h->e_counter.count == 0)
                goto begin;
			num_cas++;
			index++;
            old_current = VAL_CAS( &(h->current), current, ((index << 32) | epoch) );
			if(old_current == current){
				current = ((index << 32) | epoch);
				num_cas_useful++;
			}
			else
				current = old_current;
        }
        else
			current = new_current;
    } while(1);

    critical_exit();    
    return INFTY;
}

void pq_report(int TID)
{
    printf("%d- "
	"Enqueue: %.10f LEN: %.10f ### "
	"Dequeue: %.10f LEN: %.10f NUMCAS: %llu : %llu ### "
	"NEAR: %llu "
	"RTC:%d, M:%lld\n",
			TID,
			((float)concurrent_enqueue) /((float)performed_enqueue),
			((float)scan_list_length_en)/((float)performed_enqueue),
			((float)concurrent_dequeue) /((float)performed_dequeue),
			((float)scan_list_length)   /((float)performed_dequeue),
			num_cas, num_cas_useful,
			near,
			read_table_count	  ,
			malloc_count);
}

void pq_reset_statistics(){}

unsigned int pq_num_malloc(){ return (unsigned int) malloc_count; }
