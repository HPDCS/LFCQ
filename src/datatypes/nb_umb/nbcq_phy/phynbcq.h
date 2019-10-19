// type needed
#ifndef _PHYNBCQ_H_
#define _PHYNBCQ_H_

#include <math.h>

#include "../../../key_type.h"
#include "../../../arch/atomic.h"
#include "../../../utils/hpdcs_utils.h"
#include "../gc/ptst.h"

#include "nb_list_defs.h"

extern __thread ptst_t *ptst;
extern int gc_aid[];
extern int gc_hid[];

#define TID tid
#define NID nid
#define LTID ltid

#define GC_BUCKET 0
#define GC_NODE 1

extern __thread unsigned int TID;
extern __thread unsigned int NID;
extern __thread unsigned int LTID;

extern __thread unsigned long long malloc_count;

typedef struct _node_s      node_t;
typedef struct _bucket_s    bucket_t;
typedef struct _table_s     table_t;
typedef struct _phynbcq_s   phynbcq_t;

struct _node_s
{
    void* payload;
    unsigned long epoch;
    // 16
    pkey_t timestamp;
    char ts_pad[8-sizeof(pkey_t)];
    
    unsigned int counter;
    unsigned int pad;
    // 32
    node_t * volatile next;
    node_t * volatile replica;
    char pad2[16];
    // 64
};

#define BCKT_ITEM (0x0Ull)
#define BCKT_HEAD (0x1Ull)
#define BCKT_TAIL (0x2Ull)


struct _bucket_s 
{
    node_t head;    //64 
    
    node_t * tail;
    bucket_t * next;
    
    unsigned long index;
    unsigned int type;
    unsigned int pad;
    // 96
};

struct _table_s  
{
    table_t * volatile new_table;
    bucket_t * tail;
    bucket_t * array;
    unsigned int size;
    unsigned int pad;
    // 32
    double bucket_width;
    unsigned int read_table_period; 
	unsigned int last_resize_count; 
	unsigned int resize_count; 		
    char zpad1[12];

    atomic_t e_counter;	
	char zpad2[60];

	atomic_t d_counter;
	char zpad3[60];

    volatile unsigned long long current;
    char zpad4[56];

};

struct _phynbcq_s
{
    unsigned int threshold;
	unsigned int elem_per_bucket;
	double perc_used_bucket;
    double pub_per_epb;
    bucket_t * tail;
    unsigned int read_table_period;
	unsigned int pad;
    table_t * volatile hashtable;
};

/**
 * This function computes the index of the destination bucket in the hashtable
 *
 * @author Romolo Marotta
 *
 * @param timestamp the value to be hashed
 * @param bucket_width the depth of a bucket
 *
 * @return the linear index of a given timestamp
 */
static inline unsigned int hash(pkey_t timestamp, double bucket_width)
{
	double tmp1, tmp2, res_d = (timestamp / bucket_width);
	long long res =  (long long) res_d;
	int upA = 0;
	int upB = 0;

	if(__builtin_expect(res_d > 4294967295, 0))
	{
		error("Probable Overflow when computing the index: "
				"TS=%e,"
				"BW:%e, "
				"TS/BW:%e, "
				"2^32:%e\n",
				timestamp, bucket_width, res_d,  pow(2, 32));
	}

	tmp1 = ((double) (res)	 ) * bucket_width;
	tmp2 = ((double) (res+1) )* bucket_width;
	
	upA = - LESS(timestamp, tmp1);
	upB = GEQ(timestamp, tmp2 );
		
	return (unsigned int) (res+ upA + upB);

}

static inline void _init_queue_gc_subsystem() 
{
    gc_aid[GC_NODE]     = gc_add_allocator(sizeof(node_t));
    gc_aid[GC_BUCKET]   = gc_add_allocator(sizeof(bucket_t));
}

static inline node_t* node_malloc(void *payload, pkey_t timestamp, unsigned int tie_breaker, unsigned int node)
{
    node_t* res;

    res = gc_alloc_node(ptst, gc_aid[GC_NODE], node);
    if (unlikely(is_marked(res) || res == NULL))
	{
		error("%lu - Not aligned Node or No memory\n", TID);
		abort();
	}

    res->counter = tie_breaker;
	res->next = NULL;
	res->replica = NULL;
	res->payload = payload;
	res->epoch = 0;
	res->timestamp = timestamp;

	return res;
}

static inline bucket_t* bucket_malloc(unsigned long index, unsigned int type, unsigned int node)
{
    bucket_t* res;
    res = gc_alloc_node(ptst, gc_aid[GC_BUCKET], node);
    if (unlikely(is_marked(res) || res == NULL))
    {
        error("%lu - Not aligned Bucket or No memory\n", TID);
		abort();
    }

    res->index = index;
    res->type = type;

    res->tail = node_malloc(NULL, INFTY, 0, node);
    res->head.next = res->tail;
    res->head.timestamp = -1.0;
    res->next = NULL;

    return res;
}

static inline void connect_to_be_freed_bucket_list(bucket_t *start, unsigned int counter)
{
	bucket_t *tmp_next;
	start = get_unmarked(start);
	while(start != NULL && counter-- != 0)              
	{                                              
		tmp_next = start->next;                           
		gc_free(ptst, (void *)start, gc_aid[GC_BUCKET]);
		start =  get_unmarked(tmp_next);                  
	} 
}

static inline void connect_to_be_freed_node_list(node_t *start, unsigned int counter)
{
	node_t *tmp_next;
	start = get_unmarked(start);
	while(start != NULL && counter-- != 0)
	{
		tmp_next = start->next;
		gc_free(ptst, (void *)start, gc_aid[GC_NODE]);
		start =  get_unmarked(tmp_next);
	}
}

#endif /* !_PHYNBCQ_H_ */