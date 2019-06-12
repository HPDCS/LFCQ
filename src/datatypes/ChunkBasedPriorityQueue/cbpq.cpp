/*
 * Init structure, setup sentinel head and tail nodes.
 */

#include "ChunkedPriorityQueue.h"
#include "test.h"
#include "rand.h"
#include "../../key_type.h"


__thread unsigned long nextr = 1;
__thread ThrInf tinfo;
__thread int local_setup = 1;


#define TID tid


extern __thread unsigned int TID;

using namespace std;

	

static inline void local_init(void* p, int my_id){
	if(local_setup){
		tinfo.pq=(ChunkedPriorityQueue*)p;
		tinfo.values	= NULL;
		tinfo.id		= my_id;
	}
}

extern "C" {
int   pq_enqueue(void *queue, pkey_t timestamp, void* payload);
pkey_t pq_dequeue(void *queue, void **payload);
void* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
void pq_report(int tid);
void pq_prune();
void pq_reset_statistics();
unsigned int pq_num_malloc();
}

/* Cleanup, mark all the nodes for recycling. */
void pq_destroy(void *pq){}
void pq_report(int tid){}
void pq_reset_statistics(){ }
unsigned int pq_num_malloc(){  return 0;}


void* pq_init(unsigned int threads, double none, unsigned int max_offset)
{
	ChunkedPriorityQueue *pq = new ChunkedPriorityQueue();
	simSRandom(1000);
	return (void*) pq;
}


int 
pq_enqueue(void *p, pkey_t k, void *v){
	local_init(p, TID);
	ChunkedPriorityQueue *pq = static_cast<ChunkedPriorityQueue *>(p);
	pq->insert(k, &tinfo);
	return 1;
}


pkey_t
pq_dequeue(void *p, void **result){
	local_init(p, TID);
	ChunkedPriorityQueue *pq = static_cast<ChunkedPriorityQueue *>(p);
	return pq->delmin(&tinfo);
}