/*
 * Init structure, setup sentinel head and tail nodes.
 */

#include "ChunkedPriorityQueue.h"
#include "test.h"
#include "rand.h"


__thread unsigned long nextr = 1;
__thread ThrInf tinfo;
__thread int local_setup = 1;


#define TID tid


extern __thread unsigned int TID;

using namespace std;

extern "C" {

void*
pq_init(unsigned int threads, double none, unsigned long long max_offset)
;

/* Cleanup, mark all the nodes for recycling. */
void pq_destroy(void *pq){}
void pq_report(){}
void pq_reset_statistics(){ }
unsigned int pq_num_malloc(){  return 0;}


void 
pq_enqueue(void *p, cb_key_t k, void *v);

cb_key_t
pq_dequeue(void *p, void **result);
}
	

static inline void local_init(void* p, int my_id){
	if(local_setup){
		tinfo.pq=(ChunkedPriorityQueue*)p;
		tinfo.values	= NULL;
		tinfo.id		= my_id;
	}
}



void*
pq_init(unsigned int threads, double none, unsigned long long max_offset)
{
	ChunkedPriorityQueue *pq = new ChunkedPriorityQueue();
	simSRandom(1000);
	return (void*) pq;
}


void 
pq_enqueue(void *p, cb_key_t k, void *v){
	local_init(p, TID);
	ChunkedPriorityQueue *pq = static_cast<ChunkedPriorityQueue *>(p);
	pq->insert(k, &tinfo);
}


cb_key_t
pq_dequeue(void *p, void **result){
	local_init(p, TID);
	ChunkedPriorityQueue *pq = static_cast<ChunkedPriorityQueue *>(p);
	return pq->delmin(&tinfo);
}