/*
 * Init structure, setup sentinel head and tail nodes.
 */

#include "ChunkedPriorityQueue.h"
#include "rand.h"
#include "cb_types.h"


using namespace std;

extern "C" {

void*
pq_init(unsigned int threads, double none, unsigned long long max_offset)
{
	ChunkedPriorityQueue *pq = new ChunkedPriorityQueue();
	simSRandom(1000);
	return (void*) pq;
}

/* Cleanup, mark all the nodes for recycling. */
void
pq_destroy(void *pq)
{

}

void pq_report(){
}

void pq_reset_statistics(){ }

unsigned int pq_num_malloc(){  return 0;}



void 
pq_enqueue(void *p, cb_key_t k, void *v){
	ChunkedPriorityQueue *pq = static_cast<ChunkedPriorityQueue *>(p);
	pq->insert(k, NULL);
}


cb_key_t
pq_dequeue(void *p, void **result){
	ChunkedPriorityQueue *pq = static_cast<ChunkedPriorityQueue *>(p);
	return pq->delmin(NULL);
}
}
	