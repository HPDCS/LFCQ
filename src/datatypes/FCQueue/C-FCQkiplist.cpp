#include "FCSkipList.h"
#include "C-FCQkiplist.h"

#include "../../key_type.h"
#include "../../arch/atomic.h"
#include "../../utils/hpdcs_utils.h"

#define TID tid
extern __thread unsigned int TID;

extern "C" {
int   pq_enqueue(void *queue, pkey_t timestamp, void* payload);
pkey_t pq_dequeue(void *queue, void **payload);
void* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
void pq_report(int tid);
void pq_prune();
void pq_reset_statistics();
unsigned int pq_num_malloc();
}

struct _fc_skiplist
{
    void* fclist;
};

fc_skiplist* new_fc_skiplist() 
{
    fc_skiplist* ret = (fc_skiplist*) malloc(sizeof(fc_skiplist));
    ret->fclist = new FCSkipList();
    return ret;

}

bool fcs_enqueue(fc_skiplist* fcs, pkey_t key) 
{
    FCSkipList* x = static_cast<FCSkipList*>(fcs->fclist);
    return x->add2(TID, key);
}

int fcs_dequeue(fc_skiplist* fcs) 
{
        FCSkipList* x = static_cast<FCSkipList*>(fcs->fclist);
        return x->remove2(TID);
}

void* pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket)
{
    return new_fc_skiplist();
}

int pq_enqueue(void* q, pkey_t timestamp, void* payload)
{
    fc_skiplist* fcl = (fc_skiplist*) q;
    return fcs_enqueue(fcl, timestamp);    
}

pkey_t pq_dequeue(void *q, void** result)
{

    fc_skiplist* fcl = (fc_skiplist*) q;
    return fcs_dequeue(fcl);
}

void pq_report(int TID)
{
    return;
}

void pq_reset_statistics()
{
    return;
}

unsigned int pq_num_malloc() 
{
    return 0;
}


