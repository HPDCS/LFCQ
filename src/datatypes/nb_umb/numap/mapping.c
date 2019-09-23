#include <numa.h>
#include <numaif.h>

#include "mapping.h"

//#define MAP_DEBUG
__thread op_node** res_mapping      = NULL; // slot per "postare" la risposta su nodi diversi [NID] è la risposta che sto aspettando
__thread op_node** req_out_mapping  = NULL; // slot per "postare" la richiesta su altri nodi
__thread op_node** req_in_mapping   = NULL; // slot per "leggere" la richiesta da altri nodi

void init_mapping() 
{
    int i;
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i)
        mapping[i] = numa_alloc_onnode(sizeof(op_node)*THREADS, i);

    //printf("TID %d with LTID %d\n", TID, LTID);
    #ifdef MAP_DEBUG
    
    int j;
    for (i = 0; i < THREADS; ++i)
    {
        printf("THREAD %d \t", i);
        for (j = 0; j < ACTIVE_NUMA_NODES; ++j)
        {
            printf("%p\t", &mapping[j][i]);
        } 
        printf("\n");
    }
    printf("#########\n");  
    #endif
}

static inline void init_local_mapping() 
{
    res_mapping     = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID); // tutti su nodi numa diversi
    req_out_mapping = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);
    req_in_mapping  = numa_alloc_onnode(sizeof(op_node*)*_NUMA_NODES, NID);

    int i, j = LTID;
    
    for (i = 0; i < ACTIVE_NUMA_NODES; ++i)
    {
        res_mapping[i]      = &mapping[i][j];
        res_mapping[i]->response = 1;

        req_out_mapping[i]  = &mapping[i][TID];
        req_out_mapping[i]->response = 1;

        req_in_mapping[i]   = &mapping[NID][j];
        req_in_mapping[i]->response = 1;

        j += num_cpus_per_node;
    }
    #ifdef MAP_DEBUG
    printf("TID %d with LTID %d\n", TID, LTID);
    for (i = 0; i<ACTIVE_NUMA_NODES; ++i) 
    {
        printf("TID %d - LID %d - NODE %d - res %p - out %p - in %p\n", TID, LTID, i, res_mapping[i], req_out_mapping[i], req_in_mapping[i]);
    } 
    #endif
}

op_node* get_request_slot_from_node(unsigned int numa_node)
{
    if (unlikely(req_in_mapping==NULL))
        init_local_mapping();

    return req_in_mapping[numa_node];

}

op_node* get_request_slot_to_node(unsigned int numa_node)
{
    if (unlikely(req_out_mapping==NULL))
        init_local_mapping();

    return req_out_mapping[numa_node];
}

op_node* get_response_slot(unsigned int numa_node)
{
    if (unlikely(res_mapping==NULL))
        init_local_mapping();

    return res_mapping[numa_node];
}

