#ifndef _C_FC_SKIPLIST_
#define _C_FC_SKIPLIST_

    struct _fc_skiplist;
    typedef struct _fc_skiplist fc_skiplist;

    fc_skiplist* new_fc_skiplist();
    bool fcs_enqueue(fc_skiplist* fcs, int key);
    int fcs_dequeue(fc_skiplist* fcs);


#endif /* _C_FC_SKIPLIST_ */