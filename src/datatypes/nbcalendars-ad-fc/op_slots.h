#ifndef OPSLOT_INFO_H
#define OPSLOT_INFO_H

#include "common_nb_calqueue.h"
#include "../../gc/ptst.h"

typedef struct _operation_s operation_t;
typedef struct _slotInfo_s slotInfo_t;

struct _operation_s {
    int type;			// ENQ | DEQ | -1 is invalid
    int ret_value;
    pkey_t timestamp;			// ts of node to enqueue
    void *payload;	
};

struct _slotInfo_s {
    operation_t request;
    slotInfo_t* volatile next;
    int volatile ts; // epoch counter used for FC(?)
    void* info;
};

extern __thread ptst_t *ptst;
static int gc_entry;

static __thread slotInfo_t* _tls_slot_info = NULL;
extern slotInfo_t* volatile _tail_slot;
extern volatile int _ts;

// int gc
static inline void init_gc_slots() 
{
    gc_entry = gc_add_allocator(sizeof(slotInfo_t));
};


static inline slotInfo_t* newSlotInfo() 
{  
    slotInfo_t* ret = (slotInfo_t*) gc_alloc(ptst, gc_entry);
    ret->request.ret_value = -1;
    ret->request.type = -1;
    ret->next = NULL;
    return ret;
};

static inline void slot_free_unsafe(slotInfo_t* slot)
{
    gc_free(ptst, slot, gc_entry);
};

static inline void slot_free(slotInfo_t* slot)
{
    gc_free(ptst, slot, gc_entry);
};

static inline slotInfo_t* get_new_slot()
{
    slotInfo_t* curr_tail;
    slotInfo_t* my_slot = newSlotInfo();
    _tls_slot_info = my_slot;
    do 
    {
        curr_tail = _tail_slot;
        my_slot->next = curr_tail;
    } while(!BOOL_CAS(&_tail_slot, curr_tail, my_slot));

    return my_slot;

};

static inline void enq_slot(slotInfo_t* slot)
{
    slotInfo_t* curr_tail;
    do
    {
        curr_tail = _tail_slot;
        slot->next = curr_tail;
    } while (!BOOL_CAS(&_tail_slot, curr_tail, slot));

};

static inline void enq_if_need(slotInfo_t* slot)
{
    if(NULL == slot->next) {
		enq_slot(slot);
	}
};


#endif