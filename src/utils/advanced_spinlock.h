#ifndef __HPDCS_ADVANCED_SPIN_LOCK__
#define __HPDCS_ADVANCED_SPIN_LOCK__


#include "../arch/atomic.h"

typedef struct _advanced_spin_lock advanced_spin_lock;
typedef struct _node_spin_lock node_spin_lock;


struct _node_spin_lock
{
	spinlock_t main_spinlock;
	node_spin_lock* next;
	unsigned int tid;
};


struct _advanced_spin_lock
{
	spinlock_t main_spinlock;
	spinlock_t list_spinlock;
	node_spin_lock* next;
	node_spin_lock* tail;
};

void acquire_lock(advanced_spin_lock* spin_lock);
void release_lock(advanced_spin_lock* spin_lock);

#endif
