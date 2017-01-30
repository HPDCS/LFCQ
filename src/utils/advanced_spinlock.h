#ifndef __HPDCS_ADVANCED_SPIN_LOCK__
#define __HPDCS_ADVANCED_SPIN_LOCK__


typedef struct _advanced_spin_lock advanced_spin_lock;
typedef struct _node_spin_lock node_spin_lock;


typedef struct { volatile unsigned int lock; } hpdcs_spinlock_t;


struct _node_spin_lock
{
	hpdcs_spinlock_t main_spinlock;
	node_spin_lock* next;
	unsigned int tid;
};


struct _advanced_spin_lock
{
	hpdcs_spinlock_t main_spinlock;
	hpdcs_spinlock_t list_spinlock;
	node_spin_lock* next;
	node_spin_lock* tail;
};

void acquire_lock(advanced_spin_lock* spin_lock);
void release_lock(advanced_spin_lock* spin_lock);


void hpdcs_spin_init_lock(hpdcs_spinlock_t* my_lock);
void hpdcs_spin_lock(hpdcs_spinlock_t* my_lock);
int hpdcs_spin_trylock(hpdcs_spinlock_t* my_lock);
void hpdcs_spin_unlock(hpdcs_spinlock_t* my_lock);


#endif
