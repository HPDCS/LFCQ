#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>

#include "../datatypes/nb_calqueue.h"
#include "advanced_spinlock.h"


extern __thread unsigned int TID;
__thread node_spin_lock tid_lock;


	
void hpdcs_spin_init_lock(hpdcs_spinlock_t* my_lock)
{
	__sync_lock_release(&(my_lock->lock));
}


void hpdcs_spin_lock(hpdcs_spinlock_t* my_lock)
{
	while (__sync_lock_test_and_set(&(my_lock->lock), 1))
		while(my_lock->lock);
}
			

int hpdcs_spin_trylock(hpdcs_spinlock_t* my_lock)
{
	return  (__sync_lock_test_and_set(&(my_lock->lock), 1));
}
	
void hpdcs_spin_unlock(hpdcs_spinlock_t* my_lock)
{
	__sync_lock_release(&(my_lock->lock));
}
	


void acquire_lock(advanced_spin_lock* my_lock)
{
	if(spin_trylock(&(my_lock->main_spinlock)) == true)
	//if(hpdcs_spin_trylock(&(my_lock->main_spinlock)))
	{
	//	printf("Il prossimo sono IO  %u\n",TID);
		return;
	} 
	
	
	//spinlock_init(&(tid_lock.main_spinlock)); 
	//spin_lock(&(tid_lock.main_spinlock));
	hpdcs_spin_init_lock(&(tid_lock.main_spinlock)); 
	hpdcs_spin_lock(&(tid_lock.main_spinlock));
	
	tid_lock.next = NULL;
	tid_lock.tid = TID;
	
	//spin_lock(&(my_lock->list_spinlock));
	//while(spin_trylock(&(my_lock->list_spinlock)) == false)
	//	while(my_lock->list_spinlock.lock != 0);
	
		hpdcs_spin_lock(&(my_lock->list_spinlock));
		
		if(my_lock->tail != NULL)
		{
			my_lock->tail->next = &tid_lock;
			
		}
		else
			my_lock->next = &tid_lock;
		
		my_lock->tail = &tid_lock;
	
	//spin_unlock(&(my_lock->list_spinlock.lock));
	hpdcs_spin_unlock(&(my_lock->list_spinlock));
	
	//spin_lock(&(tid_lock.main_spinlock));
	//while(spin_trylock(&(tid_lock.main_spinlock)) == false)
		//while(tid_lock.main_spinlock.lock != 0);
	
	hpdcs_spin_lock(&(tid_lock.main_spinlock));
	
	
	
	//printf("Sono IO %u\n", TID);
}
			
	
void release_lock(advanced_spin_lock* my_lock)
{
	node_spin_lock *tmp;
	
	
	//spin_lock(&(my_lock->list_spinlock));
	//while(spin_trylock(&(my_lock->list_spinlock)) == false)
	//	while(my_lock->list_spinlock.lock != 0);
	hpdcs_spin_lock(&(my_lock->list_spinlock));
	
		tmp = my_lock->next;
		
		if(tmp != NULL)
		{
			my_lock->next = tmp->next;
			if(my_lock->next == NULL)
				my_lock->tail = NULL;
			//	printf("sarai il prossimo  %u\n", tmp->tid);
			//	spin_unlock(&(tmp->main_spinlock));
			hpdcs_spin_unlock(&(tmp->main_spinlock));
		}
		else
		{
			//	printf("sarai il prossimo  NESSUNO\n");
			//	spin_unlock(&(my_lock->main_spinlock));
			hpdcs_spin_unlock(&(my_lock->main_spinlock));
		}

	//spin_unlock(&(my_lock->list_spinlock));
	hpdcs_spin_unlock(&(my_lock->list_spinlock));
}
