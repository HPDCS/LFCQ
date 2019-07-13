#ifndef __MY_RTM
#define __MY_RTM

#include "common.h"

#define RTM

extern __thread unsigned long long rtm_prova, rtm_failed, rtm_retry, rtm_conflict, rtm_capacity, rtm_debug,  rtm_explicit,  rtm_nested, rtm_insertions, insertions;
#ifdef RTM

#define DEB(x) {}
//#define DEB(x) printf(x)

#define TM_COMMIT() _xend()
#define TM_ABORT()  _xabort(0xff)

#define ATOMIC(...)  \
{ retry_tm:\
	++rtm_prova;/*printf("Transactions %u, %u, %u\n", prova, failed, insertions);*/\
	unsigned int __status = 0;\
	unsigned int fallback = 50;\
	/*retry_tm:*/\
	if ((__status = _xbegin ()) == _XBEGIN_STARTED)
	//{
	//}

#define FALLBACK(...) \
	else{\
	\
		/*  Transaction retry is possible. */\
		if(__status & _XABORT_RETRY) {DEB("RETRY\n");rtm_retry++;while(fallback--!=0)_mm_pause();goto retry_tm;}\
		/*  Transaction abort due to a memory conflict with another thread */\
		else if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n");rtm_conflict++;}\
		/*  Transaction abort due to the transaction using too much memory */\
		else if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n");rtm_capacity++;}\
		/*  Transaction abort due to a debug trap */\
		else if(__status & _XABORT_DEBUG) {DEB("DEBUG\n");rtm_debug++;}\
		/*  Transaction abort in a inner nested transaction */\
		else if(__status & _XABORT_NESTED) {DEB("NESTES\n");rtm_nested++;}\
		/*  Transaction explicitely aborted with _xabort. */\
		else if(__status & _XABORT_EXPLICIT){DEB("EXPLICIT\n");rtm_explicit++;}\
		else{DEB("Other\n");rtm_failed++;}\
			//{
			//}

#define END_ATOMIC(...) 	\
		\
		/*else{DEB("Other\n");failed++;}*/\
	}\
}

	
#define ATOMIC2(...) ATOMIC(...)
#define FALLBACK2(...) FALLBACK(...)
#define END_ATOMIC2(...) 	END_ATOMIC(...)

#else

#include <pthread.h>

#define TM_COMMIT() /* */
#define TM_ABORT()  {aborted=1;break;}

#define ATOMIC(lock)   \
{	++rtm_prova;\
	unsigned int aborted=0;\
	do{\
		pthread_spin_lock(lock);
		//{
		//}
	
#define FALLBACK(lock) \
	}while(0);\
	if(aborted){\
		rtm_failed++;\
		pthread_spin_unlock(lock); \
		/*printf("EXPLICIT\n");*/
		//{
		//}

#define END_ATOMIC(lock) \
	}\
	pthread_spin_unlock(lock);\
}

#define ATOMIC2(locka, lockb)   \
{	++rtm_prova;\
	unsigned int aborted=0;\
	do{\
		pthread_spin_lock(locka);\
		pthread_spin_lock(lockb);
		//{
		//}
	
#define FALLBACK2(locka, lockb) \
	}while(0);\
	if(aborted){\
		rtm_failed++;\
		pthread_spin_unlock(locka); \
		pthread_spin_unlock(lockb); \
		/*printf("EXPLICIT\n");*/
		//{
		//}

#define END_ATOMIC2(locka, lockb) \
	}\
	pthread_spin_unlock(locka);\
	pthread_spin_unlock(lockb);\
}


#endif

#endif
