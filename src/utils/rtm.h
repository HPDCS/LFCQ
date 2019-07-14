#ifndef __MY_RTM
#define __MY_RTM

#include "common.h"

#define RTM

extern __thread unsigned long long rtm_prova, rtm_other, rtm_failed, rtm_retry, rtm_conflict, rtm_capacity, rtm_debug,  rtm_explicit,  rtm_nested, rtm_a, rtm_b, rtm_insertions, insertions;
#ifdef RTM

#define DEB(x) {}
//#define DEB(x) printf(x)

#define TM_COMMIT() _xend()
#define TM_ABORT(x)  _xabort(x)


unsigned int gl_tm_seed = 0;
__thread unsigned int lo_tm_seed = 0;
__thread unsigned int tm_init=0;
#define ATOMIC(...)  \
{\
if(!tm_init) {tm_init=1;lo_tm_seed = __sync_add_and_fetch(&gl_tm_seed, 1); srand(&lo_tm_seed);}\
retry_tm:\
	++rtm_prova;/*printf("Transactions %u, %u, %u\n", prova, failed, insertions);*/\
	unsigned int __status = 0;\
	unsigned int fallback = rand_r(&lo_tm_seed)%512;\
while(0 && fallback--!=0)_mm_pause();\
	/*retry_tm:*/\
	if ((__status = _xbegin ()) == _XBEGIN_STARTED)
	//{
	//}

#define FALLBACK(...) \
	else{\
	rtm_failed++;\
		/*  Transaction retry is possible. */\
		if(__status & _XABORT_RETRY) {DEB("RETRY\n");rtm_retry++;/*fallback=rand_r(&lo_tm_seed)%512;while(fallback--!=0)_mm_pause();*/goto retry_tm;}\
		/*  Transaction abort due to a memory conflict with another thread */\
		if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n");rtm_conflict++;}\
		/*  Transaction abort due to the transaction using too much memory */\
		if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n");rtm_capacity++;}\
		/*  Transaction abort due to a debug trap */\
		if(__status & _XABORT_DEBUG) {DEB("DEBUG\n");rtm_debug++;}\
		/*  Transaction abort in a inner nested transaction */\
		if(__status & _XABORT_NESTED) {DEB("NESTES\n");rtm_nested++;}\
		/*  Transaction explicitely aborted with _xabort. */\
		if(__status & _XABORT_EXPLICIT){DEB("EXPLICIT\n");rtm_explicit++;}\
		if(__status == 0){DEB("Other\n");rtm_other++;}\
		if(_XABORT_CODE(__status) == 0xf0) {rtm_a++;}\
		if(_XABORT_CODE(__status) == 0xf1) {rtm_b++;}
			//{
			//}

#define END_ATOMIC(...) 	\
		\
		/*else{DEB("Other\n");failed++;}*/\
	}\
}

#define ATOMIC2(...)  \
{ retry_tm:\
	;\
	unsigned int __status = 0;\
	if ((__status = _xbegin ()) == _XBEGIN_STARTED)
	//{
	//}

#define FALLBACK2(...) \
	else{\
	\
		/*  Transaction retry is possible. */\
		if(__status & _XABORT_RETRY) {DEB("RETRY\n");goto retry_tm;}\
		/*  Transaction abort due to a memory conflict with another thread */\
		else if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n");}\
		/*  Transaction abort due to the transaction using too much memory */\
		else if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n");}\
		/*  Transaction abort due to a debug trap */\
		else if(__status & _XABORT_DEBUG) {DEB("DEBUG\n");}\
		/*  Transaction abort in a inner nested transaction */\
		else if(__status & _XABORT_NESTED) {DEB("NESTES\n");}\
		/*  Transaction explicitely aborted with _xabort. */\
		else if(__status & _XABORT_EXPLICIT){DEB("EXPLICIT\n");}\
		else{DEB("Other\n");}
			//{
			//}

#define END_ATOMIC2(...) 	\
	}\
}


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
