#ifndef __MY_RTM
#define __MY_RTM

#include "common.h"

#define RTM

extern __thread unsigned long long rtm_prova, rtm_other, rtm_failed, rtm_retry, rtm_conflict, rtm_capacity, rtm_debug,  rtm_explicit,  rtm_nested, rtm_a, rtm_b, rtm_insertions, insertions;
extern __thread unsigned long long rtm_prova2, rtm_other2, rtm_failed2, rtm_retry2, rtm_conflict2, rtm_capacity2, rtm_debug2,  rtm_explicit2,  rtm_nested2, rtm_a2, rtm_b2, rtm_insertions2, insertions2;
#ifdef RTM

#define DEB(x) {}
//#define DEB(x) printf(x)

#define TM_COMMIT() _xend()
#define TM_ABORT(x)  _xabort(x)
#define TRY_THRESHOLD 20

unsigned int gl_tm_seed = 0;
__thread unsigned int lo_tm_seed = 0;
__thread unsigned int tm_init=0;

/*
#define ATOMIC(...)  \
{\
unsigned int __local_try=0;\
if(!tm_init) {tm_init=1;lo_tm_seed = __sync_add_and_fetch(&gl_tm_seed, 1); srand(&lo_tm_seed);}\
retry_tm:\
	++rtm_prova;\
	unsigned int __status = 0;\
	unsigned int fallback = rand_r(&lo_tm_seed)%512;\
while(0 && fallback--!=0)_mm_pause();\
fallback=50;\
	if ((__status = _xbegin ()) == _XBEGIN_STARTED)
	//{
	//}
*/


#define FALLBACK(...) \
	else{\
	rtm_failed++;\
		/*  Transaction retry is possible. */\
		if(__status & _XABORT_RETRY) {DEB("RETRY\n");rtm_retry++;\
		/*fallback=rand_r(&lo_tm_seed)%512;*/\
		while(fallback--!=0)_mm_pause();if(__local_try++ < TRY_THRESHOLD && rand_r(&lo_tm_seed)%2)goto retry_tm;}\
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
		if(_XABORT_CODE(__status) == 0xf2) {rtm_a++;}\
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
	        long rand = 0; rtm_failed2++;\
                if(blocked) printf("ERROR %u\n", __status);\
		/*  Transaction retry is possible. */\
                if(__status & _XABORT_RETRY) {DEB("RETRY\n");rtm_retry2++;lrand48_r(&seedT, &rand);if(rand & 1) goto retry_tm;}\
		/*  Transaction abort due to a memory conflict with another thread */\
                if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n");rtm_conflict2++;}\
                /*  Transaction abort due to the transaction using too much memory */\
                if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n");rtm_capacity2++;}\
                /*  Transaction abort due to a debug trap */\
                if(__status & _XABORT_DEBUG) {DEB("DEBUG\n");rtm_debug2++;}\
                /*  Transaction abort in a inner nested transaction */\
                if(__status & _XABORT_NESTED) {DEB("NESTES\n");rtm_nested2++;}\
                /*  Transaction explicitely aborted with _xabort. */\
                if(__status & _XABORT_EXPLICIT){DEB("EXPLICIT\n");rtm_explicit2++;}\
                if(__status == 0){DEB("Other\n");rtm_other2++;}\
                if(_XABORT_CODE(__status) == 0xf2) {rtm_a2++;}\
                if(_XABORT_CODE(__status) == 0xf1) {rtm_b2++;}
                        //{
                        //}


#define FALLBACK3(...) //\
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
