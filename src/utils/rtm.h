#ifndef __MY_RTM
#define __MY_RTM

#include "common.h"

#define RTM

extern __thread unsigned long long rtm_prova, rtm_failed, rtm_insertions, insertions;
#ifdef RTM

#define DEB(x) {}
//#define DEB(x) printf(x)

#define TM_COMMIT() _xend()
#define TM_ABORT()  _xabort(0xff)

#define ATOMIC  CMB();\
{	++rtm_prova;/*printf("Transactions %u, %u, %u\n", prova, failed, insertions);*/\
	unsigned int __status = 0;\
	/*retry_tm:*/\
	if ((__status = _xbegin ()) == _XBEGIN_STARTED)
	//{
	//}

#define FALLBACK() \
	else{\
	\
		/*  Transaction retry is possible. */\
		if(__status & _XABORT_RETRY) {DEB("RETRY\n");}\
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
		else{DEB("Other\n");rtm_failed++;}\
			//{
			//}

#define END_ATOMIC() 	\
		\
		/*else{DEB("Other\n");failed++;}*/\
	}\
}CMB()\

#else

#include <pthread.h>
pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER; 

#define TM_COMMIT() /* */
#define TM_ABORT()  {aborted=1;break;}

#define ATOMIC   \
{	++rtm_prova;\
	unsigned int aborted=0;\
	do{\
		pthread_mutex_lock(&glock);
		//{
		//}
	
#define FALLBACK() \
	}while(0);\
	if(aborted){\
		rtm_failed++;\
		pthread_mutex_unlock(&glock); \
		/*printf("EXPLICIT\n");*/
		//{
		//}

#define END_ATOMIC() \
	}\
	pthread_mutex_unlock(&glock);\
}

#endif

#endif
