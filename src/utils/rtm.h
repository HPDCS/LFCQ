#ifndef __MY_RTM
#define __MY_RTM

#include "common.h"

#define RTM

#ifdef RTM

#define DEB(x) {}
//#define DEB(x) printf(x)

#define TM_COMMIT() _xend()
#define TM_ABORT()  _xabort(0)

#define ATOMIC  CMB();\
{	++prova;/*printf("Transactions %u, %u, %u\n", prova, failed, insertions);*/\
	unsigned int __status = 0;\
	retry_tm:\
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
		else{DEB("Other\n");failed++;}\
			//{
			//}

#define END_ATOMIC() 	\
		\
		/*else{DEB("Other\n");failed++;}*/\
	}\
}CMB()\

#else

#define TM_COMMIT() /* */
#define TM_ABORT()  {aborted=1;break;}

#define BEGIN_ATOMIC()   {unsigned int aborted=0;	do{
#define FALLBACK() }while(0);if(aborted){{printf("EXPLICIT\n");}

#define END_ATOMIC() }}

#endif

#endif
