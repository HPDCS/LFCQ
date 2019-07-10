#ifndef __MY_RTM
#define __MY_RTM

#define RTM

#ifdef RTM

#define DEB(x) {}
//#define DEB(x) printf(x)

#define TM_COMMIT() _xend()
#define TM_ABORT()  _xabort(0xff)

#define BEGIN_ATOMIC()   {unsigned int __status;	if ((__status = _xbegin ()) == _XBEGIN_STARTED){
#define FALLBACK() }else{\
\
/*  Transaction retry is possible. */\
if(__status & _XABORT_RETRY) {DEB("RETRY\n"); continue;}\
/*  Transaction abort due to a memory conflict with another thread */\
if(__status & _XABORT_CONFLICT) {DEB("CONFLICT\n"); continue;}\
/*  Transaction abort due to the transaction using too much memory */\
if(__status & _XABORT_CAPACITY) {DEB("CAPACITY\n"); continue;}\
/*  Transaction abort due to a debug trap */\
if(__status & _XABORT_DEBUG) {DEB("DEBUG\n"); continue;}\
/*  Transaction abort in a inner nested transaction */\
if(__status & _XABORT_NESTED) {DEB("NESTES\n"); continue;}\
/*  Transaction explicitely aborted with _xabort. */\
if(__status & _XABORT_EXPLICIT)

#define END_ATOMIC() else DEB("Other\n");}}

#else

#define TM_COMMIT() /* */
#define TM_ABORT()  {aborted=1;break;}

#define BEGIN_ATOMIC()   {unsigned int aborted=0;	do{
#define FALLBACK() }while(0);if(aborted){{printf("EXPLICIT\n");}

#define END_ATOMIC() }}

#endif

#endif
