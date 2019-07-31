#ifndef __KEY_TYPES__
#define __KEY_TYPES__


#include <limits.h>

#define FLOAT 	0
#define DOUBLE 	1
#define INT 	2
#define LONG	3

#define KEY_TYPE LONG

#define LESS(a,b) 		( (a) <  (b) )
#define LEQ(a,b)		( (a) <= (b) )
#define D_EQUAL(a,b) 	( (a) == (b) )
#define GEQ(a,b) 		( (a) >= (b) )
#define GREATER(a,b) 	( (a) >  (b) )

#if KEY_TYPE == DOUBLE

	#include <float.h>
	#define INFTY 	DBL_MAX
	#define MIN 	0.0
	#define KEY_STRING "%f"
	#define LOOKAHEAD 100.00
	typedef double pkey_t;

	#define MEAN  1.00;			// Maximum distance from the current event owned by the thread

#elif KEY_TYPE == INT

	#define INFTY INT_MAX
	#define MIN 	0
	#define KEY_STRING "%d"
	typedef int pkey_t;

	#define MILLION 1000000
	#define BILLION (1000*MILLION)
	//#define TRACE_LEN (300*MILLION)

	#define MEAN 500000;			// Maximum distance from the current event owned by the thread
	#define LOOKAHEAD 0;

#elif KEY_TYPE == LONG


        #define INFTY LONG_MAX
        #define MIN     0
        #define KEY_STRING "%ld"
        typedef long pkey_t;

        #define MILLION 1000000ULL
        #define BILLION (1000ULL*MILLION)
//        #define TRACE_LEN (300*MILLION)
#define LOOKAHEAD 100
        #define MEAN 500ULL*MILLION;                        // Maximum distance from the current event owned by the thread

#endif

#ifndef __cplusplus

extern int   pq_enqueue(void *queue, pkey_t timestamp, void* payload);
extern pkey_t pq_dequeue(void *queue, void **payload);
extern void*  pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void pq_report(int tid);
extern void pq_prune();
extern void pq_reset_statistics();
extern unsigned int pq_num_malloc();

#endif

#endif

