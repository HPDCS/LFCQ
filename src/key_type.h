#ifndef __KEY_TYPES__
#define __KEY_TYPES__

#define FLOAT 	0
#define DOUBLE 	1
#define INT 	2
#define LONG	3

#define KEY_TYPE INT

#if KEY_TYPE == DOUBLE

	#include <float.h>
	#define INFTY 	DBL_MAX
	#define MIN 	0.0
	typedef double pkey_t;

	#define MEAN  1.00;			// Maximum distance from the current event owned by the thread

#elif KEY_TYPE == INT

	#include <limits.h>
	#define INFTY INT_MAX
	#define MIN 	0U
	typedef int pkey_t;

	#define MEAN  100.0;			// Maximum distance from the current event owned by the thread

#endif


extern void   pq_enqueue(void *queue, pkey_t timestamp, void* payload);
extern pkey_t pq_dequeue(void *queue, void **payload);
extern void*  pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void pq_report(int tid);
extern void pq_prune();
extern void pq_reset_statistics();
extern unsigned int pq_num_malloc();



#endif

