#ifndef __MAIN_COMMON__
#define __MAIN_COMMON__

#include "key_type.h"


extern void* nbcqueue;

extern double MEAN_INTERARRIVAL_TIME;			// Maximum distance from the current event owned by the thread



#ifdef TRACE_LEN
extern pkey_t *trace;
extern unsigned long long trace_index;
#endif


extern pkey_t dequeue(void);
extern pkey_t enqueue(int my_id, struct drand48_data* seed, pkey_t local_min, double (*current_prob) (struct drand48_data*, double));
extern void generate_trace(char distribution);

#endif