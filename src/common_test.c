#include <stdlib.h>
#include <pthread.h>

#include "key_type.h"
#include "utils/hpdcs_utils.h"
#include "utils/hpdcs_math.h"



void* nbcqueue;
double MEAN_INTERARRIVAL_TIME = MEAN;			// Maximum distance from the current event owned by the thread



#define NUM_CORES 8


static char distribution = 'A';
static pthread_t p_tid[NUM_CORES];

#ifdef TRACE_LEN
pkey_t *trace = NULL;
unsigned long long trace_index = 0;
int tr_id = 0;

void* local_trace(void *arg){
	int id = __sync_fetch_and_add(&tr_id, 1);
	int i;
	unsigned long long len = TRACE_LEN/NUM_CORES;
	unsigned long long begin = len*id;

	double (*current_dist) (struct drand48_data*, double) ;
	switch(distribution)
	{
		case 'U':
			current_dist = uniform_rand;
			break;
		case 'T':
			current_dist = triangular_rand;
			break;
		case 'N':
			current_dist = neg_triangular_rand;
			break;
		case 'E':
			current_dist = exponential_rand;
			break;
		case 'C':
			current_dist = camel_compile_time_rand;
			break;
		default:
			printf("#ERROR: Unknown distribution\n");
			exit(1);
	}

	struct drand48_data seed;
    srand48_r(359+id, &seed);
	for(i = 0; i< len;i++){
		pkey_t update;
		do{
			update = (pkey_t)  current_dist(&seed, MEAN_INTERARRIVAL_TIME);
		}while(update == 0);
		trace[begin+i] = update;
	}

	return 0;
}

void generate_trace(char d)
{
	int i;

	LOG("Building a timestamp trace...%s", "");
	fflush(NULL);
	trace = (pkey_t*) malloc(TRACE_LEN*sizeof(pkey_t));

	distribution = d;
	for(i=0;i<NUM_CORES;i++)
		pthread_create(p_tid +i, NULL, local_trace, NULL);


	for(i=0;i<NUM_CORES;i++)
		pthread_join(p_tid[i], NULL);

	trace[0] = 1;
	for(i = 1; i< TRACE_LEN;i++){
		trace[i] += trace[i-1];
		if(trace[i] < trace[i-1])
			printf("Overflow problem while generating trace i:%d t[i]:"KEY_STRING" t[i+1]:"KEY_STRING"\n", i, trace[i-1], trace[i]);
	}
	LOG("%s\n", "Done");


}

#endif






pkey_t dequeue(void)
{

	pkey_t timestamp = INFTY;
	void* free_pointer = NULL;
	
	timestamp = pq_dequeue(nbcqueue, &free_pointer);
	
	return timestamp;
}

int enqueue(int my_id, struct drand48_data* seed, pkey_t local_min, double (*current_prob) (struct drand48_data*, double))
{
	pkey_t timestamp = 0.0;
	
#ifndef TRACE_LEN
	pkey_t update = 0.0;
	do{
		update = (pkey_t)  current_prob(seed, MEAN_INTERARRIVAL_TIME);
		timestamp = local_min + update;
	}while(timestamp == INFTY);
	
	if(timestamp < 0.0)
		timestamp = 0.0;
#else
	unsigned long long index = __sync_fetch_and_add(&trace_index, 1); 
	assertf(index > TRACE_LEN, "THE TRACE IS SHORT: %d vs %llu\n", TRACE_LEN, index);
	timestamp = trace[index];
#endif

	int res = pq_enqueue(nbcqueue, timestamp, UNION_CAST(1, void*));

	return res;
}