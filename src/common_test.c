#include <stdlib.h>
#include <pthread.h>
#include <immintrin.h>

#include "key_type.h"
#include "utils/hpdcs_utils.h"
#include "utils/hpdcs_math.h"

#ifndef ENABLE_ACQUIRE_SOCKET
#define ENABLE_ACQUIRE_SOCKET 0
#endif


#define MICROSLEEP_TIME 5
#define CLUSTER_WAIT 2000
#define WAIT_LOOPS CLUSTER_WAIT //2

void* nbcqueue;
double MEAN_INTERARRIVAL_TIME = MEAN;			// Maximum distance from the current event owned by the thread

extern int NUM_CORES;
extern __thread int nid;

/*
#define NUM_CORES 8


static char distribution = 'A';
static pthread_t p_tid[NUM_CORES];

*/

static char distribution = 'A';

volatile char cache_pad[64];

volatile int socket = -1;

static inline unsigned long tacc_rdtscp(int *chip, int *core)
{
    unsigned long a,d,c;
    __asm__ volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
    *chip = (c & 0xFFF000)>>12;
    *core = c & 0xFFF;
    return ((unsigned long)a) | (((unsigned long)d) << 32);;
}

static inline void acquire_node(volatile int *socket){
//return;
	int core, l_nid;
	tacc_rdtscp(&l_nid, &core);
//	if(nid != l_nid) printf("NID: %d  LNID: %d\n", nid, l_nid);
	nid = l_nid;
	int old_socket = *socket;
	int loops = WAIT_LOOPS;
//	if(nid == 0) loops >>=1;
	if(old_socket != nid){
		if(old_socket != -1) 
			while(
				loops-- && 
				(old_socket = *socket) != nid
			)
			_mm_pause(); 
//			usleep(MICROSLEEP_TIME);	
//		if(old_socket == -1) printf("old_socket:%d\n", old_socket);
		if(old_socket != nid)
			__sync_bool_compare_and_swap(socket, old_socket, nid);
//			__sync_lock_test_and_set(socket, nid);
	}
}


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
	pthread_t p_tid[NUM_CORES];
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
			{LOG("Overflow problem while generating trace i:%d t[i]:"KEY_STRING" t[i+1]:"KEY_STRING"\n", i, trace[i-1], trace[i]);}
	}
	LOG("%s\n", "Done");


}

#endif

int debug = 0;

pkey_t dequeue(void)
{
	
	pkey_t timestamp = INFTY;
	void* free_pointer = NULL;

#if ENABLE_ACQUIRE_SOCKET == 1
	if(!debug){printf("Alternating socket enabled\n");debug=1;} 
	acquire_node(&socket);
#endif

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
