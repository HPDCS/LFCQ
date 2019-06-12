/*****************************************************************************
*
*	This file is part of NBQueue, a lock-free O(1) priority queue.
*
*   Copyright (C) 2015, Romolo Marotta
*
*   This program is free software: you can redistribute it and/or modify
*   it under the terms of the GNU General Public License as published by
*   the Free Software Foundation, either version 3 of the License, or
*   (at your option) any later version.
*
*   This is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.
*
*   You should have received a copy of the GNU General Public License
*   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
******************************************************************************/


#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <pthread.h>
#include <numa.h>
#include <float.h>
#include <numaif.h>
#include <stdarg.h>
#include <sched.h>
#include <sys/time.h>

#include "utils/hpdcs_utils.h"
#include "utils/hpdcs_math.h"
#include "utils/common.h"
#include "gc/gc.h"

#define INFTY DBL_MAX

#define NID nid
#define TID tid


extern void   pq_enqueue(void *queue, double timestamp, void* payload);
extern double pq_dequeue(void *queue, void **payload);
extern void*  pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);
extern void pq_report(unsigned int threshold);
extern void pq_prune();
extern void pq_reset_statistics();
extern unsigned int pq_num_malloc();






struct payload
{
	double timestamp;
};

typedef struct payload payload;


unsigned int THREADS;		// Number of threads
unsigned int OPERATIONS; 	// Number of operations per thread
unsigned int ITERATIONS;	// = 800000;

unsigned int TOTAL_OPS;		// = 800000;

unsigned int TOTAL_OPS1;	// = 800000;
double PROB_DEQUEUE1;		// Probability to dequeue
char   PROB_DISTRIBUTION1;

unsigned int TOTAL_OPS2;	// = 800000;
double PROB_DEQUEUE2;		// Probability to dequeue
char   PROB_DISTRIBUTION2;

unsigned int TOTAL_OPS3;	// = 800000;
double PROB_DEQUEUE3;		// Probability to dequeue
char   PROB_DISTRIBUTION3;

double MEAN_INTERARRIVAL_TIME = 1.00;			// Maximum distance from the current event owned by the thread
unsigned int EMPTY_QUEUE;
double PERC_USED_BUCKET; 	
unsigned int ELEM_PER_BUCKET;
char   TEST_MODE;
unsigned int TIME;


__thread struct drand48_data seedT;
 
void* nbcqueue;

pthread_t *p_tid;

volatile unsigned int BARRIER = 0;
volatile unsigned int lock = 1;

__thread unsigned int TID;
__thread unsigned int NID;
__thread unsigned int num_op=0;

unsigned int NUMA_NODES;

unsigned int *id;
volatile long long *ops;
volatile long long *ops_count;
volatile long long *malloc_op;

volatile unsigned int end_phase_1 = 0;
volatile unsigned int end_phase_2 = 0;
volatile unsigned int end_phase_3 = 0;
volatile unsigned int end_test = 0;
volatile long long final_ops = 0;


double dequeue(void)
{

	double timestamp = INFTY;
	void* free_pointer = NULL;
	
	timestamp = pq_dequeue(nbcqueue, &free_pointer);
	
	return timestamp;
}

double enqueue(unsigned int my_id, struct drand48_data* seed, double local_min, double (*current_prob) (struct drand48_data*, double))
{
	double timestamp = 0.0;
	double update = 0.0;

	do{
	update = current_prob(seed, MEAN_INTERARRIVAL_TIME);
	timestamp = local_min + update;
}while(timestamp == INFTY);
	
	if(timestamp < 0.0)
		timestamp = 0.0;

	pq_enqueue(nbcqueue, timestamp, UNION_CAST(1, void*));
	
	return timestamp;

}



__thread unsigned int stopforcheck = 0;

void* process(void *arg)
{
	unsigned int my_id;
	long long n_dequeue = 0;
	long long n_enqueue = 0;
	struct drand48_data seed;
	struct drand48_data seed2    ;
	cpu_set_t cpuset;
	double timestamp, local_min = 0.0;

	double (*current_dist) (struct drand48_data*, double) ;

	my_id =  *((unsigned int*)(arg));
	(TID) = my_id;
	(NID) 		= numa_node_of_cpu(tid);
	srand48_r(my_id+157, &seed2);
    srand48_r(my_id+359, &seed);
    srand48_r(my_id+254, &seedT);
    
    
	CPU_ZERO(&cpuset);
	CPU_SET(my_id, &cpuset);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);


    __sync_fetch_and_add(&BARRIER, 1);
    
    
	
	while(lock);
	
	switch(PROB_DISTRIBUTION1)
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
		
	while(n_enqueue < 25/THREADS)
	{
		enqueue(my_id, &seed, local_min, current_dist);
		n_enqueue++;
	}


	__sync_fetch_and_add(&BARRIER, 1);
	while(BARRIER != THREADS);

	while(1)
	{
		timestamp = dequeue();
		if(timestamp == INFTY)
			break;
		else
		n_dequeue++;
	}

	ops[my_id] = n_enqueue - n_dequeue;
	malloc_op[my_id] =  pq_num_malloc();
	
	
	pthread_exit(NULL);    
}


int main(int argc, char **argv)
{
	int par = 1;
	int num_par = 17;
	unsigned int i = 0;
	unsigned long numa_mask = 1;
	unsigned long long sum = 0;
	unsigned long long min = -1;
	unsigned long long max = 0;
	unsigned long long mal = 0;
	unsigned long long avg = 0;
	unsigned long long tmp = 0;
	unsigned long long qsi = 0;

	if(argc != num_par)
	{
		printf("Missing parameters %d vs %d\n", argc, num_par);
		exit(1);
	}

	THREADS  = (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	ITERATIONS  = (unsigned int) strtol(argv[par++], (char **)NULL, 10);

	PROB_DISTRIBUTION1 = argv[par++][0];
	PROB_DEQUEUE1 = strtod(argv[par++], (char **)NULL);
	TOTAL_OPS1 = (unsigned int) strtol(argv[par++], (char **)NULL, 10);

	PROB_DISTRIBUTION2 = argv[par++][0];
	PROB_DEQUEUE2 = strtod(argv[par++], (char **)NULL);
	TOTAL_OPS2 = (unsigned int) strtol(argv[par++], (char **)NULL, 10);

	PROB_DISTRIBUTION3 = argv[par++][0];
	PROB_DEQUEUE3 = strtod(argv[par++], (char **)NULL);
	TOTAL_OPS3 = (unsigned int) strtol(argv[par++], (char **)NULL, 10);

	TOTAL_OPS = TOTAL_OPS1 + TOTAL_OPS2 + TOTAL_OPS3;
	
	PERC_USED_BUCKET 			= strtod(argv[par++], (char **)NULL);
	ELEM_PER_BUCKET 			= (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	
	OPERATIONS 					= (TOTAL_OPS/THREADS);
	//PROB_ROLL 					= strtod(argv[par++], (char **)NULL);
	//MEAN_INTERARRIVAL_TIME	 	= strtod(argv[par++], (char **)NULL);
	EMPTY_QUEUE 				= (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	TEST_MODE = argv[par++][0];
	TIME 			= (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	

	id = (unsigned int*) malloc(THREADS*sizeof(unsigned int));
	ops = (long long*) malloc(THREADS*sizeof(long long));
	malloc_op = (long long*) malloc(THREADS*sizeof(long long));
	ops_count = (long long*) malloc(THREADS*sizeof(long long));
	p_tid = malloc(THREADS*sizeof(pthread_t));
	

	 
	TOTAL_OPS2 += TOTAL_OPS1;
	
	
	NUMA_NODES  = numa_num_configured_nodes();
	
	//set_mempolicy(MPOL_BIND, &numa_mask, 2);
		
	nbcqueue = pq_init(THREADS, PERC_USED_BUCKET, ELEM_PER_BUCKET);
	
	for(i=0;i<THREADS;i++)
	{
		id[i] = i;
		ops_count[i] = 0;
		ops[i] = 0;
		malloc_op[i] = 0;
		pthread_create(p_tid +i, NULL, process, id+i);
	}
	
	
	while(BARRIER != THREADS);

    __sync_bool_compare_and_swap(&BARRIER, THREADS, 0);
	
    __sync_bool_compare_and_swap(&lock, 1, 0);
    
   for(i=0;i<THREADS;i++)
		pthread_join(p_tid[i], (void*)&id);

	for(i=0;i<THREADS;i++)
	{
		qsi += ops[i];
		sum += tmp;
	}
	
	avg = sum/THREADS;
	
	printf("MISSING ITEMS:%lld\n" , qsi);
	return 0;
}
