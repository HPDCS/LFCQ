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
#include <stdarg.h>
#include <sched.h>
#include <sys/time.h>

#include "datatypes/list.h"
#include "datatypes/calqueue.h"
#include "datatypes/nb_calqueue.h"
#include "datatypes/prioq.h"
#include "datatypes/gc/gc.h"

#include "utils/hpdcs_utils.h"
#include "utils/hpdcs_math.h"
#include "mm/garbagecollector.h"


struct payload
{
	double timestamp;
};

typedef struct payload payload;


char		DATASTRUCT;

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

unsigned int PRUNE_PERIOD;	// Number of ops before calling prune
double MEAN_INTERARRIVAL_TIME = 1.00;			// Maximum distance from the current event owned by the thread
unsigned int EMPTY_QUEUE;
double PERC_USED_BUCKET; 	
unsigned int ELEM_PER_BUCKET;
char   TEST_MODE;
unsigned int TIME;


__thread struct drand48_data seedT;
extern __thread hpdcs_gc_status malloc_status;

nb_calqueue* nbcqueue;
list(payload) lqueue;
pq_t* skip_queue;

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
	payload *new_nbc_node;
	
	switch(DATASTRUCT)
	{
		case 'L':
			free_pointer = list_pop(lqueue);
			new_nbc_node =  node_payload(lqueue,free_pointer);
	
			if(free_pointer != NULL)
			{
				timestamp = new_nbc_node->timestamp;
				free(free_pointer);
			}
			break;
		case 'C':
			timestamp = calqueue_get(&free_pointer);
			break;
		case 'F':
			timestamp = nbc_dequeue(nbcqueue, &free_pointer);
			break;
		case 'S':
			timestamp = UNION_CAST(deletemin(skip_queue), double);
			break;
		default:
			break;
	}	
	
	return timestamp;
}

double enqueue(unsigned int my_id, struct drand48_data* seed, double local_min, double (*current_prob) (struct drand48_data*, double))
{
	double timestamp = 0.0;
	double update = 0.0;
	payload data;

	update = current_prob(seed, MEAN_INTERARRIVAL_TIME);
	timestamp = local_min + update;
	
	if(timestamp < 0.0)
		timestamp = 0.0;

	switch(DATASTRUCT)
	{
		case 'L':
			data.timestamp = timestamp;
			list_insert(lqueue, timestamp, &data);
			break;
		case 'C':
			calqueue_put(timestamp, NULL);
			break;
		case 'F':
			nbc_enqueue(nbcqueue, timestamp, NULL);
			break;
		case 'S':
			insert(skip_queue, timestamp, UNION_CAST(timestamp, void*));
			break;
		default:
			printf("#ERROR: Unknown data structure\n");
			exit(1);
	}
	
	return timestamp;

}




void classic_hold(
		unsigned int my_id,
		struct drand48_data* seed,
		struct drand48_data* seed2,
		long long *n_dequeue,
		long long *n_enqueue
		)
{

	double timestamp = 0.0;
	double local_min = 0.0;
	double random_num = 0.0;
	long long tot_count = 0;
	long long par_count = 0;
	long long end_operations 	;
	long long end_operations1 	;
	long long end_operations2 	;
	long long local_enqueue = 0;
	long long local_dequeue = 0;
	
	unsigned int iterations = ITERATIONS;
	unsigned int iter		= 0;
	unsigned int j;
	double (*current_dist) (struct drand48_data*, double) ;
	
	
	
	while(iter < iterations)
	{
		tot_count = 0;
		par_count = 0;
		end_operations 		= TOTAL_OPS*(iter+1);
		end_operations1 	= TOTAL_OPS1+TOTAL_OPS*(iter);
		end_operations2 	= TOTAL_OPS2+TOTAL_OPS*(iter++);
		
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
		
		
		while(tot_count < end_operations1)
		{
			
			drand48_r(seed2, &random_num);

			if( random_num < PROB_DEQUEUE1)
			{
				timestamp = dequeue();
				if(timestamp != INFTY)
				{
					local_dequeue++;
					local_min = timestamp;
				}
			}
			else
			{
				enqueue(my_id, seed, local_min, current_dist);
				local_enqueue++;
			}
			
			
			//enqueue(my_id, seed, local_min, current_dist);
			//local_enqueue++;
			++par_count;
			
			if( DATASTRUCT == 'F' && PRUNE_PERIOD != 0 &&  (ops_count[my_id] + par_count) %(PRUNE_PERIOD) == 0)
				nbc_prune(nbcqueue);
			
			
			if(par_count == THREADS)
			{	
				ops_count[my_id]+=par_count;
				par_count = 0;
				tot_count = 0;
				for(j=0;j<THREADS;j++)
					tot_count += ops_count[j];
			}
		}
		__sync_fetch_and_add(&end_phase_1, 1);
		
		while(TEST_MODE == 'T' && end_phase_1 != THREADS+1);
		
		if(TEST_MODE == 'T'){
			par_count = 0;
			ops_count[my_id] = 0;
		}
		
		switch(PROB_DISTRIBUTION2)
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
		
		near = 0;
		num_cas = 0;
		num_cas_useful = 0;
		par_count = 0;
		ops_count[my_id] = 0;
		
		while((TEST_MODE != 'T' && tot_count < end_operations2) || (TEST_MODE == 'T' && !end_test))
		{
			par_count++;
			timestamp = dequeue();
			if(timestamp != INFTY)
				local_min = timestamp;
			//pthread_yield();

			enqueue(my_id, seed, local_min, current_dist);
			
			if( DATASTRUCT == 'F' && PRUNE_PERIOD != 0 &&  (ops_count[my_id] + par_count) %(PRUNE_PERIOD) == 0)
				nbc_prune();
			else if( DATASTRUCT == 'N' && PRUNE_PERIOD != 0 &&  (ops_count[my_id] + par_count) %(PRUNE_PERIOD) == 0)
				nbc_prune();

			if(par_count == THREADS && TEST_MODE != 'T')
			{	
				ops_count[my_id]+=par_count;
				par_count = 0;
				tot_count = 0;
				for(j=0;j<THREADS;j++)
					tot_count += ops_count[j];

			}
			//pthread_yield();
		}
		
		if(end_test)
		{
			if(TEST_MODE == 'T')
			{	
				ops_count[my_id]+=par_count;
				par_count = 0;
				tot_count = 0;
				for(j=0;j<THREADS;j++)
					tot_count += ops_count[j];

			}
			for(j=0;j<THREADS;j++)
				tot_count += ops_count[j];
			
			__sync_val_compare_and_swap(&final_ops, 0, tot_count);
			(*n_dequeue) = local_dequeue;
			(*n_enqueue) = local_enqueue;
			return;
		}
		
		__sync_fetch_and_add(&end_phase_2, 1);
		
		while(tot_count < end_operations)
		{
			par_count++;
			timestamp = dequeue();

			if(timestamp != INFTY)
			{
				local_dequeue++;
				local_min = timestamp;
			}
			
			if( DATASTRUCT == 'F' && PRUNE_PERIOD != 0 &&  (ops_count[my_id] + par_count) %(PRUNE_PERIOD) == 0)
				nbc_prune();
			else if( DATASTRUCT == 'N' && PRUNE_PERIOD != 0 &&  (ops_count[my_id] + par_count) %(PRUNE_PERIOD) == 0)
				nbc_prune();
			
			if(par_count == THREADS)
			{	
				ops_count[my_id]+=par_count;
				par_count = 0;
				tot_count = 0;
				for(j=0;j<THREADS;j++)
					tot_count += ops_count[j];
			}

		}
		(*n_dequeue) = local_dequeue;
		(*n_enqueue) = local_enqueue;
	}
	
	__sync_fetch_and_add(&end_phase_3, 1);
	
	
}


void* process(void *arg)
{
	unsigned int my_id;
	long long n_dequeue = 0;
	long long n_enqueue = 0;
	struct drand48_data seed;
	struct drand48_data seed2    ;
	cpu_set_t cpuset;
	double timestamp;

	my_id =  *((unsigned int*)(arg));
	(TID) = my_id;
	(NID) 		= numa_node_of_cpu(tid);
	srand48_r(my_id+157, &seed2);
    srand48_r(my_id+359, &seed);
    srand48_r(my_id+254, &seedT);
    
    
	CPU_ZERO(&cpuset);
	CPU_SET(my_id, &cpuset);
	//sched_setaffinity(p_tid[my_id], sizeof(cpu_set_t), &cpuset);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    //printf("%u %u %u\n", TID, NID, NUMA_NODES);

    __sync_fetch_and_add(&BARRIER, 1);
    
    
	
	while(lock);
	
	classic_hold(my_id, &seed, &seed2, &n_dequeue, &n_enqueue);
	
	while(EMPTY_QUEUE)
	{
		timestamp = dequeue();
		if(timestamp == INFTY)
			break;
		else
			n_dequeue++;
	}

	ops[my_id] = n_enqueue - n_dequeue;
	malloc_op[my_id] =  malloc_status.to_remove_nodes_count;
	
	__sync_fetch_and_add(&BARRIER, 1);

	while(BARRIER != THREADS);
	
	while(lock != TID);
	
	if(DATASTRUCT == 'F' || DATASTRUCT == 'N' || DATASTRUCT == 'W')
		nbc_report(TID);
	if(DATASTRUCT == 'S')
		print_stats();
	__sync_fetch_and_add(&lock, 1);
	pthread_exit(NULL);    
}


int main(int argc, char **argv)
{
	int par = 1;
	int num_par = 19;//19;
	unsigned int i = 0;
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

	DATASTRUCT = argv[par++][0];
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
	
	
	PRUNE_PERIOD 			= (unsigned int) strtol(argv[par++], (char **)NULL, 10);

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
	

	printf("D:%c,", DATASTRUCT);
	printf("T:%u,", THREADS);
	printf("ITERATIONS:%u,", ITERATIONS);
	printf("OPS:%u,", TOTAL_OPS);
	printf("PRUNE_PER:%u,", PRUNE_PERIOD);
	printf("OPS1:%u,", TOTAL_OPS1);
	printf("PROB_DIST1:%c,",PROB_DISTRIBUTION1);
	printf("P_DEQUEUE1:%f,", PROB_DEQUEUE1);
	printf("OPS2:%u,", TOTAL_OPS2);
	printf("PROB_DIST2:%c,",PROB_DISTRIBUTION2);
	printf("P_DEQUEUE2:%f,", PROB_DEQUEUE2);
	printf("OPS3:%u,", TOTAL_OPS3);
	printf("PROB_DIST3:%c,",PROB_DISTRIBUTION3);
	printf("P_DEQUEUE3:%f,", PROB_DEQUEUE3);
	printf("MEAN_INTERARRIVAL_TIME:%f,", MEAN_INTERARRIVAL_TIME);
	printf("PERC_USED_BUCKET:%f,", PERC_USED_BUCKET);
	printf("ELEM_PER_BUCKET:%u,", ELEM_PER_BUCKET);
	printf("EMPTY_QUEUE:%u,", EMPTY_QUEUE);
	

	TOTAL_OPS2 += TOTAL_OPS1;
	
	
	NUMA_NODES  = numa_num_configured_nodes();
	switch(DATASTRUCT)
	{
		case 'L':
			lqueue = new_list(payload);
			pthread_spin_init(&(((struct rootsim_list*)lqueue)->spinlock), 0);
			break;
		case 'C':
			calqueue_init();
			break;
		case 'F':
			_init_gc_subsystem();
			nbcqueue = nbc_init(THREADS, PERC_USED_BUCKET, ELEM_PER_BUCKET);
			break;
		case 'S':
			_init_gc_subsystem();
			skip_queue = pq_init(ELEM_PER_BUCKET);
			break;
		default:
			printf("#ERROR: Unknown data structure\n");
			exit(1);
	}

	for(i=0;i<THREADS;i++)
	{
		id[i] = i;
		ops_count[i] = 0;
		ops[i] = 0;
		malloc_op[i] = 0;
		pthread_create(p_tid +i, NULL, process, id+i);
	}
	
	
	while(!__sync_bool_compare_and_swap(&BARRIER, THREADS, 0));
	
	
    __sync_lock_test_and_set(&lock, 0);
    
    struct timespec start, end;
    if(TEST_MODE == 'T'){
		while(end_phase_1 != THREADS);
		while(!__sync_bool_compare_and_swap(&end_phase_1, THREADS, THREADS+1));
		gettime(&start);
		sleep(TIME);
		__sync_bool_compare_and_swap(&end_test, 0, 1);
	}
	gettime(&end);
	for(i=0;i<THREADS;i++)
		pthread_join(p_tid[i], (void*)&id);


	struct timespec elapsed = timediff(start, end);
    double dt = elapsed.tv_sec + (double)elapsed.tv_nsec / 1000000000.0;
	for(i=0;i<THREADS;i++)
	{
		qsi += ops[i];
		tmp = ops_count[i];
		mal += malloc_op[i];
		sum += tmp;
		min = min < tmp ? min : tmp;
		max = max > tmp ? max : tmp;
		printf("OPS-%d:%lld ", i, tmp);
	}
	
	avg = sum/THREADS;
	//printf(	"CD = Average Concurrent Dequeue," 
	//		"APD = Attempts per Dequeue, "
	//		"LPD = Scan list per Dequeue, "
	//		"CE = Concurrent Enqueues, "
	//		"APE = Attempts per Enqueue, "
	//		"FPE = Flush Attempts per Enqueue, "
	//		"FSU = Flush Succes Rate, "
	//		"FFA = Flush Fail Rate,"
	//		"RTC = READ table count\n"); 

	printf("CHECK:%lld," , qsi);
	printf("SUM OP:%lld,", sum);
	if(TEST_MODE == 'T'){
		printf("TIME:%.8f,", dt);
		printf("THROUGHPUT:%.3f,", sum*2.0/dt/1000.0);
	}
	printf("MIN OP:%lld,", min);
	printf("MAX OP:%lld,", max);
	printf("AVG OP:%lld,", avg);
	printf("MAL OP:%lld,\n", mal);
	

	return 0;
}
