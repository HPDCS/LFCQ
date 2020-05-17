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
#include <limits.h>


#include "key_type.h"
#include "common_test.h"
#include "utils/hpdcs_utils.h"
#include "utils/hpdcs_math.h"
#include "utils/common.h"
#include "gc/gc.h"

// tipo esecuzione
#define DISTRIBUTED_PIN		0	// numero minimo dei thread su ogni nodo	
#define GRADUAL_PIN			1	// prima il nodo 0, poi 1, 2 e cosi via
// delay
#define ENQUEUE_SLEEP		0	// microsecondi
#define DEQUEUE_SLEEP		0	// microsecondi

#define ENQUEUE_BUSY_LOOP	0	// microsecondi
#define DEQUEUE_BUSY_LOOP	0	// microsecondi

// suddivisione o meno del ruolo dei thread
#define DISTINCT_THREAD_TYPES	0	// il ruolo dei thread è suddiviso




#define GRAD_PIN DISTRIBUTED_PIN


#define NID nid
#define TID tid


#define BOOL_CAS_DOUBLE(addr, old, new)  __sync_bool_compare_and_swap(\
                                                                               UNION_CAST(addr, volatile unsigned long long *),\
                                                                               UNION_CAST(old,  unsigned long long),\
                                                                               UNION_CAST(new,  unsigned long long)\
                                                                         )


struct payload
{
	double timestamp;
};

typedef struct payload payload;

unsigned int MAX_THREAD_NUM;
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

unsigned int EMPTY_QUEUE;
double PERC_USED_BUCKET; 	
unsigned int ELEM_PER_BUCKET;
char   TEST_MODE;
unsigned int TIME;


__thread struct drand48_data seedT;
__thread struct drand48_data seedP;
 
pthread_t *p_tid;

volatile unsigned int BARRIER = 0;
volatile unsigned int lock = 1;

__thread int TID;
__thread int NID;
__thread unsigned int num_op=0;

int NUMA_NODES;

unsigned int *id;
volatile long long *ops;
volatile long long *ops_count;
volatile long long *malloc_op;
// statistiche
volatile double *deq_times;
volatile double *enq_times;
volatile long long *enq_count;
volatile long long *deq_count;
volatile pkey_t *timestamps;
double conv = 1900.0;

volatile unsigned int end_phase_1 = 0;
volatile unsigned int end_phase_2 = 0;
volatile unsigned int end_phase_3 = 0;
volatile unsigned int end_test = 0;
volatile long long final_ops = 0;


void get_factor();




void classic_hold(
		int my_id,
		struct drand48_data* seed,
		struct drand48_data* seed2,
		long long *n_dequeue,
		long long *n_enqueue
		)
{

	pkey_t timestamp = 0.0;
	pkey_t local_min = 1.0;
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
		
		DEBUG(
			if(my_id == 0){
				printf("Populating queue...%s", "");
				fflush(NULL);
			}
		)
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
						
			if(par_count == THREADS)
			{	
				ops_count[my_id]+=par_count;
				par_count = 0;
				tot_count = 0;
				for(j=0;j<THREADS;j++)
					tot_count += ops_count[j];
			}
		}

		ops[my_id] = local_enqueue - local_dequeue;
		__sync_fetch_and_add(&end_phase_1, 1);

		
		while(TEST_MODE == 'T' && end_phase_1 != THREADS+1);

		DEBUG(
			if(my_id == 0){
				long long tmp = 0;
				int i= 0;
			 	for(i=0;i<THREADS;i++)
			            tmp += ops[i];
			  
				printf("%lld Items...Done\n", tmp);
		
		}
		)
		
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
		
		pq_reset_statistics();
		par_count = 0;
		ops_count[my_id] = 0;

		//struct drand48_data seme;
		//long int rand;

		//struct timespec start, end;
		//struct timespec elapsed;

		unsigned long long start;
		double elapsed, aus;

#if DISTINCT_THREAD_TYPES
		int ind = my_id / 2;
		int i;
        pkey_t enq_tmp;
        if(my_id % 2 == 0)
            BOOL_CAS_DOUBLE(&timestamps[ind], 0.0, local_min);
        else
        	while(timestamps[ind] == 0.0);
#else
#endif


        while((TEST_MODE != 'T' && tot_count < end_operations2) || (TEST_MODE == 'T' && !end_test))
		{

#if DISTINCT_THREAD_TYPES

			if(my_id % 2 == 0 && timestamps[ind] == 0.0){
				par_count++;

				start = read_tsc_p();
				timestamp = dequeue();
				elapsed = (read_tsc_p() - start) / conv;

				#if DEQUEUE_SLEEP	
					usleep(DEQUEUE_SLEEP);
				#endif

				#if DEQUEUE_BUSY_LOOP
					start = read_tsc_p();
					aus = DEQUEUE_BUSY_LOOP * conv;
					while((read_tsc_p() - start) < aus);
				#endif
				
	    		deq_times[my_id] += elapsed;
	    		deq_count[my_id]++;
				if(timestamp != INFTY)
					//local_min = timestamp;
					BOOL_CAS_DOUBLE(&timestamps[ind], 0.0, timestamp);
				//pthread_yield();
			}

			if(my_id % 2 == 1){
				//	          par_count++;
	        	for(i = ind; i < THREADS / 2; i++){
	        		if((enq_tmp = timestamps[i]) != 0.0){
			            if(BOOL_CAS_DOUBLE(&timestamps[i], enq_tmp, 0.0)){
			            	start = read_tsc_p();
			                enqueue(my_id, seed, enq_tmp, current_dist);
			                elapsed = (read_tsc_p() - start) / conv;

				    		enq_times[my_id] += elapsed;
				    		enq_count[my_id]++;

				    		#if ENQUEUE_SLEEP
				    			usleep(ENQUEUE_SLEEP);
				    		#endif

				    		#if ENQUEUE_BUSY_LOOP
								start = read_tsc_p();
								aus = ENQUEUE_BUSY_LOOP * conv;
								while((read_tsc_p() - start) < aus);
							#endif

			                break;
			            }
			        }	
	        	}
			}


#else
			par_count++;

			start = read_tsc_p();
			timestamp = dequeue();
			elapsed = (read_tsc_p() - start) / conv;
    		
    		deq_times[my_id] += elapsed;
    		deq_count[my_id]++;

    		#if DEQUEUE_SLEEP	
				usleep(DEQUEUE_SLEEP);
			#endif

			#if DEQUEUE_BUSY_LOOP
				start = read_tsc_p();
				aus = DEQUEUE_BUSY_LOOP * conv;
				while((read_tsc_p() - start) < aus);
			#endif

			if(timestamp != INFTY)
				local_min = timestamp;
			//pthread_yield();

			start = read_tsc_p();
			enqueue(my_id, seed, local_min, current_dist);
			elapsed = (read_tsc_p() - start) / conv;

    		enq_times[my_id] += elapsed;
    		enq_count[my_id]++;

    		# if ENQUEUE_SLEEP
				usleep(ENQUEUE_SLEEP);
			#endif

			#if ENQUEUE_BUSY_LOOP
				start = read_tsc_p();
				aus = ENQUEUE_BUSY_LOOP * conv;
				while((read_tsc_p() - start) < aus);
			#endif
						
#endif
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


int *numa_mapping;
int num_numa_nodes;
int num_cpus;
int num_cpus_per_node;

void* process(void *arg)
{
	int my_id;
	long long n_dequeue = 0;
	long long n_enqueue = 0; 
	struct drand48_data seed;
	struct drand48_data seed2    ;
	cpu_set_t cpuset;
	double timestamp;

	my_id 		=  *((int*)(arg));
	(TID) 		= my_id;
	int cpu 	= numa_mapping[my_id];

	#if GRAD_PIN == DISTRIBUTED_PIN && DISTINCT_THREAD_TYPES
	(NID) = cpu % 2;
	#else
	(NID) 		= numa_node_of_cpu(cpu);
	#endif
	srand48_r(my_id+157, &seed2);
    srand48_r(my_id+359, &seed);
    srand48_r(my_id+254, &seedT);
    srand48_r(my_id, &seedP);
    
	CPU_ZERO(&cpuset);
	CPU_SET((unsigned int)cpu, &cpuset);
	pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);


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
	malloc_op[my_id] =  pq_num_malloc();
	
	__sync_fetch_and_add(&BARRIER, 1);

	while(BARRIER != THREADS);
	
	while(lock != TID);
	
//	#ifndef NDEBUG
	pq_report(TID);
//	#endif
	
	__sync_fetch_and_add(&lock, 1);
	pthread_exit(NULL);    
}

int NUM_CORES = 0;

int main(int argc, char **argv)
{
	num_numa_nodes		= numa_max_node()+1;
	num_cpus			= numa_num_configured_cpus();
	NUM_CORES = num_cpus;
	num_cpus_per_node 	= num_cpus/num_numa_nodes;
	numa_mapping		= malloc(sizeof(int)*num_cpus);
	
	int i,j=0;

#if GRAD_PIN == GRADUAL_PIN
	int k;
	// occupazione graduale
	k = 0;
	for(i=0;i<num_numa_nodes;i++){
		for(j=0;j<num_cpus;j++){
			if( i == numa_node_of_cpu(j))
				numa_mapping[k++] = j;
		}
	}
#elif GRAD_PIN == DISTRIBUTED_PIN

	#if DISTINCT_THREAD_TYPES == 0
	// occupazione distribuita
		int numa_count[num_numa_nodes];
		for(i = 0; i < num_numa_nodes; i++)
			numa_count[i] = 0;
		for(i = 0; i < num_cpus; i++){
			j = numa_node_of_cpu(i);// ottengo il nodo
			numa_mapping[num_numa_nodes * numa_count[j] + j] = i;
			numa_count[j]++;
		}

	#else
		for(i = 0; i < num_cpus; i++){
	        if(i % 4 == 0 || i % 4 == 3)
	            numa_mapping[i] = i;
	        else if(i % 4 == 1)
	            numa_mapping[i] = i + 1;
	        else
	            numa_mapping[i] = i - 1;
	    }

    #endif
#endif

	printf("CPU PIN");
	for(i = 0; i < num_cpus; i++)
		printf(" %d", numa_node_of_cpu(numa_mapping[i]));
	printf("\n");

	get_factor();

	int par = 1;
	int num_par = 17;
	i = 0;
	//unsigned long numa_mask = 1;
	long long sum = 0;
	long long min = LONG_MAX;
	long long max = 0;
	long long mal = 0;
	long long avg = 0;
	long long tmp = 0;
	long long qsi = 0;

	if(argc != num_par)
	{
		printf("Missing parameters %d vs %d\n", argc, num_par);
		exit(1);
	}

	MAX_THREAD_NUM = (unsigned int)num_cpus;
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

	enq_times = (double*) malloc(THREADS*sizeof(double));
	deq_times = (double*) malloc(THREADS*sizeof(double));
	enq_count = (long long*) malloc(THREADS*sizeof(long long));
	deq_count = (long long*) malloc(THREADS*sizeof(long long));
	timestamps = malloc(sizeof(pkey_t)*(THREADS / 2));
	
	for(i = 0; i < THREADS / 2; i++)
		timestamps[i] = 0.0;

	for(i = 0; i < THREADS; i++){
		enq_count[i] = deq_count[i] = 0;
		enq_times[i] = deq_times[i] = 0.0;
	}
	
	printf("T:%u,", THREADS);
	printf("ITERATIONS:%u,", ITERATIONS);
	printf("OPS:%u,", TOTAL_OPS);
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
	
	//set_mempolicy(MPOL_BIND, &numa_mask, 2);
	

	LOG("\nBuilding the queue...%s", "");
	fflush(NULL);

	nbcqueue = pq_init(THREADS, PERC_USED_BUCKET, ELEM_PER_BUCKET);

	LOG("Done\n%s", "");

	#ifdef TRACE_LEN
		generate_trace(PROB_DISTRIBUTION2);
	#endif
	
	for(i=0;i<THREADS;i++)
	{
		id[i] = i;
		ops_count[i] = 0;
		ops[i] = 0;
		malloc_op[i] = 0;
		pthread_create(p_tid +i, NULL, process, id+i);
	}
	
	
    struct timespec start, end;
	struct timespec elapsed;
	double dt;
	
	while(!__sync_bool_compare_and_swap(&BARRIER, THREADS, 0));
	
	gettime(&start);
	__sync_bool_compare_and_swap(&lock, 1, 0);
    
    if(TEST_MODE == 'T'){
		while(end_phase_1 != THREADS)usleep(100);
		while(!__sync_bool_compare_and_swap(&end_phase_1, THREADS, THREADS+1));
	gettime(&end);

	elapsed = timediff(start, end);
    dt = (double)elapsed.tv_sec + (double)elapsed.tv_nsec / 1000000000.0;
	printf("\nTime to setup queue %f\n", dt);
		gettime(&start);
		sleep(TIME);
		__sync_bool_compare_and_swap(&end_test, 0, 1);
	}
	gettime(&end);
	for(i=0;i<THREADS;i++)
		pthread_join(p_tid[i], (void*)&id);


	elapsed = timediff(start, end);
    dt = (double)elapsed.tv_sec + (double)elapsed.tv_nsec / 1000000000.0;
	
    double enq_tmp_avg, deq_tmp_avg;
    long long ec, dc;
    enq_tmp_avg = deq_tmp_avg = 0.0;
    ec = dc = 0;

    for(i=0;i<THREADS;i++)
    {
            qsi += ops[i];
            tmp = ops_count[i];
            mal += malloc_op[i];
            sum += tmp;
            min = min < tmp ? min : tmp;
            max = max > tmp ? max : tmp;

            enq_tmp_avg += enq_times[i];
            deq_tmp_avg += deq_times[i];
            ec += enq_count[i];
            dc += deq_count[i];
    }

	avg = sum/THREADS;
	enq_tmp_avg /= ec;
	deq_tmp_avg /= dc;
	

	printf("CHECK:%lld," , qsi);
	printf("SUM OP:%lld,", sum);
	if(TEST_MODE == 'T'){
		printf("TIME:%.8f,", dt);
		printf("\nTHROUGHPUT:%.3f\n,", (double)sum*2.0/dt/1000.0);
		printf("TMP_AVG_ENQUEUE: %.10f us ### TMP_AVG_DEQUEUE: %.10f us\n", enq_tmp_avg, deq_tmp_avg);
	}
	printf("MIN OP:%lld,", min);
	printf("MAX OP:%lld,", max);
	printf("AVG OP:%lld,", avg);
	printf("MAL OP:%lld,", mal);
	
	for(i=0;i<THREADS;i++)
	{
        tmp = ops_count[i];
		printf("OPS-%d:%lld ", i, tmp);
	}
	

	return 0;
}

void get_factor(){
	unsigned long long exec_time;
	unsigned long long tot_time;
	unsigned long long seconds;
	unsigned long long aus;
	
	if(conv == 0.0){
		seconds=1;
		exec_time = read_tsc_p();
		sleep(seconds);

		tot_time = read_tsc_p() - exec_time;
		aus = ((int)(tot_time/1000.0/1000.0)/seconds);
		conv = (double)(aus - aus % 100);
	}

	printf("Clocks per us: %f\n", conv);
}