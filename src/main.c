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


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>

#include <pthread.h>
#include <stdarg.h>
#include <sys/time.h>

#include "datatypes/list.h"
#include "datatypes/calqueue.h"
#include "datatypes/nb_calqueue.h"


nb_calqueue* nbcqueue;
list(nbc_bucket_node) lqueue;

int payload = 0;
struct timeval startTV;
volatile double GVT = 0.0;

char		DATASTRUCT;

unsigned int THREADS;		// Number of threads
unsigned int OPERATIONS; 	// Number of operations per thread
unsigned int ITERATIONS;		// = 800000;

unsigned int TOTAL_OPS;		// = 800000;

unsigned int TOTAL_OPS1;		// = 800000;
double PROB_DEQUEUE1;		// Probability to dequeue
char   PROB_DISTRIBUTION1;

unsigned int TOTAL_OPS2;		// = 800000;
double PROB_DEQUEUE2;		// Probability to dequeue
char   PROB_DISTRIBUTION2;

unsigned int TOTAL_OPS3;		// = 800000;
double PROB_DEQUEUE3;		// Probability to dequeue
char   PROB_DISTRIBUTION3;

unsigned int PRUNE_PERIOD;	// Number of ops before calling prune
double MEAN_INTERARRIVAL_TIME = 1.0;			// Maximum distance from the current event owned by the thread
unsigned int LOG_PERIOD;	// Number of ops before printing a log
unsigned int VERBOSE;		// if 1 prints a full log on STDOUT and on individual files
unsigned int LOG;			// = 0;
double PRUNE_TRESHOLD;		// = 0.35;
unsigned int SAFETY_CHECK;
unsigned int EMPTY_QUEUE;

unsigned int *id;
volatile long long *ops;
volatile long long *ops_count;
struct timeval *malloc_time;
struct timeval *free_time;

__thread struct timespec e_time;
__thread struct timespec d_time;

unsigned int *malloc_count;
unsigned int *free_count;
volatile double* volatile array;
FILE **log_files;

void test_log(unsigned int my_id, const char *msg, ...) {
	char buffer[1024];
	va_list args;

	va_start(args, msg);
	vsnprintf(buffer, 1024, msg, args);
	va_end(args);

	printf("%s",buffer);
	fwrite(buffer,1,  strlen(buffer), log_files[my_id]);
}

double uniform_rand(struct drand48_data *seed)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num *= MEAN_INTERARRIVAL_TIME*2;
	return random_num;
}

double neg_triangular_rand(struct drand48_data *seed)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num = 1-sqrt(random_num);
	random_num *= MEAN_INTERARRIVAL_TIME*3/2;
	return random_num;
}

double triangular_rand(struct drand48_data *seed)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num = sqrt(random_num);
	random_num *= MEAN_INTERARRIVAL_TIME*3/2;
	return random_num;
}

double exponential_rand(struct drand48_data *seed)
{
	double random_num = 0.0;
	drand48_r(seed, &random_num);
	random_num =  -log(random_num);
	random_num *= MEAN_INTERARRIVAL_TIME;
	return random_num;
}

double dequeue(unsigned int my_id)
{

	struct timespec startTV2,endTV2;
	struct timeval diff;
	double timestamp = INFTY;
	unsigned int counter = 1;
	void* free_pointer;
	clock_gettime(CLOCK_MONOTONIC, &startTV2);

	if(DATASTRUCT == 'L')
	{
		free_pointer = list_pop(lqueue);
		nbc_bucket_node *new = node_payload(lqueue,free_pointer);
		if(free_pointer != NULL)
		{
			timestamp = new->timestamp;
			counter = new->counter;
		}
	}
	else if(DATASTRUCT == 'C')
	{
		calqueue_node* new = calqueue_get();
		free_pointer = new;
		if(free_pointer != NULL)
		{
			timestamp = new->timestamp;
			counter = 1;
		}
	}
	else if(DATASTRUCT == 'F')
	{
		nbc_bucket_node *new = nbc_dequeue(nbcqueue);
		free_pointer = new;
		timestamp = new->timestamp;
		counter = new->counter;
	}
	clock_gettime(CLOCK_MONOTONIC,&endTV2);


	d_time.tv_sec += endTV2.tv_sec - startTV2.tv_sec  - (endTV2.tv_nsec > startTV2.tv_nsec ? 0 : 1);

	d_time.tv_nsec += endTV2.tv_nsec > startTV2.tv_nsec ? ( endTV2.tv_nsec - startTV2.tv_nsec) : (1000000000+ endTV2.tv_nsec - startTV2.tv_nsec);

	if(counter == 0)
	{
		printf("%u-%d:%d\tDEQUEUE should never return a HEAD node %.10f - %d\n",
				my_id, (int)diff.tv_sec, (int)diff.tv_usec, timestamp, counter);
		exit(1);
	}

	if(free_pointer != NULL)
		free(free_pointer);

	if( VERBOSE )
	{
		if(timestamp == INFTY )
			test_log(my_id, "%u-%d:%d\tDEQUEUE EMPTY\n", my_id, diff.tv_sec, diff.tv_usec);
		else
			test_log(my_id, "%u-%d:%d\tDEQUEUE %.15f - %d\n", my_id, diff.tv_sec, diff.tv_usec, timestamp, counter);
	}


	return timestamp;
}

double enqueue(unsigned int my_id, struct drand48_data* seed, double local_min, char distribution)
{
	struct timespec startTV2,endTV2;
	struct timeval diff;
	double timestamp = 0.0;
	int counter = 0;
	double update = 0.0;

	if(distribution == 'U')
		update = uniform_rand(seed);
	else if(distribution == 'T')
		update = triangular_rand(seed);
	else if(distribution == 'N')
		update = neg_triangular_rand(seed);
	else if(distribution == 'E')
		update = exponential_rand(seed);

	timestamp = local_min;

	timestamp += update;
	if(timestamp < 0.0)
		timestamp = 0;
clock_gettime(CLOCK_MONOTONIC, &startTV2);

	if(DATASTRUCT == 'L')
	{
		nbc_bucket_node node;
		node.timestamp = timestamp;
		node.counter = 1;
		list_insert(lqueue, timestamp, &node);
	}
	else if(DATASTRUCT == 'C')
		calqueue_put(timestamp, NULL);
	else if(DATASTRUCT == 'F')
		nbc_enqueue(nbcqueue, timestamp, NULL);

clock_gettime(CLOCK_MONOTONIC, &endTV2);
	e_time.tv_sec += endTV2.tv_sec - startTV2.tv_sec - (endTV2.tv_nsec > startTV2.tv_nsec ? 0 : 1);
	e_time.tv_nsec +=  endTV2.tv_nsec > startTV2.tv_nsec ? ( endTV2.tv_nsec - startTV2.tv_nsec) : (1000000000+ endTV2.tv_nsec - startTV2.tv_nsec);

	//local_min = timestamp;

	if( VERBOSE )
		test_log(my_id, "%u-%d:%d\tENQUEUE %.15f - %u\n", my_id, (int)diff.tv_sec, (int)diff.tv_usec, timestamp, counter);

	return timestamp;

}

double computeGVT()
{
	unsigned int j = 0;
	double min = INFTY;
	for(;j<THREADS;j++)
			{
				double tmp = array[j];
				if(tmp < min)
					min = tmp;
			}
	return min;
}

void classic_hold(
		unsigned int my_id,
		struct drand48_data* seed,
		struct drand48_data* seed2,
		long long *n_dequeue,
		long long *n_enqueue,
		double		*max
		)
{
	struct timeval startTV, endTV, diff;

	double timestamp = 0.0;
	double local_min = 0.0;
	double random_num = 0.0;
	long long tot_count = 0;

	unsigned int iterations = ITERATIONS;

	double current_prob = PROB_DEQUEUE1;
	char current_dist = PROB_DISTRIBUTION1;


	gettimeofday(&startTV, NULL);

	while(tot_count < TOTAL_OPS*iterations)
	{
		drand48_r(seed2, &random_num);

		if( random_num < (current_prob))
		{
			timestamp = dequeue(my_id);
			if(timestamp != INFTY)
			{
				array[my_id] = timestamp;
				(*n_dequeue)++;

				*max = *max > timestamp ? *max:timestamp;

				if ( SAFETY_CHECK )
				{
					if(timestamp < GVT)
					{
						printf("%u - %d:%d ERRORE timestamp:%f > GVT:%f\n",
								my_id, (int)diff.tv_sec, (int)diff.tv_usec, timestamp, GVT);
						exit(1);
					}
					unsigned int j =0;
					for(;j<THREADS;j++)
					{
						double tmp;
						do
							tmp = array[j];
						while( tmp < timestamp || ( D_EQUAL(timestamp, tmp) && j < my_id) );
					}
					GVT = timestamp;
				}
				local_min = timestamp;
			}
		}
		else
		{
			timestamp = enqueue(my_id, seed, local_min, current_dist);
			*max = *max > timestamp ? *max:timestamp;
			(*n_enqueue)++;
		}


		if( DATASTRUCT == 'F' && ops_count[my_id]%(PRUNE_PERIOD) == 0)
		{
			double min = INFTY;
			unsigned int j =0;
			for(;j<THREADS;j++)
			{
				double tmp = array[j];
				if(tmp < min)
					min = tmp;
			}
			nbc_prune(nbcqueue, min*PRUNE_TRESHOLD);

			if( VERBOSE )
				test_log(my_id, "%u-%d:%d\tPRUNE %.10f\n", my_id, (int)diff.tv_sec, (int)diff.tv_usec, min*PRUNE_TRESHOLD);

		}

		if(my_id == 0 && ops_count[my_id]%(LOG_PERIOD) == 0 && LOG)
		{
			double min = computeGVT();

			gettimeofday(&endTV, NULL);
			timersub(&endTV, &startTV, &diff);
			printf("%u - LOG %.10f  %.2f/100.00 SEC:%d:%d\n", my_id, min, ((double)ops_count[my_id])*100/OPERATIONS, (int)diff.tv_sec, (int)diff.tv_usec);
		}

		ops_count[my_id]++;
		unsigned int j=0;
		tot_count = 0;
		for(j=0;j<THREADS;j++)
			tot_count += ops_count[j];

		if( tot_count%(TOTAL_OPS) < TOTAL_OPS1)
		{
			current_dist = PROB_DISTRIBUTION1;
			current_prob = PROB_DEQUEUE1;
		}

		if( tot_count%(TOTAL_OPS) > TOTAL_OPS1 && tot_count%(TOTAL_OPS) < TOTAL_OPS2 )
		{
			current_dist = PROB_DISTRIBUTION2;
			current_prob = PROB_DEQUEUE2;
		}

		if( tot_count%(TOTAL_OPS) > TOTAL_OPS2)
		{
			current_dist = PROB_DISTRIBUTION3;
			current_prob = PROB_DEQUEUE3;
		}

	}
}


void* process(void *arg)
{
	struct timeval endTV, diff;
	char name_file[128];
	unsigned int my_id;
	long long n_dequeue = 0;
	long long n_enqueue = 0;
	struct drand48_data seed;
	struct drand48_data seed2    ;
	FILE  *f;
	double max = 0.0;

	my_id =  *((unsigned int*)(arg));
	lid = my_id;
	sprintf(name_file, "%u.txt", my_id);
	srand48_r(my_id+157, &seed2);
        srand48_r(my_id+359, &seed);


	if(VERBOSE)
	{
		f = fopen(name_file, "w+");
		log_files[my_id] = f;
		test_log(my_id,  "HI! I'M ALIVE %u\n", my_id);
	}


	classic_hold(my_id, &seed, &seed2, &n_dequeue, &n_enqueue, &max);

	do
	{
		double timestamp = dequeue(my_id);
		if(timestamp == INFTY)
			break;
		else
		{
			n_dequeue++;
			array[my_id] = timestamp;
		}
	}while(EMPTY_QUEUE);


	gettimeofday(&endTV, NULL);
	timersub(&endTV, &startTV, &diff);


	if(LOG)
		printf("%u- DONE + %d:%d "
				"%lld, "
				"%lld, "
				"%lld, "
				"%lld"
			" - MAX + %.10f"
		//	" - MALLOC + %d:%d, %d"
		//	" - FREE + %d:%d, %d"
			" - E + %lu:%lu, %lld"
                        " - D + %lu:%lu, %llu\n" ,
			my_id, (int)diff.tv_sec, (int)diff.tv_usec, n_dequeue, n_enqueue, n_dequeue - n_enqueue, ops_count[my_id],
		//	(int)malloc_time[my_id].tv_sec, (int)malloc_time[my_id].tv_usec, malloc_count[my_id],
		//	(int)free_time[my_id].tv_sec, (int)free_time[my_id].tv_usec, free_count[my_id]),
			max,                        
			e_time.tv_sec + e_time.tv_nsec/1000000000,
			e_time.tv_nsec%1000000000, n_enqueue,
            d_time.tv_sec + d_time.tv_nsec/1000000000,
			d_time.tv_nsec%1000000000,
			ops_count[my_id]-n_enqueue);


	ops[my_id] = n_dequeue - n_enqueue;
//	array[my_id] = INFTY;

	if( VERBOSE )
	{
		fclose(f);
		test_log(my_id,"%u- DONE + %d, %u, %u, %u\n", my_id, (int)diff.tv_sec, n_dequeue, n_enqueue, ops_count[my_id]);
	}

	pthread_exit(NULL);
}


int main(int argc, char **argv)
{
	int par = 1;
	int num_par = 19;

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

	OPERATIONS = (TOTAL_OPS/THREADS);
	PRUNE_PERIOD = (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	PRUNE_TRESHOLD = strtod(argv[par++], (char **)NULL);
	//PROB_ROLL = strtod(argv[par++], (char **)NULL);
	//MEAN_INTERARRIVAL_TIME = strtod(argv[par++], (char **)NULL);
	LOG_PERIOD = (OPERATIONS/10);
	VERBOSE = (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	LOG = (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	//BUCKET_WIDTH = strtod(argv[par++], (char **)NULL);
	//COLLABORATIVE_TODO_LIST = (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	SAFETY_CHECK = (unsigned int) strtol(argv[par++], (char **)NULL, 10);
	EMPTY_QUEUE = (unsigned int) strtol(argv[par++], (char **)NULL, 10);

	id = (unsigned int*) malloc(THREADS*sizeof(unsigned int));
	ops = (long long*) malloc(THREADS*sizeof(long long));
	ops_count = (long long*) malloc(THREADS*sizeof(long long));
	malloc_time = (struct timeval*) malloc(THREADS*sizeof(struct timeval));
	free_time = (struct timeval*) malloc(THREADS*sizeof(struct timeval));
	malloc_count = (unsigned int*) malloc(THREADS*sizeof(unsigned int));
	free_count = (unsigned int*) malloc(THREADS*sizeof(unsigned int));
	array = (double*) malloc(THREADS*sizeof(double));
	log_files = (FILE**) malloc(THREADS*sizeof(FILE*));



printf("D:%c,", DATASTRUCT);
printf("T:%u,", THREADS);
printf("OPS:%u,", TOTAL_OPS);
printf("PRUNE_PER:%u,", PRUNE_PERIOD);
printf("PRUNE_T:%f,", PRUNE_TRESHOLD);
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
printf("SAFETY_CHECK:%u,", SAFETY_CHECK);
printf("EMPTY_QUEUE:%u,", EMPTY_QUEUE);


TOTAL_OPS2 += TOTAL_OPS1;
	unsigned int i = 0;
	pthread_t tid[THREADS];

	if(DATASTRUCT == 'L')
	{
		lqueue = new_list(nbc_bucket_node);
		pthread_spin_init(&(((struct rootsim_list*)lqueue)->spinlock), 0);
	}
	else if(DATASTRUCT == 'C')
		calqueue_init();
	else if(DATASTRUCT == 'F')
		nbcqueue = nb_calqueue_init(THREADS);

	gettimeofday(&startTV, NULL);

	for(;i<THREADS;i++)
	{
		id[i] = i;
		ops_count[i] = 0;
		pthread_create(tid +i, NULL, process, id+i);
	}

	for(i=0;i<THREADS;i++)
		pthread_join(tid[i], (void*)&id);

	long long tmp = 0;
	struct timeval mal,fre;
	timerclear(&mal);
	timerclear(&fre);

	for(i=0;i<THREADS;i++)
	{
		tmp += ops[i];

		mal.tv_sec+=malloc_time[i].tv_sec;
		if( (mal.tv_usec + malloc_time[i].tv_usec )%1000000 != mal.tv_usec + malloc_time[i].tv_usec )
			mal.tv_sec++;
		mal.tv_usec = (mal.tv_usec + malloc_time[i].tv_usec )%1000000;

		fre.tv_sec+=free_time[i].tv_sec;
		if( (fre.tv_usec + free_time[i].tv_usec )%1000000 != fre.tv_usec + free_time[i].tv_usec )
			fre.tv_sec++;
		fre.tv_usec = (fre.tv_usec + free_time[i].tv_usec )%1000000;
	}


	printf("CHECK:%lld,", tmp);
	printf("MALLOC_T:%d.%d,", (int)mal.tv_sec, (int)mal.tv_usec);
	printf("FREE_T:%d.%d,", (int)fre.tv_sec, (int)fre.tv_usec);

	for(i=0;i<THREADS;i++)
{
		if(array[i] != INFTY)
			printf("%d:%lld,%f,", i,ops_count[i], array[i]);
		else
			printf("%d:%lld,INF,", i,ops_count[i]);
}
	return 0;
}
