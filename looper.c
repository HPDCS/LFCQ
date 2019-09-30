#include <stdio.h>
#include <signal.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/time.h>
#include <unistd.h>
#include <string.h>

volatile __thread size_t loops = 0;
volatile __thread size_t usec = 0;
volatile __thread size_t runs = 1;

volatile bool exit_on_timeout = false;

void timeout_handler(int sig) {
	signal(SIGALRM, SIG_IGN);
	exit_on_timeout = true;
	signal(SIGALRM, timeout_handler);
}

void usage() {
	printf("Usage: looper [--runs] [value] [--loops | --timeout] [value]\n");
}

unsigned long long cs_counter;

void time_to_loops() {
	size_t tmpruns = runs;
	unsigned long long tmploops = 0;
	while (tmpruns--) {
		exit_on_timeout = false;
		signal(SIGALRM, timeout_handler);
		ualarm(usec, 0);
		while (!exit_on_timeout) {
			cs_counter += ((++tmploops % 2) ? (1) : (-1));
		}

	}
	printf ("Loops in %lu usec are: %.5f\n", usec,(double) tmploops / runs / usec);
}

void loops_to_time() {
	size_t tmpruns = runs;
	double meanTime = 0.0;
	while (tmpruns--) {
		struct timeval start, end;
		size_t l = loops;
		gettimeofday(&start, NULL);
		while (l--);
		gettimeofday(&end, NULL);
		
		meanTime += ((end.tv_sec * 1000000.0 + end.tv_usec) - (start.tv_sec * 1000000.0 + start.tv_usec));
	}
	
	printf ("Microsecs for %lu loops are: %.5f\n", loops, (double)meanTime / runs);
	
}

int main(int argc, char** argv) {
	if (argc < 3 || argc > 5) {
		usage();
		return 1;
	}
	
	int idx;
	for (idx = 1; idx < argc; idx += 2) {
		if (0 == strcmp(argv[idx],"--loops")) {
			loops = atoll(argv[idx + 1]);
		} 
		
		else if (0 == strcmp(argv[idx], "--timeout")) {
			usec = atoll(argv[idx + 1]);
		}
		
		else if (0 == strcmp(argv[idx], "--runs")) {
			runs = atoll(argv[idx + 1]);
		}
		
		else {
			printf("Invalid option %s\n", argv[1]);
			usage();
			return 1;
		}
	}
	
	if (usec != 0) {
		time_to_loops();
	}
	
	else if (loops != 0) {
		loops_to_time();
	}
	
	return 0;
}
