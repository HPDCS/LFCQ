#include <stdlib.h>
#include <stdio.h>
//#include <string.h>
//#include <unistd.h>
#include <limits.h>
#include <stdbool.h>
#include <math.h> //mauro
#include <pthread.h>
#include <float.h>

#include "calqueue.h"
#include "../../key_type.h"

#define INFTY DBL_MAX

#define LOCK pthread_spin_lock
#define UNLOCK pthread_spin_unlock

// Declare data structures needed for the schedulers
static calqueue_node *calq[CALQSPACE];	// Array of linked lists of items
static pthread_spinlock_t *locks[CALQSPACE];	// Array of linked lists of items
static calqueue_node **calendar;	// Pointer to use as a sub-array to calq


// Global variables for the calendar queueing routines
static int firstsub, nbuckets;
static volatile int qsize;
static volatile int lastbucket;
static double top_threshold, bot_threshold, lastprio;

static double buckettop, cwidth;


static pthread_rwlock_t glock;
static volatile unsigned long long resize_count = 0;
static bool resize_enabled;

static calqueue_node *calqueue_deq(void);
static void calqueue_enq(calqueue_node *new_node);



/*initializes a bucket array within the array a[].
   Bucket width is set equal to bwidth. Bucket[0] is made
   equal to a[qbase]; and the number of buckets is nbuck.
   Startprio is the priority at which dequeueing begins.
   All external variables except resize_enabled are
   initialized
*/
static void localinit(int qbase, int nbucks, double bwidth, double startprio) {

	int i;
	long int n;

	// Set position and size of nnew queue
	firstsub = qbase;
	calendar = calq + qbase;
	cwidth = bwidth;
	nbuckets = nbucks;

	// Init as empty
	qsize = 0;
	for (i = 0; i < nbuckets; i++) {
		calendar[i] = NULL;
	}

	// Setup initial position in queue
	lastprio = startprio;
	n = (long)((double)startprio / cwidth);	// Virtual bucket
	lastbucket = (int) (n % nbuckets);
	buckettop = (double)(n + 1) * cwidth + 0.5 * cwidth;

	// Setup queue-size-change thresholds
	bot_threshold = (int)((double)nbuckets / 2) - 2;
	top_threshold = 2 * nbuckets;
}

// This function returns the width that the buckets should have
// based on a random sample of the queue so that there will be about 3
// items in each bucket.
static double new_width(void) {

	//printf("calqueue: sto eseguendo new_width\n");

	int nsamples, templastbucket, i, j;
	double templastprio;
	double tempbuckettop, average, newaverage;
	calqueue_node *temp[25];

	// Init the temp node structure
	for (i = 0; i < 25; i++) {
		temp[i] = NULL;
	}

	// How many queue elements to sample?
	if (qsize < 2)
		return 1.0;

	if (qsize <= 5)
		nsamples = qsize;
	else
		nsamples = 5 + (int)((double)qsize / 10);

	if (nsamples > 25)
		nsamples = 25;

	// Store the current situation
	templastbucket = lastbucket;
	templastprio = lastprio;
	tempbuckettop = buckettop;

	resize_enabled = false;
	average = 0.;

	for (i = 0; i < nsamples; i++) {
		// Dequeue nodes to get a test sample and sum up the differences in time
		temp[i] = calqueue_deq();
		if (i > 0)
			average += temp[i]->timestamp - temp[i - 1]->timestamp;
	}

	// Get the average
	average = average / (double)(nsamples - 1);

	newaverage = 0.;
	j = 0;

	// Re-insert temp node 0
	//calqueue_put(temp[0]->timestamp, temp[0]->payload);
	calqueue_enq(temp[0]);

	// Recalculate ignoring large separations
	for (i = 1; i < nsamples; i++) {
		if ((temp[i]->timestamp - temp[i - 1]->timestamp) < (average * 2.0)) {
			newaverage += (temp[i]->timestamp - temp[i - 1]->timestamp);
			j++;
		}
		//calqueue_put(temp[i]->timestamp, temp[i]->payload);
		calqueue_enq(temp[i]);
	}

	// Free the temp structure (the events have been re-injected in the queue)
	//	for (i = 0; i < 25; i++) {
	//		if (temp[i] != NULL) {
	//			free(temp[i]);
	//		}
	//	}

	// Compute new width
	newaverage = (newaverage / (double)j) * 3.0;	/* this is the new width */

	// Restore the original condition
	lastbucket = templastbucket;	/* restore the original conditions */
	lastprio = templastprio;
	buckettop = tempbuckettop;
	resize_enabled = true;

	return newaverage;
}

// This copies the queue onto a calendar with nnewsize buckets. The new bucket
// array is on the opposite end of the array a[QSPACE] from the original        EH?!?!?!?!?!
static void resize(int newsize) {
	double bwidth;
	int i;
	int oldnbuckets;
	unsigned long long old_resize_count = resize_count;
	calqueue_node **oldcalendar, *temp, *temp2;

	if (!resize_enabled)
		return;

	pthread_rwlock_unlock(&glock); // TRY TO PROMOTE TO WRITE LOCK
	pthread_rwlock_wrlock(&glock); // 

	if(old_resize_count != resize_count)
		return; 	// THE LOCK WILL BE RELEASED OUTSIDE THIS FUNCTION		

	printf("calqueue: sto eseguendo resize\n");

	// Find new bucket width
	bwidth = new_width();
	
	// Save location and size of old calendar for use when copying calendar
	oldcalendar = calendar;
	oldnbuckets = nbuckets;

	// Init the new calendar
	if (firstsub == 0) {
		localinit(CALQSPACE - newsize, newsize, bwidth, lastprio);
	} else {
		localinit(0, newsize, bwidth, lastprio);
	}

	// Copy elements to the new calendar
	for (i = oldnbuckets - 1; i >= 0; --i) {
		temp = oldcalendar[i];
		while (temp != NULL) {
			temp2 = temp->next;
			calqueue_enq(temp);
			temp = temp2;
		}
		//		while (temp != NULL) {
		//			calqueue_put(temp->timestamp, temp->payload);
		//			temp2 = temp->next;
		//			free(temp);
		//			temp = temp2;
		//		}
	}
	resize_count++;
}

static calqueue_node *calqueue_deq(void) {

	register int i;
	int temp2;
	calqueue_node *e;
	double lowest;
	int size;
	int old_current;

  begin:
	old_current = lastbucket;
	// Is there an event to be processed?
	if (qsize == 0) {
		return NULL;
	}

	pthread_spin_lock(locks[old_current]);
	if(old_current != lastbucket){
		pthread_spin_unlock(locks[old_current]);
		goto begin;
	}



	for (i = old_current;;) {
		// Check bucket i
		if (calendar[i] != NULL && calendar[i]->timestamp < buckettop) {

 calendar_process:

			// Found an item to be processed
			e = calendar[i];

			// Update position on calendar and queue's size
			if(i != old_current){
				lastbucket = i;
				pthread_spin_unlock(locks[old_current]);
				goto begin;
			}
			lastprio = e->timestamp;

			// TODO in FAD
			size = __sync_add_and_fetch(&qsize, -1);
			

			// Remove the event from the calendar queue
			calendar[i] = calendar[i]->next;
			pthread_spin_unlock(locks[old_current]);

			// Halve the calendar size if needed
			if (size < bot_threshold)
				resize((int)((double)nbuckets / 2));

			//if(lastprio>248 && lastprio<251) printf("\tget: lastbucket %d, timestamo_head %f, buckettop %f\n", i, calendar[i]->timestamp, buckettop);

			// Processing completed
			return e;

		} else {
			// Prepare to check the next bucket, or go to a direct search
			i++;
			if (i == nbuckets)
				i = 0;

			buckettop += cwidth;

			if (i == lastbucket)
				break;	// Go to direct search
		}
	}

	// Directly search for minimum priority event
	temp2 = -1;
	lowest = (double)LLONG_MAX;
	for (i = 0; i < nbuckets; i++) {
		if ((calendar[i] != NULL) && ((temp2 == -1) || (calendar[i]->timestamp < lowest))) {
			temp2 = i;
			lowest = calendar[i]->timestamp;
		}
	}

	// Process the event found and and handle the queue
	i = temp2;
	goto calendar_process;
	return NULL;

}

static void calqueue_enq(calqueue_node *new_node) {

	int i, i_bt, size;
	calqueue_node *traverse;
	double timestamp = new_node->timestamp;
	int old_current;

	//printf("calqueue: inserendo %f, cwidth %f, bukettop %f, nbuckets %d, lastprio %f\n", timestamp, cwidth, buckettop, nbuckets, lastprio);

	if(new_node == NULL){
		printf("Out of memory in %s:%d\n", __FILE__, __LINE__);
		abort();
	}

	// Calculate the number of the bucket in which to place the new entry
	i_bt = (int) (round(timestamp / (double)cwidth));	// Virtual bucket
	i = i_bt % nbuckets;	// Actual bucket

	pthread_spin_lock(locks[i]);

	// Grab the head of events list in bucket i
	traverse = calendar[i];

	// Put at the head of the list, if appropriate
	if (traverse == NULL || traverse->timestamp > timestamp) {
		new_node->next = traverse;
		calendar[i] = new_node;
	} else {
		// Find the correct place in list
		while (traverse->next != NULL && traverse->next->timestamp <= timestamp)
			traverse = traverse->next;

		// Place the new event
		new_node->next = traverse->next;
		traverse->next = new_node;
	}

	// Update queue size
	size = __sync_add_and_fetch(&qsize, +1);

	pthread_spin_unlock(locks[i]);

  begin:
	old_current = lastbucket;
	pthread_spin_lock(locks[old_current]);
	if(old_current != lastbucket){
		pthread_spin_unlock(locks[old_current]);
		goto begin;
	}

	// Check whether we're adding something before lastprio
	if (timestamp < lastprio) {
		lastprio = timestamp;
		lastbucket = i;
		buckettop = (double)(i_bt + 1) * cwidth + 0.5 * cwidth;//
	}

	pthread_spin_unlock(locks[old_current]);

	// Double the calendar size if needed
	if (size > top_threshold && nbuckets < MAXNBUCKETS) {
		resize(2 * nbuckets);
	}

	//if(timestamp>248 && timestamp<251) printf("\tput: lastbucket %d, timestamo_head %f, buckettop %f\n", i, calendar[i]->timestamp, buckettop);
}

// This function initializes the messaging queue.
void*  pq_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket){
	int i=0;
	for(;i<CALQSPACE;i++){
		locks[i] = malloc(sizeof(pthread_spinlock_t));
		pthread_spin_init(locks[i], PTHREAD_PROCESS_PRIVATE);
	}
	localinit(0, 2, 1.0, 0.0);
	resize_enabled = true;
	pthread_rwlock_init(&glock, NULL); 

	return calendar;
}

__thread calqueue_node *cal_free_nodes_lists = NULL;

int   pq_enqueue(void *queue, pkey_t timestamp, void* payload){

	calqueue_node *new_node;

	// Fill the node entry
	if(cal_free_nodes_lists == NULL)
		new_node = malloc(sizeof(calqueue_node));
	else
	{
		new_node = cal_free_nodes_lists;
		cal_free_nodes_lists = cal_free_nodes_lists->next;
	}
	
	new_node->timestamp = timestamp;
	new_node->payload = payload;
	new_node->next = NULL;

	if(new_node == NULL){
		printf("Out of memory in %s:%d\n", __FILE__, __LINE__);
		abort();
	}

	pthread_rwlock_rdlock(&glock);
	calqueue_enq(new_node);
	pthread_rwlock_unlock(&glock);

  	return 1;
}

pkey_t pq_dequeue(void *queue, void **payload){
	calqueue_node *node;
	double ts;

	pthread_rwlock_rdlock(&glock);
	node = calqueue_deq();
	pthread_rwlock_unlock(&glock);

	if (node != NULL) {
		/*node->next = cal_free_nodes_lists;
		cal_free_nodes_lists = node;
		*/
		ts = node->timestamp;
		*payload = node->payload;
		free(node);
		return ts;
	}

	//printf("calqueue: estraendo %f, cwidth %f, bukettop %f, nbuckets %d, lastprio %f\n", node->timestamp, cwidth, buckettop, nbuckets, lastprio);
	*payload = NULL;
	return INFTY;
}




void pq_report(int tid){}
void pq_prune(){}
void pq_reset_statistics(){}
unsigned int pq_num_malloc(){ return 0; }