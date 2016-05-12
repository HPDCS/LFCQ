#pragma once
#ifndef __CALQUEUE_H
#define __CALQUEUE_H

//#define CALQSPACE 49153		// Calendar array size needed for maximum resize
#define CALQSPACE 131072//65536		// Calendar array size needed for maximum resize
#define MAXNBUCKETS 65536//32768	// Maximum number of buckets in calendar queue

//#define CALQSPACE  128        // Calendar array size needed for maximum resize
//#define MAXNBUCKETS 64       // Maximum number of buckets in calendar queue


typedef struct __calqueue_node {
	double			timestamp; // Timestamp associated to the event
	void 			*payload; // A pointer to the actual content of the node
	struct __calqueue_node 	*next;		// Pointers to other nodes
} calqueue_node;




extern void calqueue_init(void);
extern calqueue_node *calqueue_get(void);
extern void calqueue_put(double, void *);

#endif /* __CALQUEUE_H */
