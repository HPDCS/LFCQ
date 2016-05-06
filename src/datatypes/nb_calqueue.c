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
/*
 * nonblocking_queue.c
 *
 *  Created on: July 13, 2015
 *  Author: Romolo Marotta
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <float.h>
#include <pthread.h>
#include <math.h>

#include "../arch/atomic.h"
#include "../mm/myallocator.h"
#include "../datatypes/nb_calqueue.h"

__thread nbc_bucket_node *to_free_pointers = NULL;
__thread unsigned int to_remove_nodes_count = 0;
__thread unsigned int  lid;
__thread unsigned int  mark;

static nbc_bucket_node *g_tail;

#define USE_MACRO 0

#define VAL ((unsigned long long)0)
#define DEL ((unsigned long long)1)
#define MOV ((unsigned long long)2)
#define INV ((unsigned long long)3)

#define REMOVE_DEL	0
#define REMOVE_DEL_INV	1

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-conversion"

/**
 * This function computes the index of the destination bucket in the hashtable
 *
 * @author Romolo Marotta
 *
 * @param timestamp the value to be hashed
 * @param bucket_width the depth of a bucket
 *
 * @return the linear index of a given timestamp
 */
static inline unsigned int hash(double timestamp, double bucket_width)
{
	unsigned int res =  (unsigned int) (timestamp / bucket_width);
	unsigned int ret = 0;
//	unsigned int ret0 = 0;
//	unsigned int ret1 = 0;
//	unsigned int ret2 = 0;
//	unsigned int ret3 = 0;
//	unsigned int ret4 = 0;
//	unsigned int ret5 = 0;
//	double tmp1 = (res+0)*bucket_width;
//	double tmp2 = (res+1)*bucket_width;
//	double tmp3 = (res+2)*bucket_width;
//	double tmp4 = (res+3)*bucket_width;
//	double tmp5 = (res+4)*bucket_width;
	bool end = false;
	unsigned int count = 0;


	//res -= res < 3 ? res : 3;
	res -= (res & (-(res < 3))) +  (3 & (-(res >= 3)));


//	ret0 = (-1) ^ ( ( -1 ^ (res-1)) & ( -( GREATER(tmp1, timestamp) 							 ) ));
//	ret1 = (-1) ^ ( ( -1 ^ (res+0)) & ( -( D_EQUAL(tmp1, timestamp)  || GREATER(tmp2, timestamp) ) ));
//	ret2 = (-1) ^ ( ( -1 ^ (res+1)) & ( -( D_EQUAL(tmp2, timestamp)  || GREATER(tmp3, timestamp) ) ));
//	ret3 = (-1) ^ ( ( -1 ^ (res+2)) & ( -( D_EQUAL(tmp3, timestamp)  || GREATER(tmp4, timestamp) ) ));
//	ret4 = (-1) ^ ( ( -1 ^ (res+3)) & ( -( D_EQUAL(tmp4, timestamp)  || GREATER(tmp5, timestamp) ) ));
//	ret5 = (-1) ^ ( ( -1 ^ (res+4)) & ( -( D_EQUAL(tmp5, timestamp) 							 ) ));


//	ret1 = (ret1 & (-(ret1 < ret2))) + (ret2 & (-(ret1 >= ret2)));
//	ret2 = (ret3 & (-(ret3 < ret4))) + (ret3 & (-(ret3 >= ret4)));
//	ret0 = (ret0 & (-(ret0 < ret5))) + (ret5 & (-(ret0 >= ret5)));
//
//
//	ret1 = (ret1 & (-(ret1 < ret2))) + (ret2 & (-(ret1 >= ret2)));
//	ret1 = (ret1 & (-(ret1 < ret0))) + (ret0 & (-(ret1 >= ret0)));


	while( !end )
	{
		double tmp1 = res*bucket_width;

		ret = ((res) & (-(D_EQUAL(tmp1, timestamp)))) +  ((res-1) & (-(GREATER(tmp1, timestamp))));
		end = D_EQUAL(tmp1, timestamp) || GREATER(tmp1, timestamp);

		res++;

		count++;
	}

//	unsigned int res1 =  (unsigned int) (timestamp / bucket_width);
//
//	while( true )
//	{
//
//		//printf("LOCK HERE %f, %f %f\n", res*bucket_width, timestamp, bucket_width);
//		if(D_EQUAL(res1*bucket_width, timestamp))
//			break;
//		if(GREATER(res1*bucket_width, timestamp))
//		{
//			res1--;
//			break;
//		}
//		res1++;
//	}


//	if(res1 != ret)
//		printf("%u %u %u %u %u %u %u %u \n", ret, res1, ret0, ret1, ret2, ret3, ret4, ret5);
	return ret;
}

//	unsigned int ret_val_1a = 0;
//	unsigned int ret_val_1b = 0;
//	unsigned int ret_val_2a = 0;
//	unsigned int ret_val_2b = 0;
//	unsigned int ret_val_3a = 0;
//	unsigned int ret_val_3b = 0;
//	unsigned int ret_val_4a = 0;
//	unsigned int ret_val_4b = 0;
//	unsigned int ret_val_1 = 0;
//	unsigned int ret_val_2 = 0;
//	unsigned int ret_val_3 = 0;
//	unsigned int ret_val_4 = 0;
//	double tmp1;
//	double tmp2;
//	double tmp3;
//	double tmp4;
//
//
//	tmp1 = (res+0)*bucket_width;
//	tmp2 = (res+1)*bucket_width;
//	tmp3 = (res+2)*bucket_width;
//	tmp4 = (res+3)*bucket_width;
//
//
//	ret_val_1a = (res+0) & (-(D_EQUAL(tmp1, timestamp)));
//	ret_val_1b = (res-1) & (-(GREATER(tmp1, timestamp)));
//
//	ret_val_2a = (res+1) & (-(D_EQUAL(tmp2, timestamp)));
//	ret_val_2b = (res-0) & (-(GREATER(tmp2, timestamp)));
//
//	ret_val_3a = (res+2) & (-(D_EQUAL(tmp3, timestamp)));
//	ret_val_3b = (res+1) & (-(GREATER(tmp3, timestamp)));
//
//	ret_val_4a = (res+3) & (-(D_EQUAL(tmp4, timestamp)));
//	ret_val_4b = (res+2) & (-(GREATER(tmp4, timestamp)));
//
//
//	ret_val_1 = ret_val_1a + ret_val_1b;
//	ret_val_2 = ret_val_2a + ret_val_2b;
//	ret_val_3 = ret_val_3a + ret_val_3b;
//	ret_val_4 = ret_val_4a + ret_val_4b;
//
//	ret_val_1 = (ret_val_1 & (-(ret_val_1 < ret_val_2))) + (ret_val_2 & (-(ret_val_1 >= ret_val_2)));
//	ret_val_2 = (ret_val_3 & (-(ret_val_3 < ret_val_4))) + (ret_val_3 & (-(ret_val_3 >= ret_val_4)));
//
//	ret_val_1 = (ret_val_1 & (-(ret_val_1 < ret_val_2))) + (ret_val_2 & (-(ret_val_1 >= ret_val_2)));
//
//	return ret_val_1;



/**
 *  This function returns an unmarked reference
 *
 *  @author Romolo Marotta
 *
 *  @param pointer
 *
 *  @return the unmarked value of the pointer
 */
static inline void* get_unmarked(void *pointer)
{
	return (void*) (((unsigned long long) pointer) & ~((unsigned long long) 3));
}

/**
 *  This function returns a marked reference
 *
 *  @author Romolo Marotta
 *
 *  @param pointer
 *
 *  @return the marked value of the pointer
 */
static inline void* get_marked(void *pointer, unsigned long long mark)
{
	return (void*) (((unsigned long long) get_unmarked((pointer))) | (mark));
}

/**
 *  This function checks if a reference is marked
 *
 *  @author Romolo Marotta
 *
 *  @param pointer
 *
 *  @return true if the reference is marked, else false
 */
static inline bool is_marked(void *pointer, unsigned long long mask)
{
	return (bool) ((((unsigned long long) pointer) &  ((unsigned long long) 3)) == mask);
}

static inline bool is_marked_for_search(void *pointer, unsigned int mask)
{
	if(mask == REMOVE_DEL)
		return (bool) ((((unsigned long long) pointer) &  ((unsigned long long) 3)) == DEL);
	if(mask == REMOVE_DEL_INV)
	{		bool a = (bool) ((((unsigned long long) pointer) &  ((unsigned long long) 3)) == DEL);
			bool b = (bool) ((((unsigned long long) pointer) &  ((unsigned long long) 3)) == INV);
			return a || b;
	}
	return false;
}

/**
* This function generates a mark value that is unique w.r.t. the previous values for each Logical Process.
* It is based on the Cantor Pairing Function, which maps 2 naturals to a single natural.
* The two naturals are the LP gid (which is unique in the system) and a non decreasing number
* which gets incremented (on a per-LP basis) upon each function call.
* It's fast to calculate the mark, it's not fast to invert it. Therefore, inversion is not
* supported at all in the code.
*
* @author Alessandro Pellegrini
*
* @param lid The local Id of the Light Process
* @return A value to be used as a unique mark for the message within the LP
*/
static inline unsigned long long generate_ABA_mark() {
	unsigned long long k1 = lid;
	unsigned long long k2 = mark++;
	unsigned long long res = (unsigned long long)( ((k1 + k2) * (k1 + k2 + 1) / 2) + k2 );
	return ((~((unsigned long long)0))>>32) & res;
}

/**
 * This function blocks the execution of the process.
 * Used for debug purposes.
 */
static void error(const char *msg, ...) {
	char buf[1024];
	va_list args;

	va_start(args, msg);
	vsnprintf(buf, 1024, msg, args);
	va_end(args);

	printf("%s", buf);
	exit(1);
}

/**
 *  This function is an helper to allocate a node and filling its fields.
 *
 *  @author Romolo Marotta
 *
 *  @param payload is a pointer to the referred payload by the node
 *  @param timestamp the timestamp associated to the payload
 *
 *  @return the pointer to the allocated node
 *
 */
static nbc_bucket_node* node_malloc(void *payload, double timestamp, unsigned int tie_breaker)
{

	nbc_bucket_node* res = (nbc_bucket_node*) mm_std_malloc(sizeof(nbc_bucket_node));

	if (is_marked(res, DEL) || is_marked(res, MOV) || is_marked(res, INV) || res == NULL)
		error("%lu - Not aligned Node or No memory\n", pthread_self());

	res->counter = tie_breaker;
	res->next = NULL;
	res->replica = NULL;
	res->payload = payload;
	res->timestamp = timestamp;

	return res;
}

/**
 * This function connect to a private structure marked
 * nodes in order to free them later, during a synchronisation point
 *
 * @author Romolo Marotta
 *
 * @param queue used to associate freed nodes to a queue
 * @param start the pointer to the first node in the disconnected sequence
 * @param end   the pointer to the last node in the disconnected sequence
 * @param timestamp   the timestamp of the last disconnected node
 *
 *
 */
static inline void connect_to_be_freed_list(nbc_bucket_node *start, unsigned int counter)
{
	nbc_bucket_node* new_node;
	start = get_unmarked(start);

	new_node = node_malloc(start, start->timestamp, counter);
	new_node->next = to_free_pointers;

	to_free_pointers = new_node;
	to_remove_nodes_count += counter;
}


/**
 * This function implements the search of a node that contains a given timestamp t. It finds two adjacent nodes,
 * left and right, such that: left.timestamp <= t and right.timestamp > t.
 *
 * Based on the code by Timothy L. Harris. For further information see:
 * Timothy L. Harris, "A Pragmatic Implementation of Non-Blocking Linked-Lists"
 * Proceedings of the 15th International Symposium on Distributed Computing, 2001
 *
 * @author Romolo Marotta
 *
 * @param queue the queue that contains the bucket
 * @param head the head of the list in which we have to perform the search
 * @param timestamp the value to be found
 * @param left_node a pointer to a pointer used to return the left node
 * @param right_node a pointer to a pointer used to return the right node
 *
 */
static void search_std(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						nbc_bucket_node **left_node, nbc_bucket_node **right_node)
{
	nbc_bucket_node *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	tail = g_tail;

	do
	{
		// Fetch the head and its next
		tmp = head;
		tmp_next = tmp->next;
		counter = 0;
		double tmp_timestamp;
		do
		{
			bool marked = is_marked_for_search(tmp_next, REMOVE_DEL_INV);
			// Find the first unmarked node that is <= timestamp
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				counter = 0;
			}
			// Take a snap to identify the last marked node before the right node
			counter+=marked;

			// Find the next unmarked node from the left node (right node)
			tmp = get_unmarked(tmp_next);
			tmp_timestamp = tmp->timestamp;
			//if (tmp == tail)
			//	break;
			tmp_next = tmp->next;

		} while (	tmp != tail &&
					(
						is_marked_for_search(tmp_next, REMOVE_DEL_INV)
						|| ( LEQ(tmp_timestamp, timestamp) && tie_breaker == 0)
						|| ( LEQ(tmp_timestamp, timestamp) && tie_breaker != 0 && tmp->counter <= tie_breaker)
					)
				);

		// Set right node
		right = tmp;

		//left node and right node have to be adjacent. If not try with CAS
		if (get_unmarked(left_next) != right)
		{
			if(is_marked(left_next, MOV))
				right = get_marked(right, MOV);
			// if CAS succeeds connect the removed nodes to to_be_freed_list
			if (!CAS_x86(
						(volatile unsigned long long *)&(left->next),
						(unsigned long long) left_next,
						(unsigned long long) right
						)
					)
				continue;
			connect_to_be_freed_list(left_next, counter);
		}
		// at this point they are adjacent. Thus check that right node is still unmarked and return
		if (right == tail || !is_marked_for_search(right->next, REMOVE_DEL_INV))
		{
			*left_node = left;
			*right_node = get_unmarked(right);
			return;
		}
	} while (1);
}

static void search_copy(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						nbc_bucket_node **left_node, nbc_bucket_node **right_node)
{
	nbc_bucket_node *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	tail = g_tail;

	do
	{
		// Fetch the head and its next
		tmp = head;
		tmp_next = tmp->next;
		counter = 0;
		double tmp_timestamp;
		do
		{
			bool marked = is_marked_for_search(tmp_next, REMOVE_DEL);
			// Find the first unmarked node that is <= timestamp
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				counter = 0;
			}
			// Take a snap to identify the last marked node before the right node
			counter+=marked;

			// Find the next unmarked node from the left node (right node)
			tmp = get_unmarked(tmp_next);
			tmp_timestamp = tmp->timestamp;
			//if (tmp == tail)
			//	break;
			tmp_next = tmp->next;

		} while (	tmp != tail &&
					(
						is_marked_for_search(tmp_next, REMOVE_DEL)
						|| ( LEQ(tmp_timestamp, timestamp) && tie_breaker == 0)
						|| ( LEQ(tmp_timestamp, timestamp) && tie_breaker != 0 && tmp->counter <= tie_breaker)
					)
				);

		// Set right node
		right = tmp;

		//left node and right node have to be adjacent. If not try with CAS
		if (get_unmarked(left_next) != right)
		{
			// TODO check
			if(is_marked(left_next, MOV))
				right = get_marked(right, MOV);
			if(is_marked(left_next, INV))
				right = get_marked(right, INV);
			// if CAS succeeds connect the removed nodes to to_be_freed_list
			if (!CAS_x86(
						(volatile unsigned long long *)&(left->next),
						(unsigned long long) left_next,
						(unsigned long long) right
						)
					)
				continue;
			connect_to_be_freed_list(left_next, counter);
		}
		// at this point they are adjacent. Thus check that right node is still unmarked and return
		if (right == tail || !is_marked_for_search(right->next, REMOVE_DEL))
		{
			*left_node = left;
			*right_node = right;
			return;
		}
	} while (1);
}


/**
 * This function commits a value in the current field of a queue. It retries until the timestamp
 * associated with current is strictly less than the value that has to be committed
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 * @param left_node the candidate node for being next current
 *
 */
static inline void nbc_flush_current(table* h, nbc_bucket_node* node)
{
	unsigned long long oldCur;
	unsigned int oldIndex;
	unsigned int index = hash(node->timestamp, h->bucket_width);
	unsigned long long newCur =  ( ( unsigned long long ) index ) << 32;
	newCur |= generate_ABA_mark();

	// Retry until the left node has a timestamp strictly less than current and
	// the CAS fails
	do
	{

		oldCur = h->current;
		oldIndex = (unsigned int) (oldCur >> 32);
	}
	while (
			index <= oldIndex && !is_marked(node->next, DEL)
			&& !CAS_x86(
						(volatile unsigned long long *)&(h->current),
						(unsigned long long) oldCur,
						(unsigned long long) newCur
						)
					);
}

/**
 * This function insert a new event in the nonblocking queue.
 * The cost of this operation when succeeds should be O(1) as calendar queue
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 * @param timestamp the timestamp of the event
 * @param payload the event to be enqueued
 *
 */
static bool insert_std(nb_calqueue* queue, table* hashtable, nbc_bucket_node* new_node)
{
	nbc_bucket_node *left_node, *right_node, *bucket;
	unsigned int index;
	unsigned int new_node_counter = new_node->counter;
	double new_node_timestamp = new_node->timestamp;



	index = hash(new_node_timestamp, hashtable->bucket_width) % hashtable->size;

	// node to be added in the hashtable
	bucket = hashtable->array + index;

	search_std(bucket, new_node_timestamp, new_node_counter, &left_node, &right_node);

	new_node->next = right_node;

	if(new_node_counter == 0)
		new_node->counter = 1 + ( -D_EQUAL(new_node_timestamp, left_node->timestamp ) & left_node->counter );

	else if(D_EQUAL(new_node_timestamp, left_node->timestamp ) && left_node->counter == new_node_counter)
	{
		mm_free(new_node);
		return true;
	}

	return CAS_x86(
				(volatile unsigned long long*)&(left_node->next),
				(unsigned long long) right_node,
				(unsigned long long) new_node
			);

}

static bool insert_copy(nb_calqueue* queue, table* hashtable, nbc_bucket_node** new_node)
{
	nbc_bucket_node *left_node, *right_node, *bucket;
	unsigned int index;

	unsigned int new_node_counter = (*new_node)->counter;
	double new_node_timestamp = (*new_node)->timestamp;


	index = hash(new_node_timestamp, hashtable->bucket_width) % hashtable->size;

	// node to be added in the hashtable
	bucket = hashtable->array + index;

	search_copy(bucket, new_node_timestamp, new_node_counter, &left_node, &right_node);

	(*new_node)->next = get_marked(get_unmarked(right_node), INV);

	if(new_node_counter == 0)
		(*new_node)->counter = 1 + ( -D_EQUAL(new_node_timestamp, left_node->timestamp ) & left_node->counter );

	else if(D_EQUAL(new_node_timestamp, left_node->timestamp ) && left_node->counter == new_node_counter)
	{
		//mm_free((*new_node));
		*new_node = left_node;
		return true;
	}


	//if(is_marked(right_node, MOV))
	//	right_node = get_unmarked(right_node);


	if(is_marked(right_node, INV))
		*new_node = get_marked(*new_node, INV);

	if (CAS_x86(
				(volatile unsigned long long*)&(left_node->next),
				(unsigned long long) right_node,
				(unsigned long long) *new_node
			))
		{
			*new_node = get_unmarked(*new_node);
			return true;
		}
	return false;

}


static void set_new_table(table* h, unsigned int threshold )
{
	nbc_bucket_node *tail = g_tail;
	int counter = atomic_read( &h->counter );
	counter = (-(counter >= 0)) & counter;

	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int thp2 = threshold *2;
	unsigned int new_size = 0;


	if		(size == 1 && counter > thp2)
		new_size = thp2;
	else if (size == thp2 && counter < threshold)
		new_size = 1;
	else if (size >= thp2 && counter > 2*size)
		new_size = 2*size;
	else if (size > thp2 && counter < 0.5*size)
		new_size =  0.5*size;

	if(new_size > MAXIMUM_SIZE )
		new_size = 0;

	if(new_size > 0)
	{

		table *new_h = (table*) mm_std_malloc(sizeof(table));
		if(new_h == NULL)
			error("No enough memory to new table structure\n");

		new_h->bucket_width = -1.0;
		new_h->size 		= new_size;
		new_h->new_table 	= NULL;
		new_h->counter.count= 0;
		new_h->current 		= ((unsigned long long)-1) << 32;


		nbc_bucket_node *array =  (nbc_bucket_node*) mm_std_malloc(sizeof(nbc_bucket_node) * new_size);
		if(array == NULL)
		{
			mm_std_free(new_h);
			error("No enough memory to allocate new table array %u\n", new_size);
		}

		memset(array, 0, sizeof(nbc_bucket_node) * new_size);

		for (i = 0; i < new_size; i++)
			array[i].next = tail;


		new_h->array 	= array;

		if(!CAS_x86(
				(volatile unsigned long long *) &(h->new_table),
				(unsigned long long)			NULL,
				(unsigned long long) 			new_h))
		{
			mm_std_free(new_h->array);
			mm_std_free(new_h);
		}

		//else
		//	printf("CHANGE SIZE from %u to %u, items %u #\n", size, new_size, counter);
	}
}

static void block_table(table* h, unsigned int *index, double *min_timestamp)
{
	unsigned int i=0;
	unsigned int size = h->size;
	nbc_bucket_node *tail = g_tail;
	nbc_bucket_node *array = h->array;
	*min_timestamp = INFTY;

	for(i = 0; i < size; i++)
	{
		nbc_bucket_node *bucket = array + i;
		nbc_bucket_node *bucket_next, *right_node, *left_node, *right_node_next;

		bool marked = false;
		bool cas_result = false;

		do
		{
			bucket_next = bucket->next;
			marked = is_marked(bucket_next, MOV);
			if(!marked)
				cas_result = CAS_x86(
						(volatile unsigned long long *) &(bucket->next),
						(unsigned long long)			bucket_next,
						(unsigned long long) 			get_marked(bucket_next, MOV)
					);
		}
		while( !marked && !cas_result );

		//printf("bloccato il bucket %u \n", i);

		do
		{

		//	printf("Cerco Right node \n");
			search_std(bucket, 0.0, 0, &left_node, &right_node);
			right_node = get_unmarked(right_node);
			right_node_next = right_node->next;
		//	printf("Right node %p\n", right_node);
		}
		while(
				right_node != tail &&
				(
					is_marked(right_node_next, DEL) ||
					is_marked(right_node_next, INV) ||
					(	get_unmarked(right_node_next) == right_node_next && (
							cas_result = CAS_x86(
							(volatile unsigned long long *) &(right_node->next),
							(unsigned long long)			right_node_next,
							(unsigned long long) 			get_marked(right_node_next, MOV)
							))
					)
				)
			);


		//printf("bloccato il primo node del bucket %u \n", i);

//				if(cas_result)
//						printf("Locked first node in bucket #%u\n", i);

		if(LESS(right_node->timestamp, *min_timestamp) )
		{
			*min_timestamp = right_node->timestamp;
			*index = i;
		}

	}
}

static double compute_mean_separation_time(table* h,
		unsigned int new_size, unsigned int threashold,
		unsigned int index, double min_timestamp)
{
	nbc_bucket_node *tail = g_tail;

	unsigned int i;

	table *new_h = h->new_table;
	nbc_bucket_node *array = h->array;
	double old_bw = h->bucket_width;
	unsigned int size = h->size;
	double new_bw = new_h->bucket_width;

	if(new_bw < 0)
	{

		int sample_size;
		double average = 0.0;
		double newaverage = 0.0;

		if(new_size <= threashold*2)
			newaverage = 1.0;
		else
		{

			if (new_size <= 5)
				sample_size = new_size/2;
			else
				sample_size = 5 + (int)((double)new_size / 10);

			if (sample_size > SAMPLE_SIZE)
				sample_size = SAMPLE_SIZE;


			//printf("COMPUTE SAMPLE SIZE %u\n", sample_size);
			double sample_array[sample_size];

			int counter = 0;
			i = 0;

			min_timestamp = hash(min_timestamp, old_bw)*old_bw;

			double min_next_round = INFTY;
			unsigned int new_index = 0;
			unsigned int limit = h->size*3;

			while(counter != sample_size && i < limit && new_h->bucket_width == -1.0)
			{

				nbc_bucket_node *tmp = array + (index+i)%size;
				nbc_bucket_node *tmp_next = get_unmarked(tmp->next);
				double tmp_timestamp = tmp->timestamp;
				//printf("BUCKET SAMPLE %u \n", index+i);
				while( tmp != tail && counter < sample_size &&  LESS(tmp_timestamp, min_timestamp + old_bw*(i+1)) )
				{

					tmp_next = tmp->next;
					if(!is_marked(tmp_next, DEL) && tmp->counter != HEAD_ID && GEQ(tmp_timestamp, min_timestamp + old_bw*(i)))
					{
						sample_array[counter++] = tmp_timestamp;
				//		printf("SAMPLE %u %f\n", counter, tmp->timestamp);
					}
					else
					{
						if( LESS(tmp_timestamp, min_next_round) && GEQ(tmp_timestamp, min_timestamp + old_bw*(i+1)))
						{
							min_next_round = tmp_timestamp;
							new_index = i;
						}
					}
					tmp = get_unmarked(tmp_next);
					tmp_timestamp = tmp->timestamp;
				}
				i++;
				if(counter < sample_size && i >= size*3 && min_next_round != INFTY && new_h->bucket_width == -1.0)
				{
					min_timestamp = hash(min_next_round, old_bw)*old_bw;
					index = new_index;
					limit += size*3;
					min_next_round = INFTY;
					new_index = 0;
				}
			}
			if( counter < sample_size)
				sample_size = counter;

			for(i = 1; i<sample_size;i++)
				average += sample_array[i] - sample_array[i - 1];

				// Get the average
			average = average / (double)(sample_size - 1);

			int j=0;
			// Recalculate ignoring large separations
			for (i = 1; i < sample_size; i++) {
				if ((sample_array[i] - sample_array[i - 1]) < (average * 2.0))
				{
					newaverage += (sample_array[i] - sample_array[i - 1]);
					j++;
				}
			}

			// Compute new width
			newaverage = (newaverage / j) * 3.0;	/* this is the new width */
			if(newaverage < 0)
				newaverage = 1.0;
			if(isnan(newaverage))
				newaverage = 1.0;
		}

		new_bw = newaverage;

	}
	return new_bw;
}

static table* read_table(nb_calqueue* queue)
{
	table *h = queue->hashtable;
	nbc_bucket_node *tail = g_tail;

	//printf("SIZE H %d\n", h->counter.count);

	unsigned int i, index;
	if(h->new_table == NULL)
		set_new_table(h, queue->threshold);

	if(h->new_table != NULL)
	{

		//printf("%u - MOVING BUCKETS\n", lid);
		table *new_h = h->new_table;
		nbc_bucket_node *array = h->array;
		double new_bw = new_h->bucket_width;
		double min_timestamp = INFTY;

		if(new_bw < 0)
		{
			block_table(h, &index, &min_timestamp);
			double newaverage = compute_mean_separation_time(h, new_h->size, queue->threshold, index, min_timestamp);
			//if
			(
					CAS_x86(
							(volatile unsigned long long *) &(new_h->bucket_width),
							*( (unsigned long long*)  &new_bw),
							*( (unsigned long long*)  &newaverage)
					)
				)
				//printf("COMPUTE BW %.20f %u\n", newaverage, new_h->size)
				;
		}

		for(i=0; i < h->size; i++)
		{
			nbc_bucket_node *bucket = array + i;
			nbc_bucket_node *right_node, *left_node, *right_node_next;


			//printf("MOVING BUCKET %u %p \n", i, bucket);
			do
			{
				//printf("*********************\n");
				do
				{

					search_std(bucket, 0.0, 0, &left_node, &right_node);
					right_node = get_unmarked(right_node);
					right_node_next = right_node->next;
				}
//				while(
//						right_node != tail &&
//						get_unmarked(right_node_next) == right_node_next &&
//						!is_marked(right_node_next, MOV) &&
//						!CAS_x86(
//											(volatile unsigned long long *) &(right_node->next),
//											(unsigned long long)			right_node_next,
//											(unsigned long long) 			get_marked(right_node_next, MOV)
//						)
//					);
				while(
						right_node != tail &&
						(
							is_marked(right_node_next, DEL) ||
							is_marked(right_node_next, INV) ||
							(	get_unmarked(right_node_next) == right_node_next && (
									CAS_x86(
									(volatile unsigned long long *) &(right_node->next),
									(unsigned long long)			right_node_next,
									(unsigned long long) 			get_marked(right_node_next, MOV)
									))
							)
						)
					);

				if(right_node != tail)
				{
				//	printf("R: %p %.10f %u %p %p\n", right_node, right_node->timestamp,
				//			right_node->counter, right_node->replica, right_node->next);
					nbc_bucket_node *replica = node_malloc(right_node->payload, right_node->timestamp,  right_node->counter);
					nbc_bucket_node *right_replica_field;
					nbc_bucket_node *replica2;

					if(replica == tail)
						error("A\n");


					replica2 = replica;
					while(!insert_copy(queue, new_h, &replica2) && queue->hashtable == h);

				//	if(get_unmarked())


					if(queue->hashtable != h)
						return queue->hashtable;


					if(replica2 == tail)
						error("B %p %p %p\n", replica, replica2, tail);

					if(CAS_x86(
							(volatile unsigned long long *) &(right_node->replica),
							(unsigned long long)			NULL,
							(unsigned long long) 			replica2))
						atomic_inc_x86(&(new_h->counter));

				//	printf("C Replica: %p, %p\n", replica, right_node->replica);


					bool cas_result = false;
					bool marked  = false;
					right_replica_field = right_node->replica;

					if(right_replica_field == tail)
						error("%u - C %p %p %p %p %p\n", lid, replica, replica2, tail, right_replica_field, right_node->replica);

					if(replica == replica2 && replica != right_replica_field)
					{
				//		printf("Should be impossible %p, %p\n", replica, right_replica_field);
						do
						{


							right_node_next = replica->next;
							marked = is_marked(right_node_next, DEL);
							if(!marked)
								cas_result = CAS_x86(
												(volatile unsigned long long *) &(replica->next),
												(unsigned long long)			right_node_next,
												(unsigned long long) 			get_marked(right_node_next, DEL)
											);

						}while( !marked && !cas_result ) ;
					}

					if(replica != replica2)
						mm_free(replica);


					do
					{
						right_node_next = right_node->next;
						marked = is_marked(right_node_next, DEL);
						if(!marked)
							cas_result = CAS_x86(
											(volatile unsigned long long *) &(right_node->next),
											(unsigned long long)			right_node_next,
											(unsigned long long) 			get_marked(get_unmarked(right_node_next), DEL)
										);

					}while( !marked && !cas_result ) ;


			//		printf("Destroy original %p %p %p\n", right_node, right_node_next, right_node->next);



					do
					{


						right_node_next = right_replica_field->next;
						marked = is_marked(right_node_next, INV);
						if(marked)
							cas_result = CAS_x86(
											(volatile unsigned long long *) &(right_replica_field->next),
											(unsigned long long)			right_node_next,
											(unsigned long long) 			get_unmarked(right_node_next)
										);

					}while( marked && !cas_result ) ;


					nbc_flush_current(
							new_h,
							right_replica_field);

			//		printf("Validate replica %p %p %p\n", right_replica_field, right_node_next, right_replica_field->next);

			//		printf("Count new item %u\n", new_h->counter.count);
				}

			}while(right_node != tail);

		}

		CAS_x86(
				(volatile unsigned long long *) &(queue->hashtable),
				(unsigned long long)			h,
				(unsigned long long) 			new_h
		);

//		printf("%u - MOVING BUCKETS DONE "
//				"O_CUR: %u "
//				"O_SIZE: %u "
//				"O_BW: %.10f "
//				"O_TS: %.10f "
//				"N_CUR: %u "
//				"N_SIZE: %u "
//				"N_BW: %.10f "
//				"N_TS: %.10f "
//				"TAIL: %p "
//				"\n",
//				lid,
//				(unsigned int) ( (h->current)>>32),
//				(unsigned int) (h->size),
//				(h->bucket_width),
//				(h->bucket_width)*( (h->current)>>32),
//				(unsigned int) ( (new_h->current)>>32),
//				(unsigned int) (new_h->size),
//				(new_h->bucket_width),
//				(new_h->bucket_width)*( (new_h->current)>>32),
//				(g_tail)
//				);
		h = new_h;


	}

	return h;
}

/**
 * This function create an instance of a non-blocking calendar queue.
 *
 * @author Romolo Marotta
 *
 * @param queue_size is the inital size of the new queue
 *
 * @return a pointer a new queue
 */
nb_calqueue* nb_calqueue_init(unsigned int threshold)
{
	unsigned int i = 0;

	nb_calqueue* res = (nb_calqueue*) mm_std_malloc(sizeof(nb_calqueue));
	if(res == NULL)
		error("No enough memory to allocate queue\n");
	memset(res, 0, sizeof(nb_calqueue));

	res->threshold = threshold;

	res->hashtable = (table*) mm_std_malloc(sizeof(table));
	if(res->hashtable == NULL)
	{
		mm_std_free(res);
		error("No enough memory to allocate queue\n");
	}
	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = 1;


	res->hashtable->array = (nbc_bucket_node*) mm_std_malloc(sizeof(nbc_bucket_node) );
	if(res->hashtable->array == NULL)
	{
		mm_std_free(res->hashtable);
		mm_std_free(res);
		error("No enough memory to allocate queue\n");
	}

	memset(res->hashtable->array, 0, sizeof(nbc_bucket_node) );

	g_tail = node_malloc(NULL, INFTY, 0);
	g_tail->next = NULL;

	for (i = 0; i < 1; i++)
	{
		res->hashtable->array[i].next = g_tail;
		res->hashtable->array[i].timestamp = i * 1.0;
		res->hashtable->array[i].counter = 0;
	}

	res->hashtable->current = 0;

	return res;
}

/**
 * This function implements the enqueue interface of the non-blocking queue.
 * Should cost O(1) when succeeds
 *
 * @author Romolo Marotta
 *
 * @param queue
 * @param timestamp the key associated with the value
 * @param payload the event to be enqueued
 *
 * @return true if the event is inserted in the hashtable, else false
 */
void nbc_enqueue(nb_calqueue* queue, double timestamp, void* payload)
{
	nbc_bucket_node *new_node = node_malloc(payload, timestamp, 0);
	table *h;

	do
		h  = read_table(queue);
	while(!insert_std(queue, h, new_node));

	nbc_flush_current(
			h,
			new_node);
	atomic_inc_x86(&(h->counter));
}

/**
 * This function dequeue from the nonblocking queue. The cost of this operation when succeeds should be O(1)
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 *
 * @return a pointer to a node that contains the dequeued value
 *
 */
nbc_bucket_node* nbc_dequeue(nb_calqueue *queue)
{
	nbc_bucket_node *right_node, *min, *right_node_next, *res, *tail, *left_node;
	table * h;
	unsigned long long current;

	unsigned int index;
	unsigned int size;
	nbc_bucket_node *array;
	double right_timestamp;
	double bucket_width;


	tail = g_tail;
	res = NULL;
	do
	{
		h = read_table(queue);

		current = h->current;
		size = h->size;
		array = h->array;
		bucket_width = h->bucket_width;

		index = (unsigned int)(current >> 32);


		min = array + (index % size);

		search_std(min, 0.0, 0, &left_node, &right_node);


		if(is_marked(min->next, MOV))
			continue;

		right_node_next = right_node->next;
		right_timestamp = right_node->timestamp;


		if(right_node == tail && size == 1 )
			return node_malloc(NULL, INFTY, 1);

		else if( LESS(right_timestamp, (index)*bucket_width) )
		{
			nbc_flush_current(h, right_node);
		}

		else if( GREATER(right_timestamp, (index+1)*bucket_width) )
		{
			if(index+1 < index)
				error("\nOVERFLOW index:%u  %.10f %u %f\n", index, bucket_width, size, right_timestamp);

			unsigned long long newCur =  ( ( unsigned long long ) index+1 ) << 32;
			newCur |= generate_ABA_mark();
				CAS_x86(
						(volatile unsigned long long *)&(h->current),
						(unsigned long long)			current,
						(unsigned long long) 			newCur
				);
		}
		else if(!is_marked(right_node_next, DEL))
		{
			unsigned int new_current = ((unsigned int)(h->current >> 32));
			if( new_current < index )
				continue;

			res = node_malloc(right_node, right_timestamp, right_node->counter);
			if(CAS_x86(
								(volatile unsigned long long *)&(right_node->next),
								(unsigned long long) get_unmarked(right_node_next),
								(unsigned long long) get_marked(right_node_next, DEL)
								)
							)
			{
				nbc_bucket_node *left_node, *right_node;

				search_std(min, right_timestamp, -1,  &left_node, &right_node);
				atomic_dec_x86(&(h->counter));

				return res;
			}
			else
				free(res);
		}
//		else if(is_marked(right_node_next, DEL))
//		{
//				printf("IS MARKED "
//						"R:%p "
//						"R_N:%p "
//						"TIE:%u "
//						"TIME:%.10f "
//						"IS_INV:%u "
//						"CUR:%u "
//						"NUM:%u "
//						"TSIZE:%u "
//						"BW: %.10f "
//						"\n",
//						right_node,
//						right_node_next,
//						right_node->counter,
//						right_node->timestamp,
//						is_marked(right_node_next, INV),
//						index,
//						h->counter.count,
//						h->size,
//						h->bucket_width)		;
//		}
//		else if(right_node == tail )
//			error("NOOOOOOOOOOO\n");
//		}
		//else
		//	printf("NUM ELEMS %u %p %u %llu %llu\n", h->counter.count, h, index, current, current >> 32);


	}while(1);
	return NULL;
}

/**
 * This function frees any node in the hashtable with a timestamp strictly less than a given threshold,
 * assuming that any thread does not hold any pointer related to any nodes
 * with timestamp lower than the threshold.
 *
 * @author Romolo Marotta
 *
 * @param queue the interested queue
 * @param timestamp the threshold such that any node with timestamp strictly less than it is removed and freed
 *
 */
double nbc_prune(nb_calqueue *queue, double timestamp)
{
	unsigned int committed = 0;
	nbc_bucket_node **prec, *tmp, *tmp_next;
	nbc_bucket_node **meta_prec, *meta_tmp, *current_meta_node;
	unsigned int counter;

	current_meta_node = to_free_pointers;
	meta_prec = (nbc_bucket_node**)&(to_free_pointers);

	while(current_meta_node != NULL)
	{

		if(	current_meta_node->timestamp < timestamp )
		{
			counter = current_meta_node->counter;
			prec = (nbc_bucket_node**)&(current_meta_node->payload);
			tmp = *prec;
			tmp = get_unmarked(tmp);

			while(counter-- != 0)
			{
				tmp_next = tmp->next;
				if(!is_marked(tmp_next, DEL) && !is_marked(tmp_next, INV))
					error("Found a valid node during prune B "
							"%p "
							"%p "
							"%llu "
							"%f "
							"%u "
							"%u "
							"%f "
							"\n",
							tmp,
							tmp->next,
							(unsigned long long) tmp->next & (3ULL),
							tmp->timestamp,
							timestamp,
							counter,
							current_meta_node->counter);

				if(tmp->timestamp < timestamp)
				{
					current_meta_node->counter--;
					mm_free(tmp);
					committed++;
				}
				tmp =  get_unmarked(tmp_next);
			}

			if(current_meta_node->counter == 0)
			{
				//*prec = tmp = current_meta_node;
				meta_tmp = current_meta_node;
				*meta_prec = current_meta_node->next;
				current_meta_node = current_meta_node->next;
				mm_free(meta_tmp);
				//to_free_pointers = current_meta_node;
			}
			else
			{
				//prec = & ( (*prec)->next );
				meta_prec = (nbc_bucket_node**) & ( current_meta_node->next );
				current_meta_node = current_meta_node->next;

			}
		}
		else
		{
			meta_prec = (nbc_bucket_node**)  & ( current_meta_node->next );
			current_meta_node = current_meta_node->next;
		}
	}
//	printf("removed %u/%u nodes\n", committed, to_remove_nodes_count);
	to_remove_nodes_count -= committed;
	return (double)committed;
}

#pragma GCC diagnostic pop
