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

//#include <sys/types.h>
#include <float.h>
#include <pthread.h>
#include <math.h>
//#include <assert.h>

//#include "atomic.h"
#include "nb_calqueue.h"
#include "../utils/util.h"
//#include "core.h"


#define LOG_DEQUEUE 1
#define LOG_ENQUEUE 1

#define BOOL_CAS_ALE(addr, old, new)  CAS_x86(\
										UNION_CAST(addr, volatile unsigned long long *),\
										UNION_CAST(old,  unsigned long long),\
										UNION_CAST(new,  unsigned long long)\
									  )
									  	

#define BOOL_CAS_GCC(addr, old, new)  __sync_bool_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )


#define BOOL_CAS BOOL_CAS_GCC 


#define VAL (0ULL)
#define DEL (1ULL)
#define MOV (2ULL)
#define INV (3ULL)

#define MASK_PTR (-4LL)
#define MASK_MRK (3ULL)

#define REMOVE_DEL		 0
#define REMOVE_DEL_INV	 1

#define MAX_INT 4294967295
#define MASK_ABA 4294967295ULL

#define is_marked(...) macro_dispatcher(is_marked, __VA_ARGS__)(__VA_ARGS__)
#define is_marked2(w,r) is_marked_2(w,r)
#define is_marked1(w)   is_marked_1(w)


__thread nbc_bucket_node *to_free_nodes = NULL;
__thread nbc_bucket_node *to_free_nodes_old = NULL;

__thread nbc_bucket_node *to_free_tables_old = NULL;
__thread nbc_bucket_node *to_free_tables_new = NULL;

__thread unsigned long long mark;
__thread unsigned int to_remove_nodes_count = 0;
__thread unsigned int prune_count = 0;

//__thread unsigned long long cantor_p1 = ((TID)*((TID)+1)/2);
//__thread unsigned long long cantor_p2 = (TID); 

static unsigned int * volatile prune_array;
static unsigned int threads;

static nbc_bucket_node *g_tail;


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
 * This function computes the index of the destination bucket in the hashtable
 *
 * @author Romolo Marotta
 *
 * @param timestamp the value to be hashed
 * @param bucket_width the depth of a bucket
 *
 * @return the linear index of a given timestamp
 */
static inline unsigned long long hash(double timestamp, double bucket_width)
{
	double tmp1;
//	double tmp2;
	unsigned long long ret = 0;
	bool end = false;

	double res_d = (timestamp / bucket_width);
	unsigned long long res =  (unsigned long long) res_d;


	if(res_d > 4294967295)
		error("Probable Overflow when computing the index: "
				"TS=%e,"
				"BW:%e, "
				"TS/BW:%e, "
				"2^32:%e\n",
				timestamp, bucket_width, res_d,  pow(2, 32));


//	tmp1 = res * bucket_width;
//	tmp2 = tmp1 + bucket_width;
//
//	if(LESS(timestamp, tmp1))
//		return --res;
//	if(GEQ(timestamp, tmp2))
//		return ++res;
//	return res;

	res -= (res & (-(res < 3ULL))) +  (3ULL & (-(res >= 3ULL))); 

	while( !end )
	{
		tmp1 = res*bucket_width;

		ret = ((res) & (-(D_EQUAL(tmp1, timestamp)))) +  ((res-1) & (-(GREATER(tmp1, timestamp))));
		end = D_EQUAL(tmp1, timestamp) || GREATER(tmp1, timestamp);

		assertf(res+1 < res, "Overflow when computing the index\n%s", "");
		res++;
	}

	return ret;
}

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
	return UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_PTR), void *);
	//return (void*) (((unsigned long long) pointer) & MASK_PTR);
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
	return UNION_CAST((UNION_CAST(pointer, unsigned long long)|(mark)), void *);
	//return (void*) (((unsigned long long) get_unmarked((pointer))) | (mark));
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
static inline bool is_marked_2(void *pointer, unsigned long long mask)
{
	return ( (UNION_CAST(pointer, unsigned long long) & MASK_MRK) == mask );
	//return (bool) ((((unsigned long long) pointer) & MASK_MRK) == mask);
}

/**
 *  This function checks if a reference is generally marked
 *
 *  @author Romolo Marotta
 *
 *  @param pointer
 *
 *  @return true if the reference is generally marked, else false
 */
static inline bool is_marked_1(void *pointer)
{
	return (UNION_CAST(pointer, unsigned long long) & MASK_MRK);
	//return (bool) ((((unsigned long long) pointer) & MASK_MRK));
}

static inline bool is_marked_for_search(void *pointer, unsigned int research_flag)
{
	unsigned long long mask_value = (UNION_CAST(pointer, unsigned long long) & MASK_MRK);
	
	return 
		(/*research_flag == REMOVE_DEL &&*/ mask_value == DEL) 
		|| (research_flag == REMOVE_DEL_INV && (mask_value == INV) );
}

/**
 *  This function get the mark
 *
 *  @author Romolo Marotta
 *  @param pointer
 *  @return the mark
 */
static inline unsigned long long get_mark(void *pointer)
{
	return UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_MRK), unsigned long long);
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
* @param tid The local Id of the Light Process
* @return A value to be used as a unique mark for the message within the LP
*/
static inline unsigned long long generate_ABA_mark() {
	//cantor_p1 += cantor_p2++;
	//return (MASK_ABA & cantor_p1);
		
	unsigned long long sum = ((TID) + (++mark));
	return ( MASK_ABA & (( (sum)*(sum +1ULL)/2ULL) + mark ));
	//return (MASK_ABA & ((unsigned long long)( ((sum)*(sum +1)/2) + mark )));
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
	nbc_bucket_node* res = malloc(sizeof(nbc_bucket_node));
	//nbc_bucket_node* res = (nbc_bucket_node*) malloc(sizeof(nbc_bucket_node));

	if (is_marked(res) || res == NULL)
	{
		error("%lu - Not aligned Node or No memory\n", pthread_self());
		abort();
	}

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
 * @param number of node to connect to the to_be_free queue
 * @param timestamp   the timestamp of the last disconnected node
 *
 *
 */
static inline void connect_to_be_freed_node_list(nbc_bucket_node *start, unsigned int counter)
{
	nbc_bucket_node* new_node;
	start = get_unmarked(start);

	new_node = node_malloc(start, start->timestamp, counter);
	new_node->next = to_free_nodes;

	to_free_nodes = new_node;
	to_remove_nodes_count += counter;
}

static inline void connect_to_be_freed_table_list(table *h)
{
	nbc_bucket_node *tmp = node_malloc(h, INFTY, 0);
	tmp->next = to_free_tables_new;
	to_free_tables_new = tmp;
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
 * @param left_node_next a pointer to a pointer used to return the next field of the left node 
 *
 */

static void search(nbc_bucket_node *head, double timestamp, unsigned int tie_breaker,
						nbc_bucket_node **left_node, nbc_bucket_node **right_node, int flag)
{
	nbc_bucket_node *left, *right, *left_next, *tmp, *tmp_next, *tail;
	unsigned int counter;
	double tmp_timestamp;
	tail = g_tail;

	do
	{
		/// Fetch the head and its next node
		tmp = head;
		tmp_next = tmp->next;
		counter = 0;
		do
		{
			// Check if the node is marked
			bool marked = is_marked_for_search(tmp_next, flag);

			// Find the first unmarked node that is <= timestamp
			if (!marked)
			{
				left = tmp;
				left_next = tmp_next;
				counter = 0;
			}
			// Take a count of the marked node between left node and current node (tmp)
			counter+=marked;

			// Retrieve timestamp and next field from the current node (tmp)
			tmp = get_unmarked(tmp_next);
			tmp_timestamp = tmp->timestamp;
			
			tmp_next = tmp->next;

			// Exit if tmp is a tail or its timestamp is > of the searched key
		} while (	tmp != tail &&
					(
						is_marked_for_search(tmp_next, flag) ||
						LESS(tmp_timestamp, timestamp)	||  
						(
							D_EQUAL(tmp_timestamp, timestamp) &&
							(
								tie_breaker == 0 || 
								(tie_breaker != 0 && tmp->counter <= tie_breaker)
							)
						)
					)
				);

		// Set right node and copy the mark of left node
		right = get_marked(tmp,get_mark(left_next));
		//right =  ((unsigned long long)tmp | (MASK_MRK &  (unsigned long long) left_next));

		//left node and right node have to be adjacent. If not try with CAS
		if (left_next != right)
		{
			// if CAS succeeds connect the removed nodes to to_be_freed_list
			if (!BOOL_CAS(
						&(left->next),
						left_next,
						right
						)
					)
				continue;
			connect_to_be_freed_node_list(left_next, counter);
		}
		
		// at this point they are adjacent
		*left_node = left;
		*right_node = right;
		
		return;
		
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
	unsigned long long oldIndex;
	unsigned long long newIndex = ( unsigned long long ) hash(node->timestamp, h->bucket_width);
	unsigned long long newCur =  newIndex << 32;
	newCur |= generate_ABA_mark();

	// Retry until it is ensured that the current is moved back to index

	oldCur = h->current;
	oldIndex = oldCur >> 32;
	if(
		newIndex > oldIndex
		|| is_marked(node->next)
		|| BOOL_CAS(
					&(h->current),
					oldCur,
					newCur
					)
				) return;

	do
	{

		oldCur = h->current;
		oldIndex = oldCur >> 32;
	}
	while (
			newIndex <	oldIndex 
			&& is_marked(node->next, VAL)
			&& !BOOL_CAS(
						&(h->current),
						oldCur,
						newCur
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
static bool insert_std(table* hashtable, nbc_bucket_node** new_node, int flag)
{
	nbc_bucket_node *left_node, *right_node, *bucket, *new_node_pointer;
	unsigned int index;

	unsigned int new_node_counter 	;
	double 		 new_node_timestamp ;

	new_node_pointer 	= (*new_node);
	new_node_counter 	= new_node_pointer->counter;
	new_node_timestamp 	= new_node_pointer->timestamp;

	index = hash(new_node_timestamp, hashtable->bucket_width) % hashtable->size;

	// node to be added in the hashtable
	bucket = hashtable->array + index;

	search(bucket, new_node_timestamp, new_node_counter, &left_node, &right_node, flag);

	if(!is_marked(right_node, MOV))
	{
		switch(flag)
		{
		case REMOVE_DEL_INV:

			new_node_pointer->next = right_node;
			// set tie_breaker
			new_node_pointer->counter = 1 + ( -D_EQUAL(new_node_timestamp, left_node->timestamp ) & left_node->counter );

			if (BOOL_CAS
					(
						&(left_node->next),
						right_node,
						new_node_pointer
					)
			)
			{
				#if LOG_ENQUEUE == 1
				LOG("ENQUEUE: %f %u - %u %u\n", new_node_pointer->timestamp, new_node_pointer->counter,	hash(new_node_timestamp, hashtable->bucket_width), index );
				#endif
				return true;
			}

			// reset tie breaker for the new search
			new_node_pointer->counter = 0;
			break;

		case REMOVE_DEL:

			// mark the to-be.inserted node as INV
			new_node_pointer->next = get_marked(right_node, INV);
			// node already exists
			if(D_EQUAL(new_node_timestamp, left_node->timestamp ) && left_node->counter == new_node_counter)
			{
				free(new_node_pointer);
				*new_node = left_node;
				return true;
			}

			// first replica to be inserted
			
			// copy left node mark
			
			new_node_pointer = get_marked(new_node_pointer,get_mark(right_node));
			//new_node_pointer =  (nbc_bucket_node*) (
			//					((unsigned long long) new_node_pointer) | 
			//					(MASK_MRK &  (unsigned long long) right_node)
			//					);

	//		if(right_node->timestamp == INFTY)
	//		printf("MOVING %f %u between left %f %u right  INFTY %u\n",
	//				new_node_timestamp, new_node_counter,
	//				left_node->timestamp, left_node->counter,
	//				 right_node->counter);
	//		else
	//			printf("MOVING %f %u between left %f %u right  %f %u\n",
	//					new_node_timestamp, new_node_counter,
	//					left_node->timestamp, left_node->counter,
	//					right_node->timestamp, right_node->counter);

			if (BOOL_CAS(
						&(left_node->next),
						right_node,
						new_node_pointer
					))
				//printf("insert_std: evt type %u\n", ((msg_t*)(new_node[0]->payload))->type);//da_cancellare
				return true;
			break;
		}
	}
	return false;

}

static void set_new_table(table* h, unsigned int threshold )
{
	nbc_bucket_node *tail = g_tail;
	int signed_counter = atomic_read( &h->counter );
	unsigned int counter = (unsigned int) ( (-(signed_counter >= 0)) & signed_counter);
	unsigned int i = 0;
	unsigned int size = h->size;
	unsigned int thp2 = threshold *2;
	unsigned int new_size = 0;
	table *new_h;
	nbc_bucket_node *array;

	if 		(size >= thp2 && counter > 2*size)
		new_size = 2*size;
	else if (size > thp2 && counter < 0.5*size)
		new_size =  0.5*size;
	else if	(size == 1 && counter > thp2)
		new_size = thp2;
	else if (size == thp2 && counter < threshold)
		new_size = 1;
	
	
	if(new_size != 0 && new_size <= MAXIMUM_SIZE)
	{

		new_h = malloc(sizeof(table));
		//new_h = (table*) malloc(sizeof(table));
		if(new_h == NULL)
			error("No enough memory to new table structure\n");

		new_h->bucket_width = -1.0;
		new_h->size 		= new_size;
		new_h->new_table 	= NULL;
		new_h->counter.count= 0;
		new_h->current 		= ((unsigned long long)-1) << 32;


		
		array =  calloc(new_size, sizeof(nbc_bucket_node));
		//array =  (nbc_bucket_node*) malloc(sizeof(nbc_bucket_node) * new_size);
		if(array == NULL)
		{
			free(new_h);
			error("No enough memory to allocate new table array %u\n", new_size);
		}

		//memset(array, 0, sizeof(nbc_bucket_node) * new_size);

		for (i = 0; i < new_size; i++)
			array[i].next = tail;


		new_h->array 	= array;

		if(!BOOL_CAS(
				&(h->new_table),
				NULL,
				new_h))
		{
			free(new_h->array);
			free(new_h);
		}

		else
			LOG("CHANGE SIZE from %u to %u, items %u OLD_TABLE:%p NEW_TABLE:%p\n", size, new_size, counter, h, new_h);
	}
}

static void block_table(table* h)
{
	unsigned int i=0;
	unsigned int size = h->size;
	nbc_bucket_node *array = h->array;
	nbc_bucket_node *bucket, *bucket_next;
	
	for(i = 0; i < size; i++)
	{
		bucket = array + ((i + (TID)) % size);
		
		do
		{
			bucket_next = bucket->next;
		}
		while( !is_marked(bucket_next, MOV) &&
				!BOOL_CAS(
					&(bucket->next),
					bucket_next,
					get_marked(bucket_next, MOV)
				) 
		);
	}
}

static double compute_mean_separation_time(table* h,
		unsigned int new_size, unsigned int threashold)
{
	nbc_bucket_node *tail = g_tail;

	unsigned int i = 0, index;

	table *new_h = h->new_table;
	nbc_bucket_node *array = h->array;
	double old_bw = h->bucket_width;
	unsigned int size = h->size;
	double new_bw = new_h->bucket_width;
	
	unsigned int sample_size;
	double average = 0.0;
	double newaverage = 0.0;
	double tmp_timestamp;
	int counter = 0;
	
	double min_next_round = INFTY;
	double lower_bound, upper_bound;
    
    nbc_bucket_node *tmp, *tmp_next;
	
	index = (unsigned int)(h->current >> 32);
	
	if(new_bw >= 0)
		return new_bw;
	
	if(new_size <= threashold*2)
		return 1.0;
	
	sample_size = (new_size <= 5) ? (new_size/2) : (5 + (int)((double)new_size / 10));
	sample_size = (sample_size > SAMPLE_SIZE) ? SAMPLE_SIZE : sample_size;
    
	double sample_array[SAMPLE_SIZE+1]; //<--DA SISTEMARE STANDARD C90
    
    //read nodes until the total samples is reached or until someone else do it
	while(counter != sample_size && new_h->bucket_width == -1.0)
	{   
		//se non salto gli anni vuoti, posso semplificare di molto il codice 
		for (i = 0; i < size; i++) // Se togliamo la gestione degli anni vuoti, si può togliere
		{
			tmp = array + (index + i) % size; 	//get the head of the bucket
			tmp = get_unmarked(tmp->next);		//pointer to first node
			
			lower_bound = (index + i) * old_bw;
			upper_bound = (index + i + 1) * old_bw;
		
			while( tmp != tail && counter < sample_size )
			{
				tmp_timestamp = tmp->timestamp;
				tmp_next = tmp->next;
				//I will consider ognly valid nodes (VAL or MOV) In realtà se becco nodi MOV posso uscire!
				if(!is_marked(tmp_next, DEL) && !is_marked(tmp_next, INV))
				{
					if( //belong to the current bucket
						LESS(tmp_timestamp, upper_bound) &&	GEQ(tmp_timestamp, lower_bound) &&
						!D_EQUAL(tmp_timestamp, sample_array[counter])
					)
					{
						sample_array[++counter] = tmp_timestamp;
					}
					else if(GEQ(tmp_timestamp, upper_bound) && LESS(tmp_timestamp, min_next_round))
					{
							min_next_round = tmp_timestamp;
							break;
					}
				}
				tmp = get_unmarked(tmp_next);
			}
		}
		//if the calendar has no more elements I will go out
		if(min_next_round == INFTY)
			break;
		//otherwise I will restart from the next good bucket
		index = hash(min_next_round, old_bw);
		min_next_round = INFTY;
	}


	
	if( counter < sample_size)
		sample_size = counter;
    
	for(i = 2; i<=sample_size;i++)
		average += sample_array[i] - sample_array[i - 1];
    
		// Get the average
	average = average / (double)(sample_size - 1);
    
	int j=0;
	// Recalculate ignoring large separations
	for (i = 2; i <= sample_size; i++) {
		if ((sample_array[i] - sample_array[i - 1]) < (average * 2.0))
		{
			newaverage += (sample_array[i] - sample_array[i - 1]);
			j++;
		}
	}
    
	// Compute new width
	newaverage = (newaverage / j) * 3.0;	/* this is the new width */
	if(newaverage <= 0.0) //è possibile?
		newaverage = 1.0;
	//if(newaverage <  pow(10,-4))
	//	newaverage = pow(10,-3);
	if(isnan(newaverage))
		newaverage = 1.0;	
    
	return newaverage;
}

static void migrate_node(nbc_bucket_node *right_node,	table *new_h)
{
	nbc_bucket_node *replica, *right_replica_field, *right_node_next;
	
	//Create a new node inserted in the new table as as INValid
	replica = node_malloc(right_node->payload, right_node->timestamp,  right_node->counter);
	
	//if(replica == tail)
	//	error("A\n");
             
    do
	{ 
		right_replica_field = right_node->replica;
	}        
	while(right_replica_field == NULL && !insert_std(new_h, &replica, REMOVE_DEL));
	
	//if(replica2 == tail)
	//	error("B %p %p %p\n", replica, replica2, tail);
             
	if( right_replica_field == NULL && 
			BOOL_CAS(
				&(right_node->replica),
				NULL,
				replica
				)
		)
	{
		atomic_inc_x86(&(new_h->counter));
    }
             
	right_replica_field = right_node->replica;
             
	//if(right_replica_field == tail)
	//	error("%u - C %p %p %p %p %p\n", tid, replica, replica2, tail, right_replica_field, right_node->replica);
            
	do
	{
		right_node_next = right_replica_field->next;
	}while( 
		is_marked(right_node_next, INV) && 
		!BOOL_CAS(	&(right_replica_field->next),
					right_node_next,
					get_unmarked(right_node_next)
				)
		);

	nbc_flush_current(new_h, right_replica_field);
	
	do{
		//non possiamo sostituirlo con un fetch&or(00) senza do/while?
		right_node_next = right_node->next;
	}while( 
		!is_marked(right_node_next, DEL) && 
		!BOOL_CAS(	&(right_node->next),
					right_node_next,
					get_marked(get_unmarked(right_node_next), DEL)
				) 
		) ;
	
}

static table* read_table(nb_calqueue *queue)
{
	table *h = queue->hashtable		;
#if ENABLE_EXPANSION == 0
	return h;
#endif
	nbc_bucket_node *tail = g_tail	;
	unsigned int i, size = h->size	;

	table 			*new_h 			;
	double 			 new_bw 		;
	double 			 newaverage		;
	nbc_bucket_node *bucket, *array	;
	nbc_bucket_node *right_node, *left_node, *right_node_next;
	
	int counter = atomic_read(&h->counter);
	
	//printf("SIZE H %d\n", h->counter.count);

	if( 
		(counter < 0.5*size || counter > 2*size)
		&& (h->new_table == NULL)
		)
		set_new_table(h, queue->threshold);

	if(h->new_table != NULL)
	{
		//printf("%u - MOVING BUCKETS\n", tid);
		new_h 			= h->new_table;
		array 			= h->array;
		new_bw 			= new_h->bucket_width;

		if(new_bw < 0)
		{
			block_table(h);
			newaverage = compute_mean_separation_time(h, new_h->size, queue->threshold);
			if
			(
			BOOL_CAS(
					UNION_CAST(&new_h->bucket_width, unsigned long long *),
					UNION_CAST(new_bw,unsigned long long),
					UNION_CAST(newaverage, unsigned long long)
				)
			)
//			{
					LOG("COMPUTE BW -  OLD:%.20f NEW:%.20f %u\n", new_bw, newaverage, new_h->size)
				;
//			}
		}

		//First try: try to migrati the nodes, if a marked node is found, continue to the next bucket
		for(i=0; i < size; i++)
		{
			bucket = array + ((i + (TID)) % size);

			do
			{
				//Try to mark the top node
				search(bucket, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
				right_node = get_unmarked(right_node);
				right_node_next = right_node->next;
				
				if(
						!is_marked(right_node_next, MOV) &&
						right_node != tail &&
						!BOOL_CAS(
								&(right_node->next),
								get_unmarked(right_node_next),
								get_marked(right_node_next, MOV)
								)								
				)
					break;

				if(right_node == tail)
					break;
				
				migrate_node(right_node, new_h);
				
			}while(true);
		}
		
		
		//Second try: try to migrati the nodes and continue until each bucket is empty
		for(i=0; i < size; i++)
		{
			bucket = array + ((i + (TID)) % size);

			do
			{
				//Try to mark the top node
				do
				{
					search(bucket, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
					right_node = get_unmarked(right_node);
					right_node_next = right_node->next;
				}
				while(
						!is_marked(right_node_next, MOV) &&
						right_node != tail &&
						!BOOL_CAS(
								&(right_node->next),
								get_unmarked(right_node_next),
								get_marked(right_node_next, MOV)
								)								
				);

				if(right_node == tail)
					break;
				
				migrate_node(right_node, new_h);
				
			}while(true);
		}

		//Try to replace the old table with the new one
		if(BOOL_CAS(
				&(queue->hashtable),
				h,
				new_h
		))
		{
//			printf("%u %f %llu %u %f %llu\n",
//					h->counter.count, h->bucket_width, h->current>>32,
//					new_h->counter.count, new_h->bucket_width , new_h->current>>32);
			connect_to_be_freed_table_list(h);
		}

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

	threads = threshold;
	prune_array = calloc(threshold*threshold, sizeof(unsigned int));
	//prune_array = (unsigned int*) malloc(sizeof(unsigned int)*threshold*threshold);
	
	nb_calqueue* res = calloc(1, sizeof(nb_calqueue));
	//nb_calqueue* res = (nb_calqueue*) malloc(sizeof(nb_calqueue));
	if(res == NULL)
		error("No enough memory to allocate queue\n");
	
	res->threshold = threshold;

	res->hashtable = malloc(sizeof(table));
	//res->hashtable = (table*) malloc(sizeof(table));
	if(res->hashtable == NULL)
	{
		free(res);
		error("No enough memory to allocate queue\n");
	}
	res->hashtable->bucket_width = 1.0;
	res->hashtable->new_table = NULL;
	res->hashtable->size = MINIMUM_SIZE;

	res->hashtable->array = calloc(MINIMUM_SIZE, sizeof(nbc_bucket_node) );
	//res->hashtable->array = (nbc_bucket_node*) malloc(MINIMUM_SIZE * sizeof(nbc_bucket_node) );
	if(res->hashtable->array == NULL)
	{
		free(res->hashtable);
		free(res);
		error("No enough memory to allocate queue\n");
	}

	g_tail = node_malloc(NULL, INFTY, 0);
	g_tail->next = NULL;

	for (i = 0; i < MINIMUM_SIZE; i++)
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

	//printf("ENQ %p %f %u\n", new_node, new_node->timestamp, new_node->counter);
	do
	{
		h  = read_table(queue);
	} while(!insert_std(h, &new_node, REMOVE_DEL_INV));

	nbc_flush_current(h, new_node);
	
	atomic_inc_x86(&(h->counter));
	
	//printf("nbc_enqueue: evt type %u\n", ((msg_t*)(new_node->payload))->type);//da_cancellare
}

static inline bool CAS_for_mark( nbc_bucket_node* right_node, nbc_bucket_node* right_node_next)
{
	return BOOL_CAS(
			&(right_node->next),
			get_unmarked(right_node_next),
			get_marked(right_node_next, DEL)
			);

}

static inline bool CAS_for_increase_cur(table* h, unsigned long long current, unsigned long long newCur)
{
	return BOOL_CAS(
			&(h->current),
			current,
			newCur				);
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
void* nbc_dequeue(nb_calqueue *queue)
{
	nbc_bucket_node *right_node, *min, *right_node_next, *res, *tail, *left_node;
	table * h;
	
	unsigned long long current;
	unsigned long long newCur;

	unsigned long long index;
	unsigned long long newInd;
	
	unsigned int size;
	unsigned int tie;
	nbc_bucket_node *array;
	double right_timestamp;
	double bucket_width;
	//double old = INFTY;


	tail = g_tail;
	res = NULL;
	do
	{
		h = read_table(queue);

		current = h->current;
		size = h->size;
		array = h->array;
		bucket_width = h->bucket_width;

		index = current >> 32;


		min = array + (index % size);

		search(min, -1.0, 0, &left_node, &right_node, REMOVE_DEL_INV);
		
		//printf("nbc_dequeue: -1 evt type %u\n", ((msg_t*)(right_node->payload))->type); //da_cancellare

		if(is_marked(right_node, MOV))
			continue;

		right_timestamp = right_node->timestamp;


		if(right_node == tail && size == 1 )
			return NULL;

		else if( LESS(right_timestamp, (index)*bucket_width) )
		{	
			nbc_flush_current(h, right_node);
//			printf("ER %f %u %f %p %u %u %u %u - %u %u\n",
//					right_timestamp,
//					right_node->counter,
//					(index)*bucket_width,
//					h,
//					is_marked(right_node_next, DEL),
//					is_marked(right_node_next, INV),
//					is_marked(right_node_next, VAL),
//					is_marked(right_node_next, MOV),
//					index,
//					index % size
//					);
//			if(old == right_timestamp)
//				exit(1);
//			old = right_timestamp;
		}
		else if( GREATER(right_timestamp, (index+1)*bucket_width) )
		{

			newCur =  ( index+1 ) << 32;
			newCur |= (current & MASK_ABA);
			
			//LOG("INCREASE CUR - TS_LEFT:%.20f TS_RIGHT:%.20f INDEX: %llu TABLE:%p\n", min->timestamp, right_timestamp, index, h); 
			
			assertf(
				index+1 > MASK_ABA, 
				"\nOVERFLOW INDEX:%llu  BW:%.10f SIZE:%u RIGHT_TS:%.20f TAIL:%p RIGHT_NODE:%p TABLE:%p NUM_ELEM:%u\n",
				index, bucket_width, size, right_timestamp, tail, right_node, h, atomic_read(&h->counter)
			);

			
			//assertf((newCur >> 32) == MASK_ABA, "TANA %llu\n", newCur);

//			CAS_x86(
//					(volatile unsigned long long *)&(h->current),
//					(unsigned long long)			current,
//					(unsigned long long) 			newCur				);
			
			
			if(
				CAS_for_increase_cur(h, current, newCur)
				)
			//LOG("NEW INDEX : %llu\n", (newCur >> 32))
			;
			
			
			
		}
		else
		{ 
			newCur = h->current;
			newInd = (newCur >> 32);

			if( 
				newInd < index  || 
				( newInd == index && newCur != current) 
			   )
				continue;
			

			right_node_next = right_node->next;
			res = right_node->payload;
			tie = right_node->counter;

			if( !is_marked(right_node_next, DEL) &&
//					CAS_x86(
//								(volatile unsigned long long *)&(right_node->next),
//								(unsigned long long) get_unmarked(right_node_next),
//								(unsigned long long) get_marked(right_node_next, DEL)
//								)
					CAS_for_mark(right_node, right_node_next)
				)
			{
				search(min, right_timestamp, tie,  &left_node, &right_node, REMOVE_DEL_INV);
				atomic_dec_x86(&(h->counter));
				
				#if LOG_DEQUEUE == 1
					LOG("DEQUEUE: %f %u - %llu %llu\n", right_timestamp, tie, index, index % size);
				#endif
				//printf("nbc_dequeue: 2 evt type %u\n", ((msg_t*)(res->payload))->type); //da_cancellare
				return res;
			}
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
#if ENABLE_PRUNE == 0
	return 0.0;
#endif
	unsigned int committed = 0;
	nbc_bucket_node **prec, *tmp, *tmp_next;
	nbc_bucket_node **meta_prec, *meta_tmp, *current_meta_node;
	unsigned int counter;
	unsigned int i=0;
	unsigned int flag = 1;

	if(++prune_count < 100)
		return 0.0;
	
	prune_count = 0;

	LOG("PRUNE %s\n", "");

	for(;i<threads;i++)
	{
		prune_array[(TID) + i*threads] = 1;
		flag &= prune_array[(TID)*threads +i];
	}

	if(flag != 1)
		return 0.0;
	
	while(to_free_tables_old != NULL)
	{
		nbc_bucket_node *my_tmp = to_free_tables_old;
		to_free_tables_old = to_free_tables_old->next;
    
		table *h = (table*) my_tmp->payload;
		free(h->array);
		free(h);
		free(my_tmp);
	}
	
	current_meta_node = to_free_nodes_old;
	meta_prec = (nbc_bucket_node**)&(to_free_nodes_old);
    
	while(current_meta_node != NULL)
	{
		
		counter = current_meta_node->counter;
		prec = (nbc_bucket_node**)&(current_meta_node->payload);
		tmp = *prec;
		tmp = get_unmarked(tmp);
       
		while(counter-- != 0)
		{
			tmp_next = tmp->next;
//			if(!is_marked(tmp_next, DEL) && !is_marked(tmp_next, INV))
//				error("Found a %s node during prune "
//						"NODE %p "
//						"NEXT %p "
//						"TAIL %p "
//						"MARK %llu "
//						"TS %f "
//						"PRUNE TS %f "
//						"TIE %u "
//						"TIE META %u "
//						"\n",
//						is_marked(tmp_next, MOV) ? "MOV" : "VAL",
//						tmp,
//						tmp->next,
//						g_tail,
//						(unsigned long long) tmp->next & (3ULL),
//						tmp->timestamp,
//						timestamp,
//						counter,
//						current_meta_node->counter);
       
			free(tmp);
			committed++;

			tmp =  get_unmarked(tmp_next);
		}
       
		meta_tmp = current_meta_node;
		*meta_prec = current_meta_node->next;
		current_meta_node = current_meta_node->next;
		free(meta_tmp);
       
	}
	
	//to_remove_nodes_count -= committed; //<-- si usa?
	
	to_free_tables_old = to_free_tables_new;
	to_free_tables_new = NULL;
	
	to_free_nodes_old = to_free_nodes;
	to_free_nodes = NULL;
	
	for(i=0;i<threads;i++)
		prune_array[(TID)*threads +i] = 0;
    
	return (double)committed;
}

