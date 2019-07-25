#include <stdio.h>
#include <stdlib.h>
#include "msq.h"

void _init_gc_queue()
{
    //gc_aid[GC_NODES] = gc_add_allocator(sizeof(node_t));
}

void init_queue(queue_t *queue, unsigned int numa_node)
{
    node_t *node = malloc(sizeof(node_t));//gc_alloc_node(ptst, gc_aid[GC_NODES], numa_node); // Allocate a free node
    node->next = NULL;
    queue->head = queue->tail = node;
    pthread_spin_init(&(queue->h_lock), PTHREAD_PROCESS_PRIVATE);
    pthread_spin_init(&(queue->t_lock), PTHREAD_PROCESS_PRIVATE);
}

void msq_enqueue(queue_t *queue, void *payload, unsigned int numa_node)
{
    node_t *node = malloc(sizeof(node_t)); //gc_alloc_node(ptst, gc_aid[GC_NODES], numa_node);
    node->value = payload;
    node->next = NULL;

    if (pthread_spin_lock(&(queue->t_lock))) {
        printf("DeadLock Abort");
        exit(1);
    }

    queue->tail->next = node;
    queue->tail = node;

    if (pthread_spin_unlock(&(queue->t_lock))) {
        printf("Cannot unlock Abort");
        exit(1);
    }
}

bool msq_dequeue(queue_t *queue, void **result)
{
    node_t *node, *new_head;
    if (pthread_spin_lock(&(queue->h_lock))) {
        printf("DeadLock Abort");
        exit(1);
    }

    node = queue->head;
    new_head = node->next;

    if (new_head == NULL)
    {
        if (pthread_spin_unlock(&(queue->h_lock))) {
            printf("Cannot unlock Abort");
            exit(1);
        }
        *result = NULL;
        return false;
    }

    *result = new_head->value;
    queue->head = new_head;
    
    if (pthread_spin_unlock(&(queue->h_lock))) {
            printf("Cannot unlock Abort");
            exit(1);
    }
    
    //free(node);
    return true;
}