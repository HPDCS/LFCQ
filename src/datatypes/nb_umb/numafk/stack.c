#include "stack.h"

#include <stdio.h>

void _init_gc_trieber() {
    gc_aid[GC_T_STACK_NODE] = gc_add_allocator(sizeof(trieber));

}
void init_trieber(t_stack *queue, unsigned int numa_node) {
    queue->top = NULL;
    return;
}

void t_push(t_stack *queue, void* payload, unsigned int numa_node) {
    trieber* f;
    trieber *t = gc_alloc_node(ptst, gc_aid[GC_T_STACK_NODE], numa_node);
    t->payload = payload;

    do {
        f = queue->top;
        t->next = f;
        if (__sync_bool_compare_and_swap(&(queue->top), f, t))
            return;
    } while(true);
}

bool t_pop(t_stack *queue, void* *result) {
    trieber *f, *f_next; 
    do {
        f = queue->top;
        if (f == NULL) {
            *result = NULL;
            return false;
        }
        f_next = f->next;
        if (__sync_bool_compare_and_swap(&(queue->top), f, f_next)) 
        {  
            *result = f->payload;
            gc_free(ptst, f, gc_aid[GC_T_STACK_NODE]);
            return true; 
        }
    } while(true);
}
