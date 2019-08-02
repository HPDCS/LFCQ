#ifndef TRIEBER_H
#define TRIEBER_H

#include <stdbool.h>
#include "../gc/gc.h"

extern int gc_aid[];
extern int gc_hid[];
extern __thread ptst_t *ptst;

#define GC_T_STACK_NODE  0

typedef struct _trieber{
    void* payload;
    struct _trieber *next;
} trieber;

typedef struct _t_stack{
    struct _trieber *top;
} t_stack;

void _init_gc_trieber();
void init_trieber(t_stack *queue, unsigned int numa_node);
void t_push(t_stack *queue, void* payload, unsigned int numa_node);
bool t_pop(t_stack *queue, void* *result);

#endif /* !TRIEBER_H */