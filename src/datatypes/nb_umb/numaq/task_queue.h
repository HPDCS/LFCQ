#ifndef TASK_QUEUE_H
#define TASK_QUEUE_H

#include "msq.h"
//#include "lcrq.h"

#ifdef MSQ_H
typedef queue_t task_queue;
#elif defined(LCRQ_H)
typedef LCRQ task_queue;
#endif

#define _init_gc_tq() __init_gc_tq()
inline static void __init_gc_tq() {
#ifdef MSQ_H
    return _init_gc_msq();
#elif defined(LCRQ_H)
    return _init_gc_lcrq();
#endif
}

#define init_tq(queue_ptr, numa_node) _init_tq((task_queue*) queue_ptr, (unsigned int) numa_node)
inline static void _init_tq(task_queue *queue, unsigned int numa_node) {
#ifdef MSQ_H
    return init_msq(queue, numa_node);
#elif defined(LCRQ_H)
    return init_lcrq(queue, numa_node);
#endif
}

#define tq_enqueue(queue_ptr, payload, numa_node) _tq_enqueue((task_queue*) queue_ptr, (void*) payload, (unsigned int) numa_node)
inline static void _tq_enqueue(task_queue *queue, void* payload, unsigned int numa_node) {
#ifdef MSQ_H
    return msq_enqueue(queue, payload, numa_node);
#elif defined(LCRQ_H)
    return lcrq_enqueue(queue, payload, numa_node);
#endif
}

#define tq_dequeue(queue_ptr, result) _tq_dequeue((task_queue*) queue_ptr, (void**) result)
inline static bool _tq_dequeue(task_queue *queue, void* *result){
#ifdef MSQ_H
    return msq_dequeue(queue, result);
#elif defined(LCRQ_H)
    return lcrq_dequeue(queue, result);
#endif
}


#endif /* !TASK_QUEUE_H */
