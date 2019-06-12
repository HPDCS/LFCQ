/*
 * listNode.h
 *
 *      Author: Elad Gidron
 */

#ifndef LISTNODE_H_
#define LISTNODE_H_

#include <stdint.h>

#include "atomicMarkedReference.h"
#include "cb_types.h"

typedef struct listNode_t {
	cb_key_t key;
	intptr_t value;
	int topLevel;
	markable_ref next[];
} *ListNode;

ListNode makeSentinelNode(cb_key_t key);
ListNode makeNormalNode(cb_key_t key, int height, intptr_t value);
void freeListNode(ListNode node);

#endif /* LISTNODE_H_ */
