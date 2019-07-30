/*
 * listNode.h
 *
 *      Author: Elad Gidron
 */

#ifndef LISTNODE_H_
#define LISTNODE_H_

#include <stdint.h>

#include "../../key_type.h"

#include "atomicMarkedReference.h"

typedef struct listNode_t {
	pkey_t key;
	intptr_t value;
	int topLevel;
	markable_ref next[];
} *ListNode;

ListNode makeSentinelNode(pkey_t key);
ListNode makeNormalNode(pkey_t key, int height, intptr_t value);
void freeListNode(ListNode node);

#endif /* LISTNODE_H_ */
