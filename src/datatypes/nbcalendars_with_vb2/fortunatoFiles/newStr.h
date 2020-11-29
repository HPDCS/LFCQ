#ifndef NEWSTR_H
#define NEWSTR_H

#include "./array.h"

bool newStr_enqueue(void* payload, pkey_t timestamp){
	bool enqueued = false;
	unsigned int vb_idx = computeIndex(timestamp);
	bucket_t* bckt = getBucket(vb_idx);
	unsigned int idxWrite = FETCH_AND_AND(&bckt->array->indexWrite, 1);
	
	if(idxWrite >= bckt->array->length) 
		BOOL_CAS(&bckt->array->validContent, true, false);
	
	while(enqueued){
		if(bckt->array->validContent){
			enqueued = insertInArray(bckt, idxWrite, payload, timestamp);
		}else{
			stateMachine(bckt, SM_ENQUEUE);
			enqueued = insertInList(bckt, payload, timestamp);
		}
	}

	return enqueued;
}

node_t* newStr_dequeue(){
	bool unextracted = true;
	node_t* nodeToRet = NULL;
	unsigned int current_vb = -1;
	bucket_t* bckt = NULL;

	do{
		current_vb =  getIndexOfCurrentBucket();
		bckt = getBucket(current_vb);
		if(bckt == NULL)
			return NULL;
		
		if(bckt->array->validContent)
			BOOL_CAS(&bckt->array->validContent, true, false);

		stateMachine(bckt, SM_DEQUEUE);
		if(bckt->array->unlinked){
			unextracted = dequeueInArray(bckt, nodeToRet);
		}else{
			unextracted = dequeueInList(bckt);
		}
	}while(unextracted);

	return nodeToRet;
}

#endif