#ifndef __CHANNEL_H
#define __CHANNEL_H

#define OP_NONE			0ULL
#define OP_PENDING		1ULL
#define OP_COMPLETED	2ULL
#define OP_ABORTED		3ULL

#define WAIT_US 		2


typedef struct __channel channel_t;
struct __channel{
	void * payload;						// 8
	pkey_t timestamp;  					//  
	char __pad_1[8-sizeof(pkey_t)];		// 16
	volatile unsigned long long state;			// 24
	void * result_payload;				// 32
	pkey_t result_timestamp;			//  
	char __pad_2[8-sizeof(pkey_t)];		// 40
	unsigned long long op_id;
	char __pad_3[16];
};


channel_t *communication_channels = NULL;
unsigned long long rcv_id = 0ULL;
unsigned long long snd_id = 0ULL;

__thread bool have_channel_id = false;
__thread unsigned long long my_rcv_id = 0ULL;
__thread unsigned long long my_snd_id = 0ULL;
__thread bool am_i_sender = false; 


#define CLIENT 0ULL
#define SERVER 1ULL

__thread unsigned long long thread_state = 0ULL;
__thread unsigned long long op_id;


static inline void acquire_channels_ids(){
	if(have_channel_id) return;
	my_rcv_id = __sync_fetch_and_add(&rcv_id, 1ULL);
	my_snd_id = __sync_fetch_and_add(&snd_id, 1ULL);
	have_channel_id = true;
}

static inline void validate_transaction(){
	if(thread_state == SERVER){
		if(op_id != communication_channels[my_rcv_id].op_id)  		TM_ABORT(0xf2);
		if(OP_PENDING != communication_channels[my_rcv_id].state)  TM_ABORT(0xf2);
		communication_channels[my_rcv_id].state = OP_COMPLETED;
	}
}

static inline int pre_validate_transaction(){
	if(thread_state == SERVER){
		if(op_id != communication_channels[my_rcv_id].op_id)  	  return ABORT;
		if(OP_PENDING != communication_channels[my_rcv_id].state) return ABORT;
	}
	return OK;
}

#endif
