#ifndef VBPQ_H_
#define VBPQ_H_

#include "set_table.h"

typedef struct vbpq vbpq;
struct vbpq
{
	table_t * volatile hashtable;
	//char pad[24];
};


#endif