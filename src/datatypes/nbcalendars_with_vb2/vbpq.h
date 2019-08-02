#ifndef VBPQ_H_
#define VBPQ_H_

#define EXTRACTION_VIRTUAL_BUCKET_LEN 8

#include "set_table.h"

typedef struct vbpq vbpq;
struct vbpq
{
	table_t * volatile hashtable;
	//char pad[24];
};


#endif