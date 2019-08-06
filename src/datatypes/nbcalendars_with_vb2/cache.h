#ifndef __H_CACHE
#define __H_CACHE


#define ENABLE_CACHE 0

#if ENABLE_CACHE == 1
static unsigned int hash64shift(unsigned int a)
{
return a;
   a = (a+0x7ed55d16) + (a<<12);
   a = (a^0xc761c23c) ^ (a>>19);
   a = (a+0x165667b1) + (a<<5);
   a = (a+0xd3a2646c) ^ (a<<9);
   a = (a+0xfd7046c5) + (a<<3);
   a = (a^0xb55a4f09) ^ (a>>16);
   return a;
/*
  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ (key >> 24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ (key >> 14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ (key >> 28);
  key = key + (key << 31);
  return key;*/
}


#endif

#define INSERTION_CACHE_LEN 65536


__thread bucket_t* 		__cache_bckt[INSERTION_CACHE_LEN];
__thread node_t*   		__cache_node[INSERTION_CACHE_LEN];
__thread long 	   		__cache_hash[INSERTION_CACHE_LEN];
__thread unsigned int  	__cache_index[INSERTION_CACHE_LEN];
__thread unsigned long long __cache_hit[INSERTION_CACHE_LEN];
__thread unsigned long long __cache_load[INSERTION_CACHE_LEN];

static inline void update_cache(bucket_t *bckt){
  #if ENABLE_CACHE == 1
 	__cache_bckt[bckt->index % INSERTION_CACHE_LEN] = bckt;
 	__cache_hash[bckt->index % INSERTION_CACHE_LEN] = bckt->hash;
 	__cache_index[bckt->index % INSERTION_CACHE_LEN] = bckt->index;
  #endif
}

#endif