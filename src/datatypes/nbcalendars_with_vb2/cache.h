#ifndef __H_CACHE
#define __H_CACHE


#define ENABLE_CACHE 1

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


__thread bucket_t* 		       *__cache_bckt = NULL;
__thread node_t*   		       *__cache_node = NULL;
__thread long 	   		       *__cache_hash = NULL;
__thread unsigned int  	     *__cache_indx = NULL;
__thread unsigned long long  *__cache_hits = NULL;
__thread unsigned long long  *__cache_load = NULL;
__thread void*               __cache_tblt = NULL;
__thread int                 __cache_init = 1;


static inline void update_cache(bucket_t *bckt){
  #if ENABLE_CACHE == 1
 	__cache_bckt[bckt->index % INSERTION_CACHE_LEN]  = bckt;
 	__cache_hash[bckt->index % INSERTION_CACHE_LEN]  = bckt->hash;
 	__cache_indx[bckt->index % INSERTION_CACHE_LEN]  = bckt->index;
  #endif
}

static inline void init_cache(){
  #if ENABLE_CACHE == 1
    if(__cache_init){
      __cache_bckt =  malloc(sizeof(bucket_t*           )*INSERTION_CACHE_LEN);
      __cache_node =  malloc(sizeof(node_t*             )*INSERTION_CACHE_LEN);
      __cache_hash =  malloc(sizeof(long                )*INSERTION_CACHE_LEN);
      __cache_indx =  malloc(sizeof(unsigned int        )*INSERTION_CACHE_LEN);
      __cache_hits =  malloc(sizeof(unsigned long long  )*INSERTION_CACHE_LEN);
      __cache_load =  malloc(sizeof(unsigned long long  )*INSERTION_CACHE_LEN);
      __cache_init = 0;
    }
  #endif
}




#endif
