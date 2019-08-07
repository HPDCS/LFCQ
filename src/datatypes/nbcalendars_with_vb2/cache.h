#ifndef __H_CACHE
#define __H_CACHE


#define ENABLE_CACHE 1


/*static unsigned int hash64shift(unsigned int a)
{
  return a;

 a = (a+0x7ed55d16) + (a<<12);
   a = (a^0xc761c23c) ^ (a>>19);
   a = (a+0x165667b1) + (a<<5);
   a = (a+0xd3a2646c) ^ (a<<9);
   a = (a+0xfd7046c5) + (a<<3);
   a = (a^0xb55a4f09) ^ (a>>16);
   return a;

  key = (~key) + (key << 21); // key = (key << 21) - key - 1;
  key = key ^ (key >> 24);
  key = (key + (key << 3)) + (key << 8); // key * 265
  key = key ^ (key >> 14);
  key = (key + (key << 2)) + (key << 4); // key * 21
  key = key ^ (key >> 28);
  key = key + (key << 31);
  return key;
}
*/

#define __CACHE_INSERTION_ALGN_BITS 6 
#define __CACHE_INSERTION_SETS_MASK 1023
#define __CACHE_INSERTION_SETS_LEN (__CACHE_INSERTION_SETS_MASK+1)
#define __CACHE_INSERTION_WAYS 64



#define INSERTION_CACHE_LEN (__CACHE_INSERTION_WAYS * __CACHE_INSERTION_SETS_LEN)


__thread bucket_t* 		       *__cache_bckt = NULL;
__thread node_t*   		       *__cache_node = NULL;
__thread long 	   		       *__cache_hash = NULL;
__thread unsigned int  	     *__cache_indx = NULL;
__thread unsigned long long  *__cache_hits = NULL;
__thread unsigned long long  *__cache_load = NULL;
__thread void*               __cache_tblt = NULL;
__thread int                 __cache_init = 1;


static inline unsigned int get_cache_set_idx(unsigned int index){
  return (index >> __CACHE_INSERTION_ALGN_BITS) & __CACHE_INSERTION_SETS_MASK;
}

#if ENABLE_CACHE == 1
  static inline void update_cache(bucket_t *bckt){
    unsigned int set_idx, index;
    unsigned int base_offset = 0;
    unsigned int min_set = -1;
    unsigned int min_idx = -1;
    int i = 0;

    index = bckt->index;
    set_idx = get_cache_set_idx(index);

    base_offset = (set_idx)*__CACHE_INSERTION_WAYS;

    for(i=0;i<__CACHE_INSERTION_WAYS;i++)
      if(__cache_indx[base_offset + i] == index) 
        break;
    

    if(i<__CACHE_INSERTION_WAYS){
      __cache_bckt[base_offset + i]         = bckt;
      __cache_hash[base_offset + i]         = bckt->hash;
      __cache_indx[base_offset + i]         = bckt->index;
      return;
    }

    for(i=0;i<__CACHE_INSERTION_WAYS;i++){
      if(__cache_bckt[base_offset + i] == NULL) break;
      if(__cache_indx[base_offset + i] < min_set){
        min_idx = i;
        min_set = __cache_indx[base_offset + i];
      } 
    }

    if(i<__CACHE_INSERTION_WAYS){
      __cache_bckt[base_offset + i]         = bckt;
      __cache_hash[base_offset + i]         = bckt->hash;
      __cache_indx[base_offset + i]         = bckt->index;
    }
    else{
      __cache_bckt[base_offset + min_idx]   = bckt;
      __cache_hash[base_offset + min_idx]   = bckt->hash;
      __cache_indx[base_offset + min_idx]   = bckt->index; 
    }
  }

  static inline bucket_t* load_from_cache(unsigned int index){
    bucket_t* left;
    unsigned int set_idx;
    unsigned int base_offset = 0;
    int i = 0;

    set_idx = get_cache_set_idx(index);
    base_offset = (set_idx)*__CACHE_INSERTION_WAYS;

    for(i=0;i<__CACHE_INSERTION_WAYS;i++)
      if(__cache_indx[base_offset + i] == index) break;
    

    if(i == __CACHE_INSERTION_WAYS) return NULL;
   
    __cache_load[base_offset + i]++;
    left = __cache_bckt[base_offset + i];

    if(
      left != NULL && 
      index == __cache_indx[base_offset + i] && 
      left->index == index && 
      left->hash == __cache_hash[base_offset + i] && 
      !is_freezed(left->extractions) && 
      is_marked(left->next, VAL)
    )
    {
      __cache_hits[base_offset + i]++;
      return left;
    }
    return NULL;
  }

  static inline void invalidate_cache(unsigned int index){
    unsigned int set_idx;
    unsigned int base_offset = 0;
    int i = 0;

    set_idx = get_cache_set_idx(index);
    base_offset = (set_idx)*__CACHE_INSERTION_WAYS;

    for(i=0;i<__CACHE_INSERTION_WAYS;i++){
      if(__cache_indx[base_offset + i] == index) {
          __cache_bckt[base_offset + i] = NULL;
          __cache_indx[base_offset + i] = 0;
      }
    }

  }


  static inline void init_cache(){
    if(__cache_init){
      __cache_bckt =  malloc(sizeof(bucket_t*           )*INSERTION_CACHE_LEN);
      __cache_node =  malloc(sizeof(node_t*             )*INSERTION_CACHE_LEN);
      __cache_hash =  malloc(sizeof(long                )*INSERTION_CACHE_LEN);
      __cache_indx =  malloc(sizeof(unsigned int        )*INSERTION_CACHE_LEN);
      __cache_hits =  malloc(sizeof(unsigned long long  )*INSERTION_CACHE_LEN);
      __cache_load =  malloc(sizeof(unsigned long long  )*INSERTION_CACHE_LEN);
      __cache_init = 0;
    }
  }

  static inline void  flush_cache(){
    int i = 0;
    for(i=0;i<INSERTION_CACHE_LEN-1;i++){
      __cache_bckt[i]  = NULL;
      __cache_node[i]  = NULL;
      __cache_hash[i]  = 0;
      __cache_hits[i]   = 0;
      __cache_load[i]  = 0;
      __cache_indx[i] = 0;
    }
  }

  static inline unsigned long long get_loads(){ 
    int h=0;
    unsigned long long res = 0;
    for(h=0; h<INSERTION_CACHE_LEN-1;h++){
         res+=__cache_load[h];
         res+=__cache_hits[h];
    }
    return res;
  }
  
  static inline unsigned long long get_hits(){ 
    int h=0;
    unsigned long long res = 0;
    for(h=0; h<INSERTION_CACHE_LEN-1;h++){
         res+=__cache_hits[h];
    }
    return res;
  }
#else

static inline void update_cache(bucket_t *bckt){}
static inline void init_cache(){}
static inline void flush_cache(){}
static inline unsigned long long get_loads(){return 0;}
static inline unsigned long long get_hits(){return 0;} 

#endif

#endif
