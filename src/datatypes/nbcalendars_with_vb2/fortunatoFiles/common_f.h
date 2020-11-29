#ifndef COMMON_F_H
#define COMMON_F_H

#include <unistd.h>
#include <stdlib.h>

#define MAX_ATTEMPTS 10

#define XABORT_CODE_RET 0xf1

#define BLOCK 0x2 // marker that cell is blocked
#define DELETE 0x1 // marker for delete

typedef struct __nodeBool_t {
  node_t* node;
  bool unextracted;
} nodeBool_t; 

unsigned long long int rdtsc(){
  unsigned int lo,hi;
  __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
  return ((unsigned long long int)hi << 32) | lo;
}

double clockPerUSec = 0; // computed only one time
void estClockPerUs(){
  unsigned long long exec_time, tot_time, seconds = 1;
  
  exec_time = rdtsc();
  sleep(seconds);
  tot_time = rdtsc() - exec_time;
  clockPerUSec = tot_time/1000000.0/seconds;
}

void expBackoffTime(unsigned int* testSleep, unsigned int* maxSleep){
  int limitBackoff = 16;
  unsigned long long int timeOps = (int) clockPerUSec/5;
  unsigned long long int waitTime = 0;
  unsigned long long int  start = 0;
  unsigned long long int backoffEnd = 0;
  unsigned long long int end = 0;
  
  if(*testSleep == 1){
    *maxSleep = 2;
  } else {
    if(*testSleep < limitBackoff){
      *maxSleep = *maxSleep*1.8;
    }
  }
  // Default waitTime = 2*timeOps*((rand() % *maxSleep));
  waitTime = timeOps*(1 + (lrand48() % *maxSleep));
  // Default senza divisione del waitTime
  waitTime = waitTime/4;
  start = rdtsc();
  end = start + waitTime;
  while(backoffEnd < end){
    backoffEnd = rdtsc();
  }
  *testSleep += 1;
}

#endif