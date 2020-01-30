#1 Datastructure
#2 threads
#3 epb
#4 butilization
#5 distribution

DIST=$5
OPS=4000000
SIZE=64000
PRUNE=500
TIME=10
MODE=T

PROB_DEQUEUE_1=0.3
PROB_DEQUEUE_2=0.5


cmd=resize-unit-test
cmd=test
#version=Release
#version=Debug
version=GProf

if [ "$6" = "G" ]
then
    PRE1="gdb --args"
elif [ "$6" = "PR" ]
then
    PRE1="perf record -g"
    POST="perf report"
elif [ "$6" = "PS" ]
then
    PRE1="perf stat -e branch-instructions,branch-misses,cpu-cycles,stalled-cycles-backend,stalled-cycles-frontend,L1-dcache-load-misses,L1-dcache-loads,L1-dcache-prefetch-misses,L1-dcache-prefetches,"
    PRE2="LLC-loads,LLC-load-misses,LLC-stores,L1-icache-load-misses,L1-icache-loads,L1-icache-prefetches,dTLB-load-misses,dTLB-loads,iTLB-load-misses,iTLB-loads,node-load-misses,node-loads"
  #  PRE1="perf stat -e L1-dcache-load-misses," #,L1-dcache-loads,"
    PRE2="LLC-loads,LLC-load-misses,LLC-stores"
	PRE1="perf stat -e L1-dcache-load-misses,L1-dcache-loads,L1-dcache-stores,"
	#PRE2="LLC-loads,LLC-load-misses,
#PRE2="rtm_retired.aborted,rtm_retired.aborted_misc1,rtm_retired.aborted_misc3,rtm_retired.aborted_misc5,rtm_retired.start"
else	
    PRE=""
fi

lscpu | grep NUMA | grep CPU > tmp.numa.conf
lscpu | grep  On-line > tmp.cpu.list

list=`python get_cores.py | cut -d',' -f1-$2`
#list="0,28,4,32,8,36,12,40,10,38,6,34,2,30,16,44,20,48,24,52,26,54,22,50,18,46,14,42,1,29,5,33,9,37,13,41,11,39,7,35,3,31,17,45,21,49,25,53,27,55,23,51,19,47,15,43"
list=`echo $list | cut -d',' -f1-$2`
echo $list

rm tmp.numa.conf tmp.cpu.list

cmd_line="taskset -c $list $PRE1$PRE2 ./$version/$1-$cmd $2 1 $DIST ${PROB_DEQUEUE_1} $SIZE $DIST  ${PROB_DEQUEUE_2} $OPS $DIST 0 0 $4 $3 0 $MODE $TIME"

echo ${cmd_line}
time ${cmd_line}
$POST
