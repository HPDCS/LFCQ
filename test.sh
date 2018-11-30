#1 Datastructure
#2 threads
#3 epb
#4 butilization
#5 distribution

DIST=$5
OPS=4000000
SIZE=64000
PRUNE=500
TIME=20
MODE=T

if [ "$6" = "G" ]
then
    PRE1="gdb --args"
elif [ "$6" = "PR" ]
then
    PRE1="perf record"
    POST="perf report"
elif [ "$6" = "PS" ]
then
    PRE1="perf stat -e branch-instructions,branch-misses,cpu-cycles,stalled-cycles-backend,stalled-cycles-frontend,L1-dcache-load-misses,L1-dcache-loads,L1-dcache-prefetch-misses,L1-dcache-prefetches,"
    PRE2="LLC-loads,LLC-load-misses,LLC-stores,L1-icache-load-misses,L1-icache-loads,L1-icache-prefetches,dTLB-load-misses,dTLB-loads,iTLB-load-misses,iTLB-loads,node-load-misses,node-loads"
    PRE1="perf_4.15 stat -e L1-dcache-load-misses," #,L1-dcache-loads,"
    PRE2="LLC-loads,LLC-load-misses,LLC-stores"
else
    PRE=""
fi

echo $PRE ./Debug/test-$1 $2 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $4 $3 0 $MODE $TIME
time $PRE1$PRE2 ./Debug/test-$1 $2 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $4 $3 0 $MODE $TIME
$POST
