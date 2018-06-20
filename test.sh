DIST=$5
OPS=4000000
SIZE=64000
PRUNE=500
TIME=10
MODE=T

if [ "$6" = "G" ]
then
    PRE="gdb --args"
elif [ "$6" = "P" ]
then
    PRE="perf record"
    POST="perf report"
else
    PRE=""
fi

echo $PRE ./Debug/NBCQ $1 $2 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $4 $3 $PRUNE 0 $MODE $TIME
time $PRE ./Debug/NBCQ $1 $2 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $4 $3 $PRUNE 0 $MODE $TIME
$POST
