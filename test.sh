DIST=$5
OPS=40000000
SIZE=640000
PRUNE=150
TIME=7
echo ./Debug/NBCQ $1 $2 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $4 $3 $PRUNE 0 T $TIME
time ./Debug/NBCQ $1 $2 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $4 $3 $PRUNE 0 T $TIME
