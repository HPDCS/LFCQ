#!/bin/bash
source $1


MAX_RETRY=1
OPS=0
PRUNE=0
MODE=T
TIMEOUT=$((150))
count=0

results=$results/dat
mkdir -p $results
echo $results
./estimate_runtime.sh $1

cat /proc/cpuinfo | grep -e 'processor' -e 'physical id' | cut -f2 | cut -d' ' -f2 > cpu_socket.tmp

for i in $iterations; do
	for DIST in $distributions; do
		for s in $queue_sizes; do
			SIZE=`echo "$s/0.4" | bc`
			for u in $usage_factor; do
				for t in $threads; do
					for p in $data_types; do
						for e in `eval echo '$'elem_per_bucket_$p`; do
							cmd_line="../$version/$p-$cmd $t 1 $DIST 0.3 $SIZE $DIST 0.5 $OPS $DIST 0 0 $u $e 0 $MODE $TIME"
							file="$version-$cmd-$p-$t-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-$e-0-$MODE-$TIME-$i"
							file=`echo "$file" | tr '.' '_'`.dat
							file=$results/$file
							touch $file

							echo "$file"
							
							N=0 
							while [[ $(grep -c "THROUGHPUT" $file) -eq 0 ]]
							do
								#echo $cmd_line
								{ timeout $TIMEOUT /usr/bin/time -f R:%e,U:%U,S:%S $cmd_line; } &> $file
								echo $t `cat $file | grep THROUGH`
								if test $N -ge $MAX_RETRY ; then echo $cmd_line break; break; fi
								N=$(( N+1 ))
							done  
						done
					done
				done
			done
		done
	done
done
