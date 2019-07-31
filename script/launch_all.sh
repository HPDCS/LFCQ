#!/bin/bash

source $1


MAX_RETRY=5
OPS=0
PRUNE=0
MODE=T

count=0

results=$results/dat
mkdir -p $results

./estimate_runtime.sh $1

for i in $iterations; do
	for DIST in $distributions; do
		for s in $queue_sizes; do
			SIZE=`echo "$s/0.4" | bc`
			for u in $usage_factor; do
				for t in $threads; do
					for p in $data_types; do
						for e in `eval echo '$'elem_per_bucket_$p`; do
							if [ "$p" == "NBCQ" ] ; then 
							    e=`echo "$t * 2" | bc`
							fi
							if [ "$p" == "VBPQ" ]; then 
							    e=`echo "$t * 2" | bc`
								e=$(($e<48?$e:48))
							fi
							cmd_line="../$version/$p-$cmd $t 1 $DIST 0.3 $SIZE $DIST 0.5 $OPS $DIST 0 0 $u $e 0 $MODE $TIME"
							file="$version-$cmd-$p-$t-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-$e-0-$MODE-$TIME-$i"
							file=`echo "$file" | tr '.' '_'`.dat
							file=$results/$file
							touch $file

							echo "$file"
							
							N=0 
							while [[ $(grep -c "THROUGHPUT" $file) -eq 0 ]]
							do
								#/usr/bin/time -f R:%e,U:%U,S:%S $cmd_line &> $file
								{ timeout $((TIME * 2)) /usr/bin/time -f R:%e,U:%U,S:%S $cmd_line; } &> $file

								if test $N -ge $MAX_RETRY ; then echo break; break; fi
								N=$(( N+1 ))
							done  
						done
					done
				done
			done
		done
	done
done
