source $1


count=0

for i in $iterations; do
	for DIST in $distributions; do
		for s in $queue_sizes; do
			SIZE=`echo "$s/0.4" | bc`
			for u in $usage_factor; do
				for p in $data_types; do
					for e in `eval echo '$'elem_per_bucket_$p`; do
						for t in $threads; do
							cmd_line="../$version/$cmd-$p $t 1 $DIST 0.3 $SIZE $DIST  0.5 $OPS $DIST 0 0 $u $e 0 $MODE $TIME $i"
							count=$(($count + $TIME))
						done
					done
				done
			done
		done
	done
done

echo $count executions in `echo "$count/3600/24" | bc`g/`echo "$count/3600" | bc`h/`echo "$count/60" | bc`m

