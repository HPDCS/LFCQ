source $1


MAX_RETRY=5
OPS=0
PRUNE=0
MODE=T

count=0

input=$results/dat
output=$results/processed

mkdir -p $output

for DIST in $distributions; do
	for s in $queue_sizes; do
		SIZE=`echo "$s/0.4" | bc`
		for u in $usage_factor; do
			out=$output/"$version-$cmd-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-0-$MODE-$TIME.dat"
			line="THREADS"
			for e in $elem_per_bucket; do
				for p in $data_types; do
					line="$line $p-$e"
				done
			done
			echo $line > $out
			for t in $threads; do
				line="$t        "
				for e in $elem_per_bucket; do
					for p in $data_types; do
						ofile="$version-$cmd-$p-$t-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-$e-0-$MODE-$TIME"
						echo "$ofile"
						count=0
						sum=0
						ssum=0
						for i in $iterations; do
							file="$ofile-$i"
							file=`echo "$file" | tr '.' '_'`.dat
							file=$input/$file
							val=`cat $file |head -n1 | cut -d',' -f20 | cut -d':' -f2` 
							sum=`echo $sum+$val | bc`
							count=$(($count+1))
						done
						avg=`echo $sum/$count | bc`
						for i in $iterations; do
							file="$ofile-$i"
							file=`echo "$file" | tr '.' '_'`.dat
							file=$input/$file
							val=`cat $file |head -n1 | cut -d',' -f20 | cut -d':' -f2` 
							ssum=`echo "($val-$avg)*($val-$avg)" | bc`
						done
						#std=`echo "sqrt($ssum/($count-1))" | bc`
						line="$line $avg"
					done
				done
				echo -e $line >> $out
			done
		done
	done
done