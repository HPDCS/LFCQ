source $1


MAX_RETRY=5
OPS=0
PRUNE=0
MODE=T

count=0

input=$results/dat
output=$results/processed

mkdir -p $output


job(){
SIZE=$1
for u in $usage_factor; do
	out=$output/"$version-$cmd-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-0-$MODE-$TIME.dat"
	line="THREADS"
	for p in $data_types; do
		out2=$output/"$version-$cmd-$p-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-0-$MODE-$TIME.dat"
		echo -e -n "" > $out2 
		for e in `eval echo '$'elem_per_bucket_$p`; do
			line="$line $p-$e"
		done
	done
	#echo $line > $out
	prev=0

	for t in $threads; do
		a=$(($prev+2))
		echo $prev $t $a
		line="$t        "
		for p in $data_types; do
			old=1
			echo -e -n "" > tmp1$SIZE
			echo -e -n "" > tmp2$SIZE
			out2=$output/"$version-$cmd-$p-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-0-$MODE-$TIME.dat"
			
			for e in `eval echo '$'elem_per_bucket_$p`; do
				ofile="$version-$cmd-$p-$t-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-$e-0-$MODE-$TIME"
				#echo "$ofile"
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

				echo -e $prev $old $avg >> tmp1$SIZE
				echo -e $prev $e $avg >> tmp1$SIZE
				echo -e $a $old $avg >> tmp2$SIZE
				echo -e $a $e $avg >> tmp2$SIZE
				old=$e
			done 
			cat tmp1$SIZE >> $out2
			echo -e "" >> $out2 
			cat tmp2$SIZE >> $out2
			echo -e "" >> $out2
		done
		#echo -e $line >> $out
		prev=$a
	done
done
rm tmp1$SIZE
rm tmp2$SIZE
}

for DIST in $distributions; do
	for s in $queue_sizes; do
		SIZE=`echo "$s/0.4" | bc`
		job $SIZE &
	done
done

wait