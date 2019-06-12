source $1


MAX_RETRY=5
OPS=0
PRUNE=0
MODE=T

count=0

input=$results/processed
output=${results}/eps
output2=${results}/pdf

echo $count executions in `echo "$count/3600/24" | bc`g/`echo "$count/3600" | bc`h/`echo "$count/60" | bc`m

mkdir -p $output
mkdir -p $output2

for DIST in $distributions; do
	for s in $queue_sizes; do
		SIZE=`echo "$s/0.4" | bc`
		for u in $usage_factor; do
			file="$version-$cmd-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-0-$MODE-$TIME.dat"
			echo $file
			out=$input/$file
			#gnuplot -e "out='$output'" -e "file='$out'" -e "queue='$s-$e'" realtime.gnuplot
			for p in $data_types; do
				file=$input/"$version-$cmd-$p-1-$DIST-0.3-$SIZE-$DIST-0.5-$OPS-$DIST-0-0-$u-0-$MODE-$TIME.dat"
				gnuplot -e "out='$output'" -e "file='$out'" -e "queue='$s'" -e "start='$p'" -e "input='$file'"   surface.gnuplot
			done
		done
	done
done

cp $output/*.pdf $output2 
rm $output/*.pdf  