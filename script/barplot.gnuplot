reset 

set term postscript eps enhanced color font "Helvetica" 16
set key bmargin center  horizontal Left
set title 'Queue Size = '.queue
set grid lc rgb '#888888'
set xlabel '#Threads'
set ylabel 'Throughput (Ops/ms)' offset 2

set style data histogram
set style histogram cluster gap 4
set style fill pattern

set yrange[0:*]

start=2
end=13

set title 'Queue Size = '.queue

outfile=out.'/'.start.'-HReal-'.queue.'.eps'
set output outfile

plot for [col=start:end] file using col:xticlabels(1) t columnheader lc rgb '#000000'
system(sprintf("epstopdf %s", outfile)) 
