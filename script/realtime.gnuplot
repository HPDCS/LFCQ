# ***************************************************************************
# 
# This file is part of NBQueue, a lock-free O(1) priority queue.
# 
#  Copyright (C) 2015, Romolo Marotta
# 
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
# 
#  This is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
# 
# ***************************************************************************/
# 
# script.gnuplot
#
#  Created on: Nov 20, 2015
#      Author: Romolo Marotta
#
###########################################	 	 
reset

set term postscript eps enhanced color font "Helvetica" 16
set key bmargin center  horizontal Left
#set key top left
set size 0.6,0.7
set offset 2,2,10,10
set title 'Queue Size = '.queue
set xlabel '#Threads'
set grid  lc rgb "#888888"
set ylabel 'Throughput (Ops/ms)' offset 2
set xtics 0,4,50

set style line  2 dt  1 lc rgb "#000000"
set style line  3 dt  2 lc rgb "#000000"
set style line  4 dt  3 lc rgb "#000000"
set style line  5 dt  4 lc rgb "#000000"
set style line  6 dt  5 lc rgb "#000000"
set style line  7 dt  6 lc rgb "#000000"
set style line  8 dt  7 lc rgb "#000000"
set style line  9 dt  8 lc rgb "#000000"
set style line 10 dt  9 lc rgb "#000000"
set style line 11 dt 10 lc rgb "#000000"
set style line 12 dt 11 lc rgb "#000000"

	
set yrange [2000:16000]
set xrange [1:8]
set offset 2,2,1,1

maxitems=56

start=2
end=4
outfile=out.'/Real-'.queue.'.eps'

set output outfile


plot for [col=start:end] file using 1:col with lines ls col t columnheader


#plot file using 1:12 with lines ls 2 t columnheader(12),\
file using 1:13 with lines ls 3 t columnheader(13),\
file using 1:14 with lines ls 4 t columnheader(14),\
file using 1:6 with lines ls 6 t columnheader(6)

system(sprintf("epstopdf %s", outfile)) 

#start=2
#outfile=out.'/Real-'.queue.start.'.eps'
#
#set output outfile
#
#
#plot for [col=start:maxitems:5] file using 1:col with lines ls col t columnheader
#system(sprintf("epstopdf %s", outfile)) 
#
#start=3
#outfile=out.'/Real-'.queue.start.'.eps'
#
#set output outfile
#
#
#plot for [col=start:maxitems:5] file using 1:col with lines ls col t columnheader
#system(sprintf("epstopdf %s", outfile)) 
#
#start=4
#outfile=out.'/Real-'.queue.start.'.eps'
#
#set output outfile
#
#
#plot for [col=start:maxitems:5] file using 1:col with lines ls col t columnheader
#system(sprintf("epstopdf %s", outfile)) 
#
#start=5
#outfile=out.'/Real-'.queue.start.'.eps'
#
#set output outfile
#
#plot for [col=start:maxitems:5] file using 1:col with lines ls col t columnheader
#
#system(sprintf("epstopdf %s", outfile)) 
#
#start=6
#outfile=out.'/Real-'.queue.start.'.eps'
#
#set output outfile
#
#plot for [col=start:maxitems:5] file using 1:col with lines ls (col/5) t columnheader
#
#system(sprintf("epstopdf %s", outfile)) 



#plot \
'./dat/5000-1_000000-'.queue.'-E-0_300000-25000000-E-0_500000-0-E-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 title "Non-Blocking", \
'./dat/5000-1_000000-'.queue.'-N-0_300000-25000000-N-0_500000-0-N-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-T-0_300000-25000000-T-0_500000-0-T-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-U-0_300000-25000000-U-0_500000-0-U-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-E-0_300000-25000000-E-0_500000-0-E-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 title "Spin-Locked", \
'./dat/5000-1_000000-'.queue.'-N-0_300000-25000000-N-0_500000-0-N-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-T-0_300000-25000000-T-0_500000-0-T-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-U-0_300000-25000000-U-0_500000-0-U-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 notitle,\
'./dat/5000-1_000000-'.queue.'-E-0_300000-25000000-E-0_500000-0-E-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "_"  lw 1 title "Exponential", \
'./dat/5000-1_000000-'.queue.'-N-0_300000-25000000-N-0_500000-0-N-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "-"  lw 1 title "NegTriangular", \
'./dat/5000-1_000000-'.queue.'-T-0_300000-25000000-T-0_500000-0-T-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "."  lw 1 title "Triangular", \
'./dat/5000-1_000000-'.queue.'-U-0_300000-25000000-U-0_500000-0-U-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt 1    lw 1 title "Uniform", \
'./dat/5000-1_000000-'.queue.'-E-0_300000-25000000-E-0_500000-0-E-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "_"  lw 1 notitle ,\
'./dat/5000-1_000000-'.queue.'-N-0_300000-25000000-N-0_500000-0-N-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "-"  lw 1 notitle ,\
'./dat/5000-1_000000-'.queue.'-T-0_300000-25000000-T-0_500000-0-T-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "."  lw 1 notitle ,\
'./dat/5000-1_000000-'.queue.'-U-0_300000-25000000-U-0_500000-0-U-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt 1    lw 1 notitle 
