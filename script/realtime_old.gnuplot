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
#set key bmargin center  horizontal Left
set key top left
set size 0.6,0.7
set offset 2,2,10,10
set title 'Queue Size = '.queue
set xlabel '#Threads'
set grid  lc rgb "#888888"
set ylabel 'CPU Time (s)' offset 2
set xtics 0,4,32
	
set yrange [0:14]
set offset 2,2,1,1
set ylabel 'Wall-Clock Time (s)'
set output out.'/Real-'.queue.'.eps'

plot \
'./dat/5000-1_000000-'.queue.'-E-0_000000-1000000-E-0_500000-0-E-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 title "Non-Blocking", \
'./dat/5000-1_000000-'.queue.'-N-0_000000-1000000-N-0_500000-0-N-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-T-0_000000-1000000-T-0_500000-0-T-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-U-0_000000-1000000-U-0_500000-0-U-1_000000-0-F.dat' using 1:2 w points 	linecolor rgb "#000000" pt 4 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-E-0_000000-1000000-E-0_500000-0-E-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 title "Spin-Locked", \
'./dat/5000-1_000000-'.queue.'-N-0_000000-1000000-N-0_500000-0-N-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-T-0_000000-1000000-T-0_500000-0-T-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 notitle, \
'./dat/5000-1_000000-'.queue.'-U-0_000000-1000000-U-0_500000-0-U-1_000000-0-C.dat' using 1:2 w points 	linecolor rgb "#000000" pt 8 ps 1.5 notitle,\
'./dat/5000-1_000000-'.queue.'-E-0_000000-1000000-E-0_500000-0-E-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "_"  lw 1 title "Exponential", \
'./dat/5000-1_000000-'.queue.'-N-0_000000-1000000-N-0_500000-0-N-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "-"  lw 1 title "NegTriangular", \
'./dat/5000-1_000000-'.queue.'-T-0_000000-1000000-T-0_500000-0-T-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "."  lw 1 title "Triangular", \
'./dat/5000-1_000000-'.queue.'-U-0_000000-1000000-U-0_500000-0-U-1_000000-0-F.dat' using 1:2 w lines 	linecolor rgb "#000000" dt 1    lw 1 title "Uniform", \
'./dat/5000-1_000000-'.queue.'-E-0_000000-1000000-E-0_500000-0-E-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "_"  lw 1 notitle ,\
'./dat/5000-1_000000-'.queue.'-N-0_000000-1000000-N-0_500000-0-N-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "-"  lw 1 notitle ,\
'./dat/5000-1_000000-'.queue.'-T-0_000000-1000000-T-0_500000-0-T-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt "."  lw 1 notitle ,\
'./dat/5000-1_000000-'.queue.'-U-0_000000-1000000-U-0_500000-0-U-1_000000-0-C.dat' using 1:2 w lines 	linecolor rgb "#000000" dt 1    lw 1 notitle 
