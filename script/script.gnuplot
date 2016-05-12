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

set style line 1 pt 7 ps 0.8

set key bmargin center  horizontal Left

set style line 2 lt 2 lw 3
set size 1,1.4
#set offset 0.5,0.5,0.5,0.5

set xrange[0:17]

#set title filename

set xlabel '#Threads'
set grid  lc rgb "#888888"

set ylabel 'CPU Time(s)'
#set grid y

set output filename.'_1.eps'
#6,10,8,4
plot \
'./res/50000-1_000000-32000-E-0_000000-1000000-E-0_500000-32000-E-1_000000-0-F.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 8 lw 3 pt 4 ps 1 title "NBCQ Exponential", \
'./res/50000-1_000000-32000-t-0_000000-1000000-t-0_500000-32000-t-1_000000-0-F.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 7 lw 3 pt 4 ps 1 title "NBCQ NegTriangular", \
'./res/50000-1_000000-32000-T-0_000000-1000000-T-0_500000-32000-T-1_000000-0-F.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 2 lw 3 pt 4 ps 1 title "NBCQ Triangular", \
filename.'-F.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 1 lw 3 pt 4 ps 1 title "NBCQ Uniform", \
'./res/50000-1_000000-32000-E-0_000000-1000000-E-0_500000-32000-E-1_000000-0-C.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 8 lw 3 pt 6 ps 1 title "SLCQ Exponential", \
'./res/50000-1_000000-32000-t-0_000000-1000000-t-0_500000-32000-t-1_000000-0-C.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 7 lw 3 pt 6 ps 1 title "SLCQ NegTriangular", \
'./res/50000-1_000000-32000-T-0_000000-1000000-T-0_500000-32000-T-1_000000-0-C.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 2 lw 3 pt 6 ps 1 title "SLCQ Triangular", \
filename.'-C.dat'\
	using 1:4:5	w yerrorbars 	linecolor rgb "#000000" lt 1 lw 1.5 	 title "", '' \
	using 1:4	w linespoints 	linecolor rgb "#000000" lt 1 lw 3 pt 6 ps 1 title "SLCQ Uniform",\
	9+(1.5*x)**2 w l lc rgb "#880000",\
	(25+4.5*x) w l lc rgb "#880000",\
	(30*x-100) w l lc rgb "#880000"

#set yrange [:3000000]
#
#set ylabel '#OPS/Time'
#set output filename.'_2.eps'
#set key bottom left
#plot \
#filename.'-F.dat'\
#	using 1:6:7	w yerrorbars 	linecolor rgb "#000099" lt 1 lw 1.5 	 title "", '' \
#	using 1:6	w linespoints 	linecolor rgb "#000099" lt 2 lw 1.5 pt 6 title "NBCQueue", \
#filename.'-L.dat'\
#	using 1:6:7	w yerrorbars 	linecolor rgb "#009900" lt 1 lw 1.5 	 title "", '' \
#	using 1:6	w linespoints 	linecolor rgb "#009900" lt 2 lw 1.5 pt 6 title "LList", \
#filename.'-C.dat'\
#	using 1:6:7	w yerrorbars 	linecolor rgb "#990000" lt 1 lw 1.5 	 title "", '' \
#	using 1:6	w linespoints 	linecolor rgb "#990000" lt 2 lw 1.5 pt 6 title "CalQueue"
#	
#set yrange [:500000]
#	
#set ylabel '#OPS/CPU Time'
#set output filename.'_3.eps'
#set key bottom left
#plot \
#filename.'-F.dat'\
#	using 1:8:9	w yerrorbars 	linecolor rgb "#000099" lt 1 lw 1.5 	 title "", '' \
#	using 1:8	w linespoints 	linecolor rgb "#000099" lt 2 lw 1.5 pt 6 title "NBCQueue", \
#filename.'-L.dat'\
#	using 1:8:9	w yerrorbars 	linecolor rgb "#009900" lt 1 lw 1.5 	 title "", '' \
#	using 1:8	w linespoints 	linecolor rgb "#009900" lt 2 lw 1.5 pt 6 title "LList", \
#filename.'-C.dat'\
#	using 1:8:9	w yerrorbars 	linecolor rgb "#990000" lt 1 lw 1.5 	 title "", '' \
#	using 1:8	w linespoints 	linecolor rgb "#990000" lt 2 lw 1.5 pt 6 title "CalQueue"
