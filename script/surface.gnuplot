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
#set size 0.6,0.7
set title start.' Queue Size = '.queue

set xtics ("1" 1, "3" 3, "6" 5,"12" 7,"18" 9, "24" 11,"30" 13,"36" 15,"42" 17 ,"48" 19)
set ytics (3, 6, 12, 24, 48, 96, 192, 384, 768, 1536, 3072)
unset ztics

outfile=out.'/'.start.'-SReal-'.queue.'.eps'

set output outfile

#set zrange [0:6000]
set cbrange [0:5500]

maxitems=45
start=35

set pm3d
set view 0, 0

set logscale y 2

#set pal negative gray

#set palette model RGB defined(\
0    "#ffffff", \
3000 "#ffffff", \
3000 "#000000", \
6000 "#000000"  )




#set palette model RGB defined(\
0    "#ffffff", \
2000 "#ffffff", \
2000 "#e0e0e0", \
2500 "#e0e0ee", \
2500 "#cccccc", \
3000 "#cccccc", \
3000 "#aaaaaa", \
3500 "#aaaaaa", \
3500 "#888888", \
4000 "#888888", \
4000 "#666666", \
4500 "#666666", \
4500 "#444444", \
5000 "#444444", \
5000 "#222222", \
5500 "#222222", \
5500 "#000000", \
6000 "#000000"  )



#set palette model RGB defined(\
0    "white", \
1000 "white", \
1000 "light-gray", \
2000 "light-gray", \
2000 "gray", \
3000 "gray", \
3000 "dark-gray", \
4000 "dark-gray", \
4000 "black", \
5000 "black")

#set palette model RGB defined (\
0 "green", \
500 "green", \
500 "yellow", \
1000 "yellow", \
1000 "red", \
1500 "red", \
1500 "blue", \
2000 "blue", \
2000 "white", \
2500 "white", \
2500 "purple", \
3000 "purple", \
3000 "orange", \
3500 "orange", \
3500 "black", \
4000 "black", \
4000 "pink", \
4500 "pink", \
4500 "brown", \
5000 "brown", \
5000 "light-blue", \
5500 "light-blue", \
5500 "gray", \
6000 "gray")


splot input  with pm3d notitle
system(sprintf("epstopdf %s", outfile)) 
