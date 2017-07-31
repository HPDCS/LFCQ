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
# graphs.sh
#
#  Created on: Nov 20, 2015
#      Author: Romolo Marotta
#

#25 50 100 200 400 800 1600 4000 8000 16000 32000 64000

eps_dir="./img"

if [ ! -d $eps_dir ]; then
	mkdir $eps_dir
fi

for i in 25 400 4000 32000 40000 80000;  do
 gnuplot -e "queue='${i}';out='${eps_dir}'" realtime.gnuplot
done
