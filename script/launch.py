#!/usr/bin/env python

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
# launch.py
#
#  Created on: Nov 20, 2015
#      Author: Romolo Marotta
#


from subprocess import Popen
from time import sleep
import sys
import termios
import atexit

print sys.argv


conf = open(sys.argv[1])

for line in conf.readlines():
	line = line.strip()
	if line[0] == "#":
		pass
	line = line.split("#")[0].split("=")
	if line[0] == "core":
		core=int(line[1])
	elif line[0] == "all_threads":
		all_threads = [int(x) for x in line[1].split(",")]
	elif line[0] == "ops":
		ops = int(line[1])
	elif line[0] == "ops1":
		ops1 = int(line[1])
	elif line[0] == "ops2":
		ops2 = int(line[1])
	elif line[0] == "iterations":
		overall_iterations = int(line[1])
	elif line[0] == "start_from":
		start_from = int(line[1])
	elif line[0] == "overall_iterations":
		iterations = int(line[1])
	elif line[0] == "verbose":
		verbose = int(line[1])
	elif line[0] == "log":
		log = int(line[1])
	elif line[0] == "prune_period":
		prune_period = int(line[1])
	elif line[0] == "init_size":
		init_size = int(line[1])
	elif line[0] == "collaborative":
		collaborative = int(line[1])
	elif line[0] == "safety":
		safety = int(line[1])
	elif line[0] == "empty_queue":
		empty_queue = int(line[1])
	elif line[0] == "prob_roll":
		prob_roll = float(line[1])
	elif line[0] == "prune_tresh":
		prune_tresh = float(line[1])
	elif line[0] == "width":
		width = float(line[1])
	elif line[0] == "prob_dequeue":
		prob_dequeue = float(line[1])
	elif line[0] == "prob_dequeue1":
		prob_dequeue1 = float(line[1])
	elif line[0] == "prob_dequeue2":
		prob_dequeue2 = float(line[1])
	elif line[0] == "look_pool":
		look_pool = [float(x) for x in line[1].split(",")]
	elif line[0] == "data_type":
		data_type = line[1].split(",")
	elif line[0] == "distribution":
		distribution = line[1]#.split(",")
	elif line[0] == "distribution1":
		distribution1 = line[1]#.split(",")
	elif line[0] == "distribution2":
		distribution2 = line[1]#.split(",")
	elif line[0] == "enable_log":
		enable_log = line[1] == "1"

cmd1="time"
cmd2="-f"
cmd3="R:%e,U:%U,S:%S"
cmd4="../Debug/NBCQ"
tmp_dir="tmp"


def enable_echo(fd, enabled):
       (iflag, oflag, cflag, lflag, ispeed, ospeed, cc) \
                     = termios.tcgetattr(fd)
       if enabled:
               lflag |= termios.ECHO
       else:
               lflag &= ~termios.ECHO
       new_attr = [iflag, oflag, cflag, lflag, ispeed, ospeed, cc]
       termios.tcsetattr(fd, termios.TCSANOW, new_attr)

def get_next_set(threads, available):
	res = 0
	for i in threads:
		if min(i,core) <= available:
			res = max(res, i)
	return res

def print_log2(first=False):
	if not enable_log:
		return
	if not first:
		for i in range(3+buf_line):
			sys.stdout.write( "\033[1A\033[K")
	print "Number of test: "+str(num_test-count_test)+"/"+str(num_test)+" Usage Core: "+str(core-core_avail)+"/"+str(core)+"\r"
	print "-------------------------------------------------------------------------\r"
	for d in distribution:
		for j in look_pool:
			for i in all_threads:
				for k in data_type:
					print " ".join([k,str(i),str(j),d,str(residual_iter[(d,i,j,k)]),str(instance[(d,i,j,k)])])+" ",
				print ""
	print "-------------------------------------------------------------------------"

def print_log(first=False):
	if not enable_log:
		return
	if not first:
		for i in range(3):
			sys.stdout.write( "\033[1A\033[K")
	print "-------------------------------------------------------------------------\r"
	print "Number of test: "+str(num_test-count_test)+"/"+str(num_test)+" Usage Core: "+str(core-core_avail)+"/"+str(core)+"\r"
	print "-------------------------------------------------------------------------"


if __name__ == "__main__":

	print "#########################################################################"
	print "              TESTS v2.0"
	print "#########################################################################"

	threads = all_threads[:]
	m_core = max(threads)
	if(m_core > core):
		print "-------------------------------------------------------------------------\r"
		print "WARNING more threads ("+str(m_core)+") than core available ("+str(core)+"). Using time-sharing!"
		print "-------------------------------------------------------------------------\r"
	
	buf_line = len(threads)*len(distribution)
	residual_iter = {}
	instance = {}
	
	cmd = [cmd1, cmd2, cmd3, cmd4]
	core_avail = core
	
	test_pool = {}
	file_pool = {}
	run_pool = set([])
	
	verbose		=str(verbose		)
	log			=str(log			)
	prune_period=str(prune_period	)
	ops			=str(ops			)
	prob_dequeue=str(prob_dequeue	)
	ops1			=str(ops1			)
	prob_dequeue1=str(prob_dequeue1	)
	ops2			=str(ops2			)
	prob_dequeue2=str(prob_dequeue2	)
	prune_tresh	=str(prune_tresh	)
	
	count_test = 0
	
	
	
	if enable_log:
		atexit.register(enable_echo, sys.stdin.fileno(), True)
		enable_echo(sys.stdin.fileno(), False)
	
	#for d in distribution:
	#for d in distribution:
	#for d in distribution:
		#distribution1 = d
		#distribution2 = d
	for struct in data_type:
		for t in threads:
			if not test_pool.has_key(t):
				test_pool[t] = []
			for run in xrange(start_from, iterations + start_from):
				test_pool[t] += [ [  struct, str(t), str(overall_iterations), 				# STRUCT, THREADS, OVERALL SIZE
									 distribution, prob_dequeue, ops, 						# DISTRIBUTION, PROB_DEQUEUE, OPS  
									 distribution1, prob_dequeue1, ops1, 					# DISTRIBUTION, PROB_DEQUEUE, OPS  
									 distribution2, prob_dequeue2, ops2, 					# DISTRIBUTION, PROB_DEQUEUE, OPS  
									 prune_period, prune_tresh, 							# PRUNE_PERIOD, PRUNE_TRESHOLD
									 verbose,												# VERBOSE
									 log,													# LOG
									 str(safety),											# SAFETY 
									 str(empty_queue),										# EMPTY_QUEUE
									 str(run)]	]											# RUN
				count_test +=1
				id_string = struct + str(t) + str(overall_iterations) + distribution + prob_dequeue + ops 	  + distribution1 + prob_dequeue1 + ops1    +  distribution2 + prob_dequeue2 + ops2 
				residual_iter[id_string] = iterations
				instance[id_string] = 0

	num_test = count_test
	
	
	print_log(True)
	while count_test > 0:
		t = get_next_set(threads, core_avail)
		if t > 0 and t < 128:
			pool = test_pool[t]
			if len(pool) == 0:
				threads.remove(t)
				del test_pool[t]
			else:
				cmdline = pool[0]
				test_pool[t] = pool[1:]
				filename = tmp_dir+"/"+"-".join(cmdline).replace(".", "_")
				f = open(filename, "w")
				p = Popen(cmd+cmdline[:-1], stdout=f, stderr=f)
				id_string = cmdline[0] + cmdline[1] + cmdline[2] + cmdline[3] + cmdline[4] + cmdline[5] + cmdline[6] + cmdline[7] + cmdline[8] + cmdline[9] + cmdline[10] + cmdline[11] 
				residual_iter[id_string]-=1
				instance[id_string]+=1
				core_avail -= min(t,core)
				run_pool.add( (p,t,id_string, cmdline[-1]) )
				file_pool[p]=f
				count_test -= 1
				print filename + "\t" +  "#TEST: "+str(num_test-count_test)+"/"+str(num_test)+" #Core: "+str(core-core_avail)+"/"+str(core)+"\r"
			continue
		print_log()
		sleep(1)
		to_remove = set([])
		for	p,t,id_string,i in run_pool:
			
			if p.poll() != None:
				to_remove.add((p,t,id_string,i))
				file_pool[p].close()
				del file_pool[p]
				instance[id_string]-=1
				core_avail += min(t,core)
		run_pool -= to_remove
		
	while core_avail < core:
		to_remove = set([])
		for	(p,t,id_string,i) in run_pool:
			if p.poll() != None:
				to_remove.add((p,t,id_string,i))
				file_pool[p].close()
				del file_pool[p]
				instance[id_string]-=1
				core_avail +=  min(t,core)
		run_pool -= to_remove
		sleep(1)
		print_log()
