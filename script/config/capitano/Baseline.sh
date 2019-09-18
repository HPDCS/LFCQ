###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################

data_types="NBCQ"
threads="32 28 24 20 16 12 8 4 1"
iterations="1 2" # 3 4 5" # 6 7 8 9 10"
distributions="E" # U T N C"
queue_sizes="2560 25600" #256000 2560000"
usage_factor="0.33333333333333333333333"
results="Results/NBCQ-Baseline"


#############################
# elem_per_bucket per datastructure
#############################

elem_per_bucket_NBCQ="3 6 12 24 48 96" 

#############################
# compile parameters
#############################




