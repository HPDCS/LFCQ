###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################

#data_types="NBCQ LIND MARO CBCQ"
data_types="NUMAFK" #"NBCQ UNBCQ NUMAQ NUMAFK"
#threads="48 42 36 30 24 18 12 6 3 1"
threads="48 42 36 30 24 18 12 6 3 1"
iterations="1 2" # 3 4 5" # 6 7 8 9 10"
distributions="E" #E" # U T N C"
queue_sizes="25600" # 256000 2560000"
elem_per_bucket="24" # 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
usage_factor="0.33333333333333333333333"
results="Results/NUMAFK-NOWS-Malloc-POW6"


#############################
# elem_per_bucket per datastructure
#############################

elem_per_bucket_NUMAFK="24" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"

#############################
# compile parameters
#############################




