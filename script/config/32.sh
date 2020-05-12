###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################
data_types="NBCQ DWCQ" # ACRCQ"
#threads="32 31 30 29 28 27 26 25 24 23 22 21 20 19 18 17 16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1" #"48 42 36 30 24 18 12 6 3 1"
threads="32 30 28 26 24 22 20 18 16 14 12 10 8 6 4 2"
#threads="32 28 24 20 16 12 8 4"
iterations="1 2 3" # 3 4 5" # 6 7 8 9 10"
distributions="E" #E" # U T N C"
queue_sizes="190000 390000 1900000 3900000" #7900000"
usage_factor="0.33333333333333333333333"
results="INT-results"

#############################
# elem_per_bucket per datastructure
#############################

elem_per_bucket_NBCQ="64" #"6 12 24 48 96 192 384" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_DWCQ="800"
#elem_per_bucket_ACRCQ="8"

#############################
# compile parameters
#############################




