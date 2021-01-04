 
 ###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################

#data_types="NBCQ MARO ACRCQ VBPQ"
data_types="VBPQ"
threads="1 5 10 20 30 40"
iterations="1 2 3" #" 4 5" # 6 7 8 9 10"
distributions="E" #U T N C"
queue_sizes="256000 2560000 25600 2560" #256"
elem_per_bucket="3 6 12 24 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
usage_factor="0.33333333333333333333333"
results="giorno040120"


#############################
# elem_per_bucket per datastructure
#############################
elem_per_bucket_VBPQ="40 60 80 100" #" 768 3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
#elem_per_bucket_VBPQ="96 192 384" #" 768 3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_NBCQ="3 96" # 12 24 48 96 192 384 768 1536" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_CBCQ="1" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_SLCQ="1" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_ACRCQ="1" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_LIND="3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_MARO="3072" #"3 6 12 24 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"


#############################
# compile parameters
#############################




