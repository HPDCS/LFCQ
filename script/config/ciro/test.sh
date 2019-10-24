###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################

data_types="ACRCQ NUMAP NUMAPSKT NUMAQBLK NUMA2Q" #"NBCQ NUMAQBLK" #"NBCQ UNBCQ NUMAP NUMAQ NUMAQBLK" #"NBCQ UNBCQ NUMAPNOP NUMAFK" #"NBCQ UNBCQ NUMAP NUMAQ" #"NBCQ UNBCQ NUMAP NUMAPNOP NUMAPBL NUMAQ NUMAFK NUMAFKBL"
threads="48 36 24 18 12 6 3 1"
iterations="1 2 3" # 4 5" # 6 7 8 9 10"
distributions="E" #E" # U T N C"
queue_sizes="25600 256000 2560000" # 256000 2560000"
elem_per_bucket="96" # 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
usage_factor="0.33333333333333333333333"
results="Results/NewBaseline"


#############################
# elem_per_bucket per datastructure
#############################

elem_per_bucket_ACRCQ="3" # no need to change this
elem_per_bucket_NUMAP="24 48 96"
elem_per_bucket_NUMAPSKT="24 48 96"
elem_per_bucket_NUMAQBLK="48"
elem_per_bucket_NUMA2Q='48'

elem_per_bucket_NBCQ="48 96" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_UNBCQ="48 96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_NUMAPNOP="96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_NUMAPBL="96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_NUMAQ="96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_NUMAFK="96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
elem_per_bucket_NUMAFKBL="96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
#elem_per_bucket_CBCQ="1" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
#elem_per_bucket_LIND="12 24 48 96" #"3 6 12 24 48 96 192 384 768" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
#elem_per_bucket_MARO="3072" #"3 6 12 24 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"


#############################
# compile parameters
#############################

