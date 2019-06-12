###############################
# script parameters
##############################
cmd=test
version=Release
TIME=5

##############################
# cmd parameters
##############################

data_types="NBCQ LIND MARO V2CQ V3CQ"
threads="1 2 4 8"
iterations="1" # 2" # 3 4 5" # 6 7 8 9 10"
distributions="U" #E" # U T N C"
queue_sizes="256 2560" #25600 256000 2560000"
elem_per_bucket="3" # 6 12 24 48" # 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
usage_factor="0.33333333333333333333333"
results="nuovo"

#############################
# compile parameters
#############################




