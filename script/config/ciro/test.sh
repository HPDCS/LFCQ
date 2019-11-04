###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################
# NBCQ UNBCQ
# 

# ACRCQ NBCQ UNBCQ NUMA2Q NUMA2QFK NUMA2QBL NUMAP NUMAPNOP NUMAPBL NUMAQ NUMAFK NUMAFKBL NUMAQBLK NUMAPSKT NUMAPSKTFK NUMAPSKTBL
# ACRCQ NUMAQ NUMAQBLK NUMAFK NUMAFKBL NUMAP NUMAPNOP NUMAPBL NUMAPSKT NUMAPSKTFK NUMAPSKTBL NUMAPNBC NUMAPNBCFK NUMAPNBCNOP NUMAPFNB NUMA2Q NUMA2QFK NUMA2QBL NUMA2QBLK 
# ACRCQH NUMAPLCRQBLK NUMA2QNC + altri con cache on

data_types="ACRCQ NUMAQ NUMAQBLK NUMAP NUMAPSKT NUMAPNBC NUMAPFNB NUMA2Q NUMA2QBLK " #NUMAPBL NUMAPNBCBL NUMAPSKTBL NUMAFKBL NUMA2QBL" #"ACRCQ NUMAFK NUMAFKBL NUMAPNOP NUMAPBL NUMAPSKTFK NUMAPSKTBL NUMAPNBCBL NUMAPNBCFK NUMA2QFK NUMA2QBL" #"ACRCQ NUMA2Q NUMA2QFK NUMA2QBL NUMAP NUMAPNOP NUMAPBL NUMAQ NUMAFK NUMAFKBL NUMAQBLK NUMAPSKT NUMAPSKTFK NUMAPSKTBL"
threads="48 36 24 18 12 6 3 1"
iterations="1 2 3" # 4 5" # 6 7 8 9 10"
distributions="E" #E" # U T N C"
queue_sizes="25600 256000 2560000" # 256000 2560000"
elem_per_bucket="96" # 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
usage_factor="0.33333333333333333333333"
results="Results/FinalFix"


#############################
# elem_per_bucket per datastructure
#############################

#"ACRCQ NBCQ UNBCQ NUMA2Q NUMA2QFK NUMA2QBL NUMAP NUMAPNOP NUMAPBL NUMAQ NUMAFK NUMAFKBL NUMAQBLK NUMAPSKT NUMAPSKTFK NUMAPSKTBL"

elem_per_bucket_ACRCQ="1" # no need to change this
elem_per_bucket_NUMAFK="1"
elem_per_bucket_NUMAFKBL="1"
elem_per_bucket_NUMAPNOP="1"
elem_per_bucket_NUMAPBL="1"
elem_per_bucket_NUMAPSKTFK="1"
elem_per_bucket_NUMAPSKTBL="1"
elem_per_bucket_NUMA2QFK="1"
elem_per_bucket_NUMA2QBL="1"
elem_per_bucket_NUMAPNBCBL="1"
elem_per_bucket_NUMAPNBCFK="1"

elem_per_bucket_NBCQ="24 48 96" #"24 48 96"
elem_per_bucket_UNBCQ="24 48 96"

elem_per_bucket_NUMAQ="24 48 96"
elem_per_bucket_NUMAQBLK="24 48 96"
elem_per_bucket_NUMAP="24 48 96"
elem_per_bucket_NUMAPSKT="24 48 96"
elem_per_bucket_NUMAPNBC="24 48 96"
elem_per_bucket_NUMAPFNB="24 48 96"
elem_per_bucket_NUMA2Q="24 48 96"
elem_per_bucket_NUMA2QBLK="24 48 96"



#############################
# compile parameters
#############################

