###############################
# script parameters
##############################
cmd=test
version=Release
TIME=10

##############################
# cmd parameters
##############################

# ACRCQ NUMAP RNUMAP NUMAPFK NUMAPBL NUMAPSKT RNUMAPSKT NUMAPSKTFK NUMAPSKTBL NUMAQ RNUMAQ NUMAQFK NUMAQBL NUMAQSKT RNUMAQSKT NUMAQSKTFK NUMAQSKTBL NUMA2Q RNUMA2Q NUMA2QFK NUMA2QBL NUMA2QSKT RNUMA2QSKT NUMA2QSKTFK NUMA2QSKTBL

data_types="ACRCQ NUMAP RNUMAP NUMAPFK NUMAPBL NUMAPSKT RNUMAPSKT NUMAPSKTFK NUMAPSKTBL NUMAQ RNUMAQ NUMAQFK NUMAQBL NUMAQSKT RNUMAQSKT NUMAQSKTFK NUMAQSKTBL NUMA2Q RNUMA2Q NUMA2QFK NUMA2QBL NUMA2QSKT RNUMA2QSKT NUMA2QSKTFK NUMA2QSKTBL"
threads="48 36 24 18 12 6 3 1"
iterations="1 2 3" # 4 5" # 6 7 8 9 10"
distributions="E" #E" # U T N C"
queue_sizes="25600 256000 2560000" # 256000 2560000"
elem_per_bucket="1" # 48 96 192 384 768 1536 3072" #"3 6 12 24 48 96 192 288 384 480 576 768 960 1152 1440 1782 2016 3168"
usage_factor="0.33333333333333333333333"
results="Results/Ordered"


#############################
# elem_per_bucket per datastructure
#############################

#ACRCQ NUMAP RNUMAP NUMAPFK NUMAPBL NUMAPSKT RNUMAPSKT NUMAPSKTFK NUMAPSKTBL NUMAQ RNUMAQ NUMAQFK NUMAQBL NUMAQSKT RNUMAQSKT NUMAQSKTFK NUMAQSKTBL NUMA2Q RNUMA2Q NUMA2QFK NUMA2QBL NUMA2QSKT RNUMA2QSKT NUMA2QSKTFK NUMA2QSKTBL

elem_per_bucket_ACRCQ="1"
elem_per_bucket_NUMAP="1"
elem_per_bucket_RNUMAP="1"
elem_per_bucket_NUMAPFK="1"
elem_per_bucket_NUMAPBL="1"
elem_per_bucket_NUMAPSKT="1"
elem_per_bucket_RNUMAPSKT="1"
elem_per_bucket_NUMAPSKTFK="1"
elem_per_bucket_NUMAPSKTBL="1"
elem_per_bucket_NUMAQ="1"
elem_per_bucket_RNUMAQ="1"
elem_per_bucket_NUMAQFK="1"
elem_per_bucket_NUMAQBL="1"
elem_per_bucket_NUMAQSKT="1"
elem_per_bucket_RNUMAQSKT="1"
elem_per_bucket_NUMAQSKTFK="1"
elem_per_bucket_NUMAQSKTBL="1"
elem_per_bucket_NUMA2Q="1"
elem_per_bucket_RNUMA2Q="1"
elem_per_bucket_NUMA2QFK="1"
elem_per_bucket_NUMA2QBL="1"
elem_per_bucket_NUMA2QSKT="1"
elem_per_bucket_RNUMA2QSKT="1"
elem_per_bucket_NUMA2QSKTFK="1"
elem_per_bucket_NUMA2QSKTBL="1"


#############################
# compile parameters
#############################

