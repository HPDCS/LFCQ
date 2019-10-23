# All of the sources participating in the build are defined here


OBJ_SRCS := 
ASM_SRCS := 
O_SRCS := 
S_UPPER_SRCS := 
EXECUTABLES := 
USER_OBJS :=

LIBS := -lpthread -lm -lnuma -lrt -mrtm
SRC_DIR := src
TARGETS := NBCQ ACRCQ UNBCQ NUMAP NUMAPBL NUMAPNOP NUMAQ NUMAFK NUMAFKBL #NBCQ LIND MARO CBCQ SLCQ NBVB 2CAS VBPQ ACRCQ #V2CQ NUMA WORK


UTIL_value := src/utils/common.o src/utils/hpdcs_math.o 
GACO_value := src/gc/gc.o src/gc/ptst.o
ARCH_value := src/arch/x86.o


NGACO_value := src/datatypes/nb_umb/gc/gc.o src/datatypes/nb_umb/gc/ptst.o

PHYNBCQ_value := src/datatypes/nb_umb/nbcq_phy/phynbcq.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
PHYNBCQ_link := gcc

NUMAQ_value := src/datatypes/nb_umb/numaq/msq.o src/datatypes/nb_umb/numaq/stack.o src/datatypes/nb_umb/numaq/lcrq.o src/datatypes/nb_umb/numaq/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAQ_link := gcc

NUMAQBLK_value := src/datatypes/nb_umb/numaq_blk/lcrq.o src/datatypes/nb_umb/numaq_blk/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAQBLK_link := gcc

NUMAFK_value := src/datatypes/nb_umb/numafk/lcrq.o src/datatypes/nb_umb/numafk/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAFK_link := gcc

NUMAFKBL_value := src/datatypes/nb_umb/numafk_bl/lcrq.o src/datatypes/nb_umb/numafk_bl/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAFKBL_link := gcc

NUMAFK_MALLOC_value := src/datatypes/nb_umb/numafk_malloc/lcrq.o src/datatypes/nb_umb/numafk_malloc/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAFK_MALLOC_link := gcc

NUMAFK_NUMA_value := src/datatypes/nb_umb/numafk_numa/lcrq.o src/datatypes/nb_umb/numafk_numa/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAFK_NUMA_link := gcc

UNBCQ_value := src/datatypes/nb_umb/unbcq/nb_calqueue.o src/datatypes/nb_umb/unbcq/common_nb_calqueue.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
UNBCQ_link := gcc

NUMAP_value := src/datatypes/nb_umb/numap/common_nb_calqueue.o src/datatypes/nb_umb/numap/mapping.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAP_link := gcc

NUMAPNOP_value := src/datatypes/nb_umb/numap_noop/common_nb_calqueue.o src/datatypes/nb_umb/numap_noop/mapping.o $(UTIL_value) $(ARCH_value) $(NGACO_value) 
NUMAPNOP_link := gcc

NUMAPBL_value := src/datatypes/nb_umb/numap_bl/common_nb_calqueue.o src/datatypes/nb_umb/numap_bl/mapping.o $(UTIL_value) $(ARCH_value) $(NGACO_value)
NUMAPBL_link := gcc

SLCQ_value := src/datatypes/slcalqueue/calqueue.o  $(UTIL_value)

VBPQ_value := src/datatypes/nbcalendars_with_vb2/vbpq.o $(UTIL_value) $(GACO_value) $(ARCH_value)

NBCQ_value := src/datatypes/nbcalendars/nb_calqueue.o src/datatypes/nbcalendars/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
ACRCQ_value := src/datatypes/nbcalendars-ad/nb_calqueue.o src/datatypes/nbcalendars-ad/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
NBVB_value := src/datatypes/nbcalendars_with_vb/nb_calqueue.o src/datatypes/nbcalendars_with_vb/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
2CAS_value := src/datatypes/nbcalendars_with_vb_2CAS/nb_calqueue.o src/datatypes/nbcalendars_with_vb_2CAS/common_nb_calqueue.o src/datatypes/nbcalendars_with_vb_2CAS/bucket.o $(UTIL_value) $(GACO_value) $(ARCH_value)

LIND_value := src/datatypes/nbskiplists/prioq.o    src/datatypes/nbskiplists/common_prioq.o $(UTIL_value) $(GACO_value) $(ARCH_value)
MARO_value := src/datatypes/nbskiplists/prioq_v2.o src/datatypes/nbskiplists/common_prioq.o $(UTIL_value) $(GACO_value) $(ARCH_value)

CBCQ_value := src/datatypes/ChunkBasedPriorityQueue/cbpq.opp\
			  src/datatypes/ChunkBasedPriorityQueue/Atomicable.opp\
  			  src/datatypes/ChunkBasedPriorityQueue/listNode.opp\
			  src/datatypes/ChunkBasedPriorityQueue/skipListCommon.opp\
			  src/datatypes/ChunkBasedPriorityQueue/skipList.opp\
			  src/datatypes/ChunkBasedPriorityQueue/ChunkedPriorityQueue.opp $(UTIL_value) 




SLCQ_link := gcc

NBCQ_link := gcc 
ACRCQ_link := gcc 
VBPQ_link := gcc 
NBVB_link := gcc
2CAS_link := gcc 

LIND_link := gcc 
MARO_link := gcc 

CBCQ_link := g++


NUMA_link := gcc 
WORK_link := gcc 
NUMA_value := src/datatypes/numa_queue.o  src/datatypes/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
WORK_value := src/datatypes/worker_queue.o  src/datatypes/common_nb_calqueue.o  $(UTIL_value) $(GACO_value) $(ARCH_value)


L1_CACHE_LINE_SIZE := $(shell getconf LEVEL1_DCACHE_LINESIZE)
NUMA_NODES := $(shell lscpu | grep -e "NUMA node(s)" | cut -d' ' -f 10-)
NUM_CPUS := $(shell nproc --all)
CPU_PER_SOCKET := $(shell cat /proc/cpuinfo | grep 'physical id' | uniq -c | head -n1 | xargs | cut -d' ' -f1)
MACRO := -DARCH_X86_64  -DCACHE_LINE_SIZE=$(L1_CACHE_LINE_SIZE) -DINTEL -D_NUMA_NODES=$(NUMA_NODES) -DNUM_CPUS=$(NUM_CPUS) -DCPU_PER_SOCKET=$(CPU_PER_SOCKET)


FILTER_OUT_C_SRC := src/main.c src/main_2.c
                  

OBJS_DIR 	:= $(strip $(MAKECMDGOALS))


ifeq ($(OBJS_DIR),)
	OBJS_DIR 	:= Debug
	OPTIMIZATION := -O0
else ifeq ($(OBJS_DIR), all)
	OBJS_DIR 	:= Debug
	OPTIMIZATION := -O0
else ifeq ($(OBJS_DIR), Debug)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), Release)
	OBJS_DIR 	:= Release
	MACRO += -DNDEBUG
	OPTIMIZATION:=-O3
	DEBUG:=
else ifeq ($(OBJS_DIR), GProf)
	OBJS_DIR 	:= GProf
	DEBUG+=-pg
endif


#OPTIMIZATION := -fauto-inc-dec \
-fbranch-count-reg \
-fcombine-stack-adjustments \
-fcompare-elim \
-fcprop-registers \
-fdce \
-fdefer-pop \
-fdelayed-branch \
-fdse \
-fforward-propagate \
-fguess-branch-probability \
-fif-conversion \
-fif-conversion2 \
-finline-functions-called-once \
-fipa-profile \
-fipa-pure-const \
-fipa-reference \
-fmerge-constants \
-fmove-loop-invariants \
-fomit-frame-pointer \
-freorder-blocks \
-fshrink-wrap \
-fshrink-wrap-separate \
-fsplit-wide-types \
-fssa-backprop \
-fssa-phiopt \
-ftree-bit-ccp \
-ftree-ccp \
-ftree-ch \
-ftree-coalesce-vars \
-ftree-copy-prop \
-ftree-dce \
-ftree-dominator-opts \
-ftree-dse \
-ftree-forwprop \
-ftree-fre \
-ftree-phiprop \
-ftree-pta \
-ftree-scev-cprop \
-ftree-sink \
-ftree-slsr \
-ftree-sra \
-ftree-ter \
-funit-at-a-time \
-falign-functions  -falign-jumps \
-falign-labels  -falign-loops \
-fcaller-saves \
-fcode-hoisting \
-fcrossjumping \
-fcse-follow-jumps  -fcse-skip-blocks \
-fdelete-null-pointer-checks \
-fdevirtualize  -fdevirtualize-speculatively \
-fexpensive-optimizations \
-fgcse  -fgcse-lm  \
-fhoist-adjacent-loads \
-finline-small-functions \
-findirect-inlining \
-fipa-bit-cp  -fipa-cp  -fipa-icf \
-fipa-ra  -fipa-sra  -fipa-vrp \
-fisolate-erroneous-paths-dereference \
-flra-remat \
-foptimize-sibling-calls \
-foptimize-strlen \
-fpartial-inlining \
-fpeephole2 \
-freorder-blocks-algorithm=stc \
-freorder-blocks-and-partition  -freorder-functions \
-frerun-cse-after-loop  \
-fschedule-insns  -fschedule-insns2 \
-fsched-interblock  -fsched-spec \
-fstore-merging \
-fstrict-aliasing \
-fthread-jumps \
-ftree-builtin-call-dce \
-ftree-pre \
-ftree-switch-conversion  -ftree-tail-merge \
-ftree-vrp \
-fgcse-after-reload \
-finline-functions \
-fipa-cp-clone\
-floop-interchange \
-floop-unroll-and-jam \
-fpeel-loops \
-fpredictive-commoning \
-fsplit-paths \
-ftree-loop-distribute-patterns \
-ftree-loop-distribution \
-ftree-loop-vectorize \
-ftree-partial-pre \
-ftree-slp-vectorize \
-funswitch-loops \
-fvect-cost-model 


C_SUBDIRS 		:= src src/datatypes/nbcalendars-ad src/datatypes/nbcalendars src/datatypes/nbcalendars_with_vb src/datatypes/nbcalendars_with_vb2  src/datatypes/nbcalendars_with_vb_2CAS  src/datatypes/nbskiplists src/datatypes/slcalqueue  src/arch src/gc src/utils src/datatypes/nb_umb/unbcq src/datatypes/nb_umb/numaq src/datatypes/nb_umb/numaq_blk src/datatypes/nb_umb/numafk src/datatypes/nb_umb/numafk_bl src/datatypes/nb_umb/numap src/datatypes/nb_umb/numap_bl src/datatypes/nb_umb/nbcq_phy src/datatypes/nb_umb/numap_noop src/datatypes/nb_umb/gc

C_SRCS			:= $(shell ls   $(patsubst %, %/*.c, $(C_SUBDIRS)) )
C_SRCS 			:= $(filter-out $(FILTER_OUT_C_SRC), $(C_SRCS))
C_OBJS			:= $(strip $(subst .c,.o, $(C_SRCS)))
C_DEPS			:= $(patsubst %, $(OBJS_DIR)/%, $(subst .o,.d, $(C_OBJS)))


CPP_SUBDIRS 	:= src/datatypes/ChunkBasedPriorityQueue
CPP_SRCS		:= $(shell ls   $(patsubst %, %/*.cpp, $(CPP_SUBDIRS)) )
CPP_SRCS 		:= $(filter-out $(FILTER_OUT_CPP_SRC), $(CPP_SRCS))
CPP_OBJS		:= $(strip $(subst .cpp,.opp, $(CPP_SRCS)))
CPP_ASM			:= $(strip $(subst .cpp,.S, $(CPP_SRCS)))
CPP_DEPS		:= $(patsubst %, $(OBJS_DIR)/%, $(subst .opp,.d, $(CPP_OBJS)))

REAL_TARGETS := $(patsubst %, $(OBJS_DIR)/%-test, $(TARGETS))
UNIT_TARGETS := $(patsubst %, $(OBJS_DIR)/%-resize-unit-test, $(TARGETS))

FLAGS=-mcx16 -g

RM := rm -rf

ifdef RESIZE_PERIOD_FACTOR
FLAGS := $(FLAGS) -DRESIZE_PERIOD_FACTOR=$(RESIZE_PERIOD_FACTOR)
endif

all Debug Release GProf: $(patsubst %, $(OBJS_DIR)/%, $(C_OBJS)) $(patsubst %, $(OBJS_DIR)/%, $(CPP_OBJS)) $(REAL_TARGETS) $(UNIT_TARGETS)

-include $(patsubst %, %.d, $(REAL_TARGETS))
-include $(patsubst %, %.d, $(UNIT_TARGETS))


# Tool invocations
$(OBJS_DIR)/%-test: 
	@echo 'Objects: $^'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	@echo 'Specific OBJS for $(strip $(subst -test,, $(@F))): $($(strip $(subst -test,, $(@F)))_value)'
	echo $(OBJS_DIR)/$(@F): $(OBJS_DIR)/$(SRC_DIR)/main_faster.o $(OBJS_DIR)/$(SRC_DIR)/common_test.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst -test,, $(@F)))_value)) > $(OBJS_DIR)/$(@F).d
	$($(strip $(subst -test,, $(@F)))_link)  -o "$(OBJS_DIR)/$(@F)" $(OBJS_DIR)/$(SRC_DIR)/main_faster.o $(OBJS_DIR)/$(SRC_DIR)/common_test.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst -test,, $(@F)))_value)) $(USER_OBJS) $(LIBS) $(DEBUG)
	@echo 'Finished building target: $@'
	@echo ' '


$(OBJS_DIR)/%-resize-unit-test: 
	@echo 'Objects: $^'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	@echo 'Specific OBJS for $(strip $(subst -resize-unit-test,, $(@F))): $($(strip $(subst -resize-unit-test,, $(@F)))_value)'
	echo $(OBJS_DIR)/$(@F): $(OBJS_DIR)/$(SRC_DIR)/unit_test_resize.o $(OBJS_DIR)/$(SRC_DIR)/common_test.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst -resize-unit-test,, $(@F)))_value)) > $(OBJS_DIR)/$(@F).d
	$($(strip $(subst -resize-unit-test,, $(@F)))_link)  -o "$(OBJS_DIR)/$(@F)" $(OBJS_DIR)/$(SRC_DIR)/unit_test_resize.o $(OBJS_DIR)/$(SRC_DIR)/common_test.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst -resize-unit-test,, $(@F)))_value)) $(USER_OBJS) $(LIBS) $(DEBUG)
	@echo 'Finished building target: $@'
	@echo ' '

-include $(C_DEPS)
-include $(CPP_DEPS)

$(OBJS_DIR)/%.o: %.c
	@echo 'Building target: $@'
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	-mkdir -p  $(subst $(shell basename $@),, $@)
	gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall  -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '

$(OBJS_DIR)/%.opp: %.cpp
	@echo 'Building target: $@'
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	-mkdir -p  $(subst $(shell basename $@),, $@)
	g++  $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall  -c -fmessage-length=0 -MMD -MP -MF"$(@:%.opp=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '



clean:
	-$(RM) Debug
	-$(RM) Release
	-$(RM) GProf	
	

	
.PHONY: clean 



#gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall -S -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$(@:%.o=%.s)" "$<"
#objdump -S --disassemble $(@) > $(@:%.o=%.s) 
#gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall -S -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$(@:%.o=%.s)" "$<"
#objdump -S --disassemble $(@) > $(@:%.opp=%.s) 
