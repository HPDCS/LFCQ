# All of the sources participating in the build are defined here
OBJ_SRCS := 
ASM_SRCS := 
O_SRCS := 
S_UPPER_SRCS := 
EXECUTABLES := 
USER_OBJS :=
LIBS := -lpthread -lm -lnuma -lrt
SRC_DIR := src
TARGETS := NBCQ V2CQ V3CQ LIND MARO NOHO NOH2 LMCQ #NUMA #WORK

UTIL_value := src/utils/common.o src/utils/hpdcs_math.o 
GACO_value := src/gc/gc.o src/gc/ptst.o
ARCH_value := src/arch/x86.o

NBCQ_value := src/datatypes/nbcalendars/nb_calqueue.o   src/datatypes/nbcalendars/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
V2CQ_value := src/datatypes/nbcalendars/nb_calqueue_last_min.o   src/datatypes/nbcalendars/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
LMCQ_value := src/datatypes/nblastmin/nb_calqueue_last_min_v2.o   src/datatypes/nblastmin/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
V3CQ_value := src/datatypes/nbcachecq/nb_calqueue_last_min.o   src/datatypes/nbcachecq/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
LIND_value := src/datatypes/nbskiplists/prioq.o  src/datatypes/nbskiplists/common_prioq.o $(UTIL_value) $(GACO_value) $(ARCH_value)
MARO_value := src/datatypes/nbskiplists/prioq_v2.o src/datatypes/nbskiplists/common_prioq.o $(UTIL_value) $(GACO_value) $(ARCH_value)
NOHO_value := src/datatypes/nohotspot/background.o  src/datatypes/nohotspot/nohotspot_ops.o src/datatypes/nohotspot/skiplist.o $(UTIL_value) $(GACO_value) $(ARCH_value)
NOH2_value := src/datatypes/nohotspot2/background.o  src/datatypes/nohotspot2/nohotspot_ops.o src/datatypes/nohotspot2/skiplist.o\
              src/datatypes/nohotspot2/garbagecoll.o src/datatypes/nohotspot2/ptst.o  $(UTIL_value) 
NUMA_value := src/datatypes/numa_queue.o  src/datatypes/common_nb_calqueue.o $(UTIL_value) $(GACO_value) $(ARCH_value)
WORK_value := src/datatypes/worker_queue.o  src/datatypes/common_nb_calqueue.o  $(UTIL_value) $(GACO_value) $(ARCH_value)

L1_CACHE_LINE_SIZE := $(shell getconf LEVEL1_DCACHE_LINESIZE)
MACRO := -DARCH_X86_64  -DCACHE_LINE_SIZE=$(L1_CACHE_LINE_SIZE) -DINTEL
DEBUG := -g3

FILTER_OUT_C_SRC := src/main.c src/main_2.c src/mm/mm.c src/datatypes/nbcalendars/numa_queue.c\
                  src/datatypes/nbcalendars/worker_calqueue.c src/datatypes/nohotspot/test.c src/datatypes/nohotspot2/test.c\
                  src/datatypes/rotating/test.c src/datatypes/numask/test.c src/datatypes/ChunkBasedPriorityQueue/test.c
FILTER_OUT_CPP_SRC := src/datatypes/numask/test.cpp src/datatypes/ChunkBasedPriorityQueue/test.cpp

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
else
	OBJS_DIR 	:= 
endif


SUBDIRS 		:= $(shell find src -type d)
SUBDIRS 		:= $(filter-out src/datatypes, $(SUBDIRS))

C_SRCS			:= $(shell ls   $(patsubst %, %/*.c, $(SUBDIRS)) )
C_SRCS 			:= $(filter-out $(FILTER_OUT_C_SRC), $(C_SRCS))
C_OBJS			:= $(strip $(subst .c,.o, $(C_SRCS)))
C_ASM				:= $(strip $(subst .c,.S, $(C_SRCS)))
C_DEPS			:= $(patsubst %, $(OBJS_DIR)/%, $(subst .o,.d, $(C_OBJS)))

SUBDIRS 		:= $(filter-out src/datatypes src src/utils src/gc src/arch\
                                src/datatypes/nbcalendars src/datatypes/nblastmin src/datatypes/nbcachecq\
                                src/datatypes/nbskiplists src/datatypes/nohotspot\
                                src/datatypes/nohotspot2 src/datatypes/rotating, $(SUBDIRS))

CPP_SRCS		:= $(shell ls   $(patsubst %, %/*.cpp, $(SUBDIRS)) )
CPP_SRCS 		:= $(filter-out $(FILTER_OUT_CPP_SRC), $(CPP_SRCS))
CPP_OBJS		:= $(strip $(subst .cpp,.opp, $(CPP_SRCS)))
CPP_ASM			:= $(strip $(subst .cpp,.S, $(CPP_SRCS)))
CPP_DEPS		:= $(patsubst %, $(OBJS_DIR)/%, $(subst .opp,.d, $(CPP_OBJS)))

REAL_TARGETS := $(patsubst %, $(OBJS_DIR)/test-%, $(TARGETS))
UNIT_TARGETS := $(patsubst %, $(OBJS_DIR)/resize-unit-test-%, $(TARGETS))


FLAGS=

RM := rm -rf

ifdef RESIZE_PERIOD_FACTOR
FLAGS := $(FLAGS) -DRESIZE_PERIOD_FACTOR=$(RESIZE_PERIOD_FACTOR)
endif

all Debug Release GProf: $(patsubst %, $(OBJS_DIR)/%, $(C_OBJS)) $(patsubst %, $(OBJS_DIR)/%, $(CPP_OBJS)) $(REAL_TARGETS) $(UNIT_TARGETS)

-include $(patsubst %, %.d, $(REAL_TARGETS))
-include $(patsubst %, %.d, $(UNIT_TARGETS))


# Tool invocations
$(OBJS_DIR)/test-%: 
	@echo 'Objects: $^'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	@echo 'Specific OBJS for $(strip $(subst test-,, $(@F))): $($(strip $(subst test-,, $(@F)))_value)'
	gcc  -o "$(OBJS_DIR)/$(@F)" $(OBJS_DIR)/$(SRC_DIR)/main_faster.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst test-,, $(@F)))_value)) $(USER_OBJS) $(LIBS) $(DEBUG)
	echo $(OBJS_DIR)/$(@F): $(OBJS_DIR)/$(SRC_DIR)/main_faster.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst test-,, $(@F)))_value)) > $(OBJS_DIR)/$(@F).d
	@echo 'Finished building target: $@'
	@echo ' '


$(OBJS_DIR)/resize-unit-test-%: 
	@echo 'Objects: $^'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	@echo 'Specific OBJS for $(strip $(subst resize-unit-test-,, $(@F))): $($(strip $(subst resize-unit-test-,, $(@F)))_value)'
	gcc  -o "$(OBJS_DIR)/$(@F)" $(OBJS_DIR)/$(SRC_DIR)/unit_test_resize.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst resize-unit-test-,, $(@F)))_value)) $(USER_OBJS) $(LIBS) $(DEBUG)
	echo $(OBJS_DIR)/$(@F): $(OBJS_DIR)/$(SRC_DIR)/unit_test_resize.o $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst resize-unit-test-,, $(@F)))_value)) > $(OBJS_DIR)/$(@F).d
	@echo 'Finished building target: $@'
	@echo ' '

-include $(C_DEPS)

$(OBJS_DIR)/%.o: %.c
	@echo 'Building target: $@'
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	-mkdir -p  $(subst $(shell basename $@),, $@)
	gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	#gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall -S -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$(@:%.o=%.s)" "$<"
	#objdump -S --disassemble $(@) > $(@:%.o=%.s) 
	@echo 'Finished building: $<'
	@echo ' '

$(OBJS_DIR)/%.opp: %.cpp
	@echo 'Building target: $@'
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	-mkdir -p  $(subst $(shell basename $@),, $@)
	g++  $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	#gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) $(FLAGS) $(LIBS) -Wall -S -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$(@:%.o=%.s)" "$<"
	#objdump -S --disassemble $(@) > $(@:%.opp=%.s) 
	@echo 'Finished building: $<'
	@echo ' '


# Other Targets
clean:
	-$(RM) Debug
	-$(RM) Release
	-$(RM) GProf	
	-$(RM) NBCQ	
	-$(RM) $(EXECUTABLES) $(OBJS) $(C_DEPS) $(subst .o,.S, $(OBJS))
	-@echo ' '
	#-clear

.PHONY: all clean 

#.PRECIOUS: $(OBJS)
#.SECONDARY: $(OBJS)


#Debug GProf Release: %/NBCQ

#Debug: Debug/NBCQ
#
#
#Release: Release/NBCQ

#ifneq ($(MAKECMDGOALS),clean)
#ifneq ($(strip $(C_DEPS)),)
#-include $(C_DEPS)
#endif
#endif

#-include ../makefile.defs
#-include ../makefile.targets


#%/NBCQ: $(addprefix %/, $(OBJS)) $(USER_OBJS)  
#	@echo 'Objects: $(addprefix $(dir $@), $(OBJS))'
#	@echo 'Building target: $@'
#	@echo 'Invoking: Cross GCC Linker'
#	gcc  -o "$(addprefix $(dir $@), NBCQ)"  $(addprefix $(dir $@), $(OBJS)) $(USER_OBJS) $(LIBS)
#	@echo 'Finished building target: $@'
#	@echo ' '
#
#Debug/%.o Release/%.o GProg/%.o: %.c
#	@echo 'Building target: $@'
#	@echo 'Building file: $<'
#	@echo 'Invoking: Cross GCC Compiler'
#	-mkdir -p  $(subst $(shell basename $@),, $@)
#	gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
#	@echo 'Finished building: $<'
#	@echo ' '
