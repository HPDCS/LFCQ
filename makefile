# All of the sources participating in the build are defined here
OBJ_SRCS := 
ASM_SRCS := 
O_SRCS := 
S_UPPER_SRCS := 
EXECUTABLES := 
USER_OBJS :=
LIBS := -lpthread -lm -lnuma
SRC_DIR := src
TARGETS := NBCQ LIND MARO NUMA #WORK

NBCQ_value := src/datatypes/nb_calqueue.o   src/datatypes/common_nb_calqueue.o 
LIND_value := src/datatypes/prioq.o  
MARO_value := src/datatypes/prioq_v2.o
NUMA_value := src/datatypes/numa_queue.o  src/datatypes/common_nb_calqueue.o 
WORK_value := src/datatypes/worker_queue.o  src/datatypes/common_nb_calqueue.o 

L1_CACHE_LINE_SIZE := $(shell getconf LEVEL1_DCACHE_LINESIZE)
MACRO := -DARCH_X86_64  -DCACHE_LINE_SIZE=$(L1_CACHE_LINE_SIZE) -DINTEL
OPTIMIZATION := -O3
DEBUG := -g3

FILTER_OUT_SRC := src/main.c src/main_2.c src/mm/mm.c src/mm/mm.h 
FILTER_OUT_DIR := src/datatypes

OBJS_DIR 	:= $(strip $(MAKECMDGOALS))


ifeq ($(OBJS_DIR),)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), all)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), Debug)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), Release)
	OBJS_DIR 	:= Release
	OPTIMIZATION:=-O0 
	DEBUG:=
else ifeq ($(OBJS_DIR), GProf)
	OBJS_DIR 	:= GProf
	DEBUG+=-pg
else
	OBJS_DIR 	:= 
endif


SUBDIRS 		:= $(shell find src -type d)
C_SRCS			:= $(shell ls   $(patsubst %, %/*.c, $(SUBDIRS)) )
C_SRCS 			:= $(filter-out $(FILTER_OUT_SRC), $(C_SRCS))
OBJS			:= $(strip $(subst .c,.o, $(C_SRCS)))
COMMON_SUBDIRS 	:= $(filter-out $(FILTER_OUT_DIR), $(SUBDIRS))
COMMON_C_SRCS	:= $(shell ls   $(patsubst %, %/*.c, $(COMMON_SUBDIRS)) )
COMMON_C_SRCS 	:= $(filter-out $(FILTER_OUT_SRC), $(COMMON_C_SRCS))
COMMON_OBJS		:= $(strip $(subst .c,.o, $(COMMON_C_SRCS)))
ASM				:= $(strip $(subst .c,.S, $(C_SRCS)))
C_DEPS			:= $(patsubst %, $(OBJS_DIR)/%, $(subst .o,.d, $(OBJS)))

REAL_TARGETS := $(patsubst %, $(OBJS_DIR)/test-%, $(TARGETS))


FLAGS=

RM := rm -rf

ifdef USE_MALLOC
FLAGS := $(FLAGS) -DUSE_MALLOC=$(USE_MALLOC)
endif

ifdef UNROLLED_FACTOR
FLAGS := $(FLAGS) -DUNROLLED_FACTOR=$(UNROLLED_FACTOR)
endif
	

all Debug Release GProf: $(REAL_TARGETS)

# Tool invocations
$(OBJS_DIR)/test-%: $(patsubst %, $(OBJS_DIR)/%, $(OBJS)) $(USER_OBJS)  
	@echo 'Objects: $(OBJS)'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	@echo 'Specific OBJS for $(strip $(subst test-,, $(@F))): $($(strip $(subst test-,, $(@F)))_value)'
	gcc  -o "$(OBJS_DIR)/$(@F)" $(patsubst %, $(OBJS_DIR)/%, $(COMMON_OBJS))  $(patsubst %, $(OBJS_DIR)/%, $($(strip $(subst test-,, $(@F)))_value)) $(USER_OBJS) $(LIBS) $(DEBUG)
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
	objdump -S --disassemble $(@) > $(@:%.o=%.s) 
	@echo 'Finished building: $<'
	@echo ' '


# Other Targets
clean:
	-$(RM) Debug
	-$(RM) Release
	-$(RM) GProf	
	-$(RM) NBCQ	
	-$(RM) $(EXECUTABLES) $(OBJS) $(C_DEPS)
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
