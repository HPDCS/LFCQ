# All of the sources participating in the build are defined here
OBJ_SRCS := 
ASM_SRCS := 
O_SRCS := 
S_UPPER_SRCS := 
EXECUTABLES := 
USER_OBJS :=
LIBS := -lpthread -lm
SRC_DIR := src

L1_CACHE_LINE_SIZE := $(shell getconf LEVEL1_DCACHE_LINESIZE)
MACRO := -DARCH_X86_64  -DCACHE_LINE_SIZE=$(L1_CACHE_LINE_SIZE)
OPTIMIZATION := -O0
DEBUG := -g3



OBJS_DIR 	:= $(strip $(MAKECMDGOALS))


ifeq ($(OBJS_DIR),)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), all)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), Debug)
	OBJS_DIR 	:= Debug
else ifeq ($(OBJS_DIR), Release)
	OBJS_DIR 	:= Release
	OPTIMIZATION:=-O3 
	DEBUG:=
else ifeq ($(OBJS_DIR), GProf)
	OBJS_DIR 	:= GProf
	DEBUG+=-pg
else
	OBJS_DIR 	:= 
endif


SUBDIRS 	:= $(shell find src -type d)
C_SRCS		:= $(shell ls   $(patsubst %, %/*.c, $(SUBDIRS)) )
OBJS		:= $(strip $(subst .c,.o, $(C_SRCS)))
C_DEPS		:= $(patsubst %, $(OBJS_DIR)/%, $(subst .o,.d, $(OBJS)))



RM := rm -rf


all Debug Release GProf: $(OBJS_DIR)/NBCQ

# Tool invocations
$(OBJS_DIR)/NBCQ: $(patsubst %, $(OBJS_DIR)/%, $(OBJS)) $(USER_OBJS)  
	@echo 'Objects: $(OBJS)'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	gcc  -o "$(OBJS_DIR)/NBCQ" $(patsubst %, $(OBJS_DIR)/%, $(OBJS)) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '

-include $(C_DEPS)

$(OBJS_DIR)/%.o: %.c
	@echo 'Building target: $@'
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	-mkdir -p  $(subst $(shell basename $@),, $@)
	gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
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
	-clear

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
