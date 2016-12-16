################################################################################
# Automatically-generated file. Do not edit!
################################################################################

#-include ../makefile.init

RM := rm -rf

# All of the sources participating in the build are defined here

OBJ_SRCS := 
ASM_SRCS := 
C_SRCS := 
O_SRCS := 
S_UPPER_SRCS := 
EXECUTABLES := 
OBJS := 
C_DEPS := 

# Every subdirectory with source files must be described here
SUBDIRS := \
src/arch \
src/datatypes \
src/utils \
src \

# objects
USER_OBJS :=
LIBS := -lpthread -lm

MACRO := -DARCH_X86_64
OPTIMIZATION := -O0
DEBUG := -g3

-include src/subdir.mk
-include src/datatypes/subdir.mk
-include src/arch/subdir.mk
-include src/utils/subdir.mk

#ifneq ($(MAKECMDGOALS),clean)
#ifneq ($(strip $(C_DEPS)),)
#-include $(C_DEPS)
#endif
#endif

#-include ../makefile.defs

# Add inputs and outputs from these tool invocations to the build variables 


# All Target
all: NBCQ

# Tool invocations
NBCQ: $(OBJS) $(USER_OBJS)  
	@echo 'Objects: $(OBJS)'
	@echo 'Building target: $@'
	@echo 'Invoking: Cross GCC Linker'
	gcc  -o "NBCQ" $(OBJS) $(USER_OBJS) $(LIBS)
	@echo 'Finished building target: $@'
	@echo ' '
	
-include $(C_DEPS)

# Other Targets
clean:
	-$(RM) $(EXECUTABLES)$(OBJS)$(C_DEPS) NBCQ
	-@echo ' '
	-clear

.PHONY: all clean dependents
.SECONDARY:

release: OPTIMIZATION:=-O3  
release: DEBUG:=
release: NBCQ
		
gprof: DEBUG+=-pg
gprof: NBCQ

#-include ../makefile.targets
