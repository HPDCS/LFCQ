################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/mm/myallocator.c 

OBJS += \
./src/mm/myallocator.o 

C_DEPS += \
./src/mm/myallocator.d 


# Each subdirectory must supply rules for building sources it contributes
src/mm/%.o: ../src/mm/%.c
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc -DARCH_X86_64 -O0 -g3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


