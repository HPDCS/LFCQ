################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
../src/utils/hpdcs_math.c
##../src/utils/hpdcs_utils.c\

OBJS += \
../src/utils/hpdcs_math.o 
##../src/utils/hpdcs_utils.o\

C_DEPS += \
../src/utils/hpdcs_math.d 
##../src/utils/hpdcs_utils.d\


# Each subdirectory must supply rules for building sources it contributes
src/utils/%.o: ../src/utils/%.c ../src/utils/%.h 
	@echo 'Building file: $<'
	@echo 'Invoking: Cross GCC Compiler'
	gcc $(MACRO) $(OPTIMIZATION) $(DEBUG) -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


