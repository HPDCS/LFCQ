
L1_CACHE_LINE_SIZE := $(shell getconf LEVEL1_DCACHE_LINESIZE)
MACRO := -DARCH_X86_64  -DCACHE_LINE_SIZE=$(L1_CACHE_LINE_SIZE) -DINTEL


all: unit-test


unit-test: bucket.o nb_calqueue.o common_nb_calqueue.o
	gcc $^ -o $@


%.o: %.c
	gcc $^ -mcx16 -c $(MACRO) -o $@

