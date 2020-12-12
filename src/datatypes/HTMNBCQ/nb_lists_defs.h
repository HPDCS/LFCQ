/*****************************************************************************
*
*	This file is part of NBCQ, a lock-free O(1) priority queue.
*
*   Copyright (C) 2019, Romolo Marotta
*
*   This program is free software: you can redistribute it and/or modify
*   it under the terms of the GNU General Public License as published by
*   the Free Software Foundation, either version 3 of the License, or
*   (at your option) any later version.
*
*   This is distributed in the hope that it will be useful,
*   but WITHOUT ANY WARRANTY; without even the implied warranty of
*   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*   GNU General Public License for more details.
*
*   You should have received a copy of the GNU General Public License
*   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
******************************************************************************/
/*
 *  bucket.h
 *
 *  Author: Romolo Marotta
 */

#ifndef _NB_LISTS_DEFS_H_
#define _NB_LISTS_DEFS_H_

#define OK			0
#define ABORT		1
#define MOV_FOUND 	2
#define PRESENT		4
#define EMPTY		8
// LUCKY:
#define NEXT_BUCKET 16

#define VAL (0ULL)
#define DEL (1ULL)
#define INV (2ULL)
#define MOV (3ULL)

#define MASK_PTR 	((unsigned long long) (-4LL))
#define MASK_MRK 	(3ULL)
#define MASK_DEL 	((unsigned long long) (-3LL))

#define MAX_UINT 			  (0xffffffffU)
#define MASK_EPOCH	(0x00000000ffffffffULL)
#define MASK_CURR	(0xffffffff00000000ULL)


#define BOOL_CAS_ALE(addr, old, new)  CAS_x86(\
										UNION_CAST(addr, volatile unsigned long long *),\
										UNION_CAST(old,  unsigned long long),\
										UNION_CAST(new,  unsigned long long)\
									  )
									  	
#define BOOL_CAS_GCC(addr, old, new)  __sync_bool_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )

#define VAL_CAS_GCC(addr, old, new)  __sync_val_compare_and_swap(\
										(addr),\
										(old),\
										(new)\
									  )

#define VAL_CAS  VAL_CAS_GCC 
#define BOOL_CAS BOOL_CAS_GCC


#define FETCH_AND_AND 				__sync_fetch_and_and
#define FETCH_AND_OR 				__sync_fetch_and_or

#define ATOMIC_INC					atomic_inc_x86
#define ATOMIC_DEC					atomic_dec_x86

#define ATOMIC_READ					atomic_read



#define is_marked(...) macro_dispatcher(is_marked, __VA_ARGS__)(__VA_ARGS__)
#define is_marked2(w,r) is_marked_2(w,r)
#define is_marked1(w)   is_marked_1(w)
#define is_marked_2(pointer, mask)	( (UNION_CAST(pointer, unsigned long long) & MASK_MRK) == mask )
#define is_marked_1(pointer)		(UNION_CAST(pointer, unsigned long long) & MASK_MRK)
#define get_unmarked(pointer)		(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_PTR), void *))
#define get_marked(pointer, mark)	(UNION_CAST((UNION_CAST(pointer, unsigned long long)|(mark)), void *))
#define get_mark(pointer)			(UNION_CAST((UNION_CAST(pointer, unsigned long long) & MASK_MRK), unsigned long long))




static inline void clflush(volatile void *p){ asm volatile ("clflush (%0)" :: "r"(p)); }

//#define ENABLE_PREFETCH 

#ifdef ENABLE_PREFETCH
#define PREFETCH(x, y) {if((x)){unsigned int step = 0; while(step++<10)_mm_pause();__builtin_prefetch(x, y, 0);}}
#else
#define PREFETCH(x, y) {}
#endif


#define HEAD 0ULL
#define ITEM 1ULL
#define TAIL 2ULL

#define MOV_BIT_POS 63
#define DEL_BIT_POS 62
#define EPO_BIT_POS 61
// LUCKY: Bit per il passaggio da unlinked a linked
#define LNK_BIT_POS 60

#define FREEZE_FOR_MOV (1ULL << MOV_BIT_POS)
#define FREEZE_FOR_DEL (1ULL << DEL_BIT_POS)
#define FREEZE_FOR_EPO (1ULL << EPO_BIT_POS)
// LUCKY: Bit mask linked
#define FREEZE_FOR_LNK (1ULL << LNK_BIT_POS)


#define is_freezed(extractions)  ((extractions >> 32) != 0ULL)
#define is_freezed_for_del(extractions) (extractions & FREEZE_FOR_DEL)
#define is_freezed_for_mov(extractions) (!is_freezed_for_del(extractions) &&  (extractions & FREEZE_FOR_MOV))
#define is_freezed_for_epo(extractions) (!is_freezed_for_del(extractions) &&  (extractions & FREEZE_FOR_EPO))
// LUCKY: check
#define is_freezed_for_lnk(extractions) (!is_freezed_for_del(extractions) &&  (extractions & FREEZE_FOR_LNK))

#define get_freezed(extractions, flag)  (( (extractions) << 32) | (extractions) | flag)
#define get_cleaned_extractions(extractions) (( (extractions) & (~(FREEZE_FOR_LNK | FREEZE_FOR_EPO | FREEZE_FOR_MOV | FREEZE_FOR_DEL))) >> 32)
// LUCKY: Remove possibile locked bits and return extractions
#define get_extractions_wtoutlk(extractions) ( (extractions) & (~(FREEZE_FOR_LNK)))






#endif