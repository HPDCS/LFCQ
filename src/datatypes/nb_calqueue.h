	/*****************************************************************************
* 
*	This file is part of NBQueue, a lock-free O(1) priority queue.
* 
*   Copyright (C) 2015, Romolo Marotta      
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
 * nonblockingqueue.h
 *
 *  Created on: Jul 13, 2015    
 *      Author: Romolo Marotta   
 */

#ifndef DATATYPES_NONBLOCKING_CALQUEUE_H_
#define DATATYPES_NONBLOCKING_CALQUEUE_H_

#include "common_nb_calqueue.h"



extern void nbc_enqueue(nb_calqueue *queue, double timestamp, void* payload);
extern double nbc_dequeue(nb_calqueue *queue, void **payload);
extern nb_calqueue* nbc_init(unsigned int threshold, double perc_used_bucket, unsigned int elem_per_bucket);

#endif /* DATATYPES_NONBLOCKING_QUEUE_H_ */
