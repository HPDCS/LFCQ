#ifndef MYQUICKSORT_H
#define MYQUICKSORT_H
 
 //#include "array.h"

// Function to swap two elements 
static inline void swap(nodeElem_t* a, nodeElem_t* b) 
{ 
	nodeElem_t* t = a;
	a = b;
	b = t;
} 
  
/** Function takes last element as pivot, places the pivot 
 * element at its correct position in sorted array, and 
 * places all smaller (smaller than pivot) to left of pivot 
 * and all greater elements to right of pivot
*/
static inline unsigned long long partition (nodeElem_t* arr, unsigned long long low, unsigned long long high){
	pkey_t pivot = arr[high].timestamp;
	unsigned long long i = (low - 1);  // Index of smaller element

	for (unsigned long long j = low; j <= high- 1; j++){
		// If current element is smaller than the pivot
		if (arr[j].timestamp < pivot){
			i++;
			swap(&arr[i], &arr[j]);
		}
	} 
	swap(arr+(i + 1), arr+high);
	return (i + 1);
} 

/* Function to print an array */
void printArray(nodeElem_t* arr, int size, int low){
	for (; low < size; low++)
		printf("%f ", arr[low].timestamp);
	printf("\n");
}

/** Function that implements QuickSort in-place
 * @param array to order
 * @param low the lowest element to consider
 * @param high the highest element to consider
 * @return The passed array ordered
 */
void quickSort(nodeElem_t* arr, unsigned long long low, unsigned long long high){
	if (low < high){
		// pi is partitioning index
		unsigned long long pi = partition(arr, low, high);

		// Separately sort elements before
		// partition and after partition
		quickSort(arr, low, pi - 1);
		quickSort(arr, pi + 1, high);
	}
	printArray(arr, high-low, low);
}

#endif