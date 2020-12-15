#ifndef MYQUICKSORT_H
#define MYQUICKSORT_H
 
 //#include "array.h"

// Function to swap two elements 
static inline void swap(nodeElem_t* a, nodeElem_t* b) 
{
	nodeElem_t t = *a;
	*a = *b;
	*b = t;
	
} 
  
/** Function takes last element as pivot, places the pivot 
 * element at its correct position in sorted array, and 
 * places all smaller (smaller than pivot) to left of pivot 
 * and all greater elements to right of pivot
*/
static inline  long long partition (nodeElem_t* arr,  long long int low,  long long int high){
	pkey_t pivot = arr[high].timestamp;
	 long long int i = (low - 1);  // Index of smaller element

	for ( long long int j = low; j <= high- 1; j++){
		// If current element is smaller than the pivot
		if (arr[j].timestamp < pivot){
			i++;
			swap(&arr[i], &arr[j]);
		}
	}
	
	if(i+1 != high)
		swap(&arr[i + 1], &arr[high]);
	return (i + 1);
} 

/* Function to print an array */
void printArray(nodeElem_t* arr, int size, int low){
	for (; low < size; low++){
		printf("%ld, %p %f\n", syscall(SYS_gettid) , arr[low].ptr, arr[low].timestamp);
		if(arr[low].ptr == NULL) break;
	}
	printf("\n");
}

// A recursive binary search function. It returns 
// location of x in given array arr[l..r] is present, 
// otherwise -1 
/** A recursive binary search function
 * @param array where to search
 * @param lindex the left index to consider
 * @param rindex the right index to consider
 * @param key_timestamp the timestamp key to find
 * @return It returns location of x in given array 
 * arr[l..r] is present, otherwise -1
 */
int binarySearch(nodeElem_t* arr, long long int l, long long int r, pkey_t x) 
{ 
	if (r >= l) { 
		long long int mid = l + (r - l) / 2; 

		// If the element is present at the middle 
		// itself 
		assert(arr+mid != NULL);
		if (arr[mid].timestamp == x) 
				return mid; 

		// If element is smaller than mid, then 
		// it can only be present in left subarray 
		if (arr[mid].timestamp > x) 
				return binarySearch(arr, l, mid - 1, x); 

		// Else the element can only be present 
		// in right subarray 
			return binarySearch(arr, mid + 1, r, x); 
	} 

	// We reach here when element is not 
	// present in array 
	return -1; 
}


/** Function that implements QuickSort in-place
 * @param array to order
 * @param low the lowest element to consider
 * @param high the highest element to consider
 * @return The passed array ordered
 */
void quickSort(nodeElem_t* arr,  long long int low,  long long int high){
	if (low < high){
		// pi is partitioning index
		 long long int pi = partition(arr, low, high);

		// Separately sort elements before
		// partition and after partition
		quickSort(arr, low, pi-1);
		quickSort(arr, pi+1, high);
	}
}

#endif