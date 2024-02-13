package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// merge function combines two sorted slices into a single sorted slice.
func merge(left, right []int) []int {
	result := make([]int, 0, len(left)+len(right))
	for len(left) > 0 || len(right) > 0 {
		if len(left) == 0 {
			return append(result, right...)
		}
		if len(right) == 0 {
			return append(result, left...)
		}
		if left[0] <= right[0] {
			result = append(result, left[0])
			left = left[1:]
		} else {
			result = append(result, right[0])
			right = right[1:]
		}
	}
	return result
}

// concurrentMergeSort function sorts a slice of integers using the merge sort algorithm concurrently.
func concurrentMergeSort(arr []int) []int {
	if len(arr) <= 1 {
		return arr
	}

	mid := len(arr) / 2
	var left, right []int
	var wg sync.WaitGroup
	wg.Add(2)

	// Sort the left half in a separate goroutine
	go func() {
		defer wg.Done()
		left = concurrentMergeSort(arr[:mid])
	}()

	// Sort the right half in a separate goroutine
	go func() {
		defer wg.Done()
		right = concurrentMergeSort(arr[mid:])
	}()

	wg.Wait() // Wait for both goroutines to finish
	return merge(left, right)
}

// readCSV function reads numbers from a CSV file and returns them as a slice of integers.
func readCSV(filename string) ([]int, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	lines, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var numbers []int
	for _, line := range lines {
		for _, value := range line {
			number, err := strconv.Atoi(value)
			if err != nil {
				return nil, err
			}
			numbers = append(numbers, number)
		}
	}

	return numbers, nil
}

// writeCSV function writes a slice of integers to a CSV file.
func writeCSV(filename string, numbers []int) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, number := range numbers {
		err := writer.Write([]string{strconv.Itoa(number)})
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	// Read numbers from the CSV file
	numbers, err := readCSV("big-numbers.csv")
	if err != nil {
		fmt.Println("Error reading CSV:", err)
		return
	}

	// Sort the numbers using concurrent merge sort
	sortedNumbers := concurrentMergeSort(numbers)

	// Write the sorted numbers to a new CSV file
	err = writeCSV("sorted-numbers.csv", sortedNumbers)
	if err != nil {
		fmt.Println("Error writing CSV:", err)
		return
	}

	fmt.Println("Sorting complete. Check 'sorted-numbers.csv'")
}
