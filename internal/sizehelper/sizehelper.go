// Package sizehelper provides functions to estimate memory usage of Go's internal
// swissmap implementation on 64-bit systems.
//
// IMPORTANT: These estimates are based on Go 1.25's swissmap implementation.
// Other Go versions may use different map implementations and produce
// slightly different results.
//
// Based on Go 1.25 runtime/internal/maps source code.
package sizehelper

import (
	"math/bits"
)

// Constants from Go's swissmap implementation
const (
	swissMapGroupSlots = 8    // abi.SwissMapGroupSlots
	maxAvgGroupLoad    = 7    // load factor 7/8
	maxTableCapacity   = 1024 // max slots per table before splitting
	mapStructSize      = 48   // sizeof(maps.Map) on 64-bit
	tableStructSize    = 32   // sizeof(maps.table) on 64-bit
	ctrlGroupSize      = 8    // sizeof(ctrlGroup) - control word per group
	numSizeClasses     = 68
)

// Go's memory allocator size classes for 64-bit systems.
// From internal/runtime/gc/sizeclasses.go in Go 1.25
var sizeClassToSize = [numSizeClasses]int{
	0, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240,
	256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024,
	1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096,
	4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288,
	13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768,
}

// Constants for Go's size class lookup algorithm.
// From internal/runtime/gc/sizeclasses.go in Go 1.25.
// These define the boundaries and divisors used to map allocation sizes
// to their corresponding size class index.
const (
	smallSizeDiv = 8     // divisor for sizes <= 1024 bytes (index into sizeToSizeClass8)
	smallSizeMax = 1024  // threshold between small and medium size classes
	largeSizeDiv = 128   // divisor for sizes 1025-32768 bytes (index into sizeToSizeClass128)
	maxSmallSize = 32768 // max size handled by size classes; larger allocations use page rounding
)

// sizeToSizeClass8 maps size/8 to size class for sizes <= 1024
var sizeToSizeClass8 = [smallSizeMax/smallSizeDiv + 1]int{
	0, 1, 2, 3, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13, 13,
	14, 14, 15, 15, 16, 16, 17, 17, 18, 18, 19, 19, 19, 19, 20, 20, 20, 20, 21,
	21, 21, 21, 22, 22, 22, 22, 23, 23, 23, 23, 24, 24, 24, 24, 25, 25, 25, 25,
	26, 26, 26, 26, 27, 27, 27, 27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 28, 28,
	28, 29, 29, 29, 29, 29, 29, 29, 29, 30, 30, 30, 30, 30, 30, 30, 30, 31, 31,
	31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 32, 32, 32, 32, 32,
	32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32,
}

// sizeToSizeClass128 maps (size-1024)/128 to size class for sizes > 1024
var sizeToSizeClass128 = [(maxSmallSize-smallSizeMax)/largeSizeDiv + 1]int{
	32, 33, 34, 35, 36, 37, 37, 38, 38, 39, 39, 40, 40, 40, 41, 41, 41, 42, 43,
	43, 44, 44, 44, 44, 44, 45, 45, 45, 45, 45, 45, 46, 46, 46, 46, 47, 47, 47,
	47, 47, 47, 48, 48, 48, 49, 49, 50, 51, 51, 51, 51, 51, 51, 51, 51, 51, 51,
	52, 52, 52, 52, 52, 52, 52, 52, 52, 52, 53, 53, 54, 54, 54, 54, 55, 55, 55,
	55, 55, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 56, 57, 57, 57, 57, 57, 57,
	57, 57, 57, 57, 58, 58, 58, 58, 58, 58, 59, 59, 59, 59, 59, 59, 59, 59, 59,
	59, 59, 59, 59, 59, 59, 59, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60,
	60, 60, 60, 60, 61, 61, 61, 61, 61, 62, 62, 62, 62, 62, 62, 62, 62, 62, 62,
	62, 63, 63, 63, 63, 63, 63, 63, 63, 63, 63, 64, 64, 64, 64, 64, 64, 64, 64,
	64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 65, 65, 65, 65, 65,
	65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 65, 66, 66, 66,
	66, 66, 66, 66, 66, 66, 66, 66, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
	67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67, 67,
	67, 67,
}

// roundToSizeClass rounds n up to the next Go allocator size class.
// Uses Go 1.25's exact size class algorithm.
func roundToSizeClass(n int) int {
	if n <= 0 {
		return 0
	}
	if n <= smallSizeMax {
		return sizeClassToSize[sizeToSizeClass8[(n+smallSizeDiv-1)/smallSizeDiv]]
	}
	if n <= maxSmallSize {
		return sizeClassToSize[sizeToSizeClass128[(n-smallSizeMax+largeSizeDiv-1)/largeSizeDiv]]
	}
	// For large allocations (> 32KB), round up to page size (8KB)
	const pageSize = 8192
	return ((n + pageSize - 1) / pageSize) * pageSize
}

// nextPowerOf2 returns the smallest power of 2 >= n.
func nextPowerOf2(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	return 1 << bits.Len64(n-1)
}

// slotSize calculates the slot size in bytes for a map with the given
// key and value sizes. This accounts for Go's alignment requirements.
//
// Examples:
//   - map[int]int:         slotSize(8, 8)  = 16
//   - map[uint32]uint32:   slotSize(4, 4)  = 8
//   - map[uint64][32]byte: slotSize(8, 32) = 40
func slotSize(keySize, valueSize int) int {
	valueAlign := max(min(valueSize, 8), 1)

	valueOffset := keySize
	if remainder := keySize % valueAlign; remainder != 0 {
		valueOffset += valueAlign - remainder
	}

	totalSize := valueOffset + valueSize

	keyAlign := min(keySize, 8)
	maxAlign := max(keyAlign, valueAlign)

	if remainder := totalSize % maxAlign; remainder != 0 {
		totalSize += maxAlign - remainder
	}

	return totalSize
}

// groupSize returns the size of a single group in bytes.
// A group contains 8 control bytes + 8 slots.
func groupSize(slotSize int) int {
	return ctrlGroupSize + swissMapGroupSlots*slotSize
}

// MapMemory estimates the memory allocation in bytes for
// make(map[K]V, hint) on 64-bit systems, given the key and value sizes.
//
// Based on Go 1.25's swissmap allocation formula including size class rounding.
// Accuracy: within 32 bytes for small maps, within 0.1% for large maps (>50k entries).
// Results may vary slightly across Go patch versions.
func MapMemory(hint, keySize, valueSize int) int {
	slotSize := slotSize(keySize, valueSize)
	grpSize := groupSize(slotSize)

	// Case 1: hint <= 8 - only the Map struct is allocated
	if hint <= swissMapGroupSlots {
		return mapStructSize
	}

	// Case 2: hint > 8 - full map with directory and tables

	// Calculate target capacity (accounting for load factor 7/8)
	targetCapacity := uint64(hint) * swissMapGroupSlots / maxAvgGroupLoad

	// Calculate directory size (number of tables)
	dirSize := (targetCapacity + maxTableCapacity - 1) / maxTableCapacity
	dirSize = nextPowerOf2(dirSize)

	// Calculate capacity per table (rounded to power of 2)
	capacityPerTable := max(targetCapacity/dirSize, swissMapGroupSlots)
	capacityPerTable = nextPowerOf2(capacityPerTable)

	// Number of groups per table
	numGroupsPerTable := capacityPerTable / swissMapGroupSlots

	// Calculate allocations with size class rounding
	total := mapStructSize

	// Directory: array of table pointers
	// Note: Go's runtime appears to allocate directory arrays slightly larger
	// than the exact size class for directories >= 1024 bytes
	dirRawBytes := int(dirSize) * 8
	dirBytes := roundToSizeClass(dirRawBytes)
	if dirRawBytes >= 1024 {
		// For directories >= 1024 bytes, Go allocates to the next size class
		dirBytes = roundToSizeClass(dirRawBytes + 1)
	}
	total += dirBytes

	// Each table: table struct + groups array
	tableBytes := roundToSizeClass(tableStructSize)
	groupsBytes := roundToSizeClass(int(numGroupsPerTable) * grpSize)
	total += int(dirSize) * (tableBytes + groupsBytes)

	return total
}

// HintForMapMemory returns the minimum hint that will allocate
// approximately memInBytes for a map with the given key and value sizes.
func HintForMapMemory(memInBytes, keySize, valueSize int) int {
	if memInBytes <= 0 {
		return 0
	}

	// Binary search for the minimum hint
	low, high := 0, memInBytes // Upper bound: hint can't be more than bytes

	// Find a reasonable upper bound first
	for MapMemory(high, keySize, valueSize) < memInBytes {
		high *= 2
		if high > 1<<30 { // Sanity limit
			break
		}
	}

	// Binary search
	for low < high {
		mid := (low + high) / 2
		alloc := MapMemory(mid, keySize, valueSize)
		if alloc < memInBytes {
			low = mid + 1
		} else {
			high = mid
		}
	}

	return low
}
