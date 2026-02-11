package sizehelper

import (
	"fmt"
	"math"
	"runtime"
	"testing"
)

// Global sink variables prevent the compiler from optimizing away map allocations.
// Without assigning to a package-level variable, the compiler may eliminate
// the allocation entirely or move it to the stack.
var (
	globalMapIntInt       map[int]int
	globalMapUint64Arr32  map[uint64][32]byte
	globalMapUint32Uint32 map[uint32]uint32
	globalMapByteByte     map[byte]byte
	globalMapIntArr16     map[int][16]byte
	globalMapIntArr24     map[int][24]byte
	sink                  any // for dynamic map types in table-driven tests
)

// TestMapMemory verifies the exact formula against runtime measurements
func TestMapMemory(t *testing.T) {
	// Test hints covering boundary conditions and common sizes
	hints := []int{0, 8, 9, 14, 15, 28, 29, 56, 57, 100, 112, 113, 224, 225, 448, 449, 896, 897, 898, 1000}

	tests := []struct {
		name      string
		keySize   int
		valueSize int
		makeMap   func(hint int)
	}{
		{"map[int]int", 8, 8, func(h int) { globalMapIntInt = make(map[int]int, h) }},
		{"map[uint32]uint32", 4, 4, func(h int) { globalMapUint32Uint32 = make(map[uint32]uint32, h) }},
		{"map[uint64][32]byte", 8, 32, func(h int) { globalMapUint64Arr32 = make(map[uint64][32]byte, h) }},
		{"map[byte]byte", 1, 1, func(h int) { globalMapByteByte = make(map[byte]byte, h) }},
		{"map[int][16]byte", 8, 16, func(h int) { globalMapIntArr16 = make(map[int][16]byte, h) }},
		{"map[int][24]byte", 8, 24, func(h int) { globalMapIntArr24 = make(map[int][24]byte, h) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, hint := range hints {
				t.Run(fmt.Sprintf("hint=%d", hint), func(t *testing.T) {
					// Run GC twice to ensure stable measurements
					runtime.GC()
					runtime.GC()

					predicted := MapMemory(hint, tt.keySize, tt.valueSize)

					var before runtime.MemStats
					runtime.ReadMemStats(&before)
					tt.makeMap(hint)
					var after runtime.MemStats
					runtime.ReadMemStats(&after)
					actual := int(after.TotalAlloc - before.TotalAlloc)

					// Allow small variance (32 bytes) for minor Go version differences
					diff := predicted - actual
					if diff < 0 {
						diff = -diff
					}
					if diff > 32 {
						t.Errorf("MapMemory(%d, %d, %d) = %d, actual = %d (diff=%d)",
							hint, tt.keySize, tt.valueSize, predicted, actual, diff)
					}
				})
			}
		})
	}
}

// TestHintForMapMemory verifies the inverse function
func TestHintForMapMemory(t *testing.T) {
	// Test int/int (slotSize=16)
	intIntCases := []struct {
		bytes        int
		expectedHint int
	}{
		{0, 0},
		{48, 0},
		{49, 9},
		{376, 9},
		{377, 15},
		{664, 15},
		{665, 29},
		{1240, 29},
		{2392, 57},
		{4952, 113},
		{9560, 225},
		{18520, 449},
		{19072, 897},
		{19073, 898},
		{36992, 898},
		{295600, 7182},
	}

	for _, tc := range intIntCases {
		t.Run(fmt.Sprintf("int_int_bytes=%d", tc.bytes), func(t *testing.T) {
			hint := HintForMapMemory(tc.bytes, 8, 8)
			if hint != tc.expectedHint {
				t.Errorf("HintForMapMemory(%d, 8, 8) = %d, want %d", tc.bytes, hint, tc.expectedHint)
			}

			// Verify roundtrip: the returned hint should produce >= bytes
			produced := MapMemory(hint, 8, 8)
			if produced < tc.bytes {
				t.Errorf("HintForMapMemory(%d, 8, 8) = %d produces %d bytes, want >= %d",
					tc.bytes, hint, produced, tc.bytes)
			}
		})
	}

	// Verify larger value size gives smaller hint for same memory
	hintSmallValue := HintForMapMemory(10000, 8, 8)
	hintLargeValue := HintForMapMemory(10000, 8, 32)
	if hintLargeValue >= hintSmallValue {
		t.Errorf("Larger value size should give smaller hint: got %d >= %d", hintLargeValue, hintSmallValue)
	}
}

// TestHintForMapMemoryVsRuntime compares predictions against actual runtime allocations
func TestHintForMapMemoryVsRuntime(t *testing.T) {
	tests := []struct {
		name      string
		keySize   int
		valueSize int
		targetMem int
		makeMap   func(hint int)
	}{
		{
			name:      "map[uint64][32]byte",
			keySize:   8,
			valueSize: 32,
			targetMem: 50000,
			makeMap: func(hint int) {
				globalMapUint64Arr32 = make(map[uint64][32]byte, hint)
			},
		},
		{
			name:      "map[uint32]uint32",
			keySize:   4,
			valueSize: 4,
			targetMem: 10000,
			makeMap: func(hint int) {
				globalMapUint32Uint32 = make(map[uint32]uint32, hint)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hint := HintForMapMemory(tt.targetMem, tt.keySize, tt.valueSize)
			slotSz := slotSize(tt.keySize, tt.valueSize)

			var before runtime.MemStats
			runtime.ReadMemStats(&before)
			tt.makeMap(hint)
			var after runtime.MemStats
			runtime.ReadMemStats(&after)

			actual := int(after.TotalAlloc - before.TotalAlloc)
			predicted := MapMemory(hint, tt.keySize, tt.valueSize)

			t.Logf("target=%d, hint=%d, predicted=%d, actual=%d (slot=%d)",
				tt.targetMem, hint, predicted, actual, slotSz)

			// Actual should be >= target (we want at least this much memory)
			if actual < tt.targetMem {
				t.Errorf("actual %d < target %d", actual, tt.targetMem)
			}

			// Predicted should match actual exactly
			if predicted != actual {
				t.Errorf("predicted %d != actual %d", predicted, actual)
			}
		})
	}
}

// TestRoundtrip verifies that hint -> bytes -> hint produces consistent results
func TestRoundtrip(t *testing.T) {
	hints := []int{0, 8, 9, 14, 15, 28, 29, 56, 100, 500, 1000, 5000, 10000}

	for _, hint := range hints {
		bytes := MapMemory(hint, 8, 8)
		recoveredHint := HintForMapMemory(bytes, 8, 8)
		recoveredBytes := MapMemory(recoveredHint, 8, 8)

		// The recovered hint should produce the same bytes
		if recoveredBytes != bytes {
			t.Errorf("Roundtrip failed: hint=%d -> bytes=%d -> hint=%d -> bytes=%d",
				hint, bytes, recoveredHint, recoveredBytes)
		}
	}
}

// TestSlotSize verifies the slot size calculation for various map types
func TestSlotSize(t *testing.T) {
	tests := []struct {
		name      string
		keySize   int
		valueSize int
		expected  int
	}{
		{"int/int", 8, 8, 16},
		{"uint64/uint64", 8, 8, 16},
		{"uint32/uint32", 4, 4, 8},
		{"uint64/[32]byte", 8, 32, 40},
		{"uint32/uint64", 4, 8, 16}, // 4 bytes key + 4 padding + 8 bytes value
		{"byte/byte", 1, 1, 2},
		{"uint64/byte", 8, 1, 16}, // 8 bytes key + 1 byte value, aligned to 8
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slotSize(tt.keySize, tt.valueSize)
			if got != tt.expected {
				t.Errorf("slotSize(%d, %d) = %d, want %d", tt.keySize, tt.valueSize, got, tt.expected)
			}
		})
	}
}

// TestLargeMapsBounded tests that for very large hints, the formula is within
// 0.1% tolerance due to runtime measurement variability.
func TestLargeMapsBounded(t *testing.T) {
	testCases := []struct {
		name      string
		keySize   int
		valueSize int
		makeMap   func(hint int)
	}{
		{"map[int]int", 8, 8, func(h int) { sink = make(map[int]int, h) }},
		{"map[uint32]uint32", 4, 4, func(h int) { sink = make(map[uint32]uint32, h) }},
		{"map[uint64][32]byte", 8, 32, func(h int) { sink = make(map[uint64][32]byte, h) }},
	}

	largeHints := []int{50_000, 100_000, 500_000, 1_000_000}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, hint := range largeHints {
				t.Run(fmt.Sprintf("hint=%d", hint), func(t *testing.T) {
					// For large maps, take the minimum of multiple measurements
					// to account for runtime variability
					minActual := math.MaxInt
					for range 3 {
						runtime.GC()
						runtime.GC()
						var before runtime.MemStats
						runtime.ReadMemStats(&before)
						tc.makeMap(hint)
						var after runtime.MemStats
						runtime.ReadMemStats(&after)
						actual := int(after.TotalAlloc - before.TotalAlloc)
						minActual = min(minActual, actual)
					}

					predicted := MapMemory(hint, tc.keySize, tc.valueSize)

					// For very large maps, allow up to 0.1% variance due to runtime overhead
					tolerance := max(predicted/1000, 128)

					diff := minActual - predicted
					if diff < 0 {
						diff = -diff
					}

					if diff > tolerance {
						t.Errorf("predicted=%d, actual=%d (min of 3), diff=%d > tolerance=%d",
							predicted, minActual, diff, tolerance)
					} else {
						t.Logf("predicted=%d, actual=%d (min of 3), diff=%d within tolerance=%d",
							predicted, minActual, diff, tolerance)
					}
				})
			}
		})
	}
}

// TestHintFor30GB verifies that HintForMapMemory correctly calculates
// the hint needed for a 30GB map allocation.
func TestHintFor30GB(t *testing.T) {
	target := 30 * 1024 * 1024 * 1024 // 30GB
	testCases := []struct {
		name      string
		keySize   int
		valueSize int
	}{
		{"map[int]int", 8, 8},
		{"map[uint32]uint32", 4, 4},
		{"map[uint64][32]byte", 8, 32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			hint := HintForMapMemory(target, tc.keySize, tc.valueSize)
			predicted := MapMemory(hint, tc.keySize, tc.valueSize)
			t.Logf("target=30GB, hint=%d, predicted=%.2fGB", hint, float64(predicted)/(1024*1024*1024))

			if predicted < target {
				t.Errorf("predicted %d < target %d", predicted, target)
			}
		})
	}
}
