//replacer:generated-file

package mathh

// Min2Uint returns minimum of two passed uint.
func Min2Uint(a, b uint) uint {
	if a <= b {
		return a
	}
	return b
}

// Max2Uint returns maximum of two passed uint.
func Max2Uint(a, b uint) uint {
	if a >= b {
		return a
	}
	return b
}

// Min2Uint8 returns minimum of two passed uint8.
func Min2Uint8(a, b uint8) uint8 {
	if a <= b {
		return a
	}
	return b
}

// Max2Uint8 returns maximum of two passed uint8.
func Max2Uint8(a, b uint8) uint8 {
	if a >= b {
		return a
	}
	return b
}

// Min2Uint16 returns minimum of two passed uint16.
func Min2Uint16(a, b uint16) uint16 {
	if a <= b {
		return a
	}
	return b
}

// Max2Uint16 returns maximum of two passed uint16.
func Max2Uint16(a, b uint16) uint16 {
	if a >= b {
		return a
	}
	return b
}

// Min2Uint32 returns minimum of two passed uint32.
func Min2Uint32(a, b uint32) uint32 {
	if a <= b {
		return a
	}
	return b
}

// Max2Uint32 returns maximum of two passed uint32.
func Max2Uint32(a, b uint32) uint32 {
	if a >= b {
		return a
	}
	return b
}

// Min2Int returns minimum of two passed int.
func Min2Int(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// Max2Int returns maximum of two passed int.
func Max2Int(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// Min2Int8 returns minimum of two passed int8.
func Min2Int8(a, b int8) int8 {
	if a <= b {
		return a
	}
	return b
}

// Max2Int8 returns maximum of two passed int8.
func Max2Int8(a, b int8) int8 {
	if a >= b {
		return a
	}
	return b
}

// Min2Int16 returns minimum of two passed int16.
func Min2Int16(a, b int16) int16 {
	if a <= b {
		return a
	}
	return b
}

// Max2Int16 returns maximum of two passed int16.
func Max2Int16(a, b int16) int16 {
	if a >= b {
		return a
	}
	return b
}

// Min2Int32 returns minimum of two passed int32.
func Min2Int32(a, b int32) int32 {
	if a <= b {
		return a
	}
	return b
}

// Max2Int32 returns maximum of two passed int32.
func Max2Int32(a, b int32) int32 {
	if a >= b {
		return a
	}
	return b
}

// Min2Int64 returns minimum of two passed int64.
func Min2Int64(a, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

// Max2Int64 returns maximum of two passed int64.
func Max2Int64(a, b int64) int64 {
	if a >= b {
		return a
	}
	return b
}

// Min2Float64 returns minimum of two passed uint64.
func Min2Float64(a, b float64) float64 {
	if IsNaNFloat64(a) || IsNaNFloat64(b) {
		return NaNFloat64()
	}
	if IsPositiveZeroFloat64(a) && IsNegativeZeroFloat64(b) {
		return NegativeZeroFloat64()
	}
	if a <= b {
		return a
	}
	return b
}

// Max2Uint64 returns maximum of two passed uint64.
func Max2Float64(a, b float64) float64 {
	if IsNaNFloat64(a) || IsNaNFloat64(b) {
		return NaNFloat64()
	}
	if IsNegativeZeroFloat64(a) && IsPositiveZeroFloat64(b) {
		return PositiveZeroFloat64()
	}
	if a >= b {
		return a
	}
	return b
}
