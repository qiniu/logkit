package mathh

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

import "math"

// RoundFloat64 returns the nearest integer, rounding half away from zero.
func RoundFloat64(f float64) float64 { return math.Round(f) }

// RoundFloat32 returns the nearest integer, rounding half away from zero.
func RoundFloat32(f float32) float32 { return float32(math.Round(float64(f))) }

//replacer:replace
//replacer:old float32	Float32	int64	Int64
//replacer:new float32	Float32	int		Int
//replacer:new float32	Float32	int8	Int8
//replacer:new float32	Float32	int16	Int16
//replacer:new float32	Float32	int32	Int32
//replacer:new float64	Float64	int64	Int64
//replacer:new float64	Float64	int		Int
//replacer:new float64	Float64	int8	Int8
//replacer:new float64	Float64	int16	Int16
//replacer:new float64	Float64	int32	Int32

// RoundFloat32ToInt64 returns nearest int64 for given float32.
func RoundFloat32ToInt64(f float32) int64 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < MinInt64+d:
		return MinInt64
	case f > MaxInt64-d:
		return MaxInt64
	case f < d && f > -d:
		return 0
	case f > 0:
		return int64(f + d)
	default:
		return int64(f - d)
	}
}

//replacer:replace
//replacer:old float32	Float32	uint64	Uint64
//replacer:new float32	Float32	uint	Uint
//replacer:new float32	Float32	uint8	Uint8
//replacer:new float32	Float32	uint16	Uint16
//replacer:new float32	Float32	uint32	Uint32
//replacer:new float64	Float64	uint64	Uint64
//replacer:new float64	Float64	uint	Uint
//replacer:new float64	Float64	uint8	Uint8
//replacer:new float64	Float64	uint16	Uint16
//replacer:new float64	Float64	uint32	Uint32

// RoundFloat32ToUint64 returns nearest int64 for given float32.
func RoundFloat32ToUint64(f float32) uint64 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint64-d:
		return MaxUint64
	default:
		return uint64(f + d)
	}
}
