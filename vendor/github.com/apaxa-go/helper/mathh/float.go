package mathh

import "math"

//
// Float64
//

// SignBitFloat64 returns true if f is negative or negative zero.
func SignBitFloat64(f float64) bool { return math.Signbit(f) }

// PositiveInfFloat64 returns positive infinity (+inf).
func PositiveInfFloat64() float64 { return math.Inf(0) }

// IsPositiveInfFloat64 reports whether f is an positive infinity.
func IsPositiveInfFloat64(f float64) bool { return math.IsInf(f, 1) }

// NegativeInfFloat64 returns negative infinity (-inf).
func NegativeInfFloat64() float64 { return math.Inf(-1) }

// IsNegativeInfFloat64 reports whether f is a negative infinity.
func IsNegativeInfFloat64(f float64) bool { return math.IsInf(f, -1) }

// IsInfFloat64 reports whether f is a infinity (positive or negative).
func IsInfFloat64(f float64) bool { return math.IsInf(f, 0) }

// PositiveZeroFloat64 returns positive zero (0).
func PositiveZeroFloat64() float64 { return 0 }

// IsPositiveZeroFloat64 reports whether f is an positive zero.
func IsPositiveZeroFloat64(f float64) bool { return IsZeroFloat64(f) && !SignBitFloat64(f) }

// NegativeZeroFloat64 returns negative zero (-0).
func NegativeZeroFloat64() float64 { return math.Copysign(0, -1) }

// IsNegativeZeroFloat64 reports whether f is a negative zero.
func IsNegativeZeroFloat64(f float64) bool { return IsZeroFloat64(f) && SignBitFloat64(f) }

// IsZeroFloat64 reports whether f is a zero (positive or negative).
func IsZeroFloat64(f float64) bool { return f == 0 }

// NaNFloat64 returns Not a Number (NaN).
func NaNFloat64() float64 { return math.NaN() }

// IsNaNFloat64 reports whether f is an IEEE 754 “not-a-number” value.
func IsNaNFloat64(f float64) bool { return math.IsNaN(f) }

//
// Float32
//

// SignBitFloat32 returns true if f is negative or negative zero.
func SignBitFloat32(f float32) bool { return math.Signbit(float64(f)) }

// PositiveInfFloat32 returns positive infinity (+inf).
func PositiveInfFloat32() float32 { return float32(math.Inf(0)) }

// IsPositiveInfFloat32 reports whether f is an positive infinity.
func IsPositiveInfFloat32(f float32) bool { return math.IsInf(float64(f), 1) }

// NegativeInfFloat32 returns negative infinity (-inf).
func NegativeInfFloat32() float32 { return float32(math.Inf(-1)) }

// IsNegativeInfFloat32 reports whether f is a negative infinity.
func IsNegativeInfFloat32(f float32) bool { return math.IsInf(float64(f), -1) }

// IsInfFloat32 reports whether f is a infinity (positive or negative).
func IsInfFloat32(f float32) bool { return math.IsInf(float64(f), 0) }

// PositiveZeroFloat32 returns positive zero (0).
func PositiveZeroFloat32() float32 { return 0 }

// IsPositiveZeroFloat32 reports whether f is an positive zero.
func IsPositiveZeroFloat32(f float32) bool { return IsZeroFloat32(f) && !SignBitFloat32(f) }

// NegativeZeroFloat32 returns negative zero (-0).
func NegativeZeroFloat32() float32 { return float32(math.Copysign(0, -1)) }

// IsNegativeZeroFloat32 reports whether f is a negative zero.
func IsNegativeZeroFloat32(f float32) bool { return IsZeroFloat32(f) && SignBitFloat32(f) }

// IsZeroFloat32 reports whether f is a zero (positive or negative).
func IsZeroFloat32(f float32) bool { return f == 0 }

// NaNFloat32 returns Not a Number (NaN).
func NaNFloat32() float32 { return float32(math.NaN()) }

// IsNaNFloat32 reports whether f is an IEEE 754 “not-a-number” value.
func IsNaNFloat32(f float32) bool { return math.IsNaN(float64(f)) }
