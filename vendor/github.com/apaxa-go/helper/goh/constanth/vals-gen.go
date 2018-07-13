//replacer:generated-file

package constanth

import (
	"github.com/apaxa-go/helper/mathh"
	"go/constant"
)

// IntVal returns the Go int value of x and whether operation successful.
func IntVal(x constant.Value) (int, bool) {
	i64, ok := Int64Val(x)
	if !ok {
		return 0, false
	}
	if i64 < mathh.MinInt || i64 > mathh.MaxInt {
		return 0, false
	}
	return int(i64), true
}

// UintVal returns the Go int value of x and whether operation successful.
func UintVal(x constant.Value) (uint, bool) {
	u64, ok := Uint64Val(x)
	if !ok {
		return 0, false
	}
	if u64 > mathh.MaxUint {
		return 0, false
	}
	return uint(u64), true
}

// Int8Val returns the Go int8 value of x and whether operation successful.
func Int8Val(x constant.Value) (int8, bool) {
	i64, ok := Int64Val(x)
	if !ok {
		return 0, false
	}
	if i64 < mathh.MinInt8 || i64 > mathh.MaxInt8 {
		return 0, false
	}
	return int8(i64), true
}

// Uint8Val returns the Go int8 value of x and whether operation successful.
func Uint8Val(x constant.Value) (uint8, bool) {
	u64, ok := Uint64Val(x)
	if !ok {
		return 0, false
	}
	if u64 > mathh.MaxUint8 {
		return 0, false
	}
	return uint8(u64), true
}

// Int16Val returns the Go int16 value of x and whether operation successful.
func Int16Val(x constant.Value) (int16, bool) {
	i64, ok := Int64Val(x)
	if !ok {
		return 0, false
	}
	if i64 < mathh.MinInt16 || i64 > mathh.MaxInt16 {
		return 0, false
	}
	return int16(i64), true
}

// Uint16Val returns the Go int16 value of x and whether operation successful.
func Uint16Val(x constant.Value) (uint16, bool) {
	u64, ok := Uint64Val(x)
	if !ok {
		return 0, false
	}
	if u64 > mathh.MaxUint16 {
		return 0, false
	}
	return uint16(u64), true
}
