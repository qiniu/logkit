package constanth

import (
	"github.com/apaxa-go/helper/mathh"
	"go/constant"
)

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

import "math"

// BoolVal returns the Go boolean value of x and whether operation successful.
func BoolVal(x constant.Value) (r bool, ok bool) {
	if x.Kind() != constant.Bool {
		return false, false
	}
	return constant.BoolVal(x), true
}

// Int64Val returns the Go int64 value of x and whether operation successful.
func Int64Val(x constant.Value) (int64, bool) {
	x = constant.ToInt(x)
	if x.Kind() != constant.Int {
		return 0, false
	}
	return constant.Int64Val(x)
}

// Uint64Val returns the Go uint64 value of x and whether operation successful.
func Uint64Val(x constant.Value) (uint64, bool) {
	x = constant.ToInt(x)
	if x.Kind() != constant.Int {
		return 0, false
	}
	return constant.Uint64Val(x)
}

// Float32Val is like Float64Val but for float32 instead of float64.
func Float32Val(x constant.Value) (float32, bool) {
	x = constant.ToFloat(x)
	if x.Kind() != constant.Float {
		return 0, false
	}
	r, _ := constant.Float32Val(x)
	if math.IsInf(float64(r), 0) {
		return 0, false
	}
	return r, true
}

// Float64Val returns the nearest Go float64 value of x and whether operation successful.
// For values too small (too close to 0) to represent as float64, Float64Val silently underflows to 0.
// The result sign always matches the sign of x, even for 0.
func Float64Val(x constant.Value) (float64, bool) {
	x = constant.ToFloat(x)
	if x.Kind() != constant.Float {
		return 0, false
	}
	r, _ := constant.Float64Val(x)
	if math.IsInf(r, 0) {
		return 0, false
	}
	return r, true
}

// Complex64Val returns the Go complex64 value of x and whether operation successful.
func Complex64Val(x constant.Value) (complex64, bool) {
	x = constant.ToComplex(x)
	if x.Kind() != constant.Complex {
		return 0, false
	}
	realC := constant.Real(x)
	imagC := constant.Imag(x)
	r, ok := Float32Val(realC)
	if !ok {
		return 0, false
	}
	i, ok := Float32Val(imagC)
	if !ok {
		return 0, false
	}
	return complex(r, i), true
}

// Complex128Val returns the Go complex128 value of x and whether operation successful.
func Complex128Val(x constant.Value) (complex128, bool) {
	x = constant.ToComplex(x)
	if x.Kind() != constant.Complex {
		return 0, false
	}
	realC := constant.Real(x)
	imagC := constant.Imag(x)
	r, ok := Float64Val(realC)
	if !ok {
		return 0, false
	}
	i, ok := Float64Val(imagC)
	if !ok {
		return 0, false
	}
	return complex(r, i), true
}

// StringVal returns the Go string value of x and whether operation successful.
func StringVal(x constant.Value) (string, bool) {
	if x.Kind() != constant.String {
		return "", false
	}
	return constant.StringVal(x), true
}

// RuneVal returns the Go rune value of x and whether operation successful.
func RuneVal(x constant.Value) (rune, bool) {
	return Int32Val(x)
}

//replacer:replace
//replacer:old int32	Int32
//replacer:new int	Int
//replacer:new int8	Int8
//replacer:new int16	Int16

// Int32Val returns the Go int32 value of x and whether operation successful.
func Int32Val(x constant.Value) (int32, bool) {
	i64, ok := Int64Val(x)
	if !ok {
		return 0, false
	}
	if i64 < mathh.MinInt32 || i64 > mathh.MaxInt32 {
		return 0, false
	}
	return int32(i64), true
}

// Uint32Val returns the Go int32 value of x and whether operation successful.
func Uint32Val(x constant.Value) (uint32, bool) {
	u64, ok := Uint64Val(x)
	if !ok {
		return 0, false
	}
	if u64 > mathh.MaxUint32 {
		return 0, false
	}
	return uint32(u64), true
}
