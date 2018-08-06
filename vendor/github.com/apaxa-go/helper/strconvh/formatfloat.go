package strconvh

import "strconv"

// FormatFloat32Prec returns the string representation of f in the 10-base.
// The precision prec controls the number of digits after the decimal point.
// The special precision -1 uses the smallest number of digits necessary such that ParseFloat32Prec will return f exactly.
func FormatFloat32Prec(f float32, prec int) string {
	return strconv.FormatFloat(float64(f), 'f', prec, 32)
}

// FormatFloat32 is a shortcut for FormatFloat32Prec with prec=-1.
func FormatFloat32(f float32) string {
	return FormatFloat32Prec(f, -1)
}

// FormatFloat64Prec returns the string representation of f in the 10-base.
// The precision prec controls the number of digits after the decimal point.
// The special precision -1 uses the smallest number of digits necessary such that ParseFloat64Prec will return f exactly.
func FormatFloat64Prec(f float64, prec int) string {
	return strconv.FormatFloat(f, 'f', prec, 64)
}

// FormatFloat64 is a shortcut for FormatFloat64Prec with prec=-1.
func FormatFloat64(f float64) string {
	return FormatFloat64Prec(f, -1)
}
