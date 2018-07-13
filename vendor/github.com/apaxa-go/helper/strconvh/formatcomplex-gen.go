//replacer:generated-file

package strconvh

// FormatComplex128Prec returns the string representation of c in form of "(-1.2+3.4i)".
// The precision prec controls the number of digits after the decimal point.
// The special precision -1 uses the smallest number of digits necessary such that ParseComplex128Prec will return c exactly.
func FormatComplex128Prec(c complex128, prec int) string {
	var optSign string
	if imag(c) >= +0 {
		optSign = "+"
	}
	return "(" + FormatFloat64Prec(real(c), prec) + optSign + FormatFloat64Prec(imag(c), prec) + "i)"
}

// FormatComplex128 is a shortcut for FormatComplex128Prec with prec=-1.
func FormatComplex128(c complex128) string {
	return FormatComplex128Prec(c, -1)
}
