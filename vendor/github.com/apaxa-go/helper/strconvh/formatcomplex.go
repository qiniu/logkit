package strconvh

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE
//replacer:replace
//replacer:old 32	64
//replacer:new 64	128

// FormatComplex64Prec returns the string representation of c in form of "(-1.2+3.4i)".
// The precision prec controls the number of digits after the decimal point.
// The special precision -1 uses the smallest number of digits necessary such that ParseComplex64Prec will return c exactly.
func FormatComplex64Prec(c complex64, prec int) string {
	var optSign string
	if imag(c) >= +0 {
		optSign = "+"
	}
	return "(" + FormatFloat32Prec(real(c), prec) + optSign + FormatFloat32Prec(imag(c), prec) + "i)"
}

// FormatComplex64 is a shortcut for FormatComplex64Prec with prec=-1.
func FormatComplex64(c complex64) string {
	return FormatComplex64Prec(c, -1)
}
