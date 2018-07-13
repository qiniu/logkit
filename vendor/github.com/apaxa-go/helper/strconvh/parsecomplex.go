package strconvh

import "strconv"

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

import (
	"github.com/apaxa-go/helper/stringsh"
	"strings"
)

// Empty real or imag result means that this part is absent in string.
// Function strip ending 'i' in imag part, so both returned real and imag ready for parsing float (if not empty).
// real & imag can not be empty at the simultaneously (this case means that pa
func splitComplexStr(s string) (real, imag string, err error) {
	// Trim braces if both present exists
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		s = s[1 : len(s)-1]
	}

	// Perform some checks to simplify following code
	if len(s) == 0 || s == "i" {
		return "", "", strconv.ErrSyntax // errors.New("empty string passed") | errors.New( "invalid string passed")
	}

	// Find position of second (imaginary) part (if exists).
	secPartAt, _ := stringsh.IndexMulti(s[1:], "-", "+")
	if secPartAt != -1 {
		secPartAt++ // as we passed previously s[1:]
	}

	// Split to parts.
	switch secPartAt {
	case -1: // only one part (real or imaginary) presented in string
		switch s[len(s)-1] {
		case 'i': // imaginary part
			imag = s[:len(s)-1] // imag cannot be empty string because case s == "i" has been already checked
		default: // real part
			real = s
		}
	default: // passed both part - real and imaginary
		if s[len(s)-1] != 'i' {
			return "", "", strconv.ErrSyntax // errors.New( "imaginary part does not end with 'i'")
		}
		real = s[:secPartAt]
		imag = s[secPartAt : len(s)-1]
	}
	return
}

//replacer:replace
//replacer:old 32	64
//replacer:new 64	128

// ParseComplex64 converts the string s to a complex64.
// Valid form are "<float32>","<float32>i","<float32><float32 with sign>i" and optionally can be parenthesized.
// ParseComplex64 internally uses ParseFloat32 for parsing real and imaginary parts.
// The errors that ParseComplex64 returns have concrete type *NumError and include err.Num = s.
func ParseComplex64(s string) (c complex64, err error) {
	const fn = "ParseComplex"
	var realStr, imagStr string
	realStr, imagStr, err = splitComplexStr(s)
	if err != nil {
		err = &strconv.NumError{Func: fn, Num: s, Err: err}
		return
	}

	var realPart, imagPart float32
	if realStr != "" {
		realPart, err = ParseFloat32(realStr)
		if err != nil {
			return 0, &strconv.NumError{Func: fn, Num: s, Err: err} // newParseComplexError(s, "ivalid real part - "+err.Error())
		}
	}
	if imagStr != "" {
		imagPart, err = ParseFloat32(imagStr)
		if err != nil {
			return 0, &strconv.NumError{Func: fn, Num: s, Err: err} // newParseComplexError(s, "ivalid imaginary part - "+err.Error())
		}
	}
	c = complex(realPart, imagPart)
	return
}
