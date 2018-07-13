//replacer:generated-file

package strconvh

import "strconv"

// ParseComplex128 converts the string s to a complex128.
// Valid form are "<float64>","<float64>i","<float64><float64 with sign>i" and optionally can be parenthesized.
// ParseComplex128 internally uses ParseFloat64 for parsing real and imaginary parts.
// The errors that ParseComplex128 returns have concrete type *NumError and include err.Num = s.
func ParseComplex128(s string) (c complex128, err error) {
	const fn = "ParseComplex"
	var realStr, imagStr string
	realStr, imagStr, err = splitComplexStr(s)
	if err != nil {
		err = &strconv.NumError{Func: fn, Num: s, Err: err}
		return
	}

	var realPart, imagPart float64
	if realStr != "" {
		realPart, err = ParseFloat64(realStr)
		if err != nil {
			return 0, &strconv.NumError{Func: fn, Num: s, Err: err} // newParseComplexError(s, "ivalid real part - "+err.Error())
		}
	}
	if imagStr != "" {
		imagPart, err = ParseFloat64(imagStr)
		if err != nil {
			return 0, &strconv.NumError{Func: fn, Num: s, Err: err} // newParseComplexError(s, "ivalid imaginary part - "+err.Error())
		}
	}
	c = complex(realPart, imagPart)
	return
}
