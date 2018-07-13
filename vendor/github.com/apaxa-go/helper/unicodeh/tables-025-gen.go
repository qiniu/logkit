package unicodeh

import "unicode"

// Unicode property "ASCII_Hex_Digit" (known as "AHex", "ASCII_Hex_Digit").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	ASCIIHexDigitNo  = aSCIIHexDigitNo  // Value "No" (known as "N", "No", "F", "False").
	ASCIIHexDigitYes = aSCIIHexDigitYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	aSCIIHexDigitNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x2f, 0x1}, {0x3a, 0x40, 0x1}, {0x47, 0x60, 0x1}, {0x67, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 3}
	aSCIIHexDigitYes = &unicode.RangeTable{[]unicode.Range16{{0x30, 0x39, 0x1}, {0x41, 0x46, 0x1}, {0x61, 0x66, 0x1}}, nil, 3}
)
