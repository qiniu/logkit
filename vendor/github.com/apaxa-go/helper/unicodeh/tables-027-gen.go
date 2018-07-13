package unicodeh

import "unicode"

// Unicode property "Bidi_Control" (known as "Bidi_C", "Bidi_Control").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	BidiControlNo  = bidiControlNo  // Value "No" (known as "N", "No", "F", "False").
	BidiControlYes = bidiControlYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	bidiControlNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x61b, 0x1}, {0x61d, 0x200d, 0x1}, {0x2010, 0x2029, 0x1}, {0x202f, 0x2065, 0x1}, {0x206a, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	bidiControlYes = &unicode.RangeTable{[]unicode.Range16{{0x61c, 0x200e, 0x19f2}, {0x200f, 0x202a, 0x1b}, {0x202b, 0x202e, 0x1}, {0x2066, 0x2069, 0x1}}, nil, 0}
)
