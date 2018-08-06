package unicodeh

import "unicode"

// Unicode property "Other_ID_Start" (known as "OIDS", "Other_ID_Start").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	OtherIDStartNo  = otherIDStartNo  // Value "No" (known as "N", "No", "F", "False").
	OtherIDStartYes = otherIDStartYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	otherIDStartNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x1884, 0x1}, {0x1887, 0x2117, 0x1}, {0x2119, 0x212d, 0x1}, {0x212f, 0x309a, 0x1}, {0x309d, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	otherIDStartYes = &unicode.RangeTable{[]unicode.Range16{{0x1885, 0x1886, 0x1}, {0x2118, 0x212e, 0x16}, {0x309b, 0x309c, 0x1}}, nil, 0}
)
