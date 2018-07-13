package unicodeh

import "unicode"

// Unicode property "Radical" (known as "Radical", "Radical").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	RadicalNo  = radicalNo  // Value "No" (known as "N", "No", "F", "False").
	RadicalYes = radicalYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	radicalNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x2e7f, 0x1}, {0x2e9a, 0x2ef4, 0x5a}, {0x2ef5, 0x2eff, 0x1}, {0x2fd6, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	radicalYes = &unicode.RangeTable{[]unicode.Range16{{0x2e80, 0x2e99, 0x1}, {0x2e9b, 0x2ef3, 0x1}, {0x2f00, 0x2fd5, 0x1}}, nil, 0}
)
