package unicodeh

import "unicode"

// Unicode property "Regional_Indicator" (known as "RI", "Regional_Indicator").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	RegionalIndicatorNo  = regionalIndicatorNo  // Value "No" (known as "N", "No", "F", "False").
	RegionalIndicatorYes = regionalIndicatorYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	regionalIndicatorNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x1f1e5, 0x1}, {0x1f200, 0x10ffff, 0x1}}, 0}
	regionalIndicatorYes = &unicode.RangeTable{nil, []unicode.Range32{{0x1f1e6, 0x1f1ff, 0x1}}, 0}
)
