package unicodeh

import "unicode"

// Unicode property "Join_Control" (known as "Join_C", "Join_Control").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	JoinControlNo  = joinControlNo  // Value "No" (known as "N", "No", "F", "False").
	JoinControlYes = joinControlYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	joinControlNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x200b, 0x1}, {0x200e, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	joinControlYes = &unicode.RangeTable{[]unicode.Range16{{0x200c, 0x200d, 0x1}}, nil, 0}
)
