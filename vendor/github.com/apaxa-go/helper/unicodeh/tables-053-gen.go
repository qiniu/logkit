package unicodeh

import "unicode"

// Unicode property "IDS_Trinary_Operator" (known as "IDST", "IDS_Trinary_Operator").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	IDSTrinaryOperatorNo  = iDSTrinaryOperatorNo  // Value "No" (known as "N", "No", "F", "False").
	IDSTrinaryOperatorYes = iDSTrinaryOperatorYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	iDSTrinaryOperatorNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x2ff1, 0x1}, {0x2ff4, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	iDSTrinaryOperatorYes = &unicode.RangeTable{[]unicode.Range16{{0x2ff2, 0x2ff3, 0x1}}, nil, 0}
)
