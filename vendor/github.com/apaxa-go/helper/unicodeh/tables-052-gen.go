package unicodeh

import "unicode"

// Unicode property "IDS_Binary_Operator" (known as "IDSB", "IDS_Binary_Operator").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	IDSBinaryOperatorNo  = iDSBinaryOperatorNo  // Value "No" (known as "N", "No", "F", "False").
	IDSBinaryOperatorYes = iDSBinaryOperatorYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	iDSBinaryOperatorNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x2fef, 0x1}, {0x2ff2, 0x2ff3, 0x1}, {0x2ffc, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	iDSBinaryOperatorYes = &unicode.RangeTable{[]unicode.Range16{{0x2ff0, 0x2ff1, 0x1}, {0x2ff4, 0x2ffb, 0x1}}, nil, 0}
)
