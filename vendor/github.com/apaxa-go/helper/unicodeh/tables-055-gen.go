package unicodeh

import "unicode"

// Unicode property "Logical_Order_Exception" (known as "LOE", "Logical_Order_Exception").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	LogicalOrderExceptionNo  = logicalOrderExceptionNo  // Value "No" (known as "N", "No", "F", "False").
	LogicalOrderExceptionYes = logicalOrderExceptionYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	logicalOrderExceptionNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0xe3f, 0x1}, {0xe45, 0xebf, 0x1}, {0xec5, 0x19b4, 0x1}, {0x19b8, 0x19b9, 0x1}, {0x19bb, 0xaab4, 0x1}, {0xaab7, 0xaab8, 0x1}, {0xaaba, 0xaabd, 0x3}, {0xaabe, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 0}
	logicalOrderExceptionYes = &unicode.RangeTable{[]unicode.Range16{{0xe40, 0xe44, 0x1}, {0xec0, 0xec4, 0x1}, {0x19b5, 0x19b7, 0x1}, {0x19ba, 0xaab5, 0x90fb}, {0xaab6, 0xaab9, 0x3}, {0xaabb, 0xaabc, 0x1}}, nil, 0}
)
