package unicodeh

import "unicode"

// Unicode property "Other_ID_Continue" (known as "OIDC", "Other_ID_Continue").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	OtherIDContinueNo  = otherIDContinueNo  // Value "No" (known as "N", "No", "F", "False").
	OtherIDContinueYes = otherIDContinueYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	otherIDContinueNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0xb6, 0x1}, {0xb8, 0x386, 0x1}, {0x388, 0x1368, 0x1}, {0x1372, 0x19d9, 0x1}, {0x19db, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 1}
	otherIDContinueYes = &unicode.RangeTable{[]unicode.Range16{{0xb7, 0x387, 0x2d0}, {0x1369, 0x1371, 0x1}, {0x19da, 0x19da, 0x1}}, nil, 0}
)
