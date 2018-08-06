package unicodeh

import "unicode"

// Unicode property "Other_Uppercase" (known as "OUpper", "Other_Uppercase").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	OtherUppercaseNo  = otherUppercaseNo  // Value "No" (known as "N", "No", "F", "False").
	OtherUppercaseYes = otherUppercaseYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	otherUppercaseNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x215f, 0x1}, {0x2170, 0x24b5, 0x1}, {0x24d0, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x1f12f, 0x1}, {0x1f14a, 0x1f14f, 0x1}, {0x1f16a, 0x1f16f, 0x1}, {0x1f18a, 0x10ffff, 0x1}}, 0}
	otherUppercaseYes = &unicode.RangeTable{[]unicode.Range16{{0x2160, 0x216f, 0x1}, {0x24b6, 0x24cf, 0x1}}, []unicode.Range32{{0x1f130, 0x1f149, 0x1}, {0x1f150, 0x1f169, 0x1}, {0x1f170, 0x1f189, 0x1}}, 0}
)
