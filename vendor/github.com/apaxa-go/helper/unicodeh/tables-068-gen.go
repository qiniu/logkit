package unicodeh

import "unicode"

// Unicode property "Pattern_White_Space" (known as "Pat_WS", "Pattern_White_Space").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	PatternWhiteSpaceNo  = patternWhiteSpaceNo  // Value "No" (known as "N", "No", "F", "False").
	PatternWhiteSpaceYes = patternWhiteSpaceYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	patternWhiteSpaceNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x8, 0x1}, {0xe, 0x1f, 0x1}, {0x21, 0x84, 0x1}, {0x86, 0x200d, 0x1}, {0x2010, 0x2027, 0x1}, {0x202a, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 3}
	patternWhiteSpaceYes = &unicode.RangeTable{[]unicode.Range16{{0x9, 0xd, 0x1}, {0x20, 0x85, 0x65}, {0x200e, 0x200f, 0x1}, {0x2028, 0x2029, 0x1}}, nil, 2}
)
