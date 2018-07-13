package unicodeh

import "unicode"

// Unicode property "Pattern_Syntax" (known as "Pat_Syn", "Pattern_Syntax").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	PatternSyntaxNo  = patternSyntaxNo  // Value "No" (known as "N", "No", "F", "False").
	PatternSyntaxYes = patternSyntaxYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	patternSyntaxNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x20, 0x1}, {0x30, 0x39, 0x1}, {0x41, 0x5a, 0x1}, {0x5f, 0x61, 0x2}, {0x62, 0x7a, 0x1}, {0x7f, 0xa0, 0x1}, {0xa8, 0xaa, 0x2}, {0xad, 0xaf, 0x2}, {0xb2, 0xb5, 0x1}, {0xb7, 0xba, 0x1}, {0xbc, 0xbe, 0x1}, {0xc0, 0xd6, 0x1}, {0xd8, 0xf6, 0x1}, {0xf8, 0x200f, 0x1}, {0x2028, 0x202f, 0x1}, {0x203f, 0x2040, 0x1}, {0x2054, 0x205f, 0xb}, {0x2060, 0x218f, 0x1}, {0x2460, 0x24ff, 0x1}, {0x2776, 0x2793, 0x1}, {0x2c00, 0x2dff, 0x1}, {0x2e80, 0x3000, 0x1}, {0x3004, 0x3007, 0x1}, {0x3021, 0x302f, 0x1}, {0x3031, 0xfd3d, 0x1}, {0xfd40, 0xfe44, 0x1}, {0xfe47, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x10ffff, 0x1}}, 13}
	patternSyntaxYes = &unicode.RangeTable{[]unicode.Range16{{0x21, 0x2f, 0x1}, {0x3a, 0x40, 0x1}, {0x5b, 0x5e, 0x1}, {0x60, 0x7b, 0x1b}, {0x7c, 0x7e, 0x1}, {0xa1, 0xa7, 0x1}, {0xa9, 0xab, 0x2}, {0xac, 0xb0, 0x2}, {0xb1, 0xbb, 0x5}, {0xbf, 0xd7, 0x18}, {0xf7, 0x2010, 0x1f19}, {0x2011, 0x2027, 0x1}, {0x2030, 0x203e, 0x1}, {0x2041, 0x2053, 0x1}, {0x2055, 0x205e, 0x1}, {0x2190, 0x245f, 0x1}, {0x2500, 0x2775, 0x1}, {0x2794, 0x2bff, 0x1}, {0x2e00, 0x2e7f, 0x1}, {0x3001, 0x3003, 0x1}, {0x3008, 0x3020, 0x1}, {0x3030, 0xfd3e, 0xcd0e}, {0xfd3f, 0xfe45, 0x106}, {0xfe46, 0xfe46, 0x1}}, nil, 10}
)
