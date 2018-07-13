package unicodeh

import "unicode"

// Unicode property "Prepended_Concatenation_Mark" (known as "PCM", "Prepended_Concatenation_Mark").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	PrependedConcatenationMarkNo  = prependedConcatenationMarkNo  // Value "No" (known as "N", "No", "F", "False").
	PrependedConcatenationMarkYes = prependedConcatenationMarkYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	prependedConcatenationMarkNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x5ff, 0x1}, {0x606, 0x6dc, 0x1}, {0x6de, 0x70e, 0x1}, {0x710, 0x8e1, 0x1}, {0x8e3, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0x110bc, 0x1}, {0x110be, 0x10ffff, 0x1}}, 0}
	prependedConcatenationMarkYes = &unicode.RangeTable{[]unicode.Range16{{0x600, 0x605, 0x1}, {0x6dd, 0x70f, 0x32}, {0x8e2, 0x8e2, 0x1}}, []unicode.Range32{{0x110bd, 0x110bd, 0x1}}, 0}
)
