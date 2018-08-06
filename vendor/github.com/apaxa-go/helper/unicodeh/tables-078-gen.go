package unicodeh

import "unicode"

// Unicode property "Variation_Selector" (known as "VS", "Variation_Selector").
// Kind of property: "Binary".
// Based on file "PropList.txt".
var (
	VariationSelectorNo  = variationSelectorNo  // Value "No" (known as "N", "No", "F", "False").
	VariationSelectorYes = variationSelectorYes // Value "Yes" (known as "Y", "Yes", "T", "True").
)

var (
	variationSelectorNo  = &unicode.RangeTable{[]unicode.Range16{{0x0, 0x180a, 0x1}, {0x180e, 0xfdff, 0x1}, {0xfe10, 0xffff, 0x1}}, []unicode.Range32{{0x10000, 0xe00ff, 0x1}, {0xe01f0, 0x10ffff, 0x1}}, 0}
	variationSelectorYes = &unicode.RangeTable{[]unicode.Range16{{0x180b, 0x180d, 0x1}, {0xfe00, 0xfe0f, 0x1}}, []unicode.Range32{{0xe0100, 0xe01ef, 0x1}}, 0}
)
