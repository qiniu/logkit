package unicodeh

// IsVariationSelectorNo reports whether the rune has property "Variation_Selector"="No".
// Property "Variation_Selector" known as "VS", "Variation_Selector".
// Value "No" known as "N", "No", "F", "False".
func IsVariationSelectorNo(r rune) bool {
	return (r >= 0x0 && r <= 0x180a) || (r >= 0x180e && r <= 0xfdff) || (r >= 0xfe10 && r <= 0xffff) || (r >= 0x10000 && r <= 0xe00ff) || (r >= 0xe01f0 && r <= 0x10ffff)
}

// IsVariationSelectorYes reports whether the rune has property "Variation_Selector"="Yes".
// Property "Variation_Selector" known as "VS", "Variation_Selector".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsVariationSelectorYes(r rune) bool {
	return (r >= 0x180b && r <= 0x180d) || (r >= 0xfe00 && r <= 0xfe0f) || (r >= 0xe0100 && r <= 0xe01ef)
}
