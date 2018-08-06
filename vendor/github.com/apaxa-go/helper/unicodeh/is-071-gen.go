package unicodeh

// IsRadicalNo reports whether the rune has property "Radical"="No".
// Property "Radical" known as "Radical", "Radical".
// Value "No" known as "N", "No", "F", "False".
func IsRadicalNo(r rune) bool {
	return (r >= 0x0 && r <= 0x2e7f) || r == 0x2e9a || r == 0x2ef4 || (r >= 0x2ef5 && r <= 0x2eff) || (r >= 0x2fd6 && r <= 0xffff) || (r >= 0x10000 && r <= 0x10ffff)
}

// IsRadicalYes reports whether the rune has property "Radical"="Yes".
// Property "Radical" known as "Radical", "Radical".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsRadicalYes(r rune) bool {
	return (r >= 0x2e80 && r <= 0x2e99) || (r >= 0x2e9b && r <= 0x2ef3) || (r >= 0x2f00 && r <= 0x2fd5)
}
