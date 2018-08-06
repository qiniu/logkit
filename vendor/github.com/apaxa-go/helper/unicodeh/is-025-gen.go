package unicodeh

// IsASCIIHexDigitNo reports whether the rune has property "ASCII_Hex_Digit"="No".
// Property "ASCII_Hex_Digit" known as "AHex", "ASCII_Hex_Digit".
// Value "No" known as "N", "No", "F", "False".
func IsASCIIHexDigitNo(r rune) bool {
	return (r >= 0x0 && r <= 0x2f) || (r >= 0x3a && r <= 0x40) || (r >= 0x47 && r <= 0x60) || (r >= 0x67 && r <= 0xffff) || (r >= 0x10000 && r <= 0x10ffff)
}

// IsASCIIHexDigitYes reports whether the rune has property "ASCII_Hex_Digit"="Yes".
// Property "ASCII_Hex_Digit" known as "AHex", "ASCII_Hex_Digit".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsASCIIHexDigitYes(r rune) bool {
	return (r >= 0x30 && r <= 0x39) || (r >= 0x41 && r <= 0x46) || (r >= 0x61 && r <= 0x66)
}
