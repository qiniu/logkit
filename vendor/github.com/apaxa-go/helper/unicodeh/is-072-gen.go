package unicodeh

// IsRegionalIndicatorNo reports whether the rune has property "Regional_Indicator"="No".
// Property "Regional_Indicator" known as "RI", "Regional_Indicator".
// Value "No" known as "N", "No", "F", "False".
func IsRegionalIndicatorNo(r rune) bool {
	return (r >= 0x0 && r <= 0xffff) || (r >= 0x10000 && r <= 0x1f1e5) || (r >= 0x1f200 && r <= 0x10ffff)
}

// IsRegionalIndicatorYes reports whether the rune has property "Regional_Indicator"="Yes".
// Property "Regional_Indicator" known as "RI", "Regional_Indicator".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsRegionalIndicatorYes(r rune) bool { return (r >= 0x1f1e6 && r <= 0x1f1ff) }
