package unicodeh

import "unicode"

// IsOtherIDStartNo reports whether the rune has property "Other_ID_Start"="No".
// Property "Other_ID_Start" known as "OIDS", "Other_ID_Start".
// Value "No" known as "N", "No", "F", "False".
func IsOtherIDStartNo(r rune) bool { return unicode.Is(OtherIDStartNo, r) }

// IsOtherIDStartYes reports whether the rune has property "Other_ID_Start"="Yes".
// Property "Other_ID_Start" known as "OIDS", "Other_ID_Start".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsOtherIDStartYes(r rune) bool {
	return r == 0x1885 || r == 0x1886 || r == 0x2118 || r == 0x212e || r == 0x309b || r == 0x309c
}
