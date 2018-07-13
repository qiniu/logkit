package unicodeh

import "unicode"

// IsBidiControlNo reports whether the rune has property "Bidi_Control"="No".
// Property "Bidi_Control" known as "Bidi_C", "Bidi_Control".
// Value "No" known as "N", "No", "F", "False".
func IsBidiControlNo(r rune) bool { return unicode.Is(BidiControlNo, r) }

// IsBidiControlYes reports whether the rune has property "Bidi_Control"="Yes".
// Property "Bidi_Control" known as "Bidi_C", "Bidi_Control".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsBidiControlYes(r rune) bool {
	return r == 0x61c || r == 0x200e || r == 0x200f || r == 0x202a || (r >= 0x202b && r <= 0x202e) || (r >= 0x2066 && r <= 0x2069)
}
