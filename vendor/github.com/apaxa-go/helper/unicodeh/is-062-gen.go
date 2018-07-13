package unicodeh

import "unicode"

// IsOtherIDContinueNo reports whether the rune has property "Other_ID_Continue"="No".
// Property "Other_ID_Continue" known as "OIDC", "Other_ID_Continue".
// Value "No" known as "N", "No", "F", "False".
func IsOtherIDContinueNo(r rune) bool { return unicode.Is(OtherIDContinueNo, r) }

// IsOtherIDContinueYes reports whether the rune has property "Other_ID_Continue"="Yes".
// Property "Other_ID_Continue" known as "OIDC", "Other_ID_Continue".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsOtherIDContinueYes(r rune) bool {
	return r == 0xb7 || r == 0x387 || (r >= 0x1369 && r <= 0x1371) || r == 0x19da
}
