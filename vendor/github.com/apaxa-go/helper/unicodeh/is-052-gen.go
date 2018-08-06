package unicodeh

// IsIDSBinaryOperatorNo reports whether the rune has property "IDS_Binary_Operator"="No".
// Property "IDS_Binary_Operator" known as "IDSB", "IDS_Binary_Operator".
// Value "No" known as "N", "No", "F", "False".
func IsIDSBinaryOperatorNo(r rune) bool {
	return (r >= 0x0 && r <= 0x2fef) || r == 0x2ff2 || r == 0x2ff3 || (r >= 0x2ffc && r <= 0xffff) || (r >= 0x10000 && r <= 0x10ffff)
}

// IsIDSBinaryOperatorYes reports whether the rune has property "IDS_Binary_Operator"="Yes".
// Property "IDS_Binary_Operator" known as "IDSB", "IDS_Binary_Operator".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsIDSBinaryOperatorYes(r rune) bool {
	return r == 0x2ff0 || r == 0x2ff1 || (r >= 0x2ff4 && r <= 0x2ffb)
}
