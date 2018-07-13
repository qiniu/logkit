package unicodeh

// IsIDSTrinaryOperatorNo reports whether the rune has property "IDS_Trinary_Operator"="No".
// Property "IDS_Trinary_Operator" known as "IDST", "IDS_Trinary_Operator".
// Value "No" known as "N", "No", "F", "False".
func IsIDSTrinaryOperatorNo(r rune) bool {
	return (r >= 0x0 && r <= 0x2ff1) || (r >= 0x2ff4 && r <= 0xffff) || (r >= 0x10000 && r <= 0x10ffff)
}

// IsIDSTrinaryOperatorYes reports whether the rune has property "IDS_Trinary_Operator"="Yes".
// Property "IDS_Trinary_Operator" known as "IDST", "IDS_Trinary_Operator".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsIDSTrinaryOperatorYes(r rune) bool { return r == 0x2ff2 || r == 0x2ff3 }
