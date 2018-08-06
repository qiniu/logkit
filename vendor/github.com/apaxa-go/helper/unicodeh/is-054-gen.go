package unicodeh

// IsJoinControlNo reports whether the rune has property "Join_Control"="No".
// Property "Join_Control" known as "Join_C", "Join_Control".
// Value "No" known as "N", "No", "F", "False".
func IsJoinControlNo(r rune) bool {
	return (r >= 0x0 && r <= 0x200b) || (r >= 0x200e && r <= 0xffff) || (r >= 0x10000 && r <= 0x10ffff)
}

// IsJoinControlYes reports whether the rune has property "Join_Control"="Yes".
// Property "Join_Control" known as "Join_C", "Join_Control".
// Value "Yes" known as "Y", "Yes", "T", "True".
func IsJoinControlYes(r rune) bool { return r == 0x200c || r == 0x200d }
