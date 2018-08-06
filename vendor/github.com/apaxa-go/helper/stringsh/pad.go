package stringsh

import "strings"

// PadLeft returns original string padded on the left side with glyph to length l.
// Length for this function is in glyphs, not bytes or runes.
// Original string s will be returned if required length l > len(s).
// Passed glyph should be single glyph (printable char) else result will be of wrong length. But where is no check for that.
func PadLeft(s, glyph string, l int) string {
	curLen := Len(s)
	if curLen >= l {
		return s
	}
	return strings.Repeat(glyph, l-curLen) + s
}

// PadRight returns original string padded on the right side with glyph to length l.
// Length for this function is in glyphs, not bytes or runes.
// Original string s will be returned if required length l > len(s).
// Passed glyph should be single glyph (printable char) else result will be of wrong length. But where is no check for that.
func PadRight(s, glyph string, l int) string {
	curLen := Len(s)
	if curLen >= l {
		return s
	}
	return s + strings.Repeat(glyph, l-curLen)
}

// PadLeftWithByte return original string padded on the left side with char b to length l.
// Length for this function is in bytes. So this function is good only for ascii strings.
// Original string s will be returned if required length l > len(s).
func PadLeftWithByte(s string, b byte, l int) string {
	if len(s) >= l {
		return s
	}
	return strings.Repeat(string(b), l-len(s)) + s
}

// PadRightWithByte return original string padded on the left side with char b to length l.
// Length for this function is in bytes. So this function is good only for ascii strings.
// Original string s will be returned if required length l > len(s).
func PadRightWithByte(s string, b byte, l int) string {
	if len(s) >= l {
		return s
	}
	return s + strings.Repeat(string(b), l-len(s))
}
