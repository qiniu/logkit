// Package stringsh is helper for system package strings.
package stringsh

import (
	"github.com/apaxa-go/helper/unicodeh"
	"golang.org/x/text/unicode/norm"
	"strings"
	"unicode/utf8"
)

// Len returns number of glyph in UTF-8 encoded string.
func Len(s string) (l int) {
	var ia norm.Iter
	ia.InitString(norm.NFKD, s)
	for !ia.Done() {
		l++
		ia.Next()
	}
	return
}

// GetLine returns first line from s and position in s of remaining part.
// Line delimiter may be "\n" or "\r\n". In both cases delimiter does not include nor in line nor in pos (pos points to first byte of second line).
// If s does not contain delimiter than GetLine returns s as first line.
// If there is no second line in s (s does not contain delimiter or there is no other bytes after delimiter) than pos will be point to non existing position in s.
func GetLine(s string) (line string, pos int) {
	i := strings.Index(s, "\n")
	if i == -1 {
		return s, len(s)
	}

	if i > 0 && s[i-1] == '\r' {
		return s[:i-1], i + 1
	}

	return s[:i], i + 1
}

// GetFirstLine is a shortcut for GetLine but returning only first line.
// As line delimiter does not include in result it may be hard to manipulate with remaining string.
func GetFirstLine(s string) string {
	line, _ := GetLine(s)
	return line
}

// ExtractLine returns first line from s and remaining part of s.
// Line delimiter is the same as in GetLine.
// Also as in GetLine delimiter does not include nor in line nor in rem.
func ExtractLine(s string) (line, rem string) {
	line, pos := GetLine(s)
	if pos < len(s) {
		rem = s[pos:]
	}
	return
}

// IndexMulti returns the index of the first instance of any seps in s and index of founded sep, or (-1,-1) if seps are not present in s.
// Checking for seps in each position of s proceeds in order of they are passed, so if seps[5] and seps[7] both present in s at the same position (let it be 29) then result will be (29; 5), not (29; 7).
// Empty string (as seps) presents at position 0 in any string.
func IndexMulti(s string, seps ...string) (i int, sep int) {
	if len(s) == 0 { // catch case with empty string and empty sep
		for j := range seps {
			if seps[j] == "" {
				return 0, j
			}
		}
		return -1, -1
	}

	for i = range s {
		for j := range seps {
			if strings.HasPrefix(s[i:], seps[j]) {
				return i, j
			}
		}
	}

	return -1, -1
}

// ReplaceMulti returns a copy of the string s with non-overlapping instances of old elements replaced by corresponding new elements.
// If len(old) != len(new) => panic
// if len(old[i])==0 => panic
// ReplaceMulti returns s as-is if old is empty.
func ReplaceMulti(s string, old, new []string) (r string) {
	if len(old) != len(new) {
		panic("number of old elemnts and new elemnts should be the same")
	}

	for i := range old {
		if len(old[i]) == 0 {
			panic("no one of old elements can be empty string")
		}
	}

	if len(old) == 0 {
		return s
	}

	for i, j := IndexMulti(s, old...); i != -1; i, j = IndexMulti(s, old...) {
		r += s[:i] + new[j]
		s = s[i+len(old[j]):]
	}
	r += s
	return
}

// Returns prefix of s before "quote" rune and length of this (prefix + "quote").
// Escaping: "\X" converts to 'X'.
// String s is substring of original string after (not including) opening quote rune.
// TODO make implementation more effective (avoid "sub+=string(r)")
func getQuotedString(s string, quote rune) (sub string, end int) {
	l := len(s)
	escaped := false

	for end < l {
		r, rLen := utf8.DecodeRuneInString(s[end:])
		end += rLen
		switch {
		case r == quote && !escaped:
			return
		case r == '\\' && !escaped:
			escaped = true
		default:
			escaped = false
			sub += string(r)
		}
	}

	return
}

// Returns length of leading white spaces runes.
func whiteSpacesLen(s string) (l int) {
	for l < len(s) {
		r, rLen := utf8.DecodeRuneInString(s[l:])
		if !unicodeh.IsWhiteSpaceYes(r) {
			return
		}
		l += rLen
	}
	return
}

// FieldsQuoted splits the string s around each instance of one or more consecutive white space characters, as defined by unicodeh.IsWhiteSpaceYes.
// It also treat single and double quoted substrings as single field (so it is possible to have white spaces in field itself).
// In quoted substrings '\' used to escape: "...\X..." translated to "...X..." ("\\" => "\", "\"" => '"').
// Returns an array of substrings of s or an empty list if s contains only white space.
func FieldsQuoted(s string) (f []string) {
	l := len(s)

	pos := whiteSpacesLen(s) // Skip leading white spaces

	for pos < l {
		r, rLen := utf8.DecodeRuneInString(s[pos:])
		switch r {
		case '\'', '"':
			pos += rLen
			sub, subLen := getQuotedString(s[pos:], r)
			f = append(f, sub)
			pos += subLen
		default:
			end := pos + rLen
			for end < l {
				r, rLen = utf8.DecodeRuneInString(s[end:])
				if unicodeh.IsWhiteSpaceYes(r) {
					break
				}
				end += rLen
			}
			f = append(f, s[pos:end])
			pos = end
		}

		pos += whiteSpacesLen(s[pos:]) // Skip white spaces
	}

	return
}
