package asth

import (
	"unicode"
	"unicode/utf8"
)

// req:
// 	-1: s should be valid not exported identifier
//	 0: s should be valid identifier
// 	 1: s should be valid exported identifier
func validateIdent(s string, req int) bool {
	if IsBlankIdent(s) {
		return false
	}
	r, i := utf8.DecodeRuneInString(s)
	switch req {
	case -1:
		if !unicode.IsLower(r) && r != '_' {
			return false
		}
	case 0:
		if !unicode.IsLetter(r) && r != '_' {
			return false
		}
	case 1:
		if !unicode.IsUpper(r) {
			return false
		}
	default:
		panic("unknown requirements")
	}
	for _, r = range s[i:] {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

// IsBlankIdent checks if s is Go blank identifier ("_").
func IsBlankIdent(s string) bool { return s == "_" }

// IsValidIdent checks if s is valid Go ident.
// For blank identifier this function returns false.
func IsValidIdent(s string) bool { return validateIdent(s, 0) }

// IsValidExportedIdent checks if s is valid exported Go ident.
// For blank identifier this function returns false.
func IsValidExportedIdent(s string) bool { return validateIdent(s, 1) }

// IsValidNotExportedIdent checks if s is valid not exported Go ident.
// For blank identifier this function returns false.
func IsValidNotExportedIdent(s string) bool { return validateIdent(s, -1) }
