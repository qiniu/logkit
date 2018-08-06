package tokenh

import "go/token"

// IsEqualityCheck returns true if passed token t is equality check operator ("==" or "!=").
func IsEqualityCheck(t token.Token) bool {
	return t == token.EQL || t == token.NEQ
}

// IsOrderCheck returns true if passed token t is order check operator ("<", "<=", ">" or ">=").
func IsOrderCheck(t token.Token) bool {
	return t == token.LSS || t == token.LEQ || t == token.GTR || t == token.GEQ
}

// IsComparison returns true if passed token t is a comparison operator ("==", "!=", "<", "<=", ">" or ">=").
func IsComparison(t token.Token) bool {
	return IsEqualityCheck(t) || IsOrderCheck(t)
}

// IsShift returns true if passed token t is a shift operator ("<<" or ">>").
func IsShift(t token.Token) bool {
	return t == token.SHL || t == token.SHR
}
