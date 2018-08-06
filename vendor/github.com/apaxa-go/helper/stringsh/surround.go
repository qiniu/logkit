package stringsh

// Surround surrounds each passing string with prefix and suffix.
// Surround does not modify passed slice, instead of this it returns result as separate slice.
func Surround(s []string, prefix, suffix string) []string {
	r := make([]string, len(s))
	for i := range s {
		r[i] = prefix + s[i] + suffix
	}
	return r
}
