package strconvh

import "strconv"

// FormatBool returns "true" or "false" according to the value of b
func FormatBool(b bool) string {
	return strconv.FormatBool(b)
}

//ParseBool returns the boolean value represented by the string.
// It accepts 1, t, T, TRUE, true, True, 0, f, F, FALSE, false, False. Any other value returns an error.
func ParseBool(str string) (bool, error) {
	return strconv.ParseBool(str)
}
