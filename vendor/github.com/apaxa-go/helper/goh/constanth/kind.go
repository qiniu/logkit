package constanth

import "go/constant"

// KindString returns string representation of passed constant's kind.
func KindString(k constant.Kind) (r string) {
	switch k {
	case constant.Unknown:
		return "Unknown"
	case constant.Bool:
		return "Bool"
	case constant.String:
		return "String"
	case constant.Int:
		return "Int"
	case constant.Float:
		return "Float"
	case constant.Complex:
		return "Complex"
	default:
		return "Invalid"
	}
}

// IsNumeric returns true if passed constant's kind is number (int, float or complex).
func IsNumeric(k constant.Kind) bool {
	switch k {
	case constant.Int, constant.Float, constant.Complex:
		return true
	default:
		return false
	}
}
