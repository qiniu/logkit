package reflecth

import (
	"go/token"
	"reflect"
)

// ShiftOp performs shift operation <x><op><s> as Go language specification describes.
// Supported operations: << >> .
// If operation cannot be performed then error will be returned.
func ShiftOp(x reflect.Value, op token.Token, s uint) (r reflect.Value, err error) {
	r = reflect.New(x.Type()).Elem()

	switch op {
	case token.SHL:
		switch x.Kind() {
		case reflect.Int:
			r.SetInt(int64(int(x.Int()) << s))
		case reflect.Int8:
			r.SetInt(int64(int8(x.Int()) << s))
		case reflect.Int16:
			r.SetInt(int64(int16(x.Int()) << s))
		case reflect.Int32:
			r.SetInt(int64(int32(x.Int()) << s))
		case reflect.Int64:
			r.SetInt(x.Int() << s)
		case reflect.Uint:
			r.SetUint(uint64(uint(x.Uint()) << s))
		case reflect.Uint8:
			r.SetUint(uint64(uint8(x.Uint()) << s))
		case reflect.Uint16:
			r.SetUint(uint64(uint16(x.Uint()) << s))
		case reflect.Uint32:
			r.SetUint(uint64(uint32(x.Uint()) << s))
		case reflect.Uint64:
			r.SetUint(x.Uint() << s)
		default:
			return reflect.Value{}, shiftOpInvalidShiftTypeError(x, op, s)
		}
	case token.SHR:
		switch x.Kind() {
		case reflect.Int:
			r.SetInt(int64(int(x.Int()) >> s))
		case reflect.Int8:
			r.SetInt(int64(int8(x.Int()) >> s))
		case reflect.Int16:
			r.SetInt(int64(int16(x.Int()) >> s))
		case reflect.Int32:
			r.SetInt(int64(int32(x.Int()) >> s))
		case reflect.Int64:
			r.SetInt(x.Int() >> s)
		case reflect.Uint:
			r.SetUint(uint64(uint(x.Uint()) >> s))
		case reflect.Uint8:
			r.SetUint(uint64(uint8(x.Uint()) >> s))
		case reflect.Uint16:
			r.SetUint(uint64(uint16(x.Uint()) >> s))
		case reflect.Uint32:
			r.SetUint(uint64(uint32(x.Uint()) >> s))
		case reflect.Uint64:
			r.SetUint(x.Uint() >> s)
		default:
			return reflect.Value{}, shiftOpInvalidShiftTypeError(x, op, s)
		}
	default:
		return reflect.Value{}, shiftOpInvalidOperatorError(x, op, s)
	}

	return
}
