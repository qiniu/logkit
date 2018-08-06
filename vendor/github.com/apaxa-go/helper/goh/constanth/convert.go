package constanth

import (
	"go/constant"
	"reflect"
	"unicode"
)

// TODO "x is a floating-point constant, T is a floating-point type, and x is representable by a value of type T after rounding using IEEE 754 round-to-even rules, but with an IEEE -0.0 further rounded to an unsigned 0.0. The constant T(x) is the rounded value." must be applied only to Convert (not to Assign).

// ConvertibleTo checks possibility of conversion constant x to type t using Go conversion rules described in specification.
func ConvertibleTo(x constant.Value, t reflect.Type) bool {
	_, ok := Convert(x, t)
	return ok
}

// MustConvert does the same thing as Convert but if conversion is impossible it panics.
func MustConvert(x constant.Value, t reflect.Type) TypedValue {
	r, ok := Convert(x, t)
	if !ok {
		panic("unable to convert " + x.String() + " to type " + t.String())
	}
	return r
}

// Convert tries to convert constant x to type t using Go conversion rules described in specification.
// It returns typed constant and boolean flag ok.
// ok will be true if conversion done successfully.
// Convert returns ok = false if type t is an interface, use Assign instead (this is because Convert returns TypedValue which used to stores typed constant, but result of conversion untyped constant to interface is not typed constant, it is variable).
func Convert(x constant.Value, t reflect.Type) (r TypedValue, ok bool) {
	switch t.Kind() {
	case reflect.Bool:
		var v bool
		v, ok = BoolVal(x)
		if ok {
			r = TypedValue{constant.MakeBool(v), t}
		}
	case reflect.Int:
		var v int
		v, ok = IntVal(x)
		if ok {
			r = TypedValue{constant.MakeInt64(int64(v)), t}
		}
	case reflect.Int8:
		var v int8
		v, ok = Int8Val(x)
		if ok {
			r = TypedValue{constant.MakeInt64(int64(v)), t}
		}
	case reflect.Int16:
		var v int16
		v, ok = Int16Val(x)
		if ok {
			r = TypedValue{constant.MakeInt64(int64(v)), t}
		}
	case reflect.Int32:
		var v int32
		v, ok = Int32Val(x)
		if ok {
			r = TypedValue{constant.MakeInt64(int64(v)), t}
		}
	case reflect.Int64:
		var v int64
		v, ok = Int64Val(x)
		if ok {
			r = TypedValue{constant.MakeInt64(v), t}
		}
	case reflect.Uint:
		var v uint
		v, ok = UintVal(x)
		if ok {
			r = TypedValue{constant.MakeUint64(uint64(v)), t}
		}
	case reflect.Uint8:
		var v uint8
		v, ok = Uint8Val(x)
		if ok {
			r = TypedValue{constant.MakeUint64(uint64(v)), t}
		}
	case reflect.Uint16:
		var v uint16
		v, ok = Uint16Val(x)
		if ok {
			r = TypedValue{constant.MakeUint64(uint64(v)), t}
		}
	case reflect.Uint32:
		var v uint32
		v, ok = Uint32Val(x)
		if ok {
			r = TypedValue{constant.MakeUint64(uint64(v)), t}
		}
	case reflect.Uint64:
		var v uint64
		v, ok = Uint64Val(x)
		if ok {
			r = TypedValue{constant.MakeUint64(v), t}
		}
	case reflect.Float32:
		var v float32
		v, ok = Float32Val(x)
		if ok {
			r = TypedValue{constant.MakeFloat64(float64(v)), t}
		}
	case reflect.Float64:
		var v float64
		v, ok = Float64Val(x)
		if ok {
			r = TypedValue{constant.MakeFloat64(v), t}
		}
	case reflect.Complex64:
		var v complex64
		v, ok = Complex64Val(x)
		if ok {
			r = TypedValue{MakeComplex128(complex128(v)), t}
		}
	case reflect.Complex128:
		var v complex128
		v, ok = Complex128Val(x)
		if ok {
			r = TypedValue{MakeComplex128(v), t}
		}
	case reflect.String:
		var v string
		v, ok = StringVal(x)
		if ok {
			r = TypedValue{constant.MakeString(v), t}
		}
	}

	// Special case for "string(int)"
	if !ok && x.Kind() == constant.Int && t.Kind() == reflect.String {
		i, valid := RuneVal(x)
		if !valid {
			i = unicode.ReplacementChar
		}
		r = TypedValue{constant.MakeString(string(i)), t}
		ok = true
	}

	return
}

// AssignableTo checks possibility of assignation constant x to variable of type t using Go assignation rules described in specification.
func AssignableTo(x constant.Value, t reflect.Type) bool {
	_, ok := Assign(x, t)
	return ok
}

// MustAssign does the same thing as Assign but if assignation is impossible it panics.
func MustAssign(x constant.Value, t reflect.Type) reflect.Value {
	r, ok := Assign(x, t)
	if !ok {
		panic("unable to assign " + x.String() + " to type " + t.String())
	}
	return r
}

// Assign creates reflect.Value of type t and tries to assign constant x to it using Go assignation rules described in specification.
// It returns reflect.Value and boolean flag ok.
// ok will be true if assignation done successfully.
func Assign(x constant.Value, t reflect.Type) (r reflect.Value, ok bool) {
	r = reflect.New(t).Elem()
	switch t.Kind() {
	case reflect.Bool:
		var v bool
		v, ok = BoolVal(x)
		if ok {
			r.SetBool(v)
		}
	case reflect.Int:
		var v int
		v, ok = IntVal(x)
		if ok {
			r.SetInt(int64(v))
		}
	case reflect.Int8:
		var v int8
		v, ok = Int8Val(x)
		if ok {
			r.SetInt(int64(v))
		}
	case reflect.Int16:
		var v int16
		v, ok = Int16Val(x)
		if ok {
			r.SetInt(int64(v))
		}
	case reflect.Int32:
		var v int32
		v, ok = Int32Val(x)
		if ok {
			r.SetInt(int64(v))
		}
	case reflect.Int64:
		var v int64
		v, ok = Int64Val(x)
		if ok {
			r.SetInt(v)
		}
	case reflect.Uint:
		var v uint
		v, ok = UintVal(x)
		if ok {
			r.SetUint(uint64(v))
		}
	case reflect.Uint8:
		var v uint8
		v, ok = Uint8Val(x)
		if ok {
			r.SetUint(uint64(v))
		}
	case reflect.Uint16:
		var v uint16
		v, ok = Uint16Val(x)
		if ok {
			r.SetUint(uint64(v))
		}
	case reflect.Uint32:
		var v uint32
		v, ok = Uint32Val(x)
		if ok {
			r.SetUint(uint64(v))
		}
	case reflect.Uint64:
		var v uint64
		v, ok = Uint64Val(x)
		if ok {
			r.SetUint(v)
		}
	case reflect.Float32:
		var v float32
		v, ok = Float32Val(x)
		if ok {
			r.SetFloat(float64(v))
		}
	case reflect.Float64:
		var v float64
		v, ok = Float64Val(x)
		if ok {
			r.SetFloat(v)
		}
	case reflect.Complex64:
		var v complex64
		v, ok = Complex64Val(x)
		if ok {
			r.SetComplex(complex128(v))
		}
	case reflect.Complex128:
		var v complex128
		v, ok = Complex128Val(x)
		if ok {
			r.SetComplex(v)
		}
	case reflect.String:
		var v string
		v, ok = StringVal(x)
		if ok {
			r.SetString(v)
		}
	case reflect.Interface:
		if t.NumMethod() != 0 {
			break
		}
		var v reflect.Value
		v, ok = DefaultValue(x)
		if ok {
			r.Set(v)
		}
	default:
		ok = false
	}
	return
}
