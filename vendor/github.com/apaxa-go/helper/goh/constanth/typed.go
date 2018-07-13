package constanth

import (
	"github.com/apaxa-go/helper/reflecth"
	"go/constant"
	"go/token"
	"reflect"
)

// A TypedValue represents a Go typed constant.
type TypedValue struct {
	v constant.Value
	t reflect.Type
}

// MakeTypedValue tries to create typed constant from given untyped constant x with specified type t.
// If x can not be represented as type t then ok will be false.
func MakeTypedValue(x constant.Value, t reflect.Type) (r TypedValue, ok bool) {
	ok = AssignableTo(x, t)
	if ok {
		r = TypedValue{x, t}
	}
	return
}

// MustMakeTypedValue is a panic version of MakeTypedValue.
// It panics if x can not be represented as type t.
func MustMakeTypedValue(x constant.Value, t reflect.Type) TypedValue {
	r, ok := MakeTypedValue(x, t)
	if !ok {
		panic("unable to make typed constant with value " + x.String() + " and type " + t.String())
	}
	return r
}

// Untyped returns underlying untyped constant which contains typed constant value.
func (x TypedValue) Untyped() constant.Value { return x.v }

// Type returns type of typed constant.
func (x TypedValue) Type() reflect.Type { return x.t }

//
//	Assign
//

// AssignableTo checks possibility of assignation typed constant x to variable of type t using Go assignation rules described in specification.
func (x TypedValue) AssignableTo(t reflect.Type) bool {
	return x.t == t || (t.Kind() == reflect.Interface && x.t.Implements(t))
}

// MustAssign does the same thing as Assign but if assignation is impossible it panics.
func (x TypedValue) MustAssign(t reflect.Type) reflect.Value {
	r, ok := x.Assign(t)
	if !ok {
		panic("unable to assign " + x.ExactStringType() + " to type " + t.String())
	}
	return r
}

// Assign creates reflect.Value of type t and tries to assign typed constant x to it using Go assignation rules described in specification.
// It returns reflect.Value and boolean flag ok.
// ok will be true if assignation done successfully.
func (x TypedValue) Assign(t reflect.Type) (r reflect.Value, ok bool) {
	switch t.Kind() {
	case reflect.Interface:
		return reflecth.Assign(x.Value(), t)
	default:
		if !x.AssignableTo(t) {
			return
		}
		return Assign(x.v, t)
	}
}

//
//	Convert
//

// ConvertibleTo checks possibility of conversion typed constant x to type t using Go conversion rules described in specification.
func (x TypedValue) ConvertibleTo(t reflect.Type) bool { return ConvertibleTo(x.v, t) }

// MustConvert does the same thing as Convert but if conversion is impossible it panics.
func (x TypedValue) MustConvert(t reflect.Type) TypedValue { return MustConvert(x.v, t) }

// Convert tries to convert typed constant x to type t using Go conversion rules described in specification.
// It returns typed constant and boolean flag ok.
// ok will be true if conversion done successfully.
// Convert returns boolean flag = false if type t is an interface, use Assign instead (this is because Convert returns TypedValue which used to stores typed constant, but result of conversion typed constant to interface is not typed constant, it is variable).
func (x TypedValue) Convert(t reflect.Type) (TypedValue, bool) { return Convert(x.v, t) }

//
//
//

// Value returns typed constant as reflect.Value (as regular Go variable).
func (x TypedValue) Value() reflect.Value { return x.MustAssign(x.t) }

// String return string representation of underlying untyped constant (example: "123.456").
func (x TypedValue) String() string { return x.v.String() }

// StringType return string representations of underlying untyped constant and type (example: "123.456 (type float32)").
func (x TypedValue) StringType() string { return x.v.String() + " (type " + x.t.String() + ")" }

// ExactString return string representation of underlying untyped constant (example: "15432/125").
func (x TypedValue) ExactString() string { return x.v.ExactString() }

// ExactStringType return exact string representations of underlying untyped constant and type (example: "15432/125 (type float32)").
func (x TypedValue) ExactStringType() string {
	return x.v.ExactString() + " (type " + x.t.String() + ")"
}

// Equal compares two typed constants.
func (x TypedValue) Equal(y TypedValue) bool {
	eq, err := CompareOpTyped(x, token.EQL, y)
	return err == nil && eq
}
