package reflecth

import "reflect"

// ConvertibleTo is just a shortcut for x.Type().ConvertibleTo(t).
func ConvertibleTo(x reflect.Value, t reflect.Type) bool {
	return x.Type().ConvertibleTo(t)
}

// MustConvert is just a x.Convert(t).
func MustConvert(x reflect.Value, t reflect.Type) reflect.Value {
	return x.Convert(t)
}

// Convert is a MustConvert with check for conversion possibility.
func Convert(x reflect.Value, t reflect.Type) (r reflect.Value, ok bool) {
	if ok = ConvertibleTo(x, t); ok {
		r = MustConvert(x, t)
	}
	return
}

// AssignableTo is just a shortcut for x.Type().AssignableTo(t).
func AssignableTo(x reflect.Value, t reflect.Type) bool {
	return x.Type().AssignableTo(t)
}

// MustAssign assign x to newly created reflect.Value of type t.
// It panics if it is impossible to create reflect.Value of type t of if x cannot be assign to t.
func MustAssign(x reflect.Value, t reflect.Type) reflect.Value {
	r := reflect.New(t).Elem()
	r.Set(x)
	return r
}

// Assign is a MustAssign with check for assignation possibility.
func Assign(x reflect.Value, t reflect.Type) (r reflect.Value, ok bool) {
	if ok = AssignableTo(x, t); ok {
		r = MustAssign(x, t)
	}
	return
}
