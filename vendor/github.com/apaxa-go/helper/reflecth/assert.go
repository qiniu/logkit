package reflecth

import "reflect"

// TypeAssert perform GoLang type assertion (see language specs).
// Variable x must be of interface kind (or panic/undefined behaviour happened).
// Variable valid identify is assertion valid. Assertion is valid if t is of interface kind or if t implements type of x.
// In Go invalid assertion causes compile time error (something like "impossible type assertion: <t> does not implement <type of x>").
// Variable ok identity is assertion true. Assertion is true if x is not nil and the value stored in x is of type t.
// Variable ok sets to false if valid is false, so it is valid way to check only variable ok and ignore variable valid if details of false result does not have mean.
func TypeAssert(x reflect.Value, t reflect.Type) (r reflect.Value, ok bool, valid bool) {
	xV := x.Elem()
	valid = t.Kind() == reflect.Interface || t.Implements(x.Type())
	if !valid {
		return
	}
	if x.IsNil() {
		return
	}
	switch t.Kind() == reflect.Interface {
	case true:
		ok = xV.Type().Implements(t)
		if ok {
			r = reflect.New(t).Elem()
			r.Set(xV)
		}
	case false:
		ok = xV.Type() == t
		if ok {
			r = xV
		}
	}
	return
}
