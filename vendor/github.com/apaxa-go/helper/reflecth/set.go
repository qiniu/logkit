package reflecth

import (
	"reflect"
	"unsafe"
)

// MakeSettable make x settable.
// It panics if CanAddr returns false for x.
//
// Deprecated: In any case it is bad practice.
func MakeSettable(x reflect.Value) reflect.Value {
	addr := x.UnsafeAddr()
	return reflect.NewAt(x.Type(), unsafe.Pointer(addr)).Elem()
}

// Set assigns src to the value dst.
// It is similar to dst.Set(src) but this function also allow to set private fields.
// Primary reason for this is to avoid restriction with your own struct variable.
// It panics if CanAddr returns false.
// As in Go, x's value must be assignable to v's type.
//
// Deprecated: In any case it is bad practice to change private fields in 3rd party variables/classes.
func Set(dst, src reflect.Value) {
	if !dst.CanSet() {
		dst = MakeSettable(dst)
	}
	dst.Set(src)
	return
}
