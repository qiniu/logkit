package eval

import (
	"fmt"
	"github.com/apaxa-go/helper/goh/constanth"
	"go/constant"
	"reflect"
)

// ValueKind specifies the kind of value represented by a Value.
type ValueKind int

// Possible values for value's kind:
const (
	Datas       ValueKind = iota // value is Data
	Type                         // type
	BuiltInFunc                  // built-in function
	Package                      // package
)

// Value used to store arguments passed to/returned from expression.
// It can stores: data, type, built-in function and package.
// GoLang valid expression can return only data, all other kind is primary used internally while evaluation.
type Value interface {
	// Kind returns current kind of value represented by a Value.
	Kind() ValueKind

	// DeepType returns human readable kind of stored value. If value stores Data then result is kind of this Data (deep resolution).
	DeepType() string
	// String returns string human-readable representation of underlying value.
	String() string

	// Data returns data if value stores data, otherwise it panics.
	Data() Data
	// Type returns type if value stores type, otherwise it panics.
	Type() reflect.Type
	// BuiltInFunc returns name of built-in function if value stores built-in function, otherwise it panics.
	BuiltInFunc() string
	// Package returns package as map[<identifier>]<variable/method/...> if value stores package, otherwise it panics. Name of package itself is unknown.
	Package() map[string]Value

	// prevent 3rd party classes from implementing Value.
	implementsValue()
}

type (
	dataVal        struct{ v Data }
	typeVal        struct{ v reflect.Type }
	builtInFuncVal string
	packageVal     map[string]Value
)

func (dataVal) Kind() ValueKind        { return Datas }
func (typeVal) Kind() ValueKind        { return Type }
func (builtInFuncVal) Kind() ValueKind { return BuiltInFunc }
func (packageVal) Kind() ValueKind     { return Package }

func (x dataVal) DeepType() string      { return x.v.Kind().String() }
func (x typeVal) DeepType() string      { return "type" }
func (builtInFuncVal) DeepType() string { return "built-in function" }
func (packageVal) DeepType() string     { return "package" }

func (x dataVal) String() string { return x.Data().DeepString() }
func (x typeVal) String() string {
	var v string
	if x.v == nil {
		v = "nil"
	} else {
		v = x.v.String()
	}
	return fmt.Sprint("type value " + v)
}
func (x builtInFuncVal) String() string { return fmt.Sprintf("built-in function value %v", string(x)) }
func (x packageVal) String() string {
	var v = "package (exports:"
	for i := range map[string]Value(x) {
		v += " " + i
	}
	v += ")"
	return v
}

func (x dataVal) Data() Data      { return x.v }
func (typeVal) Data() Data        { panic("") }
func (builtInFuncVal) Data() Data { panic("") }
func (packageVal) Data() Data     { panic("") }

func (dataVal) Type() reflect.Type        { panic("") }
func (x typeVal) Type() reflect.Type      { return x.v }
func (builtInFuncVal) Type() reflect.Type { panic("") }
func (packageVal) Type() reflect.Type     { panic("") }

func (dataVal) BuiltInFunc() string          { panic("") }
func (typeVal) BuiltInFunc() string          { panic("") }
func (x builtInFuncVal) BuiltInFunc() string { return string(x) }
func (packageVal) BuiltInFunc() string       { panic("") }

func (dataVal) Package() map[string]Value        { panic("") }
func (typeVal) Package() map[string]Value        { panic("") }
func (builtInFuncVal) Package() map[string]Value { panic("") }
func (x packageVal) Package() map[string]Value   { return map[string]Value(x) }

func (dataVal) implementsValue()        {}
func (typeVal) implementsValue()        {}
func (builtInFuncVal) implementsValue() {}
func (packageVal) implementsValue()     {}

// MakeType makes Value which stores type x.
func MakeType(x reflect.Type) Value { return typeVal{x} }

// MakeTypeInterface makes Value which stores type of x.
func MakeTypeInterface(x interface{}) Value { return MakeType(reflect.TypeOf(x)) }

// MakeBuiltInFunc makes Value which stores built-in function specified by its name x.
// MakeBuiltInFunc does not perform any checks for validating name.
func MakeBuiltInFunc(x string) Value { return builtInFuncVal(x) }

// MakePackage makes Value which stores package x.
// x must be a map[<identifier>]<variable/method/...>.
// No name of package itself specified.
// Keys in args must not have dots in names.
func MakePackage(x Args) Value { return packageVal(x) }

// MakeData makes Value which stores data x.
func MakeData(x Data) Value { return dataVal{x} }

// MakeDataNil makes Value which stores data "nil".
func MakeDataNil() Value { return MakeData(MakeNil()) }

// MakeDataRegular makes Value which stores regular data x (x is a reflect.Value of required variable).
func MakeDataRegular(x reflect.Value) Value { return MakeData(MakeRegular(x)) }

// MakeDataRegularInterface makes Value which stores regular data x (x is a required variable).
func MakeDataRegularInterface(x interface{}) Value { return MakeData(MakeRegularInterface(x)) }

// MakeDataTypedConst makes Value which stores typed constant x.
func MakeDataTypedConst(x constanth.TypedValue) Value { return MakeData(MakeTypedConst(x)) }

// MakeDataUntypedConst makes Value which stores untyped constant x.
func MakeDataUntypedConst(x constant.Value) Value { return MakeData(MakeUntypedConst(x)) }

// MakeDataUntypedBool makes Value which stores untyped boolean variable with value x.
func MakeDataUntypedBool(x bool) Value { return MakeData(MakeUntypedBool(x)) }
