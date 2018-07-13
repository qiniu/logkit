package eval

import (
	"fmt"
	"github.com/apaxa-go/helper/goh/constanth"
	"github.com/apaxa-go/helper/mathh"
	"github.com/apaxa-go/helper/reflecth"
	"github.com/apaxa-go/helper/strconvh"
	"go/constant"
	"reflect"
)

// DataKind specifies the kind of value represented by a Value.
type DataKind int

// Possible values for value's kind:
const (
	Nil          DataKind = iota // just nil (result of parsing keyword "nil")
	Regular                      // regular typed variable
	TypedConst                   // typed constant
	UntypedConst                 // untyped constant
	UntypedBool                  // untyped boolean variable
)

// String returns human readable representation of data's kind.
func (k DataKind) String() string {
	switch k {
	case Nil:
		return "nil"
	case Regular:
		return "regular variable"
	case TypedConst:
		return "typed constant"
	case UntypedConst:
		return "untyped constant"
	case UntypedBool:
		return "untyped boolean"
	default:
		return "unknown data"
	}
}

// Data used to store all kind of data passed to/returned from expression.
// It can stores: nil (keyword "nil"), regular typed variable, typed constant, untyped constant and untyped boolean variable (result of comparison).
// GoLang valid expression cannot return nil (it is just a keyword), but it is presented here for internal purposes.
type Data interface {
	// Kind returns current kind of data represented by a Data.
	Kind() DataKind

	// Regular returns regular variable (reflect.Value of it) if data stores regular variable, otherwise it panics.
	Regular() reflect.Value
	// TypedConst returns typed constant if data stores typed constant, otherwise it panics.
	TypedConst() constanth.TypedValue
	// UntypedConst returns untyped constant if data stores untyped constant, otherwise it panics.
	UntypedConst() constant.Value
	// UntypedBool returns value of untyped boolean variable if data stores untyped boolean variable, otherwise it panics.
	UntypedBool() bool

	// IsConst returns true if data stores typed or untyped constant.
	IsConst() bool
	// IsTyped returns true if data stores regular typed variable or typed constant.
	IsTyped() bool

	// AssignableTo reports whether data is assignable to type t.
	AssignableTo(t reflect.Type) bool
	// MustAssign creates new variable of type t and sets it to data.
	// It panics if operation cannot be performed.
	MustAssign(t reflect.Type) reflect.Value
	// Assign creates new variable of type t and tries to set it to data.
	// ok reports whether operation was successful.
	Assign(t reflect.Type) (r reflect.Value, ok bool)
	// ConvertibleTo reports whether data is convertible to type t.
	ConvertibleTo(t reflect.Type) bool
	// MustConvert converts data to type t.
	// Original data remains unchanged.
	// It panics if operation cannot be performed.
	MustConvert(t reflect.Type) Data
	// Convert tries to convert data to type t.
	// Original data remains unchanged.
	// ok reports whether operation was successful.
	Convert(t reflect.Type) (r Data, ok bool)

	// AsInt returns int value for regular variable of [u]int* kinds (if feats in int) and for constants (if it can be represent exactly; type of const does not mean anything).
	// ok reports whether operation was successful.
	AsInt() (r int, ok bool)

	// DeepType returns human readable representation of stored data's type.
	// Example: "untyped constant".
	DeepType() string
	// DeepValue returns human readable representation of stored data's value (without its type).
	// Example: "1".
	DeepValue() string
	// DeepString returns human readable representation of stored data's type and value.
	// Result is in form of "<DeepValue> (type <DeepType>)".
	DeepString() string

	// prevent 3rd party classes from implementing Data
	implementsData()
}

type (
	nilData          struct{}
	regData          reflect.Value        // typed non-const data
	typedConstData   constanth.TypedValue // typed const data
	untypedConstData struct {             // untyped const data
		c constant.Value
	}
	untypedBoolData bool
)

//
//
//
func (nilData) Kind() DataKind          { return Nil }
func (regData) Kind() DataKind          { return Regular }
func (typedConstData) Kind() DataKind   { return TypedConst }
func (untypedConstData) Kind() DataKind { return UntypedConst }
func (untypedBoolData) Kind() DataKind  { return UntypedBool }

//
//	direct access to underlying type
//
func (nilData) Regular() reflect.Value          { panic(`"nil" is not a regular value`) }
func (x regData) Regular() reflect.Value        { return reflect.Value(x) }
func (typedConstData) Regular() reflect.Value   { panic(`typed constant is not a regular value`) }
func (untypedConstData) Regular() reflect.Value { panic(`untyped constant is not a regular value`) }
func (untypedBoolData) Regular() reflect.Value  { panic(`untyped boolean is not a regular value`) }

func (nilData) TypedConst() constanth.TypedValue          { panic(`"nil" is not a typed constant`) }
func (regData) TypedConst() constanth.TypedValue          { panic(`regular value is not a typed constant`) }
func (x typedConstData) TypedConst() constanth.TypedValue { return constanth.TypedValue(x) }
func (untypedConstData) TypedConst() constanth.TypedValue {
	panic(`untyped constant is not a typed constant`)
}
func (untypedBoolData) TypedConst() constanth.TypedValue {
	panic(`untyped boolean is not a typed constant`)
}

func (nilData) UntypedConst() constant.Value { panic(`"nil" is not an untyped constant`) }
func (regData) UntypedConst() constant.Value { panic(`regular value is not an untyped constant`) }
func (typedConstData) UntypedConst() constant.Value {
	panic(`typed constant is not an untyped constant`)
}
func (x untypedConstData) UntypedConst() constant.Value { return x.c }
func (untypedBoolData) UntypedConst() constant.Value {
	panic(`untyped boolean is not an untyped constant`)
}

func (nilData) UntypedBool() bool           { panic(`"nil" is not an untyped boolean`) }
func (regData) UntypedBool() bool           { panic(`regular value is not an untyped boolean`) }
func (typedConstData) UntypedBool() bool    { panic(`typed constant is not an untyped boolean`) }
func (untypedConstData) UntypedBool() bool  { panic(`untyped constant is not an untyped boolean`) }
func (x untypedBoolData) UntypedBool() bool { return bool(x) }

//
//
//
func (nilData) IsConst() bool          { return false }
func (regData) IsConst() bool          { return false }
func (typedConstData) IsConst() bool   { return true }
func (untypedConstData) IsConst() bool { return true }
func (untypedBoolData) IsConst() bool  { return false }

func (nilData) IsTyped() bool          { return false }
func (regData) IsTyped() bool          { return true }
func (typedConstData) IsTyped() bool   { return true }
func (untypedConstData) IsTyped() bool { return false }
func (untypedBoolData) IsTyped() bool  { return false }

//
//	nilData assign & convert
//
func (nilData) AssignableTo(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Slice, reflect.Ptr, reflect.Func, reflect.Interface, reflect.Map, reflect.Chan:
		return true
	}
	return false
}
func (x nilData) MustAssign(t reflect.Type) reflect.Value {
	r, ok := x.Assign(t)
	if ok {
		return r
	}
	panic("unable to assign nil to " + t.String())
}
func (x nilData) Assign(t reflect.Type) (r reflect.Value, ok bool) {
	if x.AssignableTo(t) {
		r = reflect.New(t).Elem()
		ok = true
	}
	return
}
func (x nilData) ConvertibleTo(t reflect.Type) bool { return x.AssignableTo(t) }
func (x nilData) MustConvert(t reflect.Type) Data   { return regData(x.MustAssign(t)) }
func (x nilData) Convert(t reflect.Type) (r Data, ok bool) {
	tmp, ok := x.Assign(t)
	if ok {
		r = regData(tmp)
	}
	return
}

//
//	regData assign & convert
//
func (x regData) Assign(t reflect.Type) (r reflect.Value, ok bool) {
	if ok = x.Regular().Type().AssignableTo(t); ok {
		r = x.MustAssign(t)
	}
	return
}
func (x regData) AssignableTo(t reflect.Type) bool { return x.Regular().Type().AssignableTo(t) }
func (x regData) Convert(t reflect.Type) (r Data, ok bool) {
	if ok = x.Regular().Type().ConvertibleTo(t); ok {
		r = x.MustConvert(t)
	}
	return
}
func (x regData) ConvertibleTo(t reflect.Type) bool { return x.Regular().Type().ConvertibleTo(t) }
func (x regData) MustAssign(t reflect.Type) reflect.Value {
	r := reflect.New(t).Elem()
	r.Set(x.Regular())
	return r
}
func (x regData) MustConvert(t reflect.Type) Data { return regData(x.Regular().Convert(t)) }

//
//	typedConstData assign & convert
//
func (x typedConstData) Assign(t reflect.Type) (r reflect.Value, ok bool) {
	return x.TypedConst().Assign(t)
}
func (x typedConstData) AssignableTo(t reflect.Type) bool { return x.TypedConst().AssignableTo(t) }
func (x typedConstData) Convert(t reflect.Type) (r Data, ok bool) {
	// Need split logic - more details in constanth.TypedValue.Convert.
	switch t.Kind() {
	case reflect.Interface:
		var tmp reflect.Value
		tmp, ok = x.Assign(t)
		if ok {
			r = MakeRegular(tmp)
		}
		return
	default:
		var tmp constanth.TypedValue
		tmp, ok = x.TypedConst().Convert(t)
		if ok {
			r = MakeTypedConst(tmp)
		}
		return
	}
}
func (x typedConstData) ConvertibleTo(t reflect.Type) bool {
	//return x.TypedConst().ConvertibleTo(t) 	// does not work for interfaces
	_, ok := x.Convert(t)
	return ok
}
func (x typedConstData) MustAssign(t reflect.Type) reflect.Value { return x.TypedConst().MustAssign(t) }
func (x typedConstData) MustConvert(t reflect.Type) Data {
	// return typedConstData(x.TypedConst().MustConvert(t))	// does not work for interfaces
	r, ok := x.Convert(t)
	if !ok {
		panic("unable to convert " + x.TypedConst().String() + " to type " + t.String())
	}
	return r
}

//
//	untypedConstData assign & convert
//
func (x untypedConstData) Assign(t reflect.Type) (r reflect.Value, ok bool) {
	return constanth.Assign(x.UntypedConst(), t)
}
func (x untypedConstData) AssignableTo(t reflect.Type) bool {
	return constanth.AssignableTo(x.UntypedConst(), t)
}
func (x untypedConstData) Convert(t reflect.Type) (r Data, ok bool) {
	// Need split logic - more details in constanth.Convert.
	switch t.Kind() {
	case reflect.Interface:
		var tmp reflect.Value
		tmp, ok = constanth.Assign(x.UntypedConst(), t)
		if ok {
			r = MakeRegular(tmp) // untyped constant data after convertation to interface will be regular data
		}
		return
	default:
		var tmp constanth.TypedValue
		tmp, ok = constanth.Convert(x.UntypedConst(), t)
		if ok {
			r = MakeTypedConst(tmp) // untyped constant data after convertation will be typed constant data
		}
		return
	}
}
func (x untypedConstData) ConvertibleTo(t reflect.Type) bool {
	//return constanth.ConvertibleTo(x.UntypedConst(), t)	// does not work for interfaces
	_, ok := x.Convert(t)
	return ok
}
func (x untypedConstData) MustAssign(t reflect.Type) reflect.Value {
	return constanth.MustAssign(x.UntypedConst(), t)
}
func (x untypedConstData) MustConvert(t reflect.Type) Data {
	//return typedConstData(constanth.MustConvert(x.UntypedConst(), t))	// does not work for interfaces
	r, ok := x.Convert(t)
	if !ok {
		panic("unable to convert " + x.UntypedConst().String() + " to type " + t.String())
	}
	return r
}

//
//	untypedBoolData assign & convert
//
func (x untypedBoolData) AssignableTo(t reflect.Type) bool {
	return t.Kind() == reflect.Bool || t == reflecth.TypeEmptyInterface()
}
func (x untypedBoolData) MustAssign(t reflect.Type) reflect.Value {
	r, ok := x.Assign(t)
	if !ok {
		panic("unable to assign " + x.DeepString() + " to type " + t.String())
	}
	return r
}
func (x untypedBoolData) Assign(t reflect.Type) (r reflect.Value, ok bool) {
	ok = x.AssignableTo(t)
	if !ok {
		return
	}

	switch t.Kind() {
	case reflect.Interface:
		r = reflect.New(t).Elem()
		r.Set(x.MustAssign(reflecth.TypeBool()))
		return
	default: // Kind bool
		r = reflect.New(t).Elem()
		r.SetBool(x.UntypedBool())
		return
	}
}
func (x untypedBoolData) ConvertibleTo(t reflect.Type) bool { return x.AssignableTo(t) }
func (x untypedBoolData) MustConvert(t reflect.Type) Data   { return regData(x.MustAssign(t)) }
func (x untypedBoolData) Convert(t reflect.Type) (r Data, ok bool) {
	var rV reflect.Value
	rV, ok = x.Assign(t)
	if ok {
		r = regData(rV)
	}
	return
}

//
//	As int
//
func (nilData) AsInt() (r int, ok bool) { return }
func (x regData) AsInt() (r int, ok bool) {
	xV := x.Regular()
	switch xK := xV.Kind(); {
	case reflecth.IsInt(xK):
		r64 := xV.Int()
		if r64 >= mathh.MinInt && r64 <= mathh.MaxInt {
			r = int(r64)
			ok = true
		}
	case reflecth.IsUint(xK):
		r64 := xV.Uint()
		if r64 <= mathh.MaxInt {
			r = int(r64)
			ok = true
		}
	}
	return
}
func (x typedConstData) AsInt() (r int, ok bool) {
	return constanth.IntVal(x.TypedConst().Untyped())
}
func (x untypedConstData) AsInt() (r int, ok bool) {
	return constanth.IntVal(x.UntypedConst())
}
func (untypedBoolData) AsInt() (r int, ok bool) { return }

func (nilData) DeepType() string            { return "untyped nil" }
func (x regData) DeepType() string          { return x.Regular().Type().String() }
func (x typedConstData) DeepType() string   { return x.TypedConst().Type().String() + " constant" }
func (x untypedConstData) DeepType() string { return "untyped constant" }
func (x untypedBoolData) DeepType() string  { return "untyped bool" }

func (nilData) DeepValue() string            { return "nil" }
func (x regData) DeepValue() string          { return fmt.Sprint(x.Regular()) }
func (x typedConstData) DeepValue() string   { return x.TypedConst().Untyped().ExactString() }
func (x untypedConstData) DeepValue() string { return x.UntypedConst().ExactString() }
func (x untypedBoolData) DeepValue() string  { return strconvh.FormatBool(x.UntypedBool()) }

func (nilData) DeepString() string            { return "untyped nil" }
func (x regData) DeepString() string          { return x.DeepValue() + " (type " + x.DeepType() + ")" }
func (x typedConstData) DeepString() string   { return x.DeepValue() + " (type " + x.DeepType() + ")" }
func (x untypedConstData) DeepString() string { return x.DeepValue() + " (type " + x.DeepType() + ")" }
func (x untypedBoolData) DeepString() string  { return x.DeepValue() + " (type " + x.DeepType() + ")" }

//
//
//
func (nilData) implementsData()          {}
func (regData) implementsData()          {}
func (typedConstData) implementsData()   {}
func (untypedConstData) implementsData() {}
func (untypedBoolData) implementsData()  {}

//
//
//

// MakeNil makes Data which stores "nil".
func MakeNil() Data { return nilData{} }

// MakeRegular makes Data which stores regular typed variable x (x is a reflect.Value of required variable).
func MakeRegular(x reflect.Value) Data { return regData(x) }

// MakeRegularInterface makes Data which stores regular typed variable x (x is a required variable).
func MakeRegularInterface(x interface{}) Data { return MakeRegular(reflect.ValueOf(x)) }

// MakeTypedConst makes Data which stores typed constant x.
func MakeTypedConst(x constanth.TypedValue) Data { return typedConstData(x) }

// MakeUntypedConst makes Data which stores untyped constant x.
func MakeUntypedConst(x constant.Value) Data { return untypedConstData{x} }

// MakeUntypedBool makes Data which stores untyped boolean variable with value x.
func MakeUntypedBool(x bool) Data { return untypedBoolData(x) }
