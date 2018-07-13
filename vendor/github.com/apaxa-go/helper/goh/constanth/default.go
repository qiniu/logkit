package constanth

import (
	"github.com/apaxa-go/helper/reflecth"
	"go/constant"
	"reflect"
)

// DefaultValueInterface does the same thing as DefaultValue but returns interface instead of reflect.Value.
func DefaultValueInterface(x constant.Value) (r interface{}, ok bool) {
	switch k := x.Kind(); k {
	case constant.Bool:
		return constant.BoolVal(x), true
	case constant.String:
		return constant.StringVal(x), true
	case constant.Int:
		return IntVal(x)
	case constant.Float:
		return Float64Val(x)
	case constant.Complex:
		return Complex128Val(x)
	default:
		return nil, false
	}
}

// DefaultTypeForKind returns default type for specified constant's kind k (as Go specification describes).
// Result is nil if k is Unknown kind.
func DefaultTypeForKind(k constant.Kind) reflect.Type {
	switch k {
	case constant.Bool:
		return reflecth.TypeBool()
	case constant.String:
		return reflecth.TypeString()
	case constant.Int:
		return reflecth.TypeInt()
	case constant.Float:
		return reflecth.TypeFloat64()
	case constant.Complex:
		return reflecth.TypeComplex128()
	default:
		return nil
	}
}

// DefaultType returns default type for specified constant x (as Go specification describes).
// There is no guarantee what constant x can be represented as returned type.
func DefaultType(x constant.Value) reflect.Type { return DefaultTypeForKind(x.Kind()) }

// DefaultValue returns reflect.Value equivalent of passed constant x.
// Type of result is the default type for constant x.
// If constant can not be represented as variable with constant's default type then ok will be false.
func DefaultValue(x constant.Value) (r reflect.Value, ok bool) {
	rI, ok := DefaultValueInterface(x)
	if ok {
		r = reflect.ValueOf(rI)
	}
	return
}
