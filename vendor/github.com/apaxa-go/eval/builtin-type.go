package eval

import (
	"github.com/apaxa-go/helper/reflecth"
	"reflect"
)

func isBuiltInType(ident string) bool {
	_, ok := builtInTypes[ident]
	return ok
}

var builtInTypes = map[string]reflect.Type{
	"bool": reflecth.TypeBool(),

	//////////////// Numeric ////////////////
	"uint":   reflecth.TypeUint(),
	"uint8":  reflecth.TypeUint8(),
	"uint16": reflecth.TypeUint16(),
	"uint32": reflecth.TypeUint32(),
	"uint64": reflecth.TypeUint64(),

	"int":   reflecth.TypeInt(),
	"int8":  reflecth.TypeInt8(),
	"int16": reflecth.TypeInt16(),
	"int32": reflecth.TypeInt32(),
	"int64": reflecth.TypeInt64(),

	"float32": reflecth.TypeFloat32(),
	"float64": reflecth.TypeFloat64(),

	"complex64":  reflecth.TypeComplex64(),
	"complex128": reflecth.TypeComplex128(),

	"byte": reflecth.TypeByte(),
	"rune": reflecth.TypeRune(),

	"uintptr": reflecth.TypeUintptr(),
	/////////////////////////////////////////

	"string": reflecth.TypeString(),
}

// In some places required specific types. This variables allow to avoid using types map.
var (
	bytesSliceT = reflect.SliceOf(reflect.TypeOf(byte(0)))
)
