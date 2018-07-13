package reflecth

import "reflect"

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

// TypeBool returns type of bool.
func TypeBool() reflect.Type { return reflect.TypeOf(false) }

// TypeString returns type of string.
func TypeString() reflect.Type { return reflect.TypeOf("") }

// TypeEmptyInterface returns type of empty interface.
func TypeEmptyInterface() reflect.Type {
	i := interface{}(nil)
	return TypeOfPtr(&i)
}

//replacer:replace
//replacer:old Uint		uint
//replacer:new Uint8		uint8
//replacer:new Uint16		uint16
//replacer:new Uint32		uint32
//replacer:new Uint64		uint64
//replacer:new Int		int
//replacer:new Int8		int8
//replacer:new Int16		int16
//replacer:new Int32		int32
//replacer:new Int64		int64
//replacer:new Float32		float32
//replacer:new Float64		float64
//replacer:new Complex64	complex64
//replacer:new Complex128	complex128
//replacer:new Byte		byte
//replacer:new Rune		rune
//replacer:new Uintptr		uintptr

// TypeUint returns type of uint.
func TypeUint() reflect.Type { return reflect.TypeOf(uint(0)) }
