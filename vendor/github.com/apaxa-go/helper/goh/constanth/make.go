package constanth

import (
	"go/constant"
)

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

import "go/token"

// MakeComplex64 returns the Complex constant.Value for x.
func MakeComplex64(x complex64) constant.Value {
	return MakeComplex128(complex128(x))
}

// MakeComplex128 returns the Complex constant.Value for x.
func MakeComplex128(x complex128) constant.Value {
	return constant.BinaryOp(constant.MakeFloat64(real(x)), token.ADD, constant.MakeImag(constant.MakeFloat64(imag(x))))
}

//replacer:replace
//replacer:old uint32	Uint32	Uint64	uint64
//replacer:new uint	Uint	Uint64	uint64
//replacer:new uint8	Uint8	Uint64	uint64
//replacer:new uint16	Uint16	Uint64	uint64
//replacer:new uint64	Uint64	Uint64	uint64
//replacer:new int	Int	Int64	int64
//replacer:new int8	Int8	Int64	int64
//replacer:new int16	Int16	Int64	int64
//replacer:new int32	Int32	Int64	int64
//replacer:new int64	Int64	Int64	int64
//replacer:new float32	Float32	Float64	float64
//replacer:new float64	Float64	Float64	float64
//replacer:new bool	Bool	Bool	bool
//replacer:new string	String	String	string

// MakeUint32 returns the Int value for x.
func MakeUint32(x uint32) constant.Value {
	return constant.MakeUint64(uint64(x))
}
