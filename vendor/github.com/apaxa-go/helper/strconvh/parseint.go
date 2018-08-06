package strconvh

import (
	"github.com/apaxa-go/helper/mathh"
	"strconv"
)

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

const defaultIntegerBase = 10

//replacer:replace
//replacer:old int64	Int64	strconv.ParseInt
//replacer:new uint64	Uint64	strconv.ParseUint

// ParseInt64 interprets a string s in 10-base and returns the corresponding value i (int64) and error.
func ParseInt64(stringValue string) (int64, error) {
	return strconv.ParseInt(stringValue, defaultIntegerBase, mathh.Int64Bits)
}

//replacer:replace
//replacer:old int32	Int32	strconv.ParseInt
//replacer:new int	Int	strconv.ParseInt
//replacer:new int8	Int8	strconv.ParseInt
//replacer:new int16	Int16	strconv.ParseInt
//replacer:new uint	Uint	strconv.ParseUint
//replacer:new uint8	Uint8	strconv.ParseUint
//replacer:new uint16	Uint16	strconv.ParseUint
//replacer:new uint32	Uint32	strconv.ParseUint

// ParseInt32 interprets a string s in 10-base and returns the corresponding value i (int32) and error.
func ParseInt32(stringValue string) (i int32, err error) {
	value64, err := strconv.ParseInt(stringValue, defaultIntegerBase, mathh.Int32Bits)
	if err == nil {
		i = int32(value64)
	}
	return
}
