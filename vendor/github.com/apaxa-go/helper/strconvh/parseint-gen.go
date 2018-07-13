//replacer:generated-file

package strconvh

import (
	"github.com/apaxa-go/helper/mathh"
	"strconv"
)

// ParseUint64 interprets a string s in 10-base and returns the corresponding value i (uint64) and error.
func ParseUint64(stringValue string) (uint64, error) {
	return strconv.ParseUint(stringValue, defaultIntegerBase, mathh.Uint64Bits)
}

// ParseInt interprets a string s in 10-base and returns the corresponding value i (int) and error.
func ParseInt(stringValue string) (i int, err error) {
	value64, err := strconv.ParseInt(stringValue, defaultIntegerBase, mathh.IntBits)
	if err == nil {
		i = int(value64)
	}
	return
}

// ParseInt8 interprets a string s in 10-base and returns the corresponding value i (int8) and error.
func ParseInt8(stringValue string) (i int8, err error) {
	value64, err := strconv.ParseInt(stringValue, defaultIntegerBase, mathh.Int8Bits)
	if err == nil {
		i = int8(value64)
	}
	return
}

// ParseInt16 interprets a string s in 10-base and returns the corresponding value i (int16) and error.
func ParseInt16(stringValue string) (i int16, err error) {
	value64, err := strconv.ParseInt(stringValue, defaultIntegerBase, mathh.Int16Bits)
	if err == nil {
		i = int16(value64)
	}
	return
}

// ParseUint interprets a string s in 10-base and returns the corresponding value i (uint) and error.
func ParseUint(stringValue string) (i uint, err error) {
	value64, err := strconv.ParseUint(stringValue, defaultIntegerBase, mathh.UintBits)
	if err == nil {
		i = uint(value64)
	}
	return
}

// ParseUint8 interprets a string s in 10-base and returns the corresponding value i (uint8) and error.
func ParseUint8(stringValue string) (i uint8, err error) {
	value64, err := strconv.ParseUint(stringValue, defaultIntegerBase, mathh.Uint8Bits)
	if err == nil {
		i = uint8(value64)
	}
	return
}

// ParseUint16 interprets a string s in 10-base and returns the corresponding value i (uint16) and error.
func ParseUint16(stringValue string) (i uint16, err error) {
	value64, err := strconv.ParseUint(stringValue, defaultIntegerBase, mathh.Uint16Bits)
	if err == nil {
		i = uint16(value64)
	}
	return
}

// ParseUint32 interprets a string s in 10-base and returns the corresponding value i (uint32) and error.
func ParseUint32(stringValue string) (i uint32, err error) {
	value64, err := strconv.ParseUint(stringValue, defaultIntegerBase, mathh.Uint32Bits)
	if err == nil {
		i = uint32(value64)
	}
	return
}
