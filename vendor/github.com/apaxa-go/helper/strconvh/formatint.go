package strconvh

import "strconv"

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE
//replacer:replace
//replacer:old int32	Int32	strconv.FormatInt(int64
//replacer:new int	Int	strconv.FormatInt(int64
//replacer:new int8	Int8	strconv.FormatInt(int64
//replacer:new int16	Int16	strconv.FormatInt(int64
//replacer:new int64	Int64	strconv.FormatInt(int64
//replacer:new uint	Uint	strconv.FormatUint(uint64
//replacer:new uint8	Uint8	strconv.FormatUint(uint64
//replacer:new uint16	Uint16	strconv.FormatUint(uint64
//replacer:new uint32	Uint32	strconv.FormatUint(uint64
//replacer:new uint64	Uint64	strconv.FormatUint(uint64

// FormatInt32 returns the string representation of i in the 10-base.
func FormatInt32(i int32) string {
	return strconv.FormatInt(int64(i), defaultIntegerBase)
}
