//replacer:generated-file

package strconvh

import "strconv"

// FormatInt returns the string representation of i in the 10-base.
func FormatInt(i int) string {
	return strconv.FormatInt(int64(i), defaultIntegerBase)
}

// FormatInt8 returns the string representation of i in the 10-base.
func FormatInt8(i int8) string {
	return strconv.FormatInt(int64(i), defaultIntegerBase)
}

// FormatInt16 returns the string representation of i in the 10-base.
func FormatInt16(i int16) string {
	return strconv.FormatInt(int64(i), defaultIntegerBase)
}

// FormatInt64 returns the string representation of i in the 10-base.
func FormatInt64(i int64) string {
	return strconv.FormatInt(int64(i), defaultIntegerBase)
}

// FormatUint returns the string representation of i in the 10-base.
func FormatUint(i uint) string {
	return strconv.FormatUint(uint64(i), defaultIntegerBase)
}

// FormatUint8 returns the string representation of i in the 10-base.
func FormatUint8(i uint8) string {
	return strconv.FormatUint(uint64(i), defaultIntegerBase)
}

// FormatUint16 returns the string representation of i in the 10-base.
func FormatUint16(i uint16) string {
	return strconv.FormatUint(uint64(i), defaultIntegerBase)
}

// FormatUint32 returns the string representation of i in the 10-base.
func FormatUint32(i uint32) string {
	return strconv.FormatUint(uint64(i), defaultIntegerBase)
}

// FormatUint64 returns the string representation of i in the 10-base.
func FormatUint64(i uint64) string {
	return strconv.FormatUint(uint64(i), defaultIntegerBase)
}
