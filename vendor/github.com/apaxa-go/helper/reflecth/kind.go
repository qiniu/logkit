package reflecth

import "reflect"

// IsInt returns true if k is any signed integer kind.
func IsInt(k reflect.Kind) bool {
	return k == reflect.Int || k == reflect.Int8 || k == reflect.Int16 || k == reflect.Int32 || k == reflect.Int64
}

// IsUint returns true if k is any unsigned integer kind.
func IsUint(k reflect.Kind) bool {
	return k == reflect.Uint || k == reflect.Uint8 || k == reflect.Uint16 || k == reflect.Uint32 || k == reflect.Uint64
}

// IsAnyInt returns true if k is any integer kind (signed or unsigned).
func IsAnyInt(k reflect.Kind) bool {
	return IsInt(k) || IsUint(k)
}

// IsFloat returns true if k is any float kind.
func IsFloat(k reflect.Kind) bool {
	return k == reflect.Float32 || k == reflect.Float64
}

// IsComplex returns true if k is any complex kind.
func IsComplex(k reflect.Kind) bool {
	return k == reflect.Complex64 || k == reflect.Complex128
}
