//replacer:generated-file

package mathh

// RoundFloat32ToInt returns nearest int for given float32.
func RoundFloat32ToInt(f float32) int {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < MinInt+d:
		return MinInt
	case f > MaxInt-d:
		return MaxInt
	case f < d && f > -d:
		return 0
	case f > 0:
		return int(f + d)
	default:
		return int(f - d)
	}
}

// RoundFloat32ToInt8 returns nearest int8 for given float32.
func RoundFloat32ToInt8(f float32) int8 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < MinInt8+d:
		return MinInt8
	case f > MaxInt8-d:
		return MaxInt8
	case f < d && f > -d:
		return 0
	case f > 0:
		return int8(f + d)
	default:
		return int8(f - d)
	}
}

// RoundFloat32ToInt16 returns nearest int16 for given float32.
func RoundFloat32ToInt16(f float32) int16 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < MinInt16+d:
		return MinInt16
	case f > MaxInt16-d:
		return MaxInt16
	case f < d && f > -d:
		return 0
	case f > 0:
		return int16(f + d)
	default:
		return int16(f - d)
	}
}

// RoundFloat32ToInt32 returns nearest int32 for given float32.
func RoundFloat32ToInt32(f float32) int32 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < MinInt32+d:
		return MinInt32
	case f > MaxInt32-d:
		return MaxInt32
	case f < d && f > -d:
		return 0
	case f > 0:
		return int32(f + d)
	default:
		return int32(f - d)
	}
}

// RoundFloat64ToInt64 returns nearest int64 for given float64.
func RoundFloat64ToInt64(f float64) int64 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < MinInt64+d:
		return MinInt64
	case f > MaxInt64-d:
		return MaxInt64
	case f < d && f > -d:
		return 0
	case f > 0:
		return int64(f + d)
	default:
		return int64(f - d)
	}
}

// RoundFloat64ToInt returns nearest int for given float64.
func RoundFloat64ToInt(f float64) int {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < MinInt+d:
		return MinInt
	case f > MaxInt-d:
		return MaxInt
	case f < d && f > -d:
		return 0
	case f > 0:
		return int(f + d)
	default:
		return int(f - d)
	}
}

// RoundFloat64ToInt8 returns nearest int8 for given float64.
func RoundFloat64ToInt8(f float64) int8 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < MinInt8+d:
		return MinInt8
	case f > MaxInt8-d:
		return MaxInt8
	case f < d && f > -d:
		return 0
	case f > 0:
		return int8(f + d)
	default:
		return int8(f - d)
	}
}

// RoundFloat64ToInt16 returns nearest int16 for given float64.
func RoundFloat64ToInt16(f float64) int16 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < MinInt16+d:
		return MinInt16
	case f > MaxInt16-d:
		return MaxInt16
	case f < d && f > -d:
		return 0
	case f > 0:
		return int16(f + d)
	default:
		return int16(f - d)
	}
}

// RoundFloat64ToInt32 returns nearest int32 for given float64.
func RoundFloat64ToInt32(f float64) int32 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < MinInt32+d:
		return MinInt32
	case f > MaxInt32-d:
		return MaxInt32
	case f < d && f > -d:
		return 0
	case f > 0:
		return int32(f + d)
	default:
		return int32(f - d)
	}
}

// RoundFloat32ToUint returns nearest int64 for given float32.
func RoundFloat32ToUint(f float32) uint {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint-d:
		return MaxUint
	default:
		return uint(f + d)
	}
}

// RoundFloat32ToUint8 returns nearest int64 for given float32.
func RoundFloat32ToUint8(f float32) uint8 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint8-d:
		return MaxUint8
	default:
		return uint8(f + d)
	}
}

// RoundFloat32ToUint16 returns nearest int64 for given float32.
func RoundFloat32ToUint16(f float32) uint16 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint16-d:
		return MaxUint16
	default:
		return uint16(f + d)
	}
}

// RoundFloat32ToUint32 returns nearest int64 for given float32.
func RoundFloat32ToUint32(f float32) uint32 {
	const d = 0.5
	switch {
	case IsNaNFloat32(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint32-d:
		return MaxUint32
	default:
		return uint32(f + d)
	}
}

// RoundFloat64ToUint64 returns nearest int64 for given float64.
func RoundFloat64ToUint64(f float64) uint64 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint64-d:
		return MaxUint64
	default:
		return uint64(f + d)
	}
}

// RoundFloat64ToUint returns nearest int64 for given float64.
func RoundFloat64ToUint(f float64) uint {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint-d:
		return MaxUint
	default:
		return uint(f + d)
	}
}

// RoundFloat64ToUint8 returns nearest int64 for given float64.
func RoundFloat64ToUint8(f float64) uint8 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint8-d:
		return MaxUint8
	default:
		return uint8(f + d)
	}
}

// RoundFloat64ToUint16 returns nearest int64 for given float64.
func RoundFloat64ToUint16(f float64) uint16 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint16-d:
		return MaxUint16
	default:
		return uint16(f + d)
	}
}

// RoundFloat64ToUint32 returns nearest int64 for given float64.
func RoundFloat64ToUint32(f float64) uint32 {
	const d = 0.5
	switch {
	case IsNaNFloat64(f):
		return 0
	case f < 0:
		return 0
	case f > MaxUint32-d:
		return MaxUint32
	default:
		return uint32(f + d)
	}
}
