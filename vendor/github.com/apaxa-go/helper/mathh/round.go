package mathh

// Round64 returns nearest int64 for given float64.
//
// Deprecated: Use RoundFloat64To* instead.
func Round64(f float64) int64 {
	switch {
	case f < 0.5 && f > -0.5:
		return 0
	case f > 0:
		return int64(f + 0.5)
	default:
		return int64(f - 0.5)
	}
}

// Round32 returns nearest int64 for given float32.
//
// Deprecated: Use RoundFloat32To* instead.
func Round32(f float32) int64 {
	switch {
	case f < 0.5 && f > -0.5:
		return 0
	case f > 0:
		return int64(f + 0.5)
	default:
		return int64(f - 0.5)
	}
}
