//replacer:generated-file

package mathh

// PowUint returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowUint(a, b uint) uint {
	p := uint(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModUint computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModUint(a, b, m uint) uint {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowUint8 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowUint8(a, b uint8) uint8 {
	p := uint8(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModUint8 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModUint8(a, b, m uint8) uint8 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowUint16 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowUint16(a, b uint16) uint16 {
	p := uint16(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModUint16 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModUint16(a, b, m uint16) uint16 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowUint32 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowUint32(a, b uint32) uint32 {
	p := uint32(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModUint32 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModUint32(a, b, m uint32) uint32 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowInt returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowInt(a, b int) int {
	p := int(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModInt computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModInt(a, b, m int) int {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowInt8 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowInt8(a, b int8) int8 {
	p := int8(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModInt8 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModInt8(a, b, m int8) int8 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowInt16 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowInt16(a, b int16) int16 {
	p := int16(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModInt16 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModInt16(a, b, m int16) int16 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowInt32 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowInt32(a, b int32) int32 {
	p := int32(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModInt32 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModInt32(a, b, m int32) int32 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}

// PowInt64 returns a**b (a raised to power b).
// Warning: where is no any check for overflow.
func PowInt64(a, b int64) int64 {
	p := int64(1)
	for b > 0 {
		if b&1 != 0 {
			p *= a
		}
		b >>= 1
		a *= a
	}
	return p
}

// PowModInt64 computes a**b mod m (modular integer power) using binary powering algorithm.
func PowModInt64(a, b, m int64) int64 {
	a = a % m
	p := 1 % m
	for b > 0 {
		if b&1 != 0 {
			p = (p * a) % m
		}
		b >>= 1
		a = (a * a) % m
	}
	return p
}
