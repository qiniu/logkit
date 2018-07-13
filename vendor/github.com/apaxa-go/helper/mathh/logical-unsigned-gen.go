//replacer:generated-file

package mathh

// BtoUint does false=>0, true=>1.
func BtoUint(b bool) uint {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotUint does 0=>1, 1=>0.
func NotUint(i uint) uint {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeUint always returns 0.
func NegativeUint(i uint) uint {
	return 0
}

// NotNegativeUint always return 1.
func NotNegativeUint(i uint) uint {
	return 1
}

// PositiveUint checks if i>0.
func PositiveUint(i uint) uint {
	return NotZeroUint(i)

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveUint checks if i=0.
func NotPositiveUint(i uint) uint {
	return ZeroUint(i)

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// ZeroUint checks if i=0.
func ZeroUint(i uint) uint {
	if i == 0 {
		return 1
	}
	return 0
}

// NotZeroUint checks if i>0.
func NotZeroUint(i uint) uint {
	return NotUint(ZeroUint(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignUint returns:
//    0 if i=0,
//    1 if i>0.
func SignUint(i uint) uint {
	return PositiveUint(i)

	//if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignUint always returns 1.
func SameSignUint(a, b uint) uint {
	return 1
}

// NotSameSignUint always returns 0.
func NotSameSignUint(a, b uint) uint {
	return 0
}

// EqualUint checks if a=b.
func EqualUint(a, b uint) uint {
	return ZeroUint(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualUint checks if a<>b.
func NotEqualUint(a, b uint) uint {
	return NotUint(EqualUint(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterUint checks if a>b.
func GreaterUint(a, b uint) uint {
	return BtoUint(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterUint checks if a<=b.
func NotGreaterUint(a, b uint) uint {
	return NotUint(GreaterUint(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessUint checks if a<b.
func LessUint(a, b uint) uint {
	return BtoUint(a < b) // Looks better when in other function

	//return GreaterUint(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessUint checks if a<=b.
func NotLessUint(a, b uint) uint {
	return NotUint(LessUint(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}

// BtoUint8 does false=>0, true=>1.
func BtoUint8(b bool) uint8 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotUint8 does 0=>1, 1=>0.
func NotUint8(i uint8) uint8 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeUint8 always returns 0.
func NegativeUint8(i uint8) uint8 {
	return 0
}

// NotNegativeUint8 always return 1.
func NotNegativeUint8(i uint8) uint8 {
	return 1
}

// PositiveUint8 checks if i>0.
func PositiveUint8(i uint8) uint8 {
	return NotZeroUint8(i)

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveUint8 checks if i=0.
func NotPositiveUint8(i uint8) uint8 {
	return ZeroUint8(i)

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// ZeroUint8 checks if i=0.
func ZeroUint8(i uint8) uint8 {
	if i == 0 {
		return 1
	}
	return 0
}

// NotZeroUint8 checks if i>0.
func NotZeroUint8(i uint8) uint8 {
	return NotUint8(ZeroUint8(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignUint8 returns:
//    0 if i=0,
//    1 if i>0.
func SignUint8(i uint8) uint8 {
	return PositiveUint8(i)

	//if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignUint8 always returns 1.
func SameSignUint8(a, b uint8) uint8 {
	return 1
}

// NotSameSignUint8 always returns 0.
func NotSameSignUint8(a, b uint8) uint8 {
	return 0
}

// EqualUint8 checks if a=b.
func EqualUint8(a, b uint8) uint8 {
	return ZeroUint8(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualUint8 checks if a<>b.
func NotEqualUint8(a, b uint8) uint8 {
	return NotUint8(EqualUint8(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterUint8 checks if a>b.
func GreaterUint8(a, b uint8) uint8 {
	return BtoUint8(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterUint8 checks if a<=b.
func NotGreaterUint8(a, b uint8) uint8 {
	return NotUint8(GreaterUint8(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessUint8 checks if a<b.
func LessUint8(a, b uint8) uint8 {
	return BtoUint8(a < b) // Looks better when in other function

	//return GreaterUint8(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessUint8 checks if a<=b.
func NotLessUint8(a, b uint8) uint8 {
	return NotUint8(LessUint8(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}

// BtoUint16 does false=>0, true=>1.
func BtoUint16(b bool) uint16 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotUint16 does 0=>1, 1=>0.
func NotUint16(i uint16) uint16 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeUint16 always returns 0.
func NegativeUint16(i uint16) uint16 {
	return 0
}

// NotNegativeUint16 always return 1.
func NotNegativeUint16(i uint16) uint16 {
	return 1
}

// PositiveUint16 checks if i>0.
func PositiveUint16(i uint16) uint16 {
	return NotZeroUint16(i)

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveUint16 checks if i=0.
func NotPositiveUint16(i uint16) uint16 {
	return ZeroUint16(i)

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// ZeroUint16 checks if i=0.
func ZeroUint16(i uint16) uint16 {
	if i == 0 {
		return 1
	}
	return 0
}

// NotZeroUint16 checks if i>0.
func NotZeroUint16(i uint16) uint16 {
	return NotUint16(ZeroUint16(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignUint16 returns:
//    0 if i=0,
//    1 if i>0.
func SignUint16(i uint16) uint16 {
	return PositiveUint16(i)

	//if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignUint16 always returns 1.
func SameSignUint16(a, b uint16) uint16 {
	return 1
}

// NotSameSignUint16 always returns 0.
func NotSameSignUint16(a, b uint16) uint16 {
	return 0
}

// EqualUint16 checks if a=b.
func EqualUint16(a, b uint16) uint16 {
	return ZeroUint16(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualUint16 checks if a<>b.
func NotEqualUint16(a, b uint16) uint16 {
	return NotUint16(EqualUint16(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterUint16 checks if a>b.
func GreaterUint16(a, b uint16) uint16 {
	return BtoUint16(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterUint16 checks if a<=b.
func NotGreaterUint16(a, b uint16) uint16 {
	return NotUint16(GreaterUint16(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessUint16 checks if a<b.
func LessUint16(a, b uint16) uint16 {
	return BtoUint16(a < b) // Looks better when in other function

	//return GreaterUint16(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessUint16 checks if a<=b.
func NotLessUint16(a, b uint16) uint16 {
	return NotUint16(LessUint16(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}

// BtoUint32 does false=>0, true=>1.
func BtoUint32(b bool) uint32 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotUint32 does 0=>1, 1=>0.
func NotUint32(i uint32) uint32 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeUint32 always returns 0.
func NegativeUint32(i uint32) uint32 {
	return 0
}

// NotNegativeUint32 always return 1.
func NotNegativeUint32(i uint32) uint32 {
	return 1
}

// PositiveUint32 checks if i>0.
func PositiveUint32(i uint32) uint32 {
	return NotZeroUint32(i)

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveUint32 checks if i=0.
func NotPositiveUint32(i uint32) uint32 {
	return ZeroUint32(i)

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// ZeroUint32 checks if i=0.
func ZeroUint32(i uint32) uint32 {
	if i == 0 {
		return 1
	}
	return 0
}

// NotZeroUint32 checks if i>0.
func NotZeroUint32(i uint32) uint32 {
	return NotUint32(ZeroUint32(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignUint32 returns:
//    0 if i=0,
//    1 if i>0.
func SignUint32(i uint32) uint32 {
	return PositiveUint32(i)

	//if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignUint32 always returns 1.
func SameSignUint32(a, b uint32) uint32 {
	return 1
}

// NotSameSignUint32 always returns 0.
func NotSameSignUint32(a, b uint32) uint32 {
	return 0
}

// EqualUint32 checks if a=b.
func EqualUint32(a, b uint32) uint32 {
	return ZeroUint32(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualUint32 checks if a<>b.
func NotEqualUint32(a, b uint32) uint32 {
	return NotUint32(EqualUint32(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterUint32 checks if a>b.
func GreaterUint32(a, b uint32) uint32 {
	return BtoUint32(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterUint32 checks if a<=b.
func NotGreaterUint32(a, b uint32) uint32 {
	return NotUint32(GreaterUint32(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessUint32 checks if a<b.
func LessUint32(a, b uint32) uint32 {
	return BtoUint32(a < b) // Looks better when in other function

	//return GreaterUint32(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessUint32 checks if a<=b.
func NotLessUint32(a, b uint32) uint32 {
	return NotUint32(LessUint32(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}
