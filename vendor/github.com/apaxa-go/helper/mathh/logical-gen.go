//replacer:generated-file

package mathh

// BtoInt does false=>0, true=>1.
func BtoInt(b bool) int {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotInt does 0=>1, 1=>0.
func NotInt(i int) int {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeInt checks if i<0.
func NegativeInt(i int) int {
	return (i >> (IntBits - 1)) * -1

	//if i < 0 {
	//	return 1
	//}
	//return 0
}

// NotNegativeInt checks if i>=0.
func NotNegativeInt(i int) int {
	return NotInt(NegativeInt(i))

	//if i >= 0 {
	//	return 1
	//}
	//return 0
}

// PositiveInt checks if i>0.
func PositiveInt(i int) int {
	return NotInt((NegativeInt(i) | ZeroInt(i)))

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveInt checks if i<=0.
func NotPositiveInt(i int) int {
	return NotInt(PositiveInt(i))

	//if i <= 0 {
	//	return 1
	//}
	//return 0
}

// ZeroInt checks if i=0.
func ZeroInt(i int) int {
	return (i|-i)>>(IntBits-1) + 1

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// NotZeroInt checks if i<>0.
func NotZeroInt(i int) int {
	return NotInt(ZeroInt(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignInt returns:
//   -1 if i<0,
//    0 if i=0,
//    1 if i>0.
func SignInt(i int) int {
	return PositiveInt(i) - NegativeInt(i)

	//if i < 0 {
	//	return -1
	//} else if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignInt returns 0 if one of passed number >0 and another <0. Otherwise it returns 1.
//    SameSignInt(-100, 5)=0
//    SameSignInt(5, -100)=0
//    SameSignInt(-100, 0)=1
//    SameSignInt(50, 100)=1
//    SameSignInt(-5, -10)=1
func SameSignInt(a, b int) int {
	//return (SignInt(a)^SignInt(b))/2 + 1

	if (a < 0 && b > 0) || (a > 0 && b < 0) {
		return 0
	}
	return 1
}

// NotSameSignInt returns 1 if one of passed number >0 and another <0. Otherwise ot returns 0. See SameSignInt for examples.
func NotSameSignInt(a, b int) int {
	return NotInt(SameSignInt(a, b))
}

// EqualInt checks if a=b.
func EqualInt(a, b int) int {
	return ZeroInt(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualInt checks if a<>b.
func NotEqualInt(a, b int) int {
	return NotInt(EqualInt(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterInt checks if a>b.
func GreaterInt(a, b int) int {
	return BtoInt(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterInt checks if a<=b.
func NotGreaterInt(a, b int) int {
	return NotInt(GreaterInt(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessInt checks if a<b.
func LessInt(a, b int) int {
	return BtoInt(a < b) // Looks better when in other function

	//return GreaterInt(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessInt checks if a<=b.
func NotLessInt(a, b int) int {
	return NotInt(LessInt(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}

// BtoInt8 does false=>0, true=>1.
func BtoInt8(b bool) int8 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotInt8 does 0=>1, 1=>0.
func NotInt8(i int8) int8 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeInt8 checks if i<0.
func NegativeInt8(i int8) int8 {
	return (i >> (Int8Bits - 1)) * -1

	//if i < 0 {
	//	return 1
	//}
	//return 0
}

// NotNegativeInt8 checks if i>=0.
func NotNegativeInt8(i int8) int8 {
	return NotInt8(NegativeInt8(i))

	//if i >= 0 {
	//	return 1
	//}
	//return 0
}

// PositiveInt8 checks if i>0.
func PositiveInt8(i int8) int8 {
	return NotInt8((NegativeInt8(i) | ZeroInt8(i)))

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveInt8 checks if i<=0.
func NotPositiveInt8(i int8) int8 {
	return NotInt8(PositiveInt8(i))

	//if i <= 0 {
	//	return 1
	//}
	//return 0
}

// ZeroInt8 checks if i=0.
func ZeroInt8(i int8) int8 {
	return (i|-i)>>(Int8Bits-1) + 1

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// NotZeroInt8 checks if i<>0.
func NotZeroInt8(i int8) int8 {
	return NotInt8(ZeroInt8(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignInt8 returns:
//   -1 if i<0,
//    0 if i=0,
//    1 if i>0.
func SignInt8(i int8) int8 {
	return PositiveInt8(i) - NegativeInt8(i)

	//if i < 0 {
	//	return -1
	//} else if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignInt8 returns 0 if one of passed number >0 and another <0. Otherwise it returns 1.
//    SameSignInt8(-100, 5)=0
//    SameSignInt8(5, -100)=0
//    SameSignInt8(-100, 0)=1
//    SameSignInt8(50, 100)=1
//    SameSignInt8(-5, -10)=1
func SameSignInt8(a, b int8) int8 {
	//return (SignInt8(a)^SignInt8(b))/2 + 1

	if (a < 0 && b > 0) || (a > 0 && b < 0) {
		return 0
	}
	return 1
}

// NotSameSignInt8 returns 1 if one of passed number >0 and another <0. Otherwise ot returns 0. See SameSignInt8 for examples.
func NotSameSignInt8(a, b int8) int8 {
	return NotInt8(SameSignInt8(a, b))
}

// EqualInt8 checks if a=b.
func EqualInt8(a, b int8) int8 {
	return ZeroInt8(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualInt8 checks if a<>b.
func NotEqualInt8(a, b int8) int8 {
	return NotInt8(EqualInt8(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterInt8 checks if a>b.
func GreaterInt8(a, b int8) int8 {
	return BtoInt8(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterInt8 checks if a<=b.
func NotGreaterInt8(a, b int8) int8 {
	return NotInt8(GreaterInt8(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessInt8 checks if a<b.
func LessInt8(a, b int8) int8 {
	return BtoInt8(a < b) // Looks better when in other function

	//return GreaterInt8(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessInt8 checks if a<=b.
func NotLessInt8(a, b int8) int8 {
	return NotInt8(LessInt8(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}

// BtoInt16 does false=>0, true=>1.
func BtoInt16(b bool) int16 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotInt16 does 0=>1, 1=>0.
func NotInt16(i int16) int16 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeInt16 checks if i<0.
func NegativeInt16(i int16) int16 {
	return (i >> (Int16Bits - 1)) * -1

	//if i < 0 {
	//	return 1
	//}
	//return 0
}

// NotNegativeInt16 checks if i>=0.
func NotNegativeInt16(i int16) int16 {
	return NotInt16(NegativeInt16(i))

	//if i >= 0 {
	//	return 1
	//}
	//return 0
}

// PositiveInt16 checks if i>0.
func PositiveInt16(i int16) int16 {
	return NotInt16((NegativeInt16(i) | ZeroInt16(i)))

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveInt16 checks if i<=0.
func NotPositiveInt16(i int16) int16 {
	return NotInt16(PositiveInt16(i))

	//if i <= 0 {
	//	return 1
	//}
	//return 0
}

// ZeroInt16 checks if i=0.
func ZeroInt16(i int16) int16 {
	return (i|-i)>>(Int16Bits-1) + 1

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// NotZeroInt16 checks if i<>0.
func NotZeroInt16(i int16) int16 {
	return NotInt16(ZeroInt16(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignInt16 returns:
//   -1 if i<0,
//    0 if i=0,
//    1 if i>0.
func SignInt16(i int16) int16 {
	return PositiveInt16(i) - NegativeInt16(i)

	//if i < 0 {
	//	return -1
	//} else if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignInt16 returns 0 if one of passed number >0 and another <0. Otherwise it returns 1.
//    SameSignInt16(-100, 5)=0
//    SameSignInt16(5, -100)=0
//    SameSignInt16(-100, 0)=1
//    SameSignInt16(50, 100)=1
//    SameSignInt16(-5, -10)=1
func SameSignInt16(a, b int16) int16 {
	//return (SignInt16(a)^SignInt16(b))/2 + 1

	if (a < 0 && b > 0) || (a > 0 && b < 0) {
		return 0
	}
	return 1
}

// NotSameSignInt16 returns 1 if one of passed number >0 and another <0. Otherwise ot returns 0. See SameSignInt16 for examples.
func NotSameSignInt16(a, b int16) int16 {
	return NotInt16(SameSignInt16(a, b))
}

// EqualInt16 checks if a=b.
func EqualInt16(a, b int16) int16 {
	return ZeroInt16(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualInt16 checks if a<>b.
func NotEqualInt16(a, b int16) int16 {
	return NotInt16(EqualInt16(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterInt16 checks if a>b.
func GreaterInt16(a, b int16) int16 {
	return BtoInt16(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterInt16 checks if a<=b.
func NotGreaterInt16(a, b int16) int16 {
	return NotInt16(GreaterInt16(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessInt16 checks if a<b.
func LessInt16(a, b int16) int16 {
	return BtoInt16(a < b) // Looks better when in other function

	//return GreaterInt16(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessInt16 checks if a<=b.
func NotLessInt16(a, b int16) int16 {
	return NotInt16(LessInt16(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}

// BtoInt32 does false=>0, true=>1.
func BtoInt32(b bool) int32 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotInt32 does 0=>1, 1=>0.
func NotInt32(i int32) int32 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeInt32 checks if i<0.
func NegativeInt32(i int32) int32 {
	return (i >> (Int32Bits - 1)) * -1

	//if i < 0 {
	//	return 1
	//}
	//return 0
}

// NotNegativeInt32 checks if i>=0.
func NotNegativeInt32(i int32) int32 {
	return NotInt32(NegativeInt32(i))

	//if i >= 0 {
	//	return 1
	//}
	//return 0
}

// PositiveInt32 checks if i>0.
func PositiveInt32(i int32) int32 {
	return NotInt32((NegativeInt32(i) | ZeroInt32(i)))

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveInt32 checks if i<=0.
func NotPositiveInt32(i int32) int32 {
	return NotInt32(PositiveInt32(i))

	//if i <= 0 {
	//	return 1
	//}
	//return 0
}

// ZeroInt32 checks if i=0.
func ZeroInt32(i int32) int32 {
	return (i|-i)>>(Int32Bits-1) + 1

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// NotZeroInt32 checks if i<>0.
func NotZeroInt32(i int32) int32 {
	return NotInt32(ZeroInt32(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignInt32 returns:
//   -1 if i<0,
//    0 if i=0,
//    1 if i>0.
func SignInt32(i int32) int32 {
	return PositiveInt32(i) - NegativeInt32(i)

	//if i < 0 {
	//	return -1
	//} else if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignInt32 returns 0 if one of passed number >0 and another <0. Otherwise it returns 1.
//    SameSignInt32(-100, 5)=0
//    SameSignInt32(5, -100)=0
//    SameSignInt32(-100, 0)=1
//    SameSignInt32(50, 100)=1
//    SameSignInt32(-5, -10)=1
func SameSignInt32(a, b int32) int32 {
	//return (SignInt32(a)^SignInt32(b))/2 + 1

	if (a < 0 && b > 0) || (a > 0 && b < 0) {
		return 0
	}
	return 1
}

// NotSameSignInt32 returns 1 if one of passed number >0 and another <0. Otherwise ot returns 0. See SameSignInt32 for examples.
func NotSameSignInt32(a, b int32) int32 {
	return NotInt32(SameSignInt32(a, b))
}

// EqualInt32 checks if a=b.
func EqualInt32(a, b int32) int32 {
	return ZeroInt32(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualInt32 checks if a<>b.
func NotEqualInt32(a, b int32) int32 {
	return NotInt32(EqualInt32(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterInt32 checks if a>b.
func GreaterInt32(a, b int32) int32 {
	return BtoInt32(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterInt32 checks if a<=b.
func NotGreaterInt32(a, b int32) int32 {
	return NotInt32(GreaterInt32(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessInt32 checks if a<b.
func LessInt32(a, b int32) int32 {
	return BtoInt32(a < b) // Looks better when in other function

	//return GreaterInt32(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessInt32 checks if a<=b.
func NotLessInt32(a, b int32) int32 {
	return NotInt32(LessInt32(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}
