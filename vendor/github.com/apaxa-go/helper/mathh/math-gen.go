//replacer:generated-file

package mathh

// AbsInt returns absolute value of passed integer.
// It has a bug for MinInt (because MinInt * -1 = MinInt), see AbsFixInt for resolution.
func AbsInt(i int) int {
	return (-2*NegativeInt(i) + 1) * i

	//if i < 0 {
	//	return -i
	//}
	//return i
}

// AbsFixInt is like AbsInt but for MinInt it returns MaxInt.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func AbsFixInt(i int) int {
	return MaxInt*EqualInt(i, MinInt) + AbsInt(i)*NotEqualInt(i, MinInt)

	//if i == MinInt {
	//	return MaxInt
	//}
	//return AbsInt(i)
}

// AntiAbsInt returns -i if i>0. Otherwise it returns i as is.
func AntiAbsInt(i int) int {
	//return (-2*PositiveInt(i) + 1) * i

	if i > 0 {
		return -i
	}
	return i
}

// DivideRoundInt divides a to b and round result to nearest.
//   -3 / -2 =  2
//   -3 /  2 = -2
// It has a bug if a=MinInt and b=-1 (because MinInt/-1 = MinInt), see DivideRoundFixInt for resolution.
func DivideRoundInt(a, b int) (c int) {
	c = a / b
	delta := AntiAbsInt(a % b)
	if b < 0 && delta < (b+1)/2 {
		if a > 0 {
			c--
			return
		}
		c++
		return
	}
	if b > 0 && delta < (-b+1)/2 {
		if a < 0 {
			c--
			return
		}
		c++
		return
	}
	return

	//return a/b + LessInt(AntiAbsInt(a%b), (AntiAbsInt(b)+1)/2)*(SameSignInt(a, b)*2-1)
}

// DivideRoundFixInt is like DivideRoundInt but for a=MinInt and b=-1 it returns MaxInt.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRoundFixInt(a, b int) int {
	if a == MinInt && b == -1 {
		return MaxInt
	}
	return DivideRoundInt(a, b)
}

// DivideCeilInt divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// -3 / -2 =  2
// -3 /  2 = -1
// It has a bug if a=MinInt and b=-1 (because MinInt/-1 = MinInt), see DivideCeilFixInt for resolution.
func DivideCeilInt(a, b int) int {
	return a/b + NotZeroInt(a%b)*SameSignInt(a, b)
}

// DivideCeilFixInt is like DivideCeilInt but for a=MinInt and b=-1 it returns MaxInt.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideCeilFixInt(a, b int) int {
	if a == MinInt && b == -1 {
		return MaxInt
	}
	return DivideCeilInt(a, b)
}

// DivideFloorInt divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// -3 / -2 =  1
// -3 /  2 = -2
// It has a bug if a=MinInt and b=-1 (because MinInt/-1 = MinInt), see DivideFloorFixInt for resolution.
func DivideFloorInt(a, b int) int {
	return a/b - NotZeroInt(a%b)*NotSameSignInt(a, b)
}

// DivideFloorFixInt is like DivideFloorInt but for a=MinInt and b=-1 it returns MaxInt.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideFloorFixInt(a, b int) int {
	if a == MinInt && b == -1 {
		return MaxInt
	}
	return DivideFloorInt(a, b)
}

// DivideRafzInt divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// -3 / -2 =  2
// -3 /  2 = -2
// It has a bug if a=MinInt and b=-1 (because MinInt/-1 = MinInt), see DivideRafzFixInt for resolution.
func DivideRafzInt(a, b int) int {
	return a/b + SignInt(a%b)*SignInt(b)
}

// DivideRafzFixInt is like DivideRafzInt but for a=MinInt and b=-1 it returns MaxInt.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRafzFixInt(a, b int) int {
	if a == MinInt && b == -1 {
		return MaxInt
	}
	return DivideRafzInt(a, b)
}

// DivideTruncInt is just a/b.
// A.k.a. round away from infinity, round towards zero.
// It has a bug if a=MinInt and b=-1 (because MinInt/-1 = MinInt), see DivideTruncFixInt for resolution.
func DivideTruncInt(a, b int) int {
	return a / b
}

// DivideTruncFixInt is like DivideTruncInt but for a=MinInt and b=-1 it returns MaxInt.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideTruncFixInt(a, b int) int {
	if a == MinInt && b == -1 {
		return MaxInt
	}
	return DivideTruncInt(a, b)
}

// AbsInt8 returns absolute value of passed integer.
// It has a bug for MinInt8 (because MinInt8 * -1 = MinInt8), see AbsFixInt8 for resolution.
func AbsInt8(i int8) int8 {
	return (-2*NegativeInt8(i) + 1) * i

	//if i < 0 {
	//	return -i
	//}
	//return i
}

// AbsFixInt8 is like AbsInt8 but for MinInt8 it returns MaxInt8.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func AbsFixInt8(i int8) int8 {
	return MaxInt8*EqualInt8(i, MinInt8) + AbsInt8(i)*NotEqualInt8(i, MinInt8)

	//if i == MinInt8 {
	//	return MaxInt8
	//}
	//return AbsInt8(i)
}

// AntiAbsInt8 returns -i if i>0. Otherwise it returns i as is.
func AntiAbsInt8(i int8) int8 {
	//return (-2*PositiveInt8(i) + 1) * i

	if i > 0 {
		return -i
	}
	return i
}

// DivideRoundInt8 divides a to b and round result to nearest.
//   -3 / -2 =  2
//   -3 /  2 = -2
// It has a bug if a=MinInt8 and b=-1 (because MinInt8/-1 = MinInt8), see DivideRoundFixInt8 for resolution.
func DivideRoundInt8(a, b int8) (c int8) {
	c = a / b
	delta := AntiAbsInt8(a % b)
	if b < 0 && delta < (b+1)/2 {
		if a > 0 {
			c--
			return
		}
		c++
		return
	}
	if b > 0 && delta < (-b+1)/2 {
		if a < 0 {
			c--
			return
		}
		c++
		return
	}
	return

	//return a/b + LessInt8(AntiAbsInt8(a%b), (AntiAbsInt8(b)+1)/2)*(SameSignInt8(a, b)*2-1)
}

// DivideRoundFixInt8 is like DivideRoundInt8 but for a=MinInt8 and b=-1 it returns MaxInt8.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRoundFixInt8(a, b int8) int8 {
	if a == MinInt8 && b == -1 {
		return MaxInt8
	}
	return DivideRoundInt8(a, b)
}

// DivideCeilInt8 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// -3 / -2 =  2
// -3 /  2 = -1
// It has a bug if a=MinInt8 and b=-1 (because MinInt8/-1 = MinInt8), see DivideCeilFixInt8 for resolution.
func DivideCeilInt8(a, b int8) int8 {
	return a/b + NotZeroInt8(a%b)*SameSignInt8(a, b)
}

// DivideCeilFixInt8 is like DivideCeilInt8 but for a=MinInt8 and b=-1 it returns MaxInt8.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideCeilFixInt8(a, b int8) int8 {
	if a == MinInt8 && b == -1 {
		return MaxInt8
	}
	return DivideCeilInt8(a, b)
}

// DivideFloorInt8 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// -3 / -2 =  1
// -3 /  2 = -2
// It has a bug if a=MinInt8 and b=-1 (because MinInt8/-1 = MinInt8), see DivideFloorFixInt8 for resolution.
func DivideFloorInt8(a, b int8) int8 {
	return a/b - NotZeroInt8(a%b)*NotSameSignInt8(a, b)
}

// DivideFloorFixInt8 is like DivideFloorInt8 but for a=MinInt8 and b=-1 it returns MaxInt8.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideFloorFixInt8(a, b int8) int8 {
	if a == MinInt8 && b == -1 {
		return MaxInt8
	}
	return DivideFloorInt8(a, b)
}

// DivideRafzInt8 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// -3 / -2 =  2
// -3 /  2 = -2
// It has a bug if a=MinInt8 and b=-1 (because MinInt8/-1 = MinInt8), see DivideRafzFixInt8 for resolution.
func DivideRafzInt8(a, b int8) int8 {
	return a/b + SignInt8(a%b)*SignInt8(b)
}

// DivideRafzFixInt8 is like DivideRafzInt8 but for a=MinInt8 and b=-1 it returns MaxInt8.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRafzFixInt8(a, b int8) int8 {
	if a == MinInt8 && b == -1 {
		return MaxInt8
	}
	return DivideRafzInt8(a, b)
}

// DivideTruncInt8 is just a/b.
// A.k.a. round away from infinity, round towards zero.
// It has a bug if a=MinInt8 and b=-1 (because MinInt8/-1 = MinInt8), see DivideTruncFixInt8 for resolution.
func DivideTruncInt8(a, b int8) int8 {
	return a / b
}

// DivideTruncFixInt8 is like DivideTruncInt8 but for a=MinInt8 and b=-1 it returns MaxInt8.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideTruncFixInt8(a, b int8) int8 {
	if a == MinInt8 && b == -1 {
		return MaxInt8
	}
	return DivideTruncInt8(a, b)
}

// AbsInt16 returns absolute value of passed integer.
// It has a bug for MinInt16 (because MinInt16 * -1 = MinInt16), see AbsFixInt16 for resolution.
func AbsInt16(i int16) int16 {
	return (-2*NegativeInt16(i) + 1) * i

	//if i < 0 {
	//	return -i
	//}
	//return i
}

// AbsFixInt16 is like AbsInt16 but for MinInt16 it returns MaxInt16.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func AbsFixInt16(i int16) int16 {
	return MaxInt16*EqualInt16(i, MinInt16) + AbsInt16(i)*NotEqualInt16(i, MinInt16)

	//if i == MinInt16 {
	//	return MaxInt16
	//}
	//return AbsInt16(i)
}

// AntiAbsInt16 returns -i if i>0. Otherwise it returns i as is.
func AntiAbsInt16(i int16) int16 {
	//return (-2*PositiveInt16(i) + 1) * i

	if i > 0 {
		return -i
	}
	return i
}

// DivideRoundInt16 divides a to b and round result to nearest.
//   -3 / -2 =  2
//   -3 /  2 = -2
// It has a bug if a=MinInt16 and b=-1 (because MinInt16/-1 = MinInt16), see DivideRoundFixInt16 for resolution.
func DivideRoundInt16(a, b int16) (c int16) {
	c = a / b
	delta := AntiAbsInt16(a % b)
	if b < 0 && delta < (b+1)/2 {
		if a > 0 {
			c--
			return
		}
		c++
		return
	}
	if b > 0 && delta < (-b+1)/2 {
		if a < 0 {
			c--
			return
		}
		c++
		return
	}
	return

	//return a/b + LessInt16(AntiAbsInt16(a%b), (AntiAbsInt16(b)+1)/2)*(SameSignInt16(a, b)*2-1)
}

// DivideRoundFixInt16 is like DivideRoundInt16 but for a=MinInt16 and b=-1 it returns MaxInt16.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRoundFixInt16(a, b int16) int16 {
	if a == MinInt16 && b == -1 {
		return MaxInt16
	}
	return DivideRoundInt16(a, b)
}

// DivideCeilInt16 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// -3 / -2 =  2
// -3 /  2 = -1
// It has a bug if a=MinInt16 and b=-1 (because MinInt16/-1 = MinInt16), see DivideCeilFixInt16 for resolution.
func DivideCeilInt16(a, b int16) int16 {
	return a/b + NotZeroInt16(a%b)*SameSignInt16(a, b)
}

// DivideCeilFixInt16 is like DivideCeilInt16 but for a=MinInt16 and b=-1 it returns MaxInt16.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideCeilFixInt16(a, b int16) int16 {
	if a == MinInt16 && b == -1 {
		return MaxInt16
	}
	return DivideCeilInt16(a, b)
}

// DivideFloorInt16 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// -3 / -2 =  1
// -3 /  2 = -2
// It has a bug if a=MinInt16 and b=-1 (because MinInt16/-1 = MinInt16), see DivideFloorFixInt16 for resolution.
func DivideFloorInt16(a, b int16) int16 {
	return a/b - NotZeroInt16(a%b)*NotSameSignInt16(a, b)
}

// DivideFloorFixInt16 is like DivideFloorInt16 but for a=MinInt16 and b=-1 it returns MaxInt16.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideFloorFixInt16(a, b int16) int16 {
	if a == MinInt16 && b == -1 {
		return MaxInt16
	}
	return DivideFloorInt16(a, b)
}

// DivideRafzInt16 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// -3 / -2 =  2
// -3 /  2 = -2
// It has a bug if a=MinInt16 and b=-1 (because MinInt16/-1 = MinInt16), see DivideRafzFixInt16 for resolution.
func DivideRafzInt16(a, b int16) int16 {
	return a/b + SignInt16(a%b)*SignInt16(b)
}

// DivideRafzFixInt16 is like DivideRafzInt16 but for a=MinInt16 and b=-1 it returns MaxInt16.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRafzFixInt16(a, b int16) int16 {
	if a == MinInt16 && b == -1 {
		return MaxInt16
	}
	return DivideRafzInt16(a, b)
}

// DivideTruncInt16 is just a/b.
// A.k.a. round away from infinity, round towards zero.
// It has a bug if a=MinInt16 and b=-1 (because MinInt16/-1 = MinInt16), see DivideTruncFixInt16 for resolution.
func DivideTruncInt16(a, b int16) int16 {
	return a / b
}

// DivideTruncFixInt16 is like DivideTruncInt16 but for a=MinInt16 and b=-1 it returns MaxInt16.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideTruncFixInt16(a, b int16) int16 {
	if a == MinInt16 && b == -1 {
		return MaxInt16
	}
	return DivideTruncInt16(a, b)
}

// AbsInt32 returns absolute value of passed integer.
// It has a bug for MinInt32 (because MinInt32 * -1 = MinInt32), see AbsFixInt32 for resolution.
func AbsInt32(i int32) int32 {
	return (-2*NegativeInt32(i) + 1) * i

	//if i < 0 {
	//	return -i
	//}
	//return i
}

// AbsFixInt32 is like AbsInt32 but for MinInt32 it returns MaxInt32.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func AbsFixInt32(i int32) int32 {
	return MaxInt32*EqualInt32(i, MinInt32) + AbsInt32(i)*NotEqualInt32(i, MinInt32)

	//if i == MinInt32 {
	//	return MaxInt32
	//}
	//return AbsInt32(i)
}

// AntiAbsInt32 returns -i if i>0. Otherwise it returns i as is.
func AntiAbsInt32(i int32) int32 {
	//return (-2*PositiveInt32(i) + 1) * i

	if i > 0 {
		return -i
	}
	return i
}

// DivideRoundInt32 divides a to b and round result to nearest.
//   -3 / -2 =  2
//   -3 /  2 = -2
// It has a bug if a=MinInt32 and b=-1 (because MinInt32/-1 = MinInt32), see DivideRoundFixInt32 for resolution.
func DivideRoundInt32(a, b int32) (c int32) {
	c = a / b
	delta := AntiAbsInt32(a % b)
	if b < 0 && delta < (b+1)/2 {
		if a > 0 {
			c--
			return
		}
		c++
		return
	}
	if b > 0 && delta < (-b+1)/2 {
		if a < 0 {
			c--
			return
		}
		c++
		return
	}
	return

	//return a/b + LessInt32(AntiAbsInt32(a%b), (AntiAbsInt32(b)+1)/2)*(SameSignInt32(a, b)*2-1)
}

// DivideRoundFixInt32 is like DivideRoundInt32 but for a=MinInt32 and b=-1 it returns MaxInt32.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRoundFixInt32(a, b int32) int32 {
	if a == MinInt32 && b == -1 {
		return MaxInt32
	}
	return DivideRoundInt32(a, b)
}

// DivideCeilInt32 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// -3 / -2 =  2
// -3 /  2 = -1
// It has a bug if a=MinInt32 and b=-1 (because MinInt32/-1 = MinInt32), see DivideCeilFixInt32 for resolution.
func DivideCeilInt32(a, b int32) int32 {
	return a/b + NotZeroInt32(a%b)*SameSignInt32(a, b)
}

// DivideCeilFixInt32 is like DivideCeilInt32 but for a=MinInt32 and b=-1 it returns MaxInt32.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideCeilFixInt32(a, b int32) int32 {
	if a == MinInt32 && b == -1 {
		return MaxInt32
	}
	return DivideCeilInt32(a, b)
}

// DivideFloorInt32 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// -3 / -2 =  1
// -3 /  2 = -2
// It has a bug if a=MinInt32 and b=-1 (because MinInt32/-1 = MinInt32), see DivideFloorFixInt32 for resolution.
func DivideFloorInt32(a, b int32) int32 {
	return a/b - NotZeroInt32(a%b)*NotSameSignInt32(a, b)
}

// DivideFloorFixInt32 is like DivideFloorInt32 but for a=MinInt32 and b=-1 it returns MaxInt32.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideFloorFixInt32(a, b int32) int32 {
	if a == MinInt32 && b == -1 {
		return MaxInt32
	}
	return DivideFloorInt32(a, b)
}

// DivideRafzInt32 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// -3 / -2 =  2
// -3 /  2 = -2
// It has a bug if a=MinInt32 and b=-1 (because MinInt32/-1 = MinInt32), see DivideRafzFixInt32 for resolution.
func DivideRafzInt32(a, b int32) int32 {
	return a/b + SignInt32(a%b)*SignInt32(b)
}

// DivideRafzFixInt32 is like DivideRafzInt32 but for a=MinInt32 and b=-1 it returns MaxInt32.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideRafzFixInt32(a, b int32) int32 {
	if a == MinInt32 && b == -1 {
		return MaxInt32
	}
	return DivideRafzInt32(a, b)
}

// DivideTruncInt32 is just a/b.
// A.k.a. round away from infinity, round towards zero.
// It has a bug if a=MinInt32 and b=-1 (because MinInt32/-1 = MinInt32), see DivideTruncFixInt32 for resolution.
func DivideTruncInt32(a, b int32) int32 {
	return a / b
}

// DivideTruncFixInt32 is like DivideTruncInt32 but for a=MinInt32 and b=-1 it returns MaxInt32.
// It is not arithmetically correct but in some cases it is much better than default behaviour.
func DivideTruncFixInt32(a, b int32) int32 {
	if a == MinInt32 && b == -1 {
		return MaxInt32
	}
	return DivideTruncInt32(a, b)
}
