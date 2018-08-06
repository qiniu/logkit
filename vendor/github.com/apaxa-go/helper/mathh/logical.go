package mathh

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE
//replacer:replace
//replacer:old int64	Int64
//replacer:new int	Int
//replacer:new int8	Int8
//replacer:new int16	Int16
//replacer:new int32	Int32

// BtoInt64 does false=>0, true=>1.
func BtoInt64(b bool) int64 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotInt64 does 0=>1, 1=>0.
func NotInt64(i int64) int64 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeInt64 checks if i<0.
func NegativeInt64(i int64) int64 {
	return (i >> (Int64Bits - 1)) * -1

	//if i < 0 {
	//	return 1
	//}
	//return 0
}

// NotNegativeInt64 checks if i>=0.
func NotNegativeInt64(i int64) int64 {
	return NotInt64(NegativeInt64(i))

	//if i >= 0 {
	//	return 1
	//}
	//return 0
}

// PositiveInt64 checks if i>0.
func PositiveInt64(i int64) int64 {
	return NotInt64((NegativeInt64(i) | ZeroInt64(i)))

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveInt64 checks if i<=0.
func NotPositiveInt64(i int64) int64 {
	return NotInt64(PositiveInt64(i))

	//if i <= 0 {
	//	return 1
	//}
	//return 0
}

// ZeroInt64 checks if i=0.
func ZeroInt64(i int64) int64 {
	return (i|-i)>>(Int64Bits-1) + 1

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// NotZeroInt64 checks if i<>0.
func NotZeroInt64(i int64) int64 {
	return NotInt64(ZeroInt64(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignInt64 returns:
//   -1 if i<0,
//    0 if i=0,
//    1 if i>0.
func SignInt64(i int64) int64 {
	return PositiveInt64(i) - NegativeInt64(i)

	//if i < 0 {
	//	return -1
	//} else if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignInt64 returns 0 if one of passed number >0 and another <0. Otherwise it returns 1.
//    SameSignInt64(-100, 5)=0
//    SameSignInt64(5, -100)=0
//    SameSignInt64(-100, 0)=1
//    SameSignInt64(50, 100)=1
//    SameSignInt64(-5, -10)=1
func SameSignInt64(a, b int64) int64 {
	//return (SignInt64(a)^SignInt64(b))/2 + 1

	if (a < 0 && b > 0) || (a > 0 && b < 0) {
		return 0
	}
	return 1
}

// NotSameSignInt64 returns 1 if one of passed number >0 and another <0. Otherwise ot returns 0. See SameSignInt64 for examples.
func NotSameSignInt64(a, b int64) int64 {
	return NotInt64(SameSignInt64(a, b))
}

// EqualInt64 checks if a=b.
func EqualInt64(a, b int64) int64 {
	return ZeroInt64(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualInt64 checks if a<>b.
func NotEqualInt64(a, b int64) int64 {
	return NotInt64(EqualInt64(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterInt64 checks if a>b.
func GreaterInt64(a, b int64) int64 {
	return BtoInt64(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterInt64 checks if a<=b.
func NotGreaterInt64(a, b int64) int64 {
	return NotInt64(GreaterInt64(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessInt64 checks if a<b.
func LessInt64(a, b int64) int64 {
	return BtoInt64(a < b) // Looks better when in other function

	//return GreaterInt64(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessInt64 checks if a<=b.
func NotLessInt64(a, b int64) int64 {
	return NotInt64(LessInt64(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}
