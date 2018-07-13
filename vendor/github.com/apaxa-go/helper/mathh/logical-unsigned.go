package mathh

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE
//replacer:replace
//replacer:old uint64	Uint64
//replacer:new uint	Uint
//replacer:new uint8	Uint8
//replacer:new uint16	Uint16
//replacer:new uint32	Uint32

// BtoUint64 does false=>0, true=>1.
func BtoUint64(b bool) uint64 {
	//*(*byte)(unsafe.Pointer(&i)) = *(*byte)(unsafe.Pointer(&b))
	//return

	if b {
		return 1
	}
	return 0
}

// NotUint64 does 0=>1, 1=>0.
func NotUint64(i uint64) uint64 {
	return i ^ 1

	//if i==0 {
	//	return 1
	//}
	//return 0
}

// NegativeUint64 always returns 0.
func NegativeUint64(i uint64) uint64 {
	return 0
}

// NotNegativeUint64 always return 1.
func NotNegativeUint64(i uint64) uint64 {
	return 1
}

// PositiveUint64 checks if i>0.
func PositiveUint64(i uint64) uint64 {
	return NotZeroUint64(i)

	//if i > 0 {
	//	return 1
	//}
	//return 0
}

// NotPositiveUint64 checks if i=0.
func NotPositiveUint64(i uint64) uint64 {
	return ZeroUint64(i)

	//if i == 0 {
	//	return 1
	//}
	//return 0
}

// ZeroUint64 checks if i=0.
func ZeroUint64(i uint64) uint64 {
	if i == 0 {
		return 1
	}
	return 0
}

// NotZeroUint64 checks if i>0.
func NotZeroUint64(i uint64) uint64 {
	return NotUint64(ZeroUint64(i))

	//if i != 0 {
	//	return 1
	//}
	//return 0
}

// SignUint64 returns:
//    0 if i=0,
//    1 if i>0.
func SignUint64(i uint64) uint64 {
	return PositiveUint64(i)

	//if i == 0 {
	//	return 0
	//}
	//return 1
}

// SameSignUint64 always returns 1.
func SameSignUint64(a, b uint64) uint64 {
	return 1
}

// NotSameSignUint64 always returns 0.
func NotSameSignUint64(a, b uint64) uint64 {
	return 0
}

// EqualUint64 checks if a=b.
func EqualUint64(a, b uint64) uint64 {
	return ZeroUint64(a ^ b)

	//if a == b {
	//	return 1
	//}
	//return 0
}

// NotEqualUint64 checks if a<>b.
func NotEqualUint64(a, b uint64) uint64 {
	return NotUint64(EqualUint64(a, b))

	//if a!=b {
	//	return 1
	//}
	//return 0
}

// GreaterUint64 checks if a>b.
func GreaterUint64(a, b uint64) uint64 {
	return BtoUint64(a > b)

	//if a > b {
	//	return 1
	//}
	//return 0
}

// NotGreaterUint64 checks if a<=b.
func NotGreaterUint64(a, b uint64) uint64 {
	return NotUint64(GreaterUint64(a, b))

	//if a <= b {
	//	return 1
	//}
	//return 0
}

// LessUint64 checks if a<b.
func LessUint64(a, b uint64) uint64 {
	return BtoUint64(a < b) // Looks better when in other function

	//return GreaterUint64(b, a)

	//if a < b {
	//	return 1
	//}
	//return 0
}

// NotLessUint64 checks if a<=b.
func NotLessUint64(a, b uint64) uint64 {
	return NotUint64(LessUint64(a, b))

	//if a >= b {
	//	return 1
	//}
	//return 0
}
