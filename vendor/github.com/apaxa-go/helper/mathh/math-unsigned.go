package mathh

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE
//replacer:replace
//replacer:old uint64	Uint64
//replacer:new uint	Uint
//replacer:new uint8	Uint8
//replacer:new uint16	Uint16
//replacer:new uint32	Uint32

// DivideRoundUint64 divides a to b and round result to nearest.
//   3 / 2 =  2
func DivideRoundUint64(a, b uint64) (c uint64) {
	c = a / b
	if a%b > (b-1)/2 {
		c++
	}
	return
}

// DivideCeilUint64 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// 3 / 2 =  2
func DivideCeilUint64(a, b uint64) uint64 {
	return a/b + NotZeroUint64(a%b)
}

// DivideFloorUint64 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// 3 / 2 =  1
func DivideFloorUint64(a, b uint64) uint64 {
	return a / b
}

// DivideRafzUint64 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// 3 / 2 =  2
func DivideRafzUint64(a, b uint64) uint64 {
	return a/b + NotZeroUint64(a%b)
}

// DivideTruncUint64 is just a/b.
// A.k.a. round away from infinity, round towards zero.
func DivideTruncUint64(a, b uint64) uint64 {
	return a / b
}
