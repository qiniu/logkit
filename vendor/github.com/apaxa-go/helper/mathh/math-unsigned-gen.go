//replacer:generated-file

package mathh

// DivideRoundUint divides a to b and round result to nearest.
//   3 / 2 =  2
func DivideRoundUint(a, b uint) (c uint) {
	c = a / b
	if a%b > (b-1)/2 {
		c++
	}
	return
}

// DivideCeilUint divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// 3 / 2 =  2
func DivideCeilUint(a, b uint) uint {
	return a/b + NotZeroUint(a%b)
}

// DivideFloorUint divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// 3 / 2 =  1
func DivideFloorUint(a, b uint) uint {
	return a / b
}

// DivideRafzUint divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// 3 / 2 =  2
func DivideRafzUint(a, b uint) uint {
	return a/b + NotZeroUint(a%b)
}

// DivideTruncUint is just a/b.
// A.k.a. round away from infinity, round towards zero.
func DivideTruncUint(a, b uint) uint {
	return a / b
}

// DivideRoundUint8 divides a to b and round result to nearest.
//   3 / 2 =  2
func DivideRoundUint8(a, b uint8) (c uint8) {
	c = a / b
	if a%b > (b-1)/2 {
		c++
	}
	return
}

// DivideCeilUint8 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// 3 / 2 =  2
func DivideCeilUint8(a, b uint8) uint8 {
	return a/b + NotZeroUint8(a%b)
}

// DivideFloorUint8 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// 3 / 2 =  1
func DivideFloorUint8(a, b uint8) uint8 {
	return a / b
}

// DivideRafzUint8 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// 3 / 2 =  2
func DivideRafzUint8(a, b uint8) uint8 {
	return a/b + NotZeroUint8(a%b)
}

// DivideTruncUint8 is just a/b.
// A.k.a. round away from infinity, round towards zero.
func DivideTruncUint8(a, b uint8) uint8 {
	return a / b
}

// DivideRoundUint16 divides a to b and round result to nearest.
//   3 / 2 =  2
func DivideRoundUint16(a, b uint16) (c uint16) {
	c = a / b
	if a%b > (b-1)/2 {
		c++
	}
	return
}

// DivideCeilUint16 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// 3 / 2 =  2
func DivideCeilUint16(a, b uint16) uint16 {
	return a/b + NotZeroUint16(a%b)
}

// DivideFloorUint16 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// 3 / 2 =  1
func DivideFloorUint16(a, b uint16) uint16 {
	return a / b
}

// DivideRafzUint16 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// 3 / 2 =  2
func DivideRafzUint16(a, b uint16) uint16 {
	return a/b + NotZeroUint16(a%b)
}

// DivideTruncUint16 is just a/b.
// A.k.a. round away from infinity, round towards zero.
func DivideTruncUint16(a, b uint16) uint16 {
	return a / b
}

// DivideRoundUint32 divides a to b and round result to nearest.
//   3 / 2 =  2
func DivideRoundUint32(a, b uint32) (c uint32) {
	c = a / b
	if a%b > (b-1)/2 {
		c++
	}
	return
}

// DivideCeilUint32 divide a to b and round result to nearest not less number.
// A.k.a. round up, round towards plus infinity.
// 3 / 2 =  2
func DivideCeilUint32(a, b uint32) uint32 {
	return a/b + NotZeroUint32(a%b)
}

// DivideFloorUint32 divide a to b and round result to nearest not large number.
// A.k.a. round down, round towards minus infinity.
// 3 / 2 =  1
func DivideFloorUint32(a, b uint32) uint32 {
	return a / b
}

// DivideRafzUint32 divide a to b and Round result Away From Zero.
// A.k.a. round towards infinity.
// 3 / 2 =  2
func DivideRafzUint32(a, b uint32) uint32 {
	return a/b + NotZeroUint32(a%b)
}

// DivideTruncUint32 is just a/b.
// A.k.a. round away from infinity, round towards zero.
func DivideTruncUint32(a, b uint32) uint32 {
	return a / b
}
