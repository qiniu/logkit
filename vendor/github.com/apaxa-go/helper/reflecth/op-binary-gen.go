//replacer:generated-file

package reflecth

import (
	"github.com/apaxa-go/helper/strconvh"
	"go/token"
)

func binaryOpInt(x int, op token.Token, y int) (r int, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatInt(x), op.String(), strconvh.FormatInt(y))
	}
}

func binaryOpInt8(x int8, op token.Token, y int8) (r int8, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatInt8(x), op.String(), strconvh.FormatInt8(y))
	}
}

func binaryOpInt16(x int16, op token.Token, y int16) (r int16, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatInt16(x), op.String(), strconvh.FormatInt16(y))
	}
}

func binaryOpInt32(x int32, op token.Token, y int32) (r int32, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatInt32(x), op.String(), strconvh.FormatInt32(y))
	}
}

func binaryOpUint(x uint, op token.Token, y uint) (r uint, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatUint(x), op.String(), strconvh.FormatUint(y))
	}
}

func binaryOpUint8(x uint8, op token.Token, y uint8) (r uint8, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatUint8(x), op.String(), strconvh.FormatUint8(y))
	}
}

func binaryOpUint16(x uint16, op token.Token, y uint16) (r uint16, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatUint16(x), op.String(), strconvh.FormatUint16(y))
	}
}

func binaryOpUint32(x uint32, op token.Token, y uint32) (r uint32, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatUint32(x), op.String(), strconvh.FormatUint32(y))
	}
}

func binaryOpUint64(x uint64, op token.Token, y uint64) (r uint64, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	case token.REM:
		return x % y, nil
	case token.AND:
		return x & y, nil
	case token.OR:
		return x | y, nil
	case token.XOR:
		return x ^ y, nil
	case token.AND_NOT:
		return x &^ y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatUint64(x), op.String(), strconvh.FormatUint64(y))
	}
}

func binaryOpFloat64(x float64, op token.Token, y float64) (r float64, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatFloat64(x), op.String(), strconvh.FormatFloat64(y))
	}
}

func binaryOpComplex64(x complex64, op token.Token, y complex64) (r complex64, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatComplex64(x), op.String(), strconvh.FormatComplex64(y))
	}
}

func binaryOpComplex128(x complex128, op token.Token, y complex128) (r complex128, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		return x / y, nil
	default:
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatComplex128(x), op.String(), strconvh.FormatComplex128(y))
	}
}
