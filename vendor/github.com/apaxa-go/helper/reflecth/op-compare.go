package reflecth

import (
	"github.com/apaxa-go/helper/goh/tokenh"
	"github.com/apaxa-go/helper/strconvh"
	"go/token"
	"reflect"
)

// CompareOp performs compare operation <x><op><y> as Go language specification describes.
// Supported operations: < <= >= > == != .
// If operation cannot be performed then error will be returned.
func CompareOp(x reflect.Value, op token.Token, y reflect.Value) (r bool, err error) {
	// Basic check
	if xT, yT := x.Type(), y.Type(); !xT.AssignableTo(yT) && !yT.AssignableTo(xT) {
		return false, binaryOpIncomparableError(x, op, y)
	}

	// Choose compare
	switch {
	case tokenh.IsEqualityCheck(op):
		if xT, yT := x.Type(), y.Type(); !xT.Comparable() || !yT.Comparable() {
			return false, binaryOpIncomparableError(x, op, y)
		}
		equality := op == token.EQL
		r = x.Interface() == y.Interface() == equality
		return
	case tokenh.IsOrderCheck(op):
		switch k := x.Kind(); {
		case IsInt(k):
			return compareOpInt(x.Int(), op, y.Int())
		case IsUint(k):
			return compareOpUint(x.Uint(), op, y.Uint())
		case IsFloat(k):
			return compareOpFloat(x.Float(), op, y.Float())
		case k == reflect.String:
			return compareOpString(x.String(), op, y.String())
		default:
			return false, binaryOpUnorderedError(x, op, y)
		}
	default:
		return false, binaryOpInvalidOperatorError(x, op, y)
	}
}

func compareOpInt(x int64, op token.Token, y int64) (r bool, err error) {
	switch op {
	case token.LSS:
		return x < y, nil
	case token.LEQ:
		return x <= y, nil
	case token.GTR:
		return x > y, nil
	case token.GEQ:
		return x >= y, nil
	default:
		return false, binaryOpInvalidOperatorSError(strconvh.FormatInt64(x), op.String(), strconvh.FormatInt64(x))
	}
}

func compareOpUint(x uint64, op token.Token, y uint64) (r bool, err error) {
	switch op {
	case token.LSS:
		return x < y, nil
	case token.LEQ:
		return x <= y, nil
	case token.GTR:
		return x > y, nil
	case token.GEQ:
		return x >= y, nil
	default:
		return false, binaryOpInvalidOperatorSError(strconvh.FormatUint64(x), op.String(), strconvh.FormatUint64(x))
	}
}

func compareOpFloat(x float64, op token.Token, y float64) (r bool, err error) {
	switch op {
	case token.LSS:
		return x < y, nil
	case token.LEQ:
		return x <= y, nil
	case token.GTR:
		return x > y, nil
	case token.GEQ:
		return x >= y, nil
	default:
		return false, binaryOpInvalidOperatorSError(strconvh.FormatFloat64(x), op.String(), strconvh.FormatFloat64(y))
	}
}

func compareOpString(x string, op token.Token, y string) (r bool, err error) {
	switch op {
	case token.LSS:
		return x < y, nil
	case token.LEQ:
		return x <= y, nil
	case token.GTR:
		return x > y, nil
	case token.GEQ:
		return x >= y, nil
	default:
		return false, binaryOpInvalidOperatorSError(x, op.String(), y)
	}
}
