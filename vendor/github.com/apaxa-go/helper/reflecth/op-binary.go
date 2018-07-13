package reflecth

import (
	"github.com/apaxa-go/helper/strconvh"
	"go/token"
)

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

import "reflect"

//replacer:ignore
//go:generate go run $GOPATH/src/github.com/apaxa-go/generator/replacer/main.go -- $GOFILE

// BinaryOp performs binary operation <x><op><y> as Go language specification describes.
// Supported operations: && || + - * / % & | ^ &^ .
// If operation cannot be performed then error will be returned.
func BinaryOp(x reflect.Value, op token.Token, y reflect.Value) (r reflect.Value, err error) {
	// Basic check
	if xT, yT := x.Type(), y.Type(); !xT.AssignableTo(yT) && !yT.AssignableTo(xT) {
		return reflect.Value{}, binaryOpMismatchTypesError(x, op, y)
	}

	r = reflect.New(x.Type()).Elem()
	switch x.Kind() {
	case reflect.Bool:
		v, err := binaryOpBool(x.Bool(), op, y.Bool())
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetBool(v)
	case reflect.Int:
		v, err := binaryOpInt(int(x.Int()), op, int(y.Int()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetInt(int64(v))
	case reflect.Int8:
		v, err := binaryOpInt8(int8(x.Int()), op, int8(y.Int()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetInt(int64(v))
	case reflect.Int16:
		v, err := binaryOpInt16(int16(x.Int()), op, int16(y.Int()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetInt(int64(v))
	case reflect.Int32:
		v, err := binaryOpInt32(int32(x.Int()), op, int32(y.Int()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetInt(int64(v))
	case reflect.Int64:
		v, err := binaryOpInt64(x.Int(), op, y.Int())
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetInt(v)
	case reflect.Uint:
		v, err := binaryOpUint(uint(x.Uint()), op, uint(y.Uint()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetUint(uint64(v))
	case reflect.Uint8:
		v, err := binaryOpUint8(uint8(x.Uint()), op, uint8(y.Uint()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetUint(uint64(v))
	case reflect.Uint16:
		v, err := binaryOpUint16(uint16(x.Uint()), op, uint16(y.Uint()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetUint(uint64(v))
	case reflect.Uint32:
		v, err := binaryOpUint32(uint32(x.Uint()), op, uint32(y.Uint()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetUint(uint64(v))
	case reflect.Uint64:
		v, err := binaryOpUint64(x.Uint(), op, y.Uint())
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetUint(v)
	case reflect.Float32:
		v, err := binaryOpFloat32(float32(x.Float()), op, float32(y.Float()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetFloat(float64(v))
	case reflect.Float64:
		v, err := binaryOpFloat64(x.Float(), op, y.Float())
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetFloat(v)
	case reflect.Complex64:
		v, err := binaryOpComplex64(complex64(x.Complex()), op, complex64(y.Complex()))
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetComplex(complex128(v))
	case reflect.Complex128:
		v, err := binaryOpComplex128(x.Complex(), op, y.Complex())
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetComplex(v)
	case reflect.String:
		v, err := binaryOpString(x.String(), op, y.String())
		if err != nil {
			return reflect.Value{}, err
		}
		r.SetString(v)
	default:
		return reflect.Value{}, invBinOpTypesInvalError(x, op, y)
	}
	return
}

func binaryOpBool(x bool, op token.Token, y bool) (r bool, err error) {
	switch op {
	case token.LAND:
		return x && y, nil
	case token.LOR:
		return x || y, nil
	default:
		return false, binaryOpInvalidOperatorSError(strconvh.FormatBool(x), op.String(), strconvh.FormatBool(y))
	}
}

func binaryOpString(x string, op token.Token, y string) (r string, err error) {
	switch op {
	case token.ADD:
		return x + y, nil
	default:
		return "", binaryOpInvalidOperatorSError(x, op.String(), y)
	}
}

//replacer:replace
//replacer:old int64	Int64
//replacer:new int	Int
//replacer:new int8	Int8
//replacer:new int16	Int16
//replacer:new int32	Int32
//replacer:new uint	Uint
//replacer:new uint8	Uint8
//replacer:new uint16	Uint16
//replacer:new uint32	Uint32
//replacer:new uint64	Uint64

func binaryOpInt64(x int64, op token.Token, y int64) (r int64, err error) {
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
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatInt64(x), op.String(), strconvh.FormatInt64(y))
	}
}

//replacer:replace
//replacer:old float32		Float32
//replacer:new float64		Float64
//replacer:new complex64	Complex64
//replacer:new complex128	Complex128

func binaryOpFloat32(x float32, op token.Token, y float32) (r float32, err error) {
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
		return 0, binaryOpInvalidOperatorSError(strconvh.FormatFloat32(x), op.String(), strconvh.FormatFloat32(y))
	}
}
