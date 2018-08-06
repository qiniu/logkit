package reflecth

import (
	"go/token"
	"reflect"
)

// UnaryOp performs unary operation <op><y> as Go language specification describes.
// Supported operations: + - ^ ! & <- .
// If operation cannot be performed then error will be returned.
// Special note for token.AND (&) operation: if passed y is not addressable (reflect.Value.CanAddr) then new variable will be created with the same as y type and value and address of new variable will be returned.
func UnaryOp(op token.Token, y reflect.Value) (r reflect.Value, err error) {
	switch op {
	case token.SUB:
		return unaryOpSub(y)
	case token.XOR:
		return unaryOpXor(y)
	case token.NOT:
		return unaryOpNot(y)
	case token.AND:
		return unaryOpAnd(y)
	case token.ARROW:
		return unaryOpArrow(y)
	case token.ADD:
		if k := y.Kind(); IsAnyInt(k) || IsFloat(k) || IsComplex(k) {
			return y, nil
		}
		fallthrough
	default:
		return reflect.Value{}, unaryOpError(y, op)
	}
}

func unaryOpAnd(x reflect.Value) (r reflect.Value, err error) {
	if x.CanAddr() {
		return x.Addr(), nil
	}

	r = reflect.New(x.Type())
	r.Elem().Set(x)
	return
}

func unaryOpSub(x reflect.Value) (r reflect.Value, err error) {
	r = reflect.New(x.Type()).Elem()

	switch k := x.Kind(); {
	case IsInt(k):
		r.SetInt(-x.Int()) // looks like overflow correct (see tests)
	case IsFloat(k):
		r.SetFloat(-x.Float())
	case IsComplex(k):
		r.SetComplex(-x.Complex())
	default:
		return reflect.Value{}, unaryOpError(x, token.SUB)
	}
	return
}

func unaryOpNot(x reflect.Value) (r reflect.Value, err error) {
	if k := x.Kind(); k != reflect.Bool {
		return reflect.Value{}, unaryOpError(x, token.NOT)
	}

	r = reflect.New(x.Type()).Elem()
	r.SetBool(!x.Bool())
	return
}

func unaryOpXor(x reflect.Value) (r reflect.Value, err error) {
	r = reflect.New(x.Type()).Elem()

	switch k := x.Kind(); k {
	case reflect.Int:
		r.SetInt(int64(^int(x.Int())))
	case reflect.Int8:
		r.SetInt(int64(^int8(x.Int())))
	case reflect.Int16:
		r.SetInt(int64(^int16(x.Int())))
	case reflect.Int32:
		r.SetInt(int64(^int32(x.Int())))
	case reflect.Int64:
		r.SetInt(^x.Int())
	case reflect.Uint:
		r.SetUint(uint64(^uint(x.Uint())))
	case reflect.Uint8:
		r.SetUint(uint64(^uint8(x.Uint())))
	case reflect.Uint16:
		r.SetUint(uint64(^uint16(x.Uint())))
	case reflect.Uint32:
		r.SetUint(uint64(^uint32(x.Uint())))
	case reflect.Uint64:
		r.SetUint(^x.Uint())
	default:
		return reflect.Value{}, unaryOpError(x, token.XOR)
	}

	return
}

func unaryOpArrow(x reflect.Value) (r reflect.Value, err error) {
	if x.Kind() != reflect.Chan {
		return reflect.Value{}, unaryOpInvalidReceiverError(x, token.ARROW)
	}
	switch dir := x.Type().ChanDir(); dir {
	case reflect.RecvDir, reflect.BothDir:
		// nothing to do
	default:
		return reflect.Value{}, unaryOpReceiveFromSendOnlyError(x, token.ARROW)
	}
	r, _ = x.Recv()
	return
}
