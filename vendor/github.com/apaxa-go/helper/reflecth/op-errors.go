package reflecth

import (
	"errors"
	"github.com/apaxa-go/helper/strconvh"
	"go/token"
	"reflect"
)

func opSError(desc string) error {
	return errors.New("invalid operation: " + desc)
}
func binaryOpSError(x, op, y, reason string) error {
	return opSError(x + " " + op + " " + y + ": " + reason)
}
func unaryOpError(x reflect.Value, op token.Token) error {
	return opSError(op.String() + " " + x.Type().String())
}
func unaryOpInvalidReceiverError(x reflect.Value, op token.Token) error {
	return opSError(op.String() + " " + x.Type().String() + ": receive from non-chan type " + x.Type().String())
}
func unaryOpReceiveFromSendOnlyError(x reflect.Value, op token.Token) error {
	return opSError(op.String() + " " + x.Type().String() + ": receive from send-only type " + x.Type().String())
}
func binaryOpError(x reflect.Value, op token.Token, y reflect.Value, reason string) error {
	return binaryOpSError(x.String(), op.String(), y.String(), reason)
}
func shiftOpInvalidShiftTypeError(x reflect.Value, op token.Token, s uint) error {
	return binaryOpSError(x.String(), op.String(), strconvh.FormatUint(s), "shift of type "+x.Type().String())
}
func binaryOpInvalidOperatorSError(x, op, y string) error {
	return binaryOpSError(x, op, y, "invalid operator")
}
func binaryOpInvalidOperatorError(x reflect.Value, op token.Token, y reflect.Value) error {
	return binaryOpInvalidOperatorSError(x.String(), op.String(), y.String())
}
func shiftOpInvalidOperatorError(x reflect.Value, op token.Token, s uint) error {
	return binaryOpInvalidOperatorSError(x.String(), op.String(), strconvh.FormatUint(s))
}
func binaryOpIncomparableError(x reflect.Value, op token.Token, y reflect.Value) error {
	return binaryOpError(x, op, y, "incomparable types "+x.Type().String()+" and "+y.Type().String())
}
func binaryOpUnorderedError(x reflect.Value, op token.Token, y reflect.Value) error {
	return binaryOpError(x, op, y, "unordered types "+x.Type().String()+" and "+y.Type().String())
}
func binaryOpMismatchTypesError(x reflect.Value, op token.Token, y reflect.Value) error {
	return binaryOpError(x, op, y, "mismatched types "+x.Type().String()+" and "+y.Type().String())
}
func invBinOpTypesInvalError(x reflect.Value, op token.Token, y reflect.Value) error {
	return binaryOpError(x, op, y, "invalid types "+x.Type().String()+" and/or "+y.Type().String())
}
