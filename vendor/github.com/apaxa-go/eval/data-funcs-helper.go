package eval

import (
	"github.com/apaxa-go/helper/goh/constanth"
	"github.com/apaxa-go/helper/reflecth"
	"go/constant"
	"go/token"
	"reflect"
)

func binaryOpRegular(x reflect.Value, op token.Token, y reflect.Value) (r Data, err *intError) {
	if rV, errE := reflecth.BinaryOp(x, op, y); errE == nil {
		r = regData(rV)
	} else {
		err = toIntError(errE)
	}
	return
}
func compareOpRegular(x reflect.Value, op token.Token, y reflect.Value) (r bool, err *intError) {
	var errE error
	r, errE = reflecth.CompareOp(x, op, y)
	err = toIntError(errE)
	return
}
func shiftOpRegular(x reflect.Value, op token.Token, s uint) (r Data, err *intError) {
	if rV, errE := reflecth.ShiftOp(x, op, s); errE == nil {
		r = regData(rV)
	} else {
		err = toIntError(errE)
	}
	return
}
func unaryOpRegular(op token.Token, y reflect.Value) (r Data, err *intError) {
	if rV, errE := reflecth.UnaryOp(op, y); errE == nil {
		r = regData(rV)
	} else {
		err = toIntError(errE)
	}
	return
}
func binaryOpTypedConst(x constanth.TypedValue, op token.Token, y constanth.TypedValue) (r Data, err *intError) {
	if rTC, errE := constanth.BinaryOpTyped(x, op, y); errE == nil {
		r = typedConstData(rTC)
	} else {
		err = toIntError(errE)
	}
	return
}
func compareOpTypedConst(x constanth.TypedValue, op token.Token, y constanth.TypedValue) (r bool, err *intError) {
	var errE error
	r, errE = constanth.CompareOpTyped(x, op, y)
	err = toIntError(errE)
	return
}
func shiftOpTypedConst(x constanth.TypedValue, op token.Token, s uint) (r constanth.TypedValue, err *intError) {
	var errE error
	r, errE = constanth.ShiftOpTyped(x, op, s)
	err = toIntError(errE)
	return
}
func unaryOpTypedConst(op token.Token, y constanth.TypedValue, prec uint) (r Data, err *intError) {
	if rTC, errE := constanth.UnaryOpTyped(op, y, prec); errE == nil {
		r = typedConstData(rTC)
	} else {
		err = toIntError(errE)
	}
	return
}
func binaryOpUntypedConst(x constant.Value, op token.Token, y constant.Value) (r Data, err *intError) {
	if rC, errE := constanth.BinaryOp(x, op, y); errE == nil {
		r = untypedConstData{rC}
	} else {
		err = toIntError(errE)
	}
	return
}
func compareOpUntypedConst(x constant.Value, op token.Token, y constant.Value) (r bool, err *intError) {
	var errE error
	r, errE = constanth.CompareOp(x, op, y)
	err = toIntError(errE)
	return
}
func shiftOpUntypedConst(x constant.Value, op token.Token, s uint) (r constant.Value, err *intError) {
	var errE error
	r, errE = constanth.ShiftOp(x, op, s)
	err = toIntError(errE)
	return
}
func unaryOpUntypedConst(op token.Token, y constant.Value, prec uint) (r Data, err *intError) {
	if rC, errE := constanth.UnaryOp(op, y, prec); errE == nil {
		r = untypedConstData{rC}
	} else {
		err = toIntError(errE)
	}
	return
}
