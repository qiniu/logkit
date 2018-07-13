package constanth

import (
	"errors"
	"fmt"
	"github.com/apaxa-go/helper/goh/tokenh"
	"github.com/apaxa-go/helper/strconvh"
	"go/constant"
	"go/token"
)

// CompareOp performs compare operation <x><op><y> on untyped constants as Go language specification describes.
// Supported operations: < <= >= > == != .
// If operation cannot be performed then error will be returned.
func CompareOp(x constant.Value, op token.Token, y constant.Value) (r bool, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprint(rec))
		}
	}()
	r = constant.Compare(x, op, y)
	return
}

// CompareOpTyped performs compare operation <x><op><y> on typed constants as Go language specification describes.
// Supported operations: < <= >= > == != .
// If operation cannot be performed then error will be returned.
func CompareOpTyped(x TypedValue, op token.Token, y TypedValue) (r bool, err error) {
	if x.t != y.t {
		return false, errors.New("unable to compare " + x.t.String() + " " + op.String() + " " + y.t.String() + ": different types")
	}
	if !tokenh.IsComparison(op) {
		return false, errors.New("unable to compare " + x.t.String() + " " + op.String() + " " + y.t.String() + ": " + op.String() + " is not a comparison operator")
	}
	return CompareOp(x.v, op, y.v)
}

// BinaryOp performs binary operation <x><op><y> on untyped constants as Go language specification describes.
// Supported operations: && || + - * / % & | ^ &^ .
// If operation cannot be performed then error will be returned.
func BinaryOp(x constant.Value, op token.Token, y constant.Value) (r constant.Value, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprint(rec))
		}
	}()
	r = constant.BinaryOp(x, op, y)
	if r.Kind() == constant.Unknown {
		err = errors.New("unknown result of " + x.String() + op.String() + y.String())
		r = nil
	}
	return
}

// BinaryOpTyped performs binary operation <x><op><y> on typed constants as Go language specification describes.
// Supported operations: && || + - * / % & | ^ &^ .
// If operation cannot be performed then error will be returned.
func BinaryOpTyped(x TypedValue, op token.Token, y TypedValue) (r TypedValue, err error) {
	if x.t != y.t {
		return TypedValue{}, errors.New("unable to perform binary operation " + x.t.String() + " " + op.String() + " " + y.t.String() + ": different types")
	}
	r.t = x.t
	r.v, err = BinaryOp(x.v, op, y.v)
	if err != nil {
		return TypedValue{}, err
	}
	if !AssignableTo(r.v, r.t) {
		return TypedValue{}, errors.New("unable to perform binary operation " + x.t.String() + " " + op.String() + " " + y.t.String() + ": missmatched result value " + r.v.String() + " and result type " + r.t.String())
	}
	return
}

// UnaryOp performs unary operation <op><y> on untyped constant as Go language specification describes.
// Supported operations: + - ^ ! .
// If operation cannot be performed then error will be returned.
func UnaryOp(op token.Token, y constant.Value, prec uint) (r constant.Value, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprint(rec))
		}
	}()
	r = constant.UnaryOp(op, y, prec)
	if r.Kind() == constant.Unknown {
		err = errors.New("unknown result of " + op.String() + y.String())
		r = nil
	}
	return
}

// UnaryOpTyped performs unary operation <op><y> on typed constant as Go language specification describes.
// Supported operations: + - ^ ! .
// If operation cannot be performed then error will be returned.
func UnaryOpTyped(op token.Token, y TypedValue, prec uint) (r TypedValue, err error) {
	r.t = y.t
	r.v, err = UnaryOp(op, y.v, prec)
	if err != nil {
		return TypedValue{}, err
	}
	if !AssignableTo(r.v, r.t) {
		return TypedValue{}, errors.New("unable to perform unary operation " + op.String() + " " + y.t.String() + ": missmatched result value " + r.v.String() + " and result type " + r.t.String())
	}
	return
}

// ShiftOp performs shift operation <x><op><s> on untyped constant as Go language specification describes.
// Supported operations: << >> .
// If operation cannot be performed then error will be returned.
func ShiftOp(x constant.Value, op token.Token, s uint) (r constant.Value, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = errors.New(fmt.Sprint(rec))
		}
	}()
	r = constant.Shift(x, op, s)
	if r.Kind() == constant.Unknown {
		err = errors.New("unknown result of " + x.String() + op.String() + strconvh.FormatUint(s))
		r = nil
	}
	return
}

// ShiftOpTyped performs shift operation <x><op><s> on typed constant as Go language specification describes.
// Supported operations: << >> .
// If operation cannot be performed then error will be returned.
func ShiftOpTyped(x TypedValue, op token.Token, s uint) (r TypedValue, err error) {
	r.t = x.t
	r.v, err = ShiftOp(x.v, op, s)
	if err != nil {
		return TypedValue{}, err
	}
	if !AssignableTo(r.v, r.t) {
		return TypedValue{}, errors.New("unable to perform shift operation " + x.t.String() + " " + op.String() + " " + strconvh.FormatUint(s) + ": missmatched result value " + r.v.String() + " and result type " + r.t.String())
	}
	return
}
