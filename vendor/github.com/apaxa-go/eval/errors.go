package eval

import (
	"github.com/apaxa-go/helper/strconvh"
	"go/ast"
	"go/constant"
	"go/token"
	"reflect"
)

func identUndefinedError(ident string) *intError {
	return newIntError("undefined: " + ident)
}
func invAstError(msg string) *intError {
	return newIntError("invalid AST: " + msg)
}
func invAstNilError() *intError {
	return invAstError("evaluate nil expr")
}
func invAstUnsupportedError(e ast.Expr) *intError {
	return invAstError("expression evaluation does not support " + reflect.TypeOf(e).String())
}
func invAstSelectorError() *intError {
	return invAstError("no field specified (Sel is nil)")
}
func invAstNilStructFieldsError() *intError {
	return invAstError("nil struct's fields")
}
func invAstNilInterfaceMethodsError() *intError {
	return invAstError("nil interface's methods")
}
func unsupportedInterfaceTypeError() *intError {
	return newIntError("non empty interface type (with methods) declaration currently does not supported")
}
func invAstNonStringTagError() *intError {
	return invAstError("tag is not of type string")
}
func invSelectorXError(x Value) *intError {
	return newIntError("unable to select from " + x.DeepType())
}
func syntaxError(msg string) *intError {
	return newIntError("syntax error: " + msg)
}
func syntaxInvBasLitError(literal string) *intError {
	return syntaxError("invalid basic literal \"" + literal + "\"")
}
func indirectInvalError(x Value) *intError {
	return newIntError("invalid indirect of " + x.String() + " (type " + x.DeepType() + ")")
}
func notExprError(x Value) *intError {
	return newIntError(x.DeepType() + " is not an expression")
}

//func syntaxMisChanTypeError() *intError {
//	return syntaxError("syntax error: missing channel element type")
//}
//func syntaxMisArrayTypeError() *intError {
//	return syntaxError("syntax error: missing array element type")
//}
//func syntaxMisVariadicTypeError() *intError {
//	return syntaxError("final argument in variadic function missing type")
//}
func sliceInvTypeError(x Data) *intError {
	return newIntError("cannot slice " + x.DeepString())
}
func notTypeError(x Value) *intError {
	return newIntError(x.String() + " is not a type")
}
func initMixError() *intError {
	return newIntError("mixture of field:value and value initializers")
}
func initStructInvFieldNameError() *intError {
	return newIntError("invalid field name in struct initializer")
}
func initArrayInvIndexError() *intError {
	return newIntError("index must be non-negative integer constant")
}
func initArrayDupIndexError(i int) *intError {
	return newIntError("duplicate index in array literal: " + strconvh.FormatInt(i))
}
func initMapMisKeyError() *intError {
	return newIntError("missing key in map literal")
}
func initInvTypeError(t reflect.Type) *intError {
	return newIntError("invalid type for composite literal: " + t.String())
}
func funcInvEllipsisPos() *intError {
	return newIntError("can only use ... with final input parameter")
}
func cannotUseAsError(dst reflect.Type, src Data, in string) *intError {
	return newIntError("cannot use " + src.DeepString() + " as type " + dst.String() + " in " + in)
}
func assignTypesMismError(dst reflect.Type, src Data) *intError {
	return cannotUseAsError(dst, src, "assigment")
}
func appendMismTypeError(dst reflect.Type, src Data) *intError {
	return cannotUseAsError(dst, src, "append")
}
func assignDstUnsettableError(dst Data) *intError {
	return newIntError("cannot change " + dst.DeepString() + " in assignment")
}
func compLitInvTypeError(t reflect.Type) *intError {
	return newIntError("invalid type for composite literal: " + t.String())
}
func compLitUnknFieldError(s reflect.Value, f string) *intError {
	return newIntError("unknown " + s.Type().String() + " field '" + f + "' in struct literal")
}
func compLitArgsCountMismError(req, got int) *intError {
	if req > got {
		return newIntError("too few values in struct initializer")
	}
	return newIntError("too many values in struct initializer")
}
func compLitNegIndexError() *intError {
	return newIntError("index must be non-negative integer constant")
}
func compLitIndexOutOfBoundsError(max, i int) *intError {
	return newIntError("array index " + strconvh.FormatInt(i) + " out of bounds [0:" + strconvh.FormatInt(max) + "]")
}
func invBinOpError(x, op, y, reason string) *intError {
	return newIntError("invalid operation: " + x + " " + op + " " + y + " (" + reason + ")")
}
func invBinOpUnknOpError(x Data, op token.Token, y Data) *intError {
	return invBinOpError(x.DeepString(), op.String(), y.DeepString(), "operator "+op.String()+" not defined on nil")
}
func invBinOpTypesMismError(x Data, op token.Token, y Data) *intError {
	return invBinOpError(x.DeepString(), op.String(), y.DeepString(), "mismatched types "+x.DeepType()+" and "+y.DeepType())
}
func invBinOpTypesInvalError(x Data, op token.Token, y Data) *intError {
	return invBinOpError(x.DeepString(), op.String(), y.DeepString(), "invalid types "+x.DeepType()+" and/or "+y.DeepType())
}

//func invBinOpTypesIncompError(x Value, op token.Token, y Value) *intError {
//	return invBinOpError(x.String(), op.String(), y.String(), "incomparable types "+x.DeepType()+" and "+y.DeepType())
//}
//func invBinOpTypesUnorderError(x Value, op token.Token, y Value) *intError {
//	return invBinOpError(x.String(), op.String(), y.String(), "unordered types "+x.DeepType()+" and "+y.DeepType())
//}
//func invBinOpInvalError(x Value, op token.Token, y Value) *intError {
//	return invBinOpError(x.String(), op.String(), y.String(), "invalid operator")
//}
func invBinOpShiftCountError(x Data, op token.Token, y Data) *intError {
	return invBinOpError(x.DeepString(), op.String(), y.DeepString(), "shift count type "+y.DeepType()+", must be unsigned integer")
}
func invBinOpShiftArgError(x Data, op token.Token, y Data) *intError {
	return invBinOpError(x.DeepString(), op.String(), y.DeepString(), "shift of type "+y.DeepType())
}
func callBuiltInArgsCountMismError(fn string, req, got int) *intError {
	if req > got {
		return newIntError("not enough arguments in call to " + fn)
	}
	return newIntError("too many arguments in call to " + fn)
}
func invBuiltInArgError(fn string, x Data) *intError {
	return newIntError("invalid argument " + x.DeepString() + " for " + fn)
}
func invBuiltInArgAtError(fn string, pos int, x Data) *intError {
	return newIntError("invalid argument #" + strconvh.FormatInt(pos) + " " + x.DeepString() + " for " + fn)
}
func invBuiltInArgsError(fn string, x []Data) *intError {
	var msg string
	for i := range x {
		if i != 0 {
			msg += ", "
		}
		msg += x[i].DeepString()
	}
	return newIntError("invalid arguments " + msg + " for " + fn)
}
func callArgsCountMismError(req, got int) *intError {
	if req > got {
		return newIntError("not enough arguments in call")
	}
	return newIntError("too many arguments in call")
}
func callNonFuncError(f Value) *intError {
	return newIntError("cannot call non-function (type " + f.DeepType() + ")")
}
func callResultCountMismError(got int) *intError {
	if got > 1 {
		return newIntError("multiple-value in single-value context")
	}
	return newIntError("function call with no result used as value")
}
func callInvArgAtError(pos int, x Data, reqT reflect.Type) *intError {
	return newIntError("cannot use " + x.DeepString() + " as type " + reqT.String() + " in argument #" + strconvh.FormatInt(pos))
}
func callPanicError(p interface{}) *intError {
	return newIntErrorf("runtime panic in function call (%v)", p)
}
func convertArgsCountMismError(t reflect.Type, req int, x []Data) *intError {
	var msg string
	switch {
	case len(x) > req:
		msg = "too many arguments to conversion to " + t.String() + ": "
		for i := range x {
			if i != 0 {
				msg += ", "
			}
			msg += x[i].DeepValue()
		}
	default:
		msg = "no arguments to conversion to " + t.String()
	}
	return newIntError(msg)
}
func convertUnableError(t reflect.Type, x Data) *intError {
	return newIntError("cannot convert " + x.DeepString() + " to type " + t.String())
}

//func convertNilUnableError(t reflect.Type) *intError {
//	return newIntError("cannot convertCall nil to type " + t.String())
//}
func undefIdentError(ident string) *intError {
	return newIntError("undefined: " + ident)
}
func invSliceOpError(x Data) *intError {
	return newIntError("cannot slice " + x.DeepString())
}
func invSliceIndexError(low, high int) *intError {
	return newIntError("invalid slice index: " + strconvh.FormatInt(low) + " > " + strconvh.FormatInt(high))
}
func invSlice3IndexOmitted() *intError {
	return newIntError("only first index in 3-index slice can be omitted")
}
func invUnaryOp(x Data, op token.Token) *intError {
	return newIntError("invalid operation: " + op.String() + " " + x.DeepType())
}

//func invUnaryOpReason(x Value, op token.Token, reason interface{}) *intError {
//	return newIntErrorf("invalid operation: %v %v: %v", op.String(), x.DeepType(), reason)
//}
//func invUnaryReceiveError(x Value, op token.Token) *intError {
//	return invUnaryOpReason(x, op, "receive from non-chan type "+x.Type().String())
//}
func selectorUndefIdentError(t reflect.Type, name string) *intError {
	return undefIdentError(t.String() + "." + name)
}
func arrayBoundInvBoundError(l Data) *intError {
	return newIntError("invalid array bound " + l.DeepString())
}
func arrayBoundNegError() *intError {
	return newIntError("array bound must be non-negative")
}
func convertWithEllipsisError(t reflect.Type) *intError {
	return newIntError("invalid use of ... in type conversion to " + t.String())
}
func callBuiltInWithEllipsisError(f string) *intError {
	return newIntError("invalid use of ... with builtin " + f)
}
func callRegularWithEllipsisError() *intError {
	return newIntError("invalid use of ... in call")
}
func makeInvalidTypeError(t reflect.Type) *intError {
	return newIntError("cannot make type " + t.String())
}
func makeNotIntArgError(t reflect.Type, argPos int, arg Value) *intError {
	return newIntError("non-integer argument #" + strconvh.FormatInt(argPos) + " in make(" + t.String() + ") - " + arg.DeepType())
}
func makeNegArgError(t reflect.Type, argPos int) *intError {
	return newIntError("negative argument #" + strconvh.FormatInt(argPos) + " in make(" + t.String() + ")")
}
func makeSliceMismArgsError(t reflect.Type) *intError {
	return newIntError("len larger than cap in make(" + t.String() + ")")
}
func appendFirstNotSliceError(x Data) *intError {
	return newIntError("first argument to append must be slice; have " + x.DeepType())
}
func typeAssertLeftInvalError(x Data) *intError {
	return newIntError("invalid type assertion: (non-interface type " + x.DeepType() + " on left)")
}
func typeAssertImposError(x reflect.Value, t reflect.Type) *intError {
	return newIntError("impossible type " + t.String() + ": string does not implement " + x.Type().String())
}
func typeAssertFalseError(x reflect.Value, t reflect.Type) *intError {
	return newIntError("interface conversion: " + x.Type().String() + " is not " + t.String())
}
func invOpError(op, reason string) *intError {
	return newIntError("invalid operation: " + op + " (" + reason + ")")
}
func invIndexOpError(x Data, i Data) *intError {
	return invOpError(x.DeepString()+"["+i.DeepString()+"]", "type "+x.DeepType()+" does not support indexing")
}
func indexOutOfRangeError(i int) *intError {
	return newIntError("index " + strconvh.FormatInt(i) + " out of range")
}
func cmpWithNilError(x Data, op token.Token) *intError {
	return invBinOpError(x.DeepString(), op.String(), "nil", "compare with nil is not defined on "+x.DeepType())
}

//func invMem(x Value) *intError {
//	return newIntError("invalid memory address or nil pointer dereference (" + x.String() + " is nil)")
//}
func constOverflowType(x constant.Value, t reflect.Type) *intError {
	return newIntError("constant " + x.ExactString() + " overflow " + t.String())
}
func interfaceMethodExpr() *intError {
	return newIntError("Method expressions for interface types currently does not supported")
}
