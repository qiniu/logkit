package eval

import (
	"errors"
	"fmt"
	"github.com/apaxa-go/helper/goh/constanth"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"reflect"
)

// DefaultFileName is used as filename if expression constructor does not allow to set custom filename.
const DefaultFileName = "expression"

// Expression store expression for evaluation.
// It allows evaluate expression multiple times.
// The zero value for Expression is invalid and causes undefined behaviour.
type Expression struct {
	e       ast.Expr
	fset    *token.FileSet
	pkgPath string
}

// MakeExpression make expression with specified arguments.
// It does not perform any validation of arguments.
// e is AST of expression.
// fset is used to describe position of error and must be non nil (use token.NewFileSet instead).
// pkgPath is fully qualified package name, for more details see package level documentation.
func MakeExpression(e ast.Expr, fset *token.FileSet, pkgPath string) *Expression {
	return &Expression{e, fset, pkgPath}
}

// Parse parses filename or src for expression using parser.ParseExprFrom.
// For information about filename and src see parser.ParseExprFrom documentation.
// pkgPath is fully qualified package name, for more details see package level documentation.
func Parse(filename string, src interface{}, pkgPath string) (r *Expression, err error) {
	r = new(Expression)
	r.fset = token.NewFileSet()
	r.e, err = parser.ParseExprFrom(r.fset, filename, src, 0)
	if err != nil {
		return nil, err
	}
	r.pkgPath = pkgPath
	return
}

// ParseString parses expression from string src.
// pkgPath is fully qualified package name, for more details see package level documentation.
func ParseString(src string, pkgPath string) (r *Expression, err error) {
	return Parse(DefaultFileName, src, pkgPath)
}

// ParseBytes parses expression from []byte src.
// pkgPath is fully qualified package name, for more details see package level documentation.
func ParseBytes(src []byte, pkgPath string) (r *Expression, err error) {
	return Parse(DefaultFileName, src, pkgPath)
}

// ParseReader parses expression from io.Reader src.
// pkgPath is fully qualified package name, for more details see package level documentation.
func ParseReader(src io.Reader, pkgPath string) (r *Expression, err error) {
	return Parse(DefaultFileName, src, pkgPath)
}

// EvalRaw evaluates expression with given arguments args.
// Result of evaluation is Value.
func (e *Expression) EvalRaw(args Args) (r Value, err error) {
	defer func() {
		rec := recover()
		if rec != nil {
			err = errors.New(`BUG: unhandled panic "` + fmt.Sprint(rec) + `". Please report bug.`)
		}
	}()

	err = args.validate()
	if err != nil {
		return
	}

	args.makeAddressable()

	err = args.normalize()
	if err != nil {
		return
	}

	var posErr *posError
	r, posErr = e.astExpr(e.e, args)
	err = posErr.error(e.fset)
	return
}

// EvalToData evaluates expression with given arguments args.
// It returns error if result of evaluation is not Data.
func (e *Expression) EvalToData(args Args) (r Data, err error) {
	var tmp Value
	tmp, err = e.EvalRaw(args)
	if err != nil {
		return
	}

	if tmp.Kind() != Datas {
		err = notExprError(tmp).pos(e.e).error(e.fset)
		return
	}

	r = tmp.Data()
	return
}

// EvalToRegular evaluates expression with given arguments args.
// It returns error if result of evaluation is not Data or if it is impossible to represent it as variable using GoLang assignation rules.
func (e *Expression) EvalToRegular(args Args) (r reflect.Value, err error) {
	var tmp Data
	tmp, err = e.EvalToData(args)
	if err != nil {
		return
	}

	switch tmp.Kind() {
	case Regular:
		r = tmp.Regular()
	case TypedConst:
		r = tmp.TypedConst().Value()
	case UntypedConst:
		tmpC := tmp.UntypedConst()
		var ok bool
		r, ok = constanth.DefaultValue(tmpC)
		if !ok {
			err = constOverflowType(tmpC, constanth.DefaultType(tmpC)).pos(e.e).error(e.fset)
		}
	case UntypedBool:
		r = reflect.ValueOf(tmp.UntypedBool())
	default:
		err = notExprError(MakeData(tmp)).pos(e.e).error(e.fset)
	}
	return
}

// EvalToInterface evaluates expression with given arguments args.
// It returns error if result of evaluation is not Data or if it is impossible to represent it as variable using GoLang assignation rules.
func (e *Expression) EvalToInterface(args Args) (r interface{}, err error) {
	var tmp reflect.Value
	tmp, err = e.EvalToRegular(args)
	if err == nil {
		r = tmp.Interface()
	}
	return
}
