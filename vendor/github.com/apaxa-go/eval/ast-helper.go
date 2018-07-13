package eval

import (
	"github.com/apaxa-go/helper/reflecth"
	"go/ast"
	"reflect"
)

func (expr *Expression) funcTranslateArgs(fields *ast.FieldList, ellipsisAlowed bool, args Args) (r []reflect.Type, variadic bool, err *posError) {
	if fields == nil || len(fields.List) == 0 {
		return
	}
	r = make([]reflect.Type, len(fields.List))
	for i := range fields.List {
		// check for variadic
		if _, ellipsis := fields.List[i].Type.(*ast.Ellipsis); ellipsis {
			if !ellipsisAlowed || i != len(fields.List)-1 {
				return nil, false, funcInvEllipsisPos().pos(fields.List[i])
			}
			variadic = true
		}
		// calc type
		r[i], err = expr.astExprAsType(fields.List[i].Type, args)
		if err != nil {
			return nil, false, err
		}
	}
	return
}

// fieldByName is just a replacement for "x.FieldByName(field)" with write access to private fields.
// x must be of kind reflect.Struct.
func fieldByName(x reflect.Value, field string, pkgPath string) reflect.Value {
	r := x.FieldByName(field)
	//if pkgPath != "" && x.Type().PkgPath() == pkgPath && r.CanAddr() {
	if x.Type().PkgPath() == pkgPath && r.CanAddr() {
		r = reflecth.MakeSettable(r)
	}
	return r
}

// fieldByIndex is just a replacement for "x.Field(i)" with write access to private fields.
// x must be of kind reflect.Struct.
func fieldByIndex(x reflect.Value, i int, pkgPath string) reflect.Value {
	r := x.Field(i)
	//if pkgPath != "" && x.Type().PkgPath() == pkgPath && r.CanAddr() {
	if x.Type().PkgPath() == pkgPath && r.CanAddr() {
		r = reflecth.MakeSettable(r)
	}
	return r
}

//
//
//
type upTypesT struct {
	d   Data
	err *intError
}

func upT(d Data, err *intError) upTypesT {
	return upTypesT{d, err}
}
func (u upTypesT) pos(n ast.Node) (r Value, err *posError) {
	if u.err == nil {
		r = MakeData(u.d)
	} else {
		err = u.err.pos(n)
	}
	return
}
