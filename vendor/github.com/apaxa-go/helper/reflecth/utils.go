package reflecth

import (
	"github.com/apaxa-go/helper/goh/asth"
	"go/ast"
	"reflect"
)

// TypeOfPtr returns type of value pointed to by i.
// If i is not a pointer than TypeOfPtr returns nil.
func TypeOfPtr(i interface{}) reflect.Type {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		return nil
	}
	return t.Elem()
}

// ValueOfPtr returns value pointed to by i.
// If i is not a pointer than ValueOfPtr returns zero Value.
// This function is useful for passing interface to reflect.Value via passing pointer to interface to this function (it is impossible to make reflect.Value of kind interface via passing interface directly to ValueOf function).
func ValueOfPtr(i interface{}) reflect.Value {
	r := reflect.ValueOf(i)
	if r.Type().Kind() != reflect.Ptr {
		return reflect.Value{}
	}
	return r.Elem()
}

// ChanDirFromAst preforms conversion channel direction from ast.ChanDir representation to reflect.ChanDir representation.
// ChanDirFromAst returns 0 if dir cannot be represented as reflect.ChanDir (if dir invalid itself).
func ChanDirFromAst(dir ast.ChanDir) (r reflect.ChanDir) {
	switch dir {
	case asth.SendDir:
		return reflect.SendDir
	case asth.RecvDir:
		return reflect.RecvDir
	case asth.BothDir:
		return reflect.BothDir
	default:
		return 0
	}
}

// ChanDirToAst preforms conversion channel direction from reflect.ChanDir representation to ast.ChanDir representation.
// ChanDirToAst returns 0 if dir cannot be represented as ast.ChanDir (if dir invalid itself).
func ChanDirToAst(dir reflect.ChanDir) (r ast.ChanDir) {
	switch dir {
	case reflect.SendDir:
		return asth.SendDir
	case reflect.RecvDir:
		return asth.RecvDir
	case reflect.BothDir:
		return asth.BothDir
	default:
		return 0
	}
}
