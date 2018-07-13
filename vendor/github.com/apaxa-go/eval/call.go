package eval

import (
	"reflect"
)

// ellipsis true if last argument has ellipsis notation ("f(a,b,c...)").
func callRegular(f reflect.Value, args []Data, ellipsis bool) (r Value, err *intError) {
	if f.Kind() != reflect.Func {
		return nil, callNonFuncError(MakeDataRegular(f))
	}
	fT := f.Type()
	if fT.NumOut() != 1 {
		return nil, callResultCountMismError(fT.NumOut())
	}

	switch {
	case fT.IsVariadic() && ellipsis:
		return callRegularVariadicEllipsis(f, args)
	case fT.IsVariadic() && !ellipsis:
		return callRegularVariadic(f, args)
	case !fT.IsVariadic() && !ellipsis:
		return callRegularNonVariadic(f, args)
	}
	return nil, callRegularWithEllipsisError()
}

// f must be variadic func with single result (check must perform caller).
func callRegularVariadicEllipsis(f reflect.Value, args []Data) (r Value, err *intError) {
	fT := f.Type()
	if len(args) != fT.NumIn() {
		return nil, callArgsCountMismError(fT.NumIn(), len(args))
	}

	// Prepare arguments
	typedArgs := make([]reflect.Value, len(args))
	for i := range args {
		var ok bool
		typedArgs[i], ok = args[i].Assign(fT.In(i))
		if !ok {
			return nil, callInvArgAtError(i, args[i], fT.In(i))
		}
	}

	defer func() {
		if rec := recover(); rec != nil {
			r = nil
			err = callPanicError(rec)
		}
	}()
	rs := f.CallSlice(typedArgs)
	return MakeDataRegular(rs[0]), nil
}

// f must be variadic func with single result (check must perform caller).
func callRegularVariadic(f reflect.Value, args []Data) (r Value, err *intError) {
	fT := f.Type()
	if len(args) < fT.NumIn()-1 {
		return nil, callArgsCountMismError(fT.NumIn(), len(args)-1)
	}

	// Prepare arguments
	typedArgs := make([]reflect.Value, len(args))
	// non-variadic arguments
	for i := 0; i < fT.NumIn()-1; i++ {
		var ok bool
		typedArgs[i], ok = args[i].Assign(fT.In(i))
		if !ok {
			return nil, callInvArgAtError(i, args[i], fT.In(i))
		}
	}
	// variadic arguments
	variadicT := fT.In(fT.NumIn() - 1).Elem()
	for i := fT.NumIn() - 1; i < len(args); i++ {
		var ok bool
		typedArgs[i], ok = args[i].Assign(variadicT)
		if !ok {
			return nil, callInvArgAtError(i, args[i], variadicT)
		}
	}

	defer func() {
		if rec := recover(); rec != nil {
			r = nil
			err = callPanicError(rec)
		}
	}()
	rs := f.Call(typedArgs)
	return MakeDataRegular(rs[0]), nil
}

// f must be non-variadic func with single result (check must perform caller).
func callRegularNonVariadic(f reflect.Value, args []Data) (r Value, err *intError) {
	// Check in/out arguments count
	fT := f.Type()
	if len(args) != fT.NumIn() {
		return nil, callArgsCountMismError(fT.NumIn(), len(args))
	}

	// Prepare arguments
	typedArgs := make([]reflect.Value, len(args))
	for i := range args {
		var ok bool
		typedArgs[i], ok = args[i].Assign(fT.In(i))
		if !ok {
			return nil, callInvArgAtError(i, args[i], fT.In(i))
		}
	}

	defer func() {
		if rec := recover(); rec != nil {
			r = nil
			err = callPanicError(rec)
		}
	}()
	rs := f.Call(typedArgs)
	return MakeDataRegular(rs[0]), nil
}
