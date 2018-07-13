package eval

import (
	"reflect"
)

func assign(dst reflect.Value, src Data) (err *intError) {
	if !dst.CanSet() {
		return assignDstUnsettableError(MakeRegular(dst))
	}

	newValue, ok := src.Assign(dst.Type())
	if !ok {
		return assignTypesMismError(dst.Type(), src)
	}

	dst.Set(newValue)
	return nil
}

func compositeLitStructKeys(t reflect.Type, elts map[string]Data, pkg string) (r Value, err *intError) {
	if t.Kind() != reflect.Struct {
		return nil, compLitInvTypeError(t)
	}
	rV := reflect.New(t).Elem()

	for field, value := range elts {
		fV := fieldByName(rV, field, pkg)
		if !fV.IsValid() {
			return nil, compLitUnknFieldError(rV, field)
		}
		err = assign(fV, value)
		if err != nil {
			return
		}
	}

	return MakeDataRegular(rV), nil
}

func compositeLitStructOrdered(t reflect.Type, elts []Data, pkg string) (r Value, err *intError) {
	if t.Kind() != reflect.Struct {
		return nil, compLitInvTypeError(t)
	}
	if t.NumField() != len(elts) {
		return nil, compLitArgsCountMismError(t.NumField(), len(elts))
	}
	rV := reflect.New(t).Elem()

	for i := range elts {
		fV := fieldByIndex(rV, i, pkg)
		err = assign(fV, elts[i])
		if err != nil {
			return
		}
	}

	return MakeDataRegular(rV), nil
}

func compositeLitArrayLike(t reflect.Type, elts map[int]Data) (r Value, err *intError) {
	// Calc max index (len of slice)
	var maxIndex = -1
	for i := range elts {
		if i < 0 {
			return nil, compLitNegIndexError()
		}
		if i > maxIndex {
			maxIndex = i
		}
	}

	// Init result
	var rV reflect.Value
	switch t.Kind() {
	case reflect.Array:
		rV = reflect.New(t).Elem()
		if maxIndex > rV.Len()-1 {
			return nil, compLitIndexOutOfBoundsError(rV.Len()-1, maxIndex)
		}
	case reflect.Slice:
		rV = reflect.MakeSlice(t, maxIndex+1, maxIndex+1)
	default:
		return nil, compLitInvTypeError(t)
	}

	// Fill result
	for i := range elts {
		iV := rV.Index(i)
		err = assign(iV, elts[i])
		if err != nil {
			return
		}
	}

	return MakeDataRegular(rV), nil
}

func compositeLitMap(t reflect.Type, elts map[Data]Data) (r Value, err *intError) {
	if t.Kind() != reflect.Map {
		return nil, compLitInvTypeError(t)
	}

	rV := reflect.MakeMap(t) // reflect.New(t).Elem()
	kV := reflect.New(t.Key()).Elem()
	vV := reflect.New(t.Elem()).Elem()
	for k, v := range elts {
		err = assign(kV, k)
		if err != nil {
			return
		}
		err = assign(vV, v)
		if err != nil {
			return
		}
		rV.SetMapIndex(kV, vV)
	}

	return MakeDataRegular(rV), nil
}
