package eval

import (
	"go/constant"
	"reflect"
)

func indexMap(x reflect.Value, i Data) (r Value, err *intError) {
	if k := x.Kind(); k != reflect.Map {
		return nil, invIndexOpError(MakeRegular(x), i)
	}

	iReqT := x.Type().Key()

	iV, ok := i.Assign(iReqT)
	if !ok {
		return nil, convertUnableError(iReqT, i)
	}

	//
	rV := x.MapIndex(iV)
	if !rV.IsValid() { // Return zero value if no such key in map
		return MakeDataRegular(reflect.New(x.Type().Elem()).Elem()), nil
	}

	return MakeDataRegular(rV), nil
}

func indexOther(x reflect.Value, i Data) (r Value, err *intError) {
	if k := x.Kind(); k != reflect.String && k != reflect.Array && k != reflect.Slice {
		return nil, invIndexOpError(MakeRegular(x), i)
	}

	iInt, ok := i.AsInt()
	if !ok {
		return nil, convertUnableError(reflect.TypeOf(int(0)), i)
	}

	// check out-of-range
	if iInt < 0 || iInt >= x.Len() {
		return nil, indexOutOfRangeError(iInt)
	}

	return MakeDataRegular(x.Index(iInt)), nil
}

func indexConstant(x constant.Value, i Data) (r Value, err *intError) {
	if x.Kind() != constant.String {
		return nil, invIndexOpError(MakeUntypedConst(x), i)
	}

	iInt, ok := i.AsInt()
	if !ok {
		return nil, convertUnableError(reflect.TypeOf(int(0)), i)
	}

	xStr := constant.StringVal(x)
	// check out-of-range
	if iInt < 0 || iInt >= len(xStr) {
		return nil, indexOutOfRangeError(iInt)
	}

	if i.IsConst() {

		return MakeDataUntypedConst(constant.MakeInt64(int64(xStr[iInt]))), nil
	}
	return MakeDataRegularInterface(xStr[iInt]), nil
}
