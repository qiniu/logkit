package eval

import (
	"reflect"
)

func convertCall(t reflect.Type, args []Data) (r Value, err *intError) {
	if len(args) != 1 {
		return nil, convertArgsCountMismError(t, 1, args)
	}
	rV, ok := args[0].Convert(t)
	if ok {
		r = MakeData(rV)
	} else {
		err = convertUnableError(t, args[0])
	}
	return
}
