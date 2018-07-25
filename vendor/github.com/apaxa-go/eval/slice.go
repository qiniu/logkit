package eval

import (
	"reflect"
)

const indexSkipped int = -1

// Returns int index value for passed Value.
// i may be nil (if index omitted).
// Result also checked for not negative value (negative value cause error).
func getSliceIndex(i Data) (r int, err *intError) {
	if i == nil {
		return indexSkipped, nil
	}
	r, ok := i.AsInt()
	if !ok {
		return 0, convertUnableError(reflect.TypeOf(int(0)), i)
	}
	if r < 0 {
		return 0, indexOutOfRangeError(r)
	}
	return
}

func slice2(x reflect.Value, low, high int) (r Value, err *intError) {
	// resolve pointer to array
	if x.Kind() == reflect.Ptr && x.Elem().Kind() == reflect.Array {
		x = x.Elem()
	}

	// check slicing possibility
	if k := x.Kind(); k != reflect.Array && k != reflect.Slice && k != reflect.String {
		return nil, invSliceOpError(MakeRegular(x))
	} else if k == reflect.Array && !x.CanAddr() {
		return nil, invSliceOpError(MakeRegular(x))
	}

	// resolve default value
	if low == indexSkipped {
		low = 0
	}
	if high == indexSkipped {
		high = x.Len()
	}

	// validate indexes
	switch {
	case low < 0:
		return nil, indexOutOfRangeError(low)
	case high > x.Len():
		return nil, indexOutOfRangeError(high)
	case low > high:
		return nil, invSliceIndexError(low, high)
	}

	return MakeDataRegular(x.Slice(low, high)), nil
}

func slice3(x reflect.Value, low, high, max int) (r Value, err *intError) {
	// resolve pointer to array
	if x.Kind() == reflect.Ptr && x.Elem().Kind() == reflect.Array {
		x = x.Elem()
	}

	// check slicing possibility
	if k := x.Kind(); k != reflect.Array && k != reflect.Slice {
		return nil, invSliceOpError(MakeRegular(x))
	} else if k == reflect.Array && !x.CanAddr() {
		return nil, invSliceOpError(MakeRegular(x))
	}

	// resolve default value
	if low == indexSkipped {
		low = 0
	}

	// validate indexes
	if high == indexSkipped || max == indexSkipped {
		return nil, invSlice3IndexOmitted()
	}
	switch {
	case low < 0:
		return nil, indexOutOfRangeError(low)
	case max > x.Cap():
		return nil, indexOutOfRangeError(high)
	case low > high:
		return nil, invSliceIndexError(low, high)
	case high > max:
		return nil, invSliceIndexError(high, max)
	}

	return MakeDataRegular(x.Slice3(low, high, max)), nil
}
