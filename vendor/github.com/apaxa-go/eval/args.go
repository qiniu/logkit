package eval

import (
	"errors"
	"github.com/apaxa-go/helper/goh/asth"
	"reflect"
	"strings"
)

type (
	// Args represents arguments passed into the expression for evaluation.
	// It can be nil if no arguments required for evaluation.
	// Map indexes is the identifiers (possible with short package specification like "strconv.FormatInt" or "mypackage.MySomething", not "github.com/me/mypackage.MySomething").
	// Map elements is corresponding values. Usually elements is of kind Regular (for typed variables and function), TypedConst (for typed constant) or UntypedConst (for untyped constant). For using function in expression pass it as variable (kind Regular).
	Args map[string]Value
	// ArgsI is helper class for ArgsFromInterfaces.
	// It can be used in composite literal for this function.
	ArgsI map[string]interface{}
	// ArgsR is helper class for ArgsFromRegulars.
	// It can be used in composite literal for this function.
	ArgsR map[string]reflect.Value
)

// ArgsFromRegulars converts map[string]reflect.Value to Args (map[string]Value).
// ArgsR may be useful.
func ArgsFromRegulars(x map[string]reflect.Value) Args {
	r := make(Args, len(x))
	for i := range x {
		r[i] = MakeDataRegular(x[i])
	}
	return r
}

// ArgsFromInterfaces converts map[string]interface{} to Args (map[string]Value).
// ArgsI may be useful.
func ArgsFromInterfaces(x map[string]interface{}) Args {
	r := make(Args, len(x))
	for i := range x {
		r[i] = MakeDataRegularInterface(x[i])
	}
	return r
}

// Compute package if any
func (args Args) normalize() error {
	packages := make(map[string]Args)

	// Extract args with package specific
	for ident := range args {
		parts := strings.Split(ident, ".")
		switch len(parts) {
		case 1:
			continue
		case 2:
			if parts[0] == "_" || !asth.IsValidIdent(parts[0]) || !asth.IsValidExportedIdent(parts[1]) {
				return errors.New("invalid identifier " + ident)
			}

			if _, ok := packages[parts[0]]; !ok {
				packages[parts[0]] = make(Args)
			}
			packages[parts[0]][parts[1]] = args[ident]
			delete(args, ident)
		default:
			return errors.New("invalid identifier " + ident)
		}
	}

	// Add computed packages
	for pk, pv := range packages {
		// Check for unique package name
		if _, ok := args[pk]; ok {
			return errors.New("something with package name already exists " + pk)
		}
		args[pk] = MakePackage(pv)
	}

	return nil
}

// Make all args addressable
func (args Args) makeAddressable() {
	for ident, arg := range args {
		if arg.Kind() != Datas {
			continue
		}
		if arg.Data().Kind() != Regular {
			continue
		}
		oldV := arg.Data().Regular()
		if oldV.CanAddr() {
			continue
		}

		newV := reflect.New(oldV.Type()).Elem()
		newV.Set(oldV)
		args[ident] = MakeDataRegular(newV)
	}
}

func (args Args) validate() error {
	for ident, arg := range args {
		switch arg.Kind() {
		case Type:
			if arg.Type() == nil {
				return errors.New(ident + ": invalid type nil")
			}
		case Datas:
			if arg.Kind() == Datas && arg.Data().Kind() == Regular && !arg.Data().Regular().IsValid() {
				return errors.New(ident + ": invalid regular data")
			}
		}
	}
	return nil
}
