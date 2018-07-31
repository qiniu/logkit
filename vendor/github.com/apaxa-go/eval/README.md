# eval

[![Build Status](https://travis-ci.org/apaxa-go/eval.svg?branch=master)](https://travis-ci.org/apaxa-go/eval)
[![Coverage Status](https://coveralls.io/repos/github/apaxa-go/eval/badge.svg?branch=master)](https://coveralls.io/github/apaxa-go/eval?branch=master) 
[![Go Report Card](https://goreportcard.com/badge/github.com/apaxa-go/eval)](https://goreportcard.com/report/github.com/apaxa-go/eval)
[![GoDoc](https://godoc.org/github.com/apaxa-go/eval?status.svg)](https://godoc.org/github.com/apaxa-go/eval)

Package eval implements evaluation of GoLang expression at runtime.

# **THIS LIB NO MORE MAINTAINED!**

# For whose who wants to implement golang eval
Suggestions:
1. Implement 2-steps algorithm: first step - analogue of golang compilation (resolve all types; compute all constants; convert all untyped constant and untyped variables to typed; also simplify AST tree here / convert to your own format), second step - analogue of golang runtime (here will be passed values of external variables and computed final result). 
2. You need to use golang eval package, but keep in mind that it can simplify break your lib on update (golang's backward compatibility does not save you).
3. Follow language specification (it is hard to implement some additional custom behaviour without breaking compatibility with language accepted expressions).
4. Language specification may omit some details (so also test with golang compiler).

# Requirements for expression:
1. expression itself and all of subexpression must return exactly one value,
2. see documentation Bugs section for other requirements/restrictions.

# What does supported:

1. types (by passing predefined/custom types and by defining unnamed types in expression itself),
2. named arguments (regular variables, typed & untyped constants, untyped boolean variable),
3. "package.SomeThing" notation,
4. evaluate expression as if it is evaluates in specified package (even write access to private fields),
5. evaluate expression like Go evaluates it (following most of specification rules),
6. position of error in source on evaluation.

# Examples:
Simple:
```
src:="int8(1*(1+2))"
expr,err:=ParseString(src,"")
if err!=nil{
	return err
}
r,err:=expr.EvalToInterface(nil)
if err!=nil{
	return err
}
fmt.Printf("%v %T", r, r)	// "3 int8"
```

Complicated:
```
type exampleString string

func (s exampleString) String() exampleString { return "!" + s + "!" }

type exampleStruct struct {
	A, B int
}

func (s exampleStruct) Sum() int { return s.A + s.B }

func main(){
	c := make(chan int64, 10)
	c <- 2

	src := `exampleString(fmt.Sprint(interface{}(math.MaxInt64/exampleStruct(struct{ A, B int }{3, 5}).Sum()+int(<-(<-chan int64)(c))-cap(make([]string, 1, 100))))).String().String() + "."`

	expr, err := ParseString(src, "")
	if err != nil {
		return
	}
	a := Args{
		"exampleString": MakeTypeInterface(exampleString("")),
		"fmt.Sprint":    MakeDataRegularInterface(fmt.Sprint),
		"math.MaxInt64": MakeDataUntypedConst(constanth.MakeUint(math.MaxInt64)),
		"exampleStruct": MakeTypeInterface(exampleStruct{}),
		"c":             MakeDataRegularInterface(c),
	}
	r, err := expr.EvalToInterface(a)
	if err != nil {
		return
	}
	if r != testR {
		return
	}
	fmt.Printf("%v %T\n", r, r)	// "!!1152921504606846877!!. exampleString"
	return
}
```

With error:
```
src := `exampleString(fmt.Sprint(interface{}(math.MaxInt64/exampleStruct(struct{ A, B int }{3, 5}).Sum()+int(<-(<-chan int64)(c))-cap(make([]string, 1, 100))))).String().String() + "."`
expr, err := ParseString(src, "")
if err != nil {
	t.Error(err)
}
a := Args{
	"exampleString": MakeTypeInterface(exampleString("")),
	"fmt.Sprint":    MakeDataRegularInterface(fmt.Sprint),
	"math.MaxInt64": MakeDataUntypedConst(constanth.MakeUint(math.MaxInt64)),
	"exampleStruct": MakeTypeInterface(exampleStruct{}),
	// Remove "c" from passed arguments:
	// "c":             MakeDataRegularInterface(c),
}
_, err = expr.EvalToInterface(a)
fmt.Println(err)	// "expression:1:119: undefined: c"
```

# Bugs
Please report bug if you found it.