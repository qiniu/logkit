// Package eval implements evaluation of GoLang expression at runtime.
//
// Requirements for expression:
// 	1. expression itself and all of subexpression must return exactly one value,
// 	2. see Bugs section for other requirements/restrictions.
//
// What does supported:
//
// 	1. types (by passing predefined/custom types and by defining unnamed types in expression itself),
// 	2. named arguments (regular variables, typed & untyped constants, untyped boolean variable),
// 	3. "package.SomeThing" notation,
// 	4. evaluate expression as if it is evaluates in specified package (even write access to private fields),
// 	5. evaluate expression like Go evaluates it (following most of specification rules),
// 	6. position of error in source on evaluation.
//
// Supported expression:
// 	1. identifier ("MyVar")
// 	2. constant ("123.456")
// 	3. selector expression ("MyVar.MyField")
// 	4. call function and method ("a.F(1,a,nil)")
// 	5. unary expression ("-a")
// 	6. binary expression ("a+1")
// 	7. pointer indirection ("*p")
// 	8. pointer dereferencing ("&a")
// 	9. parenthesized expression ("2*(1+3)")
// 	10. channel type ("chan int")
// 	11. function type ("func(int)string"; only type declaration, not function implementation)
// 	12. array & slice type ("[...]int{1,2,3}")
// 	13. interface type ("interface{}"; only empty interface supported)
// 	14. index expression ("a[i]")
// 	15. short and full slice expression ("a[1:2:3]")
// 	16. composite literal ("MyStruct{nil,2,a}")
// 	17. type conversion ("MyInt(int(1))")
// 	18. type assertion ("a.(int)"; only single result form)
// 	19. receiving from channel ("<-c")
//
// Predefined types (no need to pass it via args):
// 	1. bool
// 	2. [u]int[8/16/32/64]
// 	3. float{32/64}
// 	4. complex{64/128}
// 	5. byte
// 	6. rune
// 	7. uintptr
// 	8. string
//
// Implemented built-in functions (no need to pass it via args):
// 	1. len
// 	2. cap
// 	3. complex
// 	4. real
// 	5. imag
// 	6. new
// 	7. make
// 	8. append
//
// Simple example:
//	src:="int8(1*(1+2))"
//	expr,err:=ParseString(src,"")
//	if err!=nil{
//		return err
//	}
//	r,err:=expr.EvalToInterface(nil)
//	if err!=nil{
//		return err
//	}
//	fmt.Printf("%v %T", r, r)	// "3 int8"
//
// Complicated example:
//	type exampleString string
//
//	func (s exampleString) String() exampleString { return "!" + s + "!" }
//
//	type exampleStruct struct {
//		A, B int
//	}
//
//	func (s exampleStruct) Sum() int { return s.A + s.B }
//
//	func main(){
//		c := make(chan int64, 10)
//		c <- 2
//
//		src := `exampleString(fmt.Sprint(interface{}(math.MaxInt32/exampleStruct(struct{ A, B int }{3, 5}).Sum()+int(<-(<-chan int64)(c))-cap(make([]string, 1, 100))))).String().String() + "."`
//
//		expr, err := ParseString(src, "")
//		if err != nil {
//			return
//		}
//		a := Args{
//			"exampleString": MakeTypeInterface(exampleString("")),
//			"fmt.Sprint":    MakeDataRegularInterface(fmt.Sprint),
//			"math.MaxInt32": MakeDataUntypedConst(constanth.MakeUint(math.MaxInt32)),
//			"exampleStruct": MakeTypeInterface(exampleStruct{}),
//			"c":             MakeDataRegularInterface(c),
//		}
//		r, err := expr.EvalToInterface(a)
//		if err != nil {
//			return
//		}
//		if r != testR {
//			return
//		}
//		fmt.Printf("%v %T\n", r, r)	// "!!268435357!!. eval.exampleString"
//		return
//	}
//
// Example with error in expression:
//	src := `exampleString(fmt.Sprint(interface{}(math.MaxInt32/exampleStruct(struct{ A, B int }{3, 5}).Sum()+int(<-(<-chan int64)(c))-cap(make([]string, 1, 100))))).String().String() + "."`
//	expr, err := ParseString(src, "")
//	if err != nil {
//		t.Error(err)
//	}
//	a := Args{
//		"exampleString": MakeTypeInterface(exampleString("")),
//		"fmt.Sprint":    MakeDataRegularInterface(fmt.Sprint),
//		"math.MaxInt32": MakeDataUntypedConst(constanth.MakeUint(math.MaxInt32)),
//		"exampleStruct": MakeTypeInterface(exampleStruct{}),
//		// Remove "c" from passed arguments:
//		// "c":             MakeDataRegularInterface(c),
//	}
//	_, err = expr.EvalToInterface(a)
//	fmt.Println(err)	// "expression:1:119: undefined: c"
//
// Package eval built on top of reflection, go/ast, go/parser, go/token & go/constant.
// To create Expression it possible to use MakeExpression or some of Parse* functions.
// All of them use pkgPath to control access rights.
// pkgPath used when:
//
// 1. Accessing struct private field. If type of struct belong to pkgPath then all access to struct fields can modify its value (for example, it is possible to set such fields in composite literal).
//
// 2. Creating type in composite literal. All created in expression structures belong to pkgPath.
//
// When Expression is ready it may be evaluated multiple times. Evaluation done via Eval* methods:
// 	1. EvalRaw - the most flexible, but the hardest to use,
// 	2. EvalToData,
// 	3. EvalToRegular,
// 	4. EvalToInterface - the least flexible, but the easiest to use.
// In most cases EvalToInterface should be enough and it is easy to use.
//
// Evaluation performance:
//	// Parse expression from string
//	BenchmarkDocParse-8      200000     9118 ns/op    3800 B/op    106 allocs/op
//	// Eval already parsed expression
//	BenchmarkDocEval-8       100000    15559 ns/op    6131 B/op    151 allocs/op
//	// Eval the same expression by Go
//	BenchmarkDocGoEval-8    5000000      283 ns/op      72 B/op    3 allocs/op
//
// If you found a bug (result of this package evaluation differs from evaluation by Go itself) - please report bug at github.com/apaxa-go/eval.
package eval
