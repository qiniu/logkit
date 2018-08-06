package eval

import (
	"fmt"
	"github.com/apaxa-go/helper/goh/asth"
	"go/ast"
	"go/token"
)

// Error is used for all errors related to passed expression.
// It describes problem (as text) and (in most cases) position of problem.
type Error struct {
	Msg string        // description of problem
	Pos asth.Position // position of problem
}

// Error implements standard error interface.
func (err Error) Error() string {
	return err.Pos.String() + ": " + err.Msg
}

type posError struct {
	msg      string
	pos, end token.Pos
}

func (err *posError) error(fset *token.FileSet) error {
	if err == nil {
		return nil
	}
	return Error{Msg: err.msg, Pos: asth.MakePosition(err.pos, err.end, fset)}
}

type intError string

func toIntError(err error) *intError {
	if err == nil {
		return nil
	}
	return newIntError(err.Error())
}
func newIntError(msg string) *intError {
	return (*intError)(&msg)
}
func newIntErrorf(format string, a ...interface{}) *intError {
	return newIntError(fmt.Sprintf(format, a...))
}

func (err *intError) pos(n ast.Node) *posError {
	if err == nil {
		return nil
	}
	return &posError{msg: string(*err), pos: n.Pos(), end: n.End()}
}

func (err *intError) noPos() *posError {
	if err == nil {
		return nil
	}
	return &posError{msg: string(*err)}
}
