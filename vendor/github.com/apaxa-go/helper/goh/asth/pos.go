package asth

import (
	"github.com/apaxa-go/helper/strconvh"
	"go/ast"
	"go/token"
)

// PointInSource describes an some point in source file without file itself.
// A Position is valid if the line number is > 0.
type PointInSource struct {
	Offset int // offset, starting at 0
	Line   int // line number, starting at 1
	Column int // column number, starting at 1 (byte count)
}

// TokenPositionToPoint convert token.Position to PointInSource.
func TokenPositionToPoint(p token.Position) PointInSource {
	return PointInSource{p.Offset, p.Line, p.Column}
}

// IsValid reports whether the point in source is valid.
func (p PointInSource) IsValid() bool { return p.Line > 0 }

// String returns a string "line:column" or "-" if PointInSource is invalid
func (p PointInSource) String() string {
	if !p.IsValid() {
		return "-"
	}
	return strconvh.FormatInt(p.Line) + ":" + strconvh.FormatInt(p.Column)
}

// Position describes position of piece of code in source file.
// It contains file name (if any), position of beginning and (optionally) position of end.
// End is the position of last character, not first character after piece of code.
type Position struct {
	Filename string
	Pos, End PointInSource
}

// MakePosition makes Position.
// pos is the first character of code.
// end is the first character after code.
// fset must be non nil (pass token.NewFileSet() instead).
func MakePosition(pos, end token.Pos, fset *token.FileSet) (p Position) {
	if tmp := fset.Position(pos); tmp.IsValid() {
		// file name
		p.Filename = tmp.Filename

		// pos
		p.Pos = TokenPositionToPoint(tmp)

		// end
		end--          // End() points to first char after node, but we would like to store last char of node
		if end > pos { // Do not store invalid end value and end value = pos value.
			tmp = fset.Position(end)
			if tmp.IsValid() {
				p.End = TokenPositionToPoint(tmp)
			}
		}
	}

	return
}

// NodePosition makes Position of ast.Node.
// It is just shortcut for MakePosition.
func NodePosition(n ast.Node, fset *token.FileSet) (p Position) {
	return MakePosition(n.Pos(), n.End(), fset)
}

// String returns a string in one of several forms:
//
//	file:line:column-line:column    valid position with file name
//	file:line:column                valid position with file name
//	line:column-line:column         valid position without file name
//	line:column                     valid position without file name
//	file                            invalid position with file name
//	-                               invalid position without file name
//
func (pos Position) String() string {
	// file name
	s := pos.Filename
	if !pos.Pos.IsValid() {
		if s == "" {
			s = "-"
		}
		return s
	}

	// from
	if s != "" {
		s += ":"
	}
	s += pos.Pos.String()
	if !pos.End.IsValid() {
		return s
	}

	// to
	s += "-" + pos.End.String()
	return s
}
