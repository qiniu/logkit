package asth

import "go/ast"

// Specify all possible direction of a channel type for package go/ast.
const (
	SendDir ast.ChanDir = ast.SEND
	RecvDir             = ast.RECV
	BothDir             = SendDir | RecvDir
)
