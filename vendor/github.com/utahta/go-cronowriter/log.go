package cronowriter

import (
	"fmt"
	"io"
	"os"
)

type (
	logger interface {
		Write(b []byte)
		Error(args ...interface{})
		Errorf(format string, args ...interface{})
	}

	nopLogger struct{}

	debugLogger struct {
		stdout io.Writer
		stderr io.Writer
	}

	stdoutLogger struct {
		w io.Writer
	}

	stderrLogger struct {
		w io.Writer
	}
)

func newDebugLogger() *debugLogger {
	return &debugLogger{
		stdout: os.Stdout,
		stderr: os.Stderr,
	}
}

func newStdoutLogger() *stdoutLogger {
	return &stdoutLogger{
		w: os.Stdout,
	}
}

func newStderrLogger() *stderrLogger {
	return &stderrLogger{
		w: os.Stderr,
	}
}

func (l *nopLogger) Write(b []byte)                            {}
func (l *nopLogger) Error(args ...interface{})                 {}
func (l *nopLogger) Errorf(format string, args ...interface{}) {}

func (l *debugLogger) Write(b []byte) {
	fmt.Fprintf(l.stdout, "%s", b)
}

func (l *debugLogger) Error(args ...interface{}) {
	fmt.Fprintln(l.stderr, args...)
}

func (l *debugLogger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(l.stderr, format, args...)
}

func (l *stdoutLogger) Write(b []byte) {
	fmt.Fprintf(l.w, "%s", b)
}

func (l *stdoutLogger) Error(args ...interface{}) {
	fmt.Fprintln(l.w, args...)
}

func (l *stdoutLogger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(l.w, format, args...)
}

func (l *stderrLogger) Write(b []byte) {
	fmt.Fprintf(l.w, "%s", b)
}

func (l *stderrLogger) Error(args ...interface{}) {
	fmt.Fprintln(l.w, args...)
}

func (l *stderrLogger) Errorf(format string, args ...interface{}) {
	fmt.Fprintf(l.w, format, args...)
}
