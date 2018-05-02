package os

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/qiniu/log"
)

// ----------------------------------------------------------

func WaitForInterrupt(interrupt func()) {

	// Set up channel on which to send signal notifications.
	// We must use a buffered channel or risk missing the signal
	// if we're not ready to receive when the signal is sent.
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, os.Interrupt, os.Kill, syscall.SIGQUIT)

	// Block until a signal is received.
	s := <-c

	log.Println("Receiving signal:", s)

	interrupt()
}

// ----------------------------------------------------------
