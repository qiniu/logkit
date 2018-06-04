package ip

import (
	"errors"
	"fmt"
	"strings"
)

const Null = "N/A"

var ErrInvalidIP = errors.New("invalid IP format")

type ErrInvalidFile struct {
	Format string
	Reason string
}

func (e ErrInvalidFile) Error() string {
	return fmt.Sprintf("invalid file format: %s - %s", e.Format, e.Reason)
}

// Locator represents an IP information loc.
type Locator interface {
	Find(string) (*LocationInfo, error)
}

// NewLocator returns a new IP loc based on extension of given data file.
func NewLocator(dataFile string) (Locator, error) {
	switch {
	case strings.HasSuffix(dataFile, ".dat"):
		return newDatLocator(dataFile)
	case strings.HasSuffix(dataFile, ".datx"):
		return newDatxLocator(dataFile)
	}

	return nil, errors.New("unrecognized data file format")
}
