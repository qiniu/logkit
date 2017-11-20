package base

import (
	"fmt"
	"strings"
)

type Validator interface {
	Validate() error
}

func CheckEndPoint(ep string) error {
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		return fmt.Errorf("endpoint should start with 'http://' or 'https://'")
	}
	if strings.HasSuffix(ep, "/") {
		return fmt.Errorf("endpoint should not end with '/'")
	}
	return nil
}
