package cmd

import (
	"errors"
	"fmt"
)

type fileNotExistError struct {
	srcOrDest SrcDestType
	cause     error
}

func (e *fileNotExistError) Error() string {
	return fmt.Sprintf("%s: %s", e.srcOrDest, e.cause)
}

func (e *fileNotExistError) Unwrap() error {
	return e.cause
}

func AsFileNotExistError(err error) *fileNotExistError {
	var err2 *fileNotExistError
	if errors.As(err, &err2) {
		return err2
	}
	return nil
}
