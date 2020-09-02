package cmd

import (
	"errors"
	"fmt"
	"os"
)

type fileNotExistError struct {
	srcOrDest SrcDestType
	cause     error
}

func WrapFileNotExistError(srcOrDest SrcDestType, err error) error {
	if err != nil && os.IsNotExist(err) {
		return &fileNotExistError{
			srcOrDest: srcOrDest,
			cause:     err,
		}
	}
	return err
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
