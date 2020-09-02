package cmd

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestFileNotExistError(t *testing.T) {
	err := &fileNotExistError{
		srcOrDest: Source,
		cause: &os.PathError{
			Op:   "open",
			Path: "foo.txt",
			Err:  errors.New("file not found"),
		},
	}

	if got, want := AsFileNotExistError(fmt.Errorf("do something: %w", err)), err; got != want {
		t.Errorf("cannot unwrap: got=%v, want=%v", got, want)
	}

	if got := AsFileNotExistError(fmt.Errorf("do something: %s", err)); got != nil {
		t.Errorf("should not unwrap: got=%v, want=%v", got, nil)
	}
}
