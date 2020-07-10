package cmd

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func nopFinish() error { return nil }

func withTextOutWriter(textOut string, f func(io.Writer) error) (err error) {
	tow, finish, err := newTextOutWriter(textOut)
	if err != nil {
		return nil
	}
	defer func() {
		if err2 := finish(); err2 != nil && err == nil {
			err = err2
		}
	}()
	return f(tow)
}

func newTextOutWriter(textOut string) (w io.Writer, finish func() error, err error) {
	if textOut == "" {
		return ioutil.Discard, nopFinish, nil
	}
	if textOut == "-" {
		return os.Stdout, nopFinish, nil
	}

	file, err := os.OpenFile(textOut, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, nopFinish, fmt.Errorf("cannot open file for -text-out: %s", err)
	}

	bw := bufio.NewWriter(file)
	finish = func() error {
		if err := bw.Flush(); err != nil {
			return fmt.Errorf("cannot flush buffer to file for -text-out: %s", err)
		}
		if err := file.Sync(); err != nil {
			return fmt.Errorf("cannot sync file for -text-out: %s", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("cannot close file for -text-out: %s", err)
		}
		return nil
	}
	return bw, finish, nil
}
