package cmd

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/hnakamur/whispertool"
)

type Command interface {
	Parse(fs *flag.FlagSet, args []string) error
	Execute() error
}

type timestampValue struct {
	t *whispertool.Timestamp
}

func (t timestampValue) String() string {
	if t.t == nil {
		return ""
	}
	return t.t.ToStdTime().Format(whispertool.UTCTimeLayout)
}

func (t timestampValue) Set(s string) error {
	t2, err := time.Parse(whispertool.UTCTimeLayout, s)
	if err != nil {
		return err
	}
	*t.t = whispertool.TimestampFromStdTime(t2)
	return nil
}

type fileModeValue struct {
	m *os.FileMode
}

func (v fileModeValue) String() string {
	if v.m == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*v.m), 8)
}

func (v fileModeValue) Set(s string) error {
	m, err := strconv.ParseUint(s, 8, 32)
	if err != nil {
		return err
	}
	*v.m = os.FileMode(m)
	return nil
}

type RequiredOptionError struct {
	fs     *flag.FlagSet
	option string
}

func newRequiredOptionError(fs *flag.FlagSet, option string) *RequiredOptionError {
	return &RequiredOptionError{fs: fs, option: option}
}

func (e *RequiredOptionError) Error() string {
	return fmt.Sprintf("option -%s is required.", e.option)
}

func (e *RequiredOptionError) Usage() {
	e.fs.Usage()
}

var errNeedsOneFileArg = errors.New("expected one whisper filename argument")
var errNeedsSrcAndDestFilesArg = errors.New("expected source and destination whisper filename arguments")
var errNeedsSrcAndDestDirsArg = errors.New("expected source and destination whisper directory arguments")
var errEmptyRateOutOfBounds = errors.New("emptyRate must be 0 <= r <= 1")
var errFromIsAfterUntil = errors.New("from time must not be after until time")
