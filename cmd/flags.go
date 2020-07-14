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

type aggregationMethodValue struct {
	m *whispertool.AggregationMethod
}

func (v aggregationMethodValue) String() string {
	if v.m == nil {
		return ""
	}
	return v.m.String()
}

func (v aggregationMethodValue) Set(s string) error {
	m, err := whispertool.AggregationMethodString(s)
	if err != nil {
		return err
	}
	switch m {
	case whispertool.Average, whispertool.Sum, whispertool.Last, whispertool.Max, whispertool.Min, whispertool.First:
		*v.m = m
		return nil
	default:
		return errors.New(`aggregation method must be one of "average", "sum", "last", "max", "min", or "first"`)
	}
}

type xFilesFactorValue struct {
	f *float32
}

func (v xFilesFactorValue) String() string {
	if v.f == nil {
		return ""
	}
	return strconv.FormatFloat(float64(*v.f), 'f', -1, 32)
}

func (v xFilesFactorValue) Set(s string) error {
	f, err := strconv.ParseFloat(s, 32)
	if err != nil {
		return err
	}
	if f < 0 || 1 < f {
		return errors.New("xFilesFactor must be between 0.0 and 1.0")
	}
	*v.f = float32(f)
	return nil
}

type archiveInfoListValue struct {
	l *whispertool.ArchiveInfoList
}

func (v archiveInfoListValue) String() string {
	if v.l == nil {
		return ""
	}
	return v.l.String()
}

func (v archiveInfoListValue) Set(s string) error {
	l, err := whispertool.ParseArchiveInfoList(s)
	if err != nil {
		return err
	}
	*v.l = l
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

var errEmptyRateOutOfBounds = errors.New("emptyRate must be 0 <= r <= 1")
var errFromIsAfterUntil = errors.New("from time must not be after until time")
