package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
)

type requiredOptionError struct {
	fs     *flag.FlagSet
	option string
}

func newRequiredOptionError(fs *flag.FlagSet, option string) *requiredOptionError {
	return &requiredOptionError{fs: fs, option: option}
}

func (e *requiredOptionError) Error() string {
	return fmt.Sprintf("option -%s is required.", e.option)
}

var errNeedsOneFileArg = errors.New("expected one whisper filename argument")
var errNeedsSrcAndDestFilesArg = errors.New("expected source and destination whisper filename arguments")
var errNeedsSrcAndDestDirsArg = errors.New("expected source and destination whisper directory arguments")
var errEmptyRateOutOfBounds = errors.New("emptyRate must be 0 <= r <= 1.")
var errFromIsAfterUntil = errors.New("from time must not be after until time.")

const globalUsage = `Usage: %s <subcommand> [options]

subcommands:
  diff                Show diff from src to dest whisper files.
  hole                Copy whisper file and make some holes (empty points) in dest file.
  generate            Generate random whisper file.
  merge               Update empty points with value from src whisper file.
  view                View raw content of whisper file.
  version             Show version

Run %s <subcommand> -h to show help for subcommand.
`

func main() {
	os.Exit(run())
}

var cmdName = filepath.Base(os.Args[0])

var (
	version string
	commit  string
	date    string
)

type UTCTimeValue struct {
	t *time.Time
}

func (t UTCTimeValue) String() string {
	if t.t == nil {
		return ""
	}
	return t.t.Format(whispertool.UTCTimeLayout)
}

func (t UTCTimeValue) Set(s string) error {
	t2, err := time.Parse(whispertool.UTCTimeLayout, s)
	if err != nil {
		return err
	}
	*t.t = t2
	return nil
}

func run() int {
	flag.Usage = func() {
		fmt.Printf(globalUsage, cmdName, cmdName)
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		return 2
	}

	var err error
	switch args[0] {
	case "diff":
		err = runDiffCmd(args[1:])
	case "generate":
		err = runGenerateCmd(args[1:])
	case "hole":
		err = runHoleCmd(args[1:])
	case "merge":
		err = runMergeCmd(args[1:])
	case "view":
		err = runViewCmd(args[1:])
	case "version":
		err = runShowVersion(args[1:])
	default:
		flag.Usage()
		return 2
	}
	if err != nil {
		if errors.Is(err, whispertool.ErrDiffFound) {
			return 1
		}

		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		var roerr *requiredOptionError
		if errors.As(err, &roerr) {
			fmt.Fprintf(os.Stderr, "\n", err.Error())
			roerr.fs.Usage()
		}
		return 2
	}
	return 0
}

func runShowVersion(args []string) error {
	fmt.Printf("Version: %s\n", version)
	fmt.Printf("Commit:  %s\n", commit)
	fmt.Printf("Date:    %s\n", date)
	return nil
}

const viewCmdUsage = `Usage: %s view file.wsp
`

func runViewCmd(args []string) error {
	fs := flag.NewFlagSet("view", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), viewCmdUsage, cmdName)
		fs.PrintDefaults()
	}
	raw := fs.Bool("raw", false, "View raw data.")

	now := time.Now()
	fs.Var(&UTCTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&UTCTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&UTCTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if fs.NArg() != 1 {
		return errNeedsOneFileArg
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.View(fs.Arg(0), *raw, now, from, until)
}

const mergeCmdUsage = `Usage: %s merge [options] src.wsp dest.wsp
       %s merge [options] srcDir destDir

options:
`

func runMergeCmd(args []string) error {
	fs := flag.NewFlagSet("merge", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), mergeCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	now := time.Now()
	fs.Var(&UTCTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&UTCTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&UTCTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	recursive := fs.Bool("r", false, "merge files recursively.")
	fs.Parse(args)

	if fs.NArg() != 2 {
		if *recursive {
			return errNeedsSrcAndDestDirsArg
		} else {
			return errNeedsSrcAndDestFilesArg
		}
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.Merge(fs.Arg(0), fs.Arg(1), *recursive, now, from, until)
}

const holeCmdUsage = `Usage: %s hole [options] src.wsp dest.wsp

options:
`

func runHoleCmd(args []string) error {
	fs := flag.NewFlagSet("hole", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), holeCmdUsage, cmdName)
		fs.PrintDefaults()
	}
	emptyRate := fs.Float64("empty-rate", 0.2, "empty rate (0 < r <= 1).")

	now := time.Now()
	fs.Var(&UTCTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&UTCTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&UTCTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if *emptyRate < 0 || 1 < *emptyRate {
		return errEmptyRateOutOfBounds
	}
	if fs.NArg() != 2 {
		return errNeedsSrcAndDestFilesArg
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.Hole(fs.Arg(0), fs.Arg(1), *emptyRate, now, from, until)
}

const diffCmdUsage = `Usage: %s diff [options] src.wsp dest.wsp
       %s diff [options] srcDir destDir

options:
`

func runDiffCmd(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), diffCmdUsage, cmdName)
		fs.PrintDefaults()
	}
	recursive := fs.Bool("r", false, "diff files recursively.")
	ignoreSrcEmpty := fs.Bool("ignore-src-empty", false, "ignore diff when source point is empty.")
	showAll := fs.Bool("show-all", false, "print all points when diff exists.")

	now := time.Now()
	fs.Var(&UTCTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&UTCTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&UTCTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if fs.NArg() != 2 {
		if *recursive {
			return errNeedsSrcAndDestDirsArg
		} else {
			return errNeedsSrcAndDestFilesArg
		}
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.Diff(fs.Arg(0), fs.Arg(1), *recursive, *ignoreSrcEmpty, *showAll, now, from, until)
}

const generateCmdUsage = `Usage: %s generate [options] dest.wsp

options:
`

func runGenerateCmd(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), generateCmdUsage, cmdName)
		fs.PrintDefaults()
	}
	retentionDefs := fs.String("retentions", "1m:2h,1h:2d,1d:30d", "retentions definitions.")
	randMax := fs.Int("max", 100, "random max value for shortest retention unit.")
	fill := fs.Bool("fill", true, "fill with random data.")
	fs.Parse(args)

	if *retentionDefs == "" {
		return newRequiredOptionError(fs, "retentions")
	}
	if fs.NArg() != 1 {
		return errNeedsOneFileArg
	}

	return whispertool.Generate(fs.Arg(0), *retentionDefs, *fill, *randMax)
}
