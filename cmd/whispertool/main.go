package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
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
var errEmptyRateOutOfBounds = errors.New("emptyRate must be 0 <= r <= 1")
var errFromIsAfterUntil = errors.New("from time must not be after until time")

const globalUsage = `Usage: %s <subcommand> [options]

subcommands:
  diff                Show diff from src to dest whisper files.
  hole                Copy whisper file and make some holes (empty points) in dest file.
  generate            Generate random whisper file.
  merge               Update empty points with value from src whisper file.
  sum                 Sum value of whisper files.
  sum-diff            Sum value of whisper files and compare to another whisper file.
  view                View content of whisper file.
  view-raw            View raw content of whisper file.
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

type utcTimeValue struct {
	t *time.Time
}

func (t utcTimeValue) String() string {
	if t.t == nil {
		return ""
	}
	return t.t.Format(whispertool.UTCTimeLayout)
}

func (t utcTimeValue) Set(s string) error {
	t2, err := time.Parse(whispertool.UTCTimeLayout, s)
	if err != nil {
		return err
	}
	*t.t = t2
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
	case "sum":
		err = runSumCmd(args[1:])
	case "sum-diff":
		err = runSumDiffCmd(args[1:])
	case "view":
		err = runViewCmd(args[1:])
	case "view-raw":
		err = runViewRawCmd(args[1:])
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
			fmt.Fprintf(os.Stderr, "\n")
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

const viewCmdUsage = `Usage: %s view [options] file.wsp
`

func runViewCmd(args []string) error {
	fs := flag.NewFlagSet("view", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), viewCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	now := time.Now()
	fs.Var(&utcTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format (exclusive)")

	until := now
	fs.Var(&utcTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format (inclusive)")

	retID := fs.Int("ret", whispertool.RetIDAll, "retention ID to print (-1 for all retentions)")
	textOut := fs.String("text-out", "-", "text output. empty means no output, - means stdout, other means output file.")
	showHeader := fs.Bool("header", true, "whether or not to show header (metadata and reteions)")
	fs.Parse(args)

	if fs.NArg() != 1 {
		return errNeedsOneFileArg
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.View(fs.Arg(0), now, from, until, *retID, *textOut, *showHeader)
}

const viewRawCmdUsage = `Usage: %s view-raw [options] file.wsp
`

func runViewRawCmd(args []string) error {
	fs := flag.NewFlagSet("view-raw", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), viewRawCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format (exclusive if not 0)")

	until := time.Now()
	fs.Var(&utcTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format (inclusive)")

	retID := fs.Int("ret", whispertool.RetIDAll, "retention ID to print (-1 for all retentions)")
	textOut := fs.String("text-out", "-", "text output. empty means no output, - means stdout, other means output file.")
	showHeader := fs.Bool("header", true, "whether or not to show header (metadata and reteions)")
	sortsByTime := fs.Bool("sort", false, "whether or not to sorts points by time")
	fs.Parse(args)

	if fs.NArg() != 1 {
		return errNeedsOneFileArg
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.ViewRaw(fs.Arg(0), from, until, *retID, *textOut, *showHeader, *sortsByTime)
}

const mergeCmdUsage = `Usage: %s merge [options] src.wsp dest.wsp
       %s merge [options] srcDir destDir

options:
`

func runMergeCmd(args []string) error {
	fs := flag.NewFlagSet("merge", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), mergeCmdUsage, cmdName, cmdName)
		fs.PrintDefaults()
	}

	now := time.Now()
	fs.Var(&utcTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&utcTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	recursive := fs.Bool("r", false, "merge files recursively.")
	fs.Parse(args)

	if fs.NArg() != 2 {
		if *recursive {
			return errNeedsSrcAndDestDirsArg
		}
		return errNeedsSrcAndDestFilesArg
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.Merge(fs.Arg(0), fs.Arg(1), *recursive, now, from, until)
}

const sumCmdUsage = `Usage: %s sum [options] src.wsp dest.wsp
       %s sum [options] srcDir destDir

options:
`

func runSumCmd(args []string) error {
	fs := flag.NewFlagSet("sum", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), sumCmdUsage, cmdName, cmdName)
		fs.PrintDefaults()
	}

	src := fs.String("src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	dest := fs.String("dest", "", "dest whisper filename (ex. dest.wsp).")
	textOut := fs.String("text-out", "-", "text output. empty means no output, - means stdout, other means output file.")
	retID := fs.Int("ret", whispertool.RetIDAll, "retention ID to diff (-1 is all).")

	now := time.Now()
	fs.Var(&utcTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&utcTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	fs.Parse(args)

	if *src == "" {
		return newRequiredOptionError(fs, "src")
	}

	return whispertool.RunSum(*src, *dest, *textOut, *retID, now, from, until)
}

const sumDiffCmdUsage = `Usage: %s sum-diff -item <pattern> -src <pattern> -dest <destname>

Example: %s sum-diff -item '/var/lib/graphite/whisper/test/*/*' -src 'sv*.wsp' -dest 'sum.wsp'

options:
`

func runSumDiffCmd(args []string) error {
	fs := flag.NewFlagSet("sum-diff", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), sumDiffCmdUsage, cmdName, cmdName)
		fs.PrintDefaults()
	}

	item := fs.String("item", "", "glob pattern of whisper directory")
	srcBase := fs.String("src-base", "", "src base directory")
	destBase := fs.String("dest-base", "", "dest base directory")
	src := fs.String("src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	dest := fs.String("dest", "", "dest whisper filename (ex. dest.wsp).")
	textOut := fs.String("text-out", "-", "text output. empty means no output, - means stdout, other means output file.")
	ignoreSrcEmpty := fs.Bool("ignore-src-empty", false, "ignore diff when source point is empty.")
	ignoreDestEmpty := fs.Bool("ignore-dest-empty", false, "ignore diff when destination point is empty.")
	showAll := fs.Bool("show-all", false, "print all points when diff exists.")
	retID := fs.Int("ret", whispertool.RetIDAll, "retention ID to diff (-1 is all).")
	interval := fs.Duration("interval", time.Minute, "run interval")
	intervalOffset := fs.Duration("interval-offset", 7*time.Second, "run interval offset")

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	untilOffset := fs.Duration("until-offset", 0, "until offset")
	fs.Parse(args)

	if *item == "" {
		return newRequiredOptionError(fs, "item")
	}
	if *srcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if *destBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if *src == "" {
		return newRequiredOptionError(fs, "src")
	}
	if *dest == "" {
		return newRequiredOptionError(fs, "dest")
	}

	return whispertool.SumDiff(*srcBase, *destBase, *item, *src, *dest, *textOut, *ignoreSrcEmpty, *ignoreDestEmpty, *showAll, *interval, *intervalOffset, *untilOffset, *retID, from)
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
	emptyRate := fs.Float64("empty-rate", 0.2, "empty rate (0 <= r <= 1).")

	now := time.Now()
	fs.Var(&utcTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&utcTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	textOut := fs.String("text-out", "", "text output. empty means no output, - means stdout, other means output file.")

	perm := os.FileMode(0644)
	fs.Var(&fileModeValue{m: &perm}, "perm", "whisper file permission (octal)")

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

	return whispertool.Hole(fs.Arg(0), fs.Arg(1), perm, *textOut, *emptyRate, now, from, until)
}

const diffCmdUsage = `Usage: %s diff [options] src.wsp dest.wsp
       %s diff [options] srcDir destDir

options:
`

func runDiffCmd(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), diffCmdUsage, cmdName, cmdName)
		fs.PrintDefaults()
	}
	recursive := fs.Bool("r", false, "diff files recursively.")
	ignoreSrcEmpty := fs.Bool("ignore-src-empty", false, "ignore diff when source point is empty.")
	ignoreDestEmpty := fs.Bool("ignore-dest-empty", false, "ignore diff when destination point is empty.")
	textOut := fs.String("text-out", "-", "text output. empty means no output, - means stdout, other means output file.")
	showAll := fs.Bool("show-all", false, "print all points when diff exists.")
	retID := fs.Int("ret", whispertool.RetIDAll, "retention ID to diff (-1 is all).")

	now := time.Now()
	fs.Var(&utcTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	from := time.Unix(0, 0)
	fs.Var(&utcTimeValue{t: &from}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")

	until := now
	fs.Var(&utcTimeValue{t: &until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if fs.NArg() != 2 {
		if *recursive {
			return errNeedsSrcAndDestDirsArg
		}
		return errNeedsSrcAndDestFilesArg
	}
	if from.After(until) {
		return errFromIsAfterUntil
	}

	return whispertool.Diff(fs.Arg(0), fs.Arg(1), *textOut, *recursive, *ignoreSrcEmpty, *ignoreDestEmpty, *showAll, now, from, until, *retID)
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

	now := time.Now()
	fs.Var(&utcTimeValue{t: &now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")

	textOut := fs.String("text-out", "", "text output. empty means no output, - means stdout, other means output file.")

	perm := os.FileMode(0644)
	fs.Var(&fileModeValue{m: &perm}, "perm", "whisper file permission (octal)")

	fs.Parse(args)

	if *retentionDefs == "" {
		return newRequiredOptionError(fs, "retentions")
	}
	if fs.NArg() != 1 {
		return errNeedsOneFileArg
	}

	return whispertool.Generate(fs.Arg(0), *retentionDefs, perm, *fill, *randMax, now, *textOut)
}
