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

const globalUsage = `Usage: %s <subcommand> [options]

subcommands:
  copy                Copy points from src to dest whisper file.
  diff                Show diff from src to dest whisper files.
  hole                Copy whisper file and make some holes (empty points) in dest file.
  generate            Generate random whisper file.
  server              Run web server to respond view and sum query.
  sum                 Sum value of whisper files.
  sum-copy            Copy sum of points from src to dest whisper file.
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
	case "copy":
		err = runCopyCmd(args[1:])
	case "diff":
		err = runDiffCmd(args[1:])
	case "generate":
		err = runGenerateCmd(args[1:])
	case "hole":
		err = runHoleCmd(args[1:])
	case "server":
		err = runServerCmd(args[1:])
	case "sum":
		err = runSumCmd(args[1:])
	case "sum-copy":
		err = runSumCopyCmd(args[1:])
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
		var roerr *whispertool.RequiredOptionError
		if errors.As(err, &roerr) {
			fmt.Fprintf(os.Stderr, "\n")
			roerr.Usage()
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

const copyCmdUsage = `Usage: %s copy [options] src.wsp dest.wsp

options:
`

func runCopyCmd(args []string) error {
	fs := flag.NewFlagSet("copy", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), copyCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.CopyCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const viewCmdUsage = `Usage: %s view [options] file.wsp
`

func runViewCmd(args []string) error {
	fs := flag.NewFlagSet("view", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), viewCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.ViewCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const viewRawCmdUsage = `Usage: %s view-raw [options] file.wsp
`

func runViewRawCmd(args []string) error {
	fs := flag.NewFlagSet("view-raw", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), viewRawCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.ViewRawCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const serverCmdUsage = `Usage: %s server [options]

options:
`

func runServerCmd(args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), serverCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.ServerCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const sumCmdUsage = `Usage: %s sum [options]

options:
`

func runSumCmd(args []string) error {
	fs := flag.NewFlagSet("sum", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), sumCmdUsage, cmdName, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.SumCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const sumCopyCmdUsage = `Usage: %s sum-copy [options]

options:
`

func runSumCopyCmd(args []string) error {
	fs := flag.NewFlagSet("sum-copy", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), sumCopyCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.SumCopyCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const sumDiffCmdUsage = `Usage: %s sum-diff [options]

options:
`

func runSumDiffCmd(args []string) error {
	fs := flag.NewFlagSet("sum-diff", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), sumDiffCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.SumDiffCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const holeCmdUsage = `Usage: %s hole [options]

options:
`

func runHoleCmd(args []string) error {
	fs := flag.NewFlagSet("hole", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), holeCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.HoleCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const diffCmdUsage = `Usage: %s diff [options]

options:
`

func runDiffCmd(args []string) error {
	fs := flag.NewFlagSet("diff", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), diffCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.DiffCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}

const generateCmdUsage = `Usage: %s generate [options]

options:
`

func runGenerateCmd(args []string) error {
	fs := flag.NewFlagSet("generate", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), generateCmdUsage, cmdName)
		fs.PrintDefaults()
	}

	var c whispertool.DiffCommand
	if err := c.Parse(fs, args); err != nil {
		return err
	}
	return c.Execute()
}
