package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/hnakamur/whispertool/cmd"
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

var cmdName = filepath.Base(os.Args[0])

var (
	version string
	commit  string
	date    string
)

const copyCmdUsage = `Usage: {{command}} copy [options] src.wsp dest.wsp

options:
`

const viewCmdUsage = `Usage: {{command}} view [options] file.wsp

options:
`

const viewRawCmdUsage = `Usage: {{command}} view-raw [options] file.wsp

options:
`

const serverCmdUsage = `Usage: {{command}} server [options]

options:
`

const sumCmdUsage = `Usage: {{command}} sum [options]

options:
`

const sumCopyCmdUsage = `Usage: {{command}} sum-copy [options]

options:
`

const sumDiffCmdUsage = `Usage: {{command}} sum-diff [options]

options:
`

const holeCmdUsage = `Usage: {{command}} hole [options]

options:
`

const diffCmdUsage = `Usage: {{command}} diff [options]

options:
`

const generateCmdUsage = `Usage: {{command}} generate [options]

options:
`

func main() {
	os.Exit(run())
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
		err = runSubcommand(args, &cmd.CopyCommand{}, copyCmdUsage)
	case "diff":
		err = runSubcommand(args, &cmd.DiffCommand{}, diffCmdUsage)
	case "generate":
		err = runSubcommand(args, &cmd.GenerateCommand{}, generateCmdUsage)
	case "hole":
		err = runSubcommand(args, &cmd.HoleCommand{}, holeCmdUsage)
	case "server":
		err = runSubcommand(args, &cmd.ServerCommand{}, serverCmdUsage)
	case "sum":
		err = runSubcommand(args, &cmd.SumCommand{}, sumCmdUsage)
	case "sum-copy":
		err = runSubcommand(args, &cmd.SumCopyCommand{}, sumCopyCmdUsage)
	case "sum-diff":
		err = runSubcommand(args, &cmd.SumDiffCommand{}, sumDiffCmdUsage)
	case "view":
		err = runSubcommand(args, &cmd.ViewCommand{}, viewCmdUsage)
	case "view-raw":
		err = runSubcommand(args, &cmd.ViewRawCommand{}, viewRawCmdUsage)
	case "version":
		err = runShowVersion(args[1:])
	default:
		flag.Usage()
		return 2
	}
	if err != nil {
		if errors.Is(err, cmd.ErrDiffFound) {
			return 1
		}

		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
		var roerr *cmd.RequiredOptionError
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

func runSubcommand(args []string, c cmd.Command, usageTemplate string) error {
	fs := flag.NewFlagSet(args[0], flag.ExitOnError)
	fs.Usage = func() {
		usageStr := strings.ReplaceAll(usageTemplate, "{{command}}", cmdName)
		fmt.Fprintf(fs.Output(), "%s", usageStr)
		fs.PrintDefaults()
	}

	if err := c.Parse(fs, args[1:]); err != nil {
		return err
	}
	return c.Execute()
}
