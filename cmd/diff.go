package cmd

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

var ErrDiffFound = errors.New("diff found")

type DiffCommand struct {
	SrcBase     string
	SrcRelPath  string
	DestBase    string
	DestRelPath string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	Now         whispertool.Timestamp
	RetID       int
	TextOut     string
}

func (c *DiffCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")
	fs.IntVar(&c.RetID, "ret", ArchiveIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if c.SrcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if c.SrcRelPath == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.DestBase == "" {
		return newRequiredOptionError(fs, "dest-base")
	}
	if c.DestRelPath == "" {
		return newRequiredOptionError(fs, "dest")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *DiffCommand) Execute() error {
	var srcHeader, destHeader *whispertool.Header
	var srcTsList, destTsList TimeSeriesList
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = readWhisperFile(c.SrcBase, c.SrcRelPath, c.RetID, c.From, c.Until, c.Now)
		return err
	})
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = readWhisperFile(c.DestBase, c.DestRelPath, c.RetID, c.From, c.Until, c.Now)
		return err
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	if !srcHeader.ArchiveInfoList().Equal(destHeader.ArchiveInfoList()) {
		return errors.New("retentions unmatch between src and dest whisper files")
	}
	if !srcTsList.AllEqualTimeRangeAndStep(destTsList) {
		return errors.New("timeseries time ranges and steps are unalike. " +
			"retry reading input files before diffing")
	}

	srcPlDif, destPlDif := srcTsList.Diff(destTsList)
	if srcPlDif.AllEmpty() && destPlDif.AllEmpty() {
		return nil
	}

	err := printDiff(c.TextOut, srcHeader, destHeader, srcPlDif, destPlDif)
	if err != nil {
		return err
	}

	return ErrDiffFound
}

func printDiff(textOut string, srcHeader, destHeader *whispertool.Header, srcPlDif, destPlDif PointsList) error {
	if textOut == "" {
		return nil
	}

	if textOut == "-" {
		return printDiffTo(os.Stdout, srcHeader, destHeader, srcPlDif, destPlDif)
	}

	file, err := os.Create(textOut)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	err = printDiffTo(w, srcHeader, destHeader, srcPlDif, destPlDif)
	if err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = file.Sync(); err != nil {
		return err
	}
	return nil
}

func printDiffTo(w io.Writer, srcHeader, destHeader *whispertool.Header, srcPlDif, destPlDif PointsList) error {
	for archiveID := range srcHeader.ArchiveInfoList() {
		srcPtsDif := srcPlDif[archiveID]
		destPtsDif := destPlDif[archiveID]
		for i, srcPt := range srcPtsDif {
			destPt := destPtsDif[i]
			fmt.Fprintf(w, "retID:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\n",
				archiveID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value))

		}
	}
	return nil
}
