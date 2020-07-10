package cmd

import (
	"errors"
	"flag"
	"fmt"
	"io"
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
	ArchiveID   int
	TextOut     string
}

func (c *DiffCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")
	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
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
	return withTextOutWriter(c.TextOut, c.execute)
}

func (c *DiffCommand) execute(tow io.Writer) (err error) {
	var srcHeader, destHeader *whispertool.Header
	var srcTsList, destTsList TimeSeriesList
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = readWhisperFile(c.SrcBase, c.SrcRelPath, c.ArchiveID, c.From, c.Until, c.Now)
		return err
	})
	eg.Go(func() error {
		var err error
		destHeader, destTsList, err = readWhisperFile(c.DestBase, c.DestRelPath, c.ArchiveID, c.From, c.Until, c.Now)
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

	if err := printDiff(tow, srcHeader, destHeader, srcPlDif, destPlDif); err != nil {
		return err
	}

	return ErrDiffFound
}

func printDiff(w io.Writer, srcHeader, destHeader *whispertool.Header, srcPlDif, destPlDif PointsList) error {
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
