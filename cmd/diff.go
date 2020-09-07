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
	if c.DestRelPath != "" && hasMeta(c.SrcRelPath) {
		return errNonEmptyDestRelPathForSrcRelPathWithMeta
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
	if hasMeta(c.SrcRelPath) {
		t0 := time.Now()
		fmt.Fprintf(tow, "time:%s\tmsg:start\n", formatTime(t0))
		var totalFileCount int
		diffFound := false
		defer func() {
			t1 := time.Now()
			fmt.Fprintf(tow, "time:%s\tmsg:finish\tduration:%s\ttotalFileCount:%d\tdiffFound:%v\n", formatTime(t1), t1.Sub(t0).String(), totalFileCount, diffFound)
		}()

		filenames, err := globFiles(c.SrcBase, c.SrcRelPath)
		if err != nil {
			return WrapFileNotExistError(Source, err)
		}
		totalFileCount = len(filenames)
		for _, relPath := range filenames {
			err = c.diffOneFile(relPath, relPath, tow)
			if err != nil {
				if errors.Is(err, ErrDiffFound) {
					diffFound = true
					continue
				}
				return err
			}
		}
		if diffFound {
			return ErrDiffFound
		}
		return nil
	}

	var destRelPath string
	if c.DestRelPath == "" {
		destRelPath = c.SrcRelPath
	} else {
		destRelPath = c.DestRelPath
	}
	return c.diffOneFile(c.SrcRelPath, destRelPath, tow)
}

func (c *DiffCommand) diffOneFile(srcRelPath, destRelPath string, tow io.Writer) (err error) {
	if c.DestRelPath == "" {
		fmt.Fprintf(tow, "srcRel:%s\n", srcRelPath)
	} else {
		fmt.Fprintf(tow, "srcRel:%s\tdestRel:%s\n", srcRelPath, destRelPath)
	}

	var srcHeader, destHeader *whispertool.Header
	var srcTsList, destTsList TimeSeriesList
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = readWhisperFile(c.SrcBase, srcRelPath, c.ArchiveID, c.From, c.Until, c.Now)
		return WrapFileNotExistError(Source, err)
	})
	eg.Go(func() error {
		var err error
		destHeader, destTsList, err = readWhisperFile(c.DestBase, destRelPath, c.ArchiveID, c.From, c.Until, c.Now)
		return WrapFileNotExistError(Destination, err)
	})
	if err := eg.Wait(); err != nil {
		if err2 := AsFileNotExistError(err); err2 != nil {
			fmt.Fprintf(tow, "err:%s\tsrcOrDest:%s\n", err2.cause, err2.srcOrDest)
			return ErrDiffFound
		}
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
			fmt.Fprintf(w, "archive:%d\tt:%s\tsrcVal:%s\tdestVal:%s\tdestMinusSrc:%s\n",
				archiveID, srcPt.Time, srcPt.Value, destPt.Value, destPt.Value.Diff(srcPt.Value))

		}
	}
	return nil
}
