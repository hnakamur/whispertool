package cmd

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type CopyCommand struct {
	SrcBase           string
	SrcRelPath        string
	DestBase          string
	DestRelPath       string
	AggregationMethod whispertool.AggregationMethod
	XFilesFactor      float32
	ArchiveInfoList   whispertool.ArchiveInfoList
	From              whispertool.Timestamp
	Until             whispertool.Timestamp
	ArchiveID         int
	TextOut           string
	CopyNaN           bool
}

func (c *CopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")

	fs.Var(&aggregationMethodValue{&c.AggregationMethod}, "agg-method", "aggregation method")
	fs.Var(&xFilesFactorValue{&c.XFilesFactor}, "x-files-factor", "xFilesFactor")
	fs.Var(&archiveInfoListValue{&c.ArchiveInfoList}, "retentions", "retentions definitions")

	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.CopyNaN, "copy-nan", false, "whether or not copy when source value is NaN")

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
	if isBaseURL(c.DestBase) {
		return errors.New("dest-base must be local directory")
	}
	if c.DestRelPath != "" && hasMeta(c.SrcRelPath) {
		return errNonEmptyDestRelPathForSrcRelPathWithMeta
	}
	if c.AggregationMethod == 0 {
		return newRequiredOptionError(fs, "agg-method")
	}
	if c.ArchiveInfoList == nil {
		return newRequiredOptionError(fs, "retentions")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *CopyCommand) Execute() error {
	return withTextOutWriter(c.TextOut, c.execute)
}

func (c *CopyCommand) execute(tow io.Writer) (err error) {
	if hasMeta(c.SrcRelPath) {
		t0 := time.Now()
		fmt.Fprintf(tow, "time:%s\tmsg:start\n", formatTime(t0))
		var totalFileCount int
		defer func() {
			t1 := time.Now()
			fmt.Fprintf(tow, "time:%s\tmsg:finish\tduration:%s\ttotalFileCount:%d\n", formatTime(t1), t1.Sub(t0).String(), totalFileCount)
		}()

		filenames, err := globFiles(c.SrcBase, c.SrcRelPath)
		if err != nil {
			return WrapFileNotExistError(Source, err)
		}
		totalFileCount = len(filenames)
		for _, relPath := range filenames {
			err = c.copyOneFile(relPath, relPath, tow)
			if err != nil {
				return err
			}
		}
		return nil
	}

	var destRelPath string
	if c.DestRelPath == "" {
		destRelPath = c.SrcRelPath
	} else {
		destRelPath = c.DestRelPath
	}
	return c.copyOneFile(c.SrcRelPath, destRelPath, tow)
}

func (c *CopyCommand) copyOneFile(srcRelPath, destRelPath string, tow io.Writer) (err error) {
	now := whispertool.TimestampFromStdTime(time.Now())
	var until whispertool.Timestamp
	if c.Until == 0 {
		until = now
	} else {
		until = c.Until
	}

	if c.DestRelPath == "" {
		fmt.Fprintf(tow, "now:%s\tsrcRel:%s\n", now, srcRelPath)
	} else {
		fmt.Fprintf(tow, "now:%s\tsrcRel:%s\tdestRel:%s\n", now, srcRelPath, destRelPath)
	}

	var destDB *whispertool.Whisper
	var srcHeader, destHeader *whispertool.Header
	var srcTsList, destTsList TimeSeriesList
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = readWhisperFile(c.SrcBase, srcRelPath, c.ArchiveID, c.From, until, now)
		return err
	})
	eg.Go(func() error {
		destFullPath := filepath.Join(c.DestBase, destRelPath)
		destHeaderForCreate, err := whispertool.NewHeader(c.AggregationMethod, c.XFilesFactor, c.ArchiveInfoList)
		if err != nil {
			return err
		}
		destDB, err = openOrCreateCopyDestFile(destFullPath, destHeaderForCreate)
		if err != nil {
			return err
		}
		destHeader = destDB.Header()
		destTsList, err = fetchTimeSeriesList(destDB, c.ArchiveID, c.From, until, now)
		return err
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	defer destDB.Close()

	if !srcHeader.ArchiveInfoList().Equal(destHeader.ArchiveInfoList()) {
		return errors.New("archive info list unmatch between src and dest whisper files")
	}

	if !srcTsList.AllEqualTimeRangeAndStep(destTsList) {
		return errors.New("timeseries time ranges and steps are unalike. " +
			"retry reading input files before copying")
	}

	var srcPlDif, destPlDif PointsList
	if c.CopyNaN {
		srcPlDif, destPlDif = srcTsList.Diff(destTsList)
	} else {
		srcPlDif, destPlDif = srcTsList.DiffExcludeSrcNaN(destTsList)
	}
	if srcPlDif.AllEmpty() && destPlDif.AllEmpty() {
		return nil
	}

	if err := updateFileDataWithPointsList(destDB, srcPlDif, now); err != nil {
		return err
	}

	if err := printFileData(tow, srcHeader, srcPlDif, true); err != nil {
		return err
	}

	if err := destDB.Sync(); err != nil {
		return err
	}
	return nil
}

func openOrCreateCopyDestFile(filename string, srcHeader *whispertool.Header) (*whispertool.Whisper, error) {
	destDB, err := whispertool.Open(filename)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		dir := filepath.Dir(filename)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mkdirAll: dir=%s: err=%s", dir, err)
		}

		destDB, err = whispertool.Create(filename, srcHeader.ArchiveInfoList(),
			srcHeader.AggregationMethod(), srcHeader.XFilesFactor())
		if err != nil {
			return nil, err
		}

		// NOTE: Sync header now because no sync is called later
		// if source points are all empty.
		if err := destDB.Sync(); err != nil {
			return nil, err
		}
	}
	return destDB, nil
}
