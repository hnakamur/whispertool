package cmd

import (
	"errors"
	"flag"
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
	Now               whispertool.Timestamp
	ArchiveID         int
	TextOut           string
}

func (c *CopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.SrcRelPath, "src", "", "whisper file relative path to src base")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.DestRelPath, "dest", "", "whisper file relative path to dest base")

	fs.Var(&aggregationMethodValue{&c.AggregationMethod}, "agg-method", "aggregation method")
	fs.Var(&xFilesFactorValue{&c.XFilesFactor}, "x-files-factor", "xFilesFactor")
	fs.Var(&archiveInfoListValue{&c.ArchiveInfoList}, "retentions", "retentions definitions")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "", "text output of copying data. empty means no output, - means stdout, other means output file.")

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
	if c.DestRelPath == "" {
		return newRequiredOptionError(fs, "dest")
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
	var destDB *whispertool.Whisper
	var srcHeader, destHeader *whispertool.Header
	var srcTsList, destTsList TimeSeriesList
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = readWhisperFile(c.SrcBase, c.SrcRelPath, c.ArchiveID, c.From, c.Until, c.Now)
		return err
	})
	eg.Go(func() error {
		destFullPath := filepath.Join(c.DestBase, c.DestRelPath)
		destHeaderForCreate, err := whispertool.NewHeader(c.AggregationMethod, c.XFilesFactor, c.ArchiveInfoList)
		if err != nil {
			return err
		}
		destDB, err = openOrCreateCopyDestFile(destFullPath, destHeaderForCreate)
		if err != nil {
			return err
		}
		destHeader = destDB.Header()
		destTsList, err = fetchTimeSeriesList(destDB, c.ArchiveID, c.From, c.Until, c.Now)
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

	srcPlDif, destPlDif := srcTsList.Diff(destTsList)
	if srcPlDif.AllEmpty() && destPlDif.AllEmpty() {
		return nil
	}

	if err := updateFileDataWithPointsList(destDB, srcPlDif, c.Now); err != nil {
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

		destDB, err = whispertool.Create(filename, srcHeader.ArchiveInfoList(),
			srcHeader.AggregationMethod(), srcHeader.XFilesFactor())
		if err != nil {
			return nil, err
		}
	}
	return destDB, nil
}
