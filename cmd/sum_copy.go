package cmd

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type SumCopyCommand struct {
	SrcBase           string
	DestBase          string
	ItemPattern       string
	SrcPattern        string
	DestRelPath       string
	AggregationMethod whispertool.AggregationMethod
	XFilesFactor      float32
	ArchiveInfoList   whispertool.ArchiveInfoList
	From              whispertool.Timestamp
	Until             whispertool.Timestamp
	ArchiveID         int
	TextOut           string
}

func (c *SumCopyCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or web app URL of \"whispertool server\"")
	fs.StringVar(&c.ItemPattern, "item", "", "item directory glob pattern relative to src base")
	fs.StringVar(&c.SrcPattern, "src", "", "whisper file glob pattern relative to item directory (ex. *.wsp).")
	fs.StringVar(&c.DestBase, "dest-base", "", "dest base directory")
	fs.StringVar(&c.DestRelPath, "dest", "", "dest whisper relative filename to item directory (ex. dest.wsp).")

	fs.Var(&aggregationMethodValue{&c.AggregationMethod}, "agg-method", "aggregation method")
	fs.Var(&xFilesFactorValue{&c.XFilesFactor}, "x-files-factor", "xFilesFactor")
	fs.Var(&archiveInfoListValue{&c.ArchiveInfoList}, "retentions", "retentions definitions")

	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")

	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")

	fs.Parse(args)

	if c.ItemPattern == "" {
		return newRequiredOptionError(fs, "item")
	}
	if c.SrcBase == "" {
		return newRequiredOptionError(fs, "src-base")
	}
	if c.SrcPattern == "" {
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
	return nil
}

func (c *SumCopyCommand) Execute() error {
	return withTextOutWriter(c.TextOut, c.execute)
}

func (c *SumCopyCommand) execute(tow io.Writer) (err error) {
	t0 := time.Now()
	fmt.Fprintf(tow, "time:%s\tmsg:start\n", formatTime(t0))
	var totalItemCount int
	defer func() {
		t1 := time.Now()
		fmt.Fprintf(tow, "time:%s\tmsg:finish\tduration:%s\ttotalItemCount:%d\n", formatTime(t1), t1.Sub(t0).String(), totalItemCount)
	}()

	items, err := globItems(c.SrcBase, c.ItemPattern)
	if err != nil {
		return err
	}
	totalItemCount = len(items)
	for _, item := range items {
		err = c.sumCopyItem(item, tow)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *SumCopyCommand) sumCopyItem(item string, tow io.Writer) error {
	now := whispertool.TimestampFromStdTime(time.Now())
	var until whispertool.Timestamp
	if c.Until == 0 {
		until = now
	} else {
		until = c.Until
	}

	fmt.Fprintf(tow, "now:%s\titem:%s\n", now, item)
	itemRelDir := itemToRelDir(item)

	var destDB *whispertool.Whisper
	var srcHeader, destHeader *whispertool.Header
	var srcTsList, destTsList TimeSeriesList
	var eg errgroup.Group
	eg.Go(func() error {
		var err error
		srcHeader, srcTsList, err = sumWhisperFile(c.SrcBase, itemRelDir, c.SrcPattern, c.ArchiveID, c.From, until, now)
		return err
	})
	eg.Go(func() error {
		destFullPath := filepath.Join(c.DestBase, itemRelDir, c.DestRelPath)
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

	srcPlDif, destPlDif := srcTsList.Diff(destTsList)
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
