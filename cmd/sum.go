package cmd

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type SumCommand struct {
	SrcBase     string
	ItemPattern string
	SrcPattern  string
	From        whispertool.Timestamp
	Until       whispertool.Timestamp
	ArchiveID   int
	TextOut     string
	ShowHeader  bool
}

func (c *SumCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcBase, "src-base", "", "src base directory or URL of \"whispertool server\"")
	fs.StringVar(&c.ItemPattern, "item", "", "item directory glob pattern relative to src base")
	fs.StringVar(&c.SrcPattern, "src", "", "whisper file glob pattern relative to item directory (ex. *.wsp).")
	fs.IntVar(&c.ArchiveID, "archive", ArchiveIDAll, "archive ID (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")

	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
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
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *SumCommand) Execute() error {
	return withTextOutWriter(c.TextOut, c.execute)
}

func (c *SumCommand) execute(tow io.Writer) (err error) {
	items, err := globItems(c.SrcBase, c.ItemPattern)
	if err != nil {
		return err
	}
	for _, item := range items {
		now := whispertool.TimestampFromStdTime(time.Now())
		var until whispertool.Timestamp
		if c.Until == 0 {
			until = now
		} else {
			until = c.Until
		}

		fmt.Fprintf(tow, "now:%s\titem:%s\n", now, item)
		h, tsList, err := sumWhisperFile(c.SrcBase, item, c.SrcPattern, c.ArchiveID, c.From, until, now)
		if err != nil {
			return err
		}
		if err := printFileData(tow, h, tsList.PointsList(), c.ShowHeader); err != nil {
			return err
		}
	}
	return nil
}

func sumWhisperFile(baseDirOrURL, item, srcPattern string, archiveID int, from, until, now whispertool.Timestamp) (*whispertool.Header, TimeSeriesList, error) {
	if isBaseURL(baseDirOrURL) {
		return sumWhisperFileRemote(baseDirOrURL, item, srcPattern, archiveID, from, until, now)
	}
	return sumWhisperFileLocal(baseDirOrURL, item, srcPattern, archiveID, from, until, now)
}

func sumWhisperFileLocal(baseDir, item, srcPattern string, archiveID int, from, until, now whispertool.Timestamp) (*whispertool.Header, TimeSeriesList, error) {
	itemRelDir := itemToRelDir(item)
	srcFullPattern := filepath.Join(baseDir, itemRelDir, srcPattern)
	srcFilenames, err := filepath.Glob(srcFullPattern)
	if err != nil {
		return nil, nil, err
	}
	if len(srcFilenames) == 0 {
		return nil, nil, &os.PathError{
			Op:   "glob",
			Path: srcFullPattern,
			Err:  os.ErrNotExist,
		}
	}

	hList := make([]*whispertool.Header, len(srcFilenames))
	tsListList := make([]TimeSeriesList, len(srcFilenames))
	var g errgroup.Group
	for i, srcFilename := range srcFilenames {
		i := i
		srcFilename := srcFilename
		g.Go(func() error {
			db, ptsList, err := readWhisperFileLocal(srcFilename, archiveID, from, until, now)
			if err != nil {
				return err
			}

			hList[i] = db
			tsListList[i] = ptsList
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	for i := 1; i < len(hList); i++ {
		if !hList[0].ArchiveInfoList().Equal(hList[i].ArchiveInfoList()) {
			return nil, nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
				"Resize the input before summing", srcFilenames[0], srcFilenames[i])
		}
	}
	for i := 1; i < len(tsListList); i++ {
		if !tsListList[0].AllEqualTimeRangeAndStep(tsListList[i]) {
			return nil, nil, fmt.Errorf("%s and %s timeseries time ranges and steps are unalike. "+
				"Retry reading input files before summing", srcFilenames[0], srcFilenames[i])
		}
	}

	tsList := sumTimeSeriesListList(tsListList)

	return hList[0], tsList, nil
}

func sumWhisperFileRemote(srcURL, item, srcPattern string, archiveID int, from, until, now whispertool.Timestamp) (*whispertool.Header, TimeSeriesList, error) {
	reqURL := fmt.Sprintf("%s/sum?item=%s&pattern=%s&retention=%d&from=%s&until=%s&now=%s",
		srcURL,
		url.QueryEscape(item),
		url.QueryEscape(srcPattern),
		archiveID,
		url.QueryEscape(from.String()),
		url.QueryEscape(until.String()),
		url.QueryEscape(now.String()))
	h, tsList, err := getFileDataFromRemote(reqURL)
	if err != nil {
		return nil, nil, err
	}
	return h, tsList, nil
}

func sumTimeSeriesListList(tsListList []TimeSeriesList) TimeSeriesList {
	if len(tsListList) == 0 {
		return nil
	}
	archiveCount := len(tsListList[0])
	sumTsList := make(TimeSeriesList, archiveCount)
	for archiveID := range sumTsList {
		sumTsList[archiveID] = sumTimeSeriesListForArchive(tsListList, archiveID)
	}
	return sumTsList
}

func sumTimeSeriesListForArchive(tsListList []TimeSeriesList, archiveID int) *whispertool.TimeSeries {
	if len(tsListList) == 0 {
		return nil
	}
	ts0 := tsListList[0][archiveID]
	sumValues := make([]whispertool.Value, len(ts0.Values()))
	for i := range tsListList {
		for j := range sumValues {
			if i == 0 {
				sumValues[j] = tsListList[i][archiveID].Values()[j]
			} else {
				sumValues[j] = sumValues[j].Add(tsListList[i][archiveID].Values()[j])
			}
		}
	}
	return whispertool.NewTimeSeries(ts0.FromTime(), ts0.UntilTime(), ts0.Step(), sumValues)
}
