package cmd

import (
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"github.com/hnakamur/whispertool"
	"golang.org/x/sync/errgroup"
)

type SumCommand struct {
	SrcPattern string
	From       whispertool.Timestamp
	Until      whispertool.Timestamp
	Now        whispertool.Timestamp
	RetID      int
	TextOut    string
	ShowHeader bool
}

func (c *SumCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.SrcPattern, "src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")

	c.Now = whispertool.TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if c.SrcPattern == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *SumCommand) Execute() error {
	srcFilenames, err := filepath.Glob(c.SrcPattern)
	if err != nil {
		return err
	}
	if len(srcFilenames) == 0 {
		return fmt.Errorf("no file matched for -src=%s", c.SrcPattern)
	}

	d, ptsList, err := sumWhisperFile(srcFilenames, c.RetID, c.From, c.Until, c.Now)
	if err != nil {
		return err
	}

	if err := printFileData(c.TextOut, d, ptsList, c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func sumWhisperFile(srcFilenames []string, retID int, from, until, now whispertool.Timestamp) (*whispertool.Whisper, PointsList, error) {
	srcDBList := make([]*whispertool.Whisper, len(srcFilenames))
	ptsListList := make([]PointsList, len(srcFilenames))
	var g errgroup.Group
	for i, srcFilename := range srcFilenames {
		i := i
		srcFilename := srcFilename
		g.Go(func() error {
			db, ptsList, err := readWhisperFile(srcFilename, retID, from, until, now)
			if err != nil {
				return err
			}

			srcDBList[i] = db
			ptsListList[i] = ptsList
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	for i := 1; i < len(srcDBList); i++ {
		if !whispertool.Retentions(srcDBList[0].Retentions()).Equal(srcDBList[i].Retentions()) {
			return nil, nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
				"Resize the input before summing", srcFilenames[0], srcFilenames[i])
		}
	}

	srcDB0 := srcDBList[0]
	sumDB, err := whispertool.Create("", srcDB0.Retentions(), srcDB0.AggregationMethod(), srcDB0.XFilesFactor(),
		whispertool.WithInMemory())
	if err != nil {
		return nil, nil, err
	}

	sumPtsList := sumPointsLists(ptsListList)

	return sumDB, sumPtsList, nil
}

func sumPointsLists(ptsListList []PointsList) PointsList {
	if len(ptsListList) == 0 {
		return nil
	}
	retentionCount := len(ptsListList[0])
	sumPtsList := make([]whispertool.Points, retentionCount)
	for retID := range sumPtsList {
		sumPtsList[retID] = sumPointsListsForRetention(ptsListList, retID)
	}
	return sumPtsList
}

func sumPointsListsForRetention(ptsListList []PointsList, retID int) []whispertool.Point {
	if len(ptsListList) == 0 {
		return nil
	}
	ptsCount := len(ptsListList[0][retID])
	sumPoints := make([]whispertool.Point, ptsCount)
	for i := range ptsListList {
		for j := range sumPoints {
			if i == 0 {
				sumPoints[j] = ptsListList[i][retID][j]
			} else {
				sumPoints[j].Value = sumPoints[j].Value.Add(ptsListList[i][retID][j].Value)
			}
		}
	}
	return sumPoints
}
