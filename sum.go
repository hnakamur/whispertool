package whispertool

import (
	"flag"
	"fmt"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
)

type SumCommand struct {
	Src        string
	From       Timestamp
	Until      Timestamp
	Now        Timestamp
	RetID      int
	TextOut    string
	ShowHeader bool
}

func (c *SumCommand) Parse(fs *flag.FlagSet, args []string) error {
	fs.StringVar(&c.Src, "src", "", "glob pattern of source whisper files (ex. src/*.wsp).")
	fs.IntVar(&c.RetID, "ret", RetIDAll, "retention ID to diff (-1 is all).")
	fs.StringVar(&c.TextOut, "text-out", "-", "text output of copying data. empty means no output, - means stdout, other means output file.")
	fs.BoolVar(&c.ShowHeader, "header", true, "whether or not to show header (metadata and reteions)")

	c.Now = TimestampFromStdTime(time.Now())
	c.Until = c.Now
	fs.Var(&timestampValue{t: &c.Now}, "now", "current UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.From}, "from", "range start UTC time in 2006-01-02T15:04:05Z format")
	fs.Var(&timestampValue{t: &c.Until}, "until", "range end UTC time in 2006-01-02T15:04:05Z format")
	fs.Parse(args)

	if c.Src == "" {
		return newRequiredOptionError(fs, "src")
	}
	if c.From > c.Until {
		return errFromIsAfterUntil
	}

	return nil
}

func (c *SumCommand) Execute() error {
	srcFilenames, err := filepath.Glob(c.Src)
	if err != nil {
		return err
	}
	if len(srcFilenames) == 0 {
		return fmt.Errorf("no file matched for -src=%s", c.Src)
	}

	d, ptsList, err := sumWhisperFile(srcFilenames, c.Now, c.From, c.Until, c.RetID)
	if err != nil {
		return err
	}

	if err := printFileData(c.TextOut, d, ptsList, c.ShowHeader); err != nil {
		return err
	}
	return nil
}

func sumWhisperFile(srcFilenames []string, now, from, until Timestamp, retID int) (*FileData, [][]Point, error) {
	srcDataList := make([]*FileData, len(srcFilenames))
	ptsListList := make([][][]Point, len(srcFilenames))
	var g errgroup.Group
	for i, srcFilename := range srcFilenames {
		i := i
		srcFilename := srcFilename
		g.Go(func() error {
			d, ptsList, err := readWhisperFile(srcFilename, now, from, until, retID)
			if err != nil {
				return err
			}

			srcDataList[i] = d
			ptsListList[i] = ptsList
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, nil, err
	}

	for i := 1; i < len(srcDataList); i++ {
		if !Retentions(srcDataList[0].Retentions).Equal(srcDataList[i].Retentions) {
			return nil, nil, fmt.Errorf("%s and %s archive confiugrations are unalike. "+
				"Resize the input before summing", srcFilenames[0], srcFilenames[i])
		}
	}

	sumData, err := NewFileData(srcDataList[0].Meta, srcDataList[0].Retentions)
	if err != nil {
		return nil, nil, err
	}

	sumPtsList := sumPointsLists(ptsListList)

	return sumData, sumPtsList, nil
}

func sumPointsLists(ptsListList [][][]Point) [][]Point {
	if len(ptsListList) == 0 {
		return nil
	}
	retentionCount := len(ptsListList[0])
	sumPtsList := make([][]Point, retentionCount)
	for retID := range sumPtsList {
		sumPtsList[retID] = sumPointsListsForRetention(ptsListList, retID)
	}
	return sumPtsList
}

func sumPointsListsForRetention(ptsListList [][][]Point, retID int) []Point {
	if len(ptsListList) == 0 {
		return nil
	}
	ptsCount := len(ptsListList[0][retID])
	sumPoints := make([]Point, ptsCount)
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
